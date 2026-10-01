use std::{collections::BTreeSet, sync::Arc};

use hmac::{Hmac, Mac};
use peren_runtime::{AwsSigv4Fetch, HostError, HttpRequest, HttpResponse};
use reqwest::Url;
use sha2::{Digest, Sha256};

use super::{AwsBinding, AwsCredential};

type HmacSha256 = Hmac<Sha256>;

struct SigningCredential {
    access: Arc<str>,
    secret: Arc<str>,
    token: Option<Arc<str>>,
}

pub(super) async fn fetch(
    client: &reqwest::Client,
    binding: &AwsBinding,
    request: AwsSigv4Fetch,
) -> Result<HttpResponse, HostError> {
    if request.region != binding.region || request.service != binding.service {
        return Err(HostError);
    }
    let url = Url::parse(&request.request.url).map_err(|_| HostError)?;
    let host = request_host(&url).ok_or(HostError)?;
    if !binding.hosts.contains(&host) {
        return Err(HostError);
    }
    let credential = credential(&binding.credential)?;
    let signed = sign(request.request, &url, &host, binding, &credential)?;
    send(client, url, signed).await
}

fn credential(source: &AwsCredential) -> Result<SigningCredential, HostError> {
    match source {
        AwsCredential::Static {
            access,
            secret,
            token,
        } => Ok(SigningCredential {
            access: Arc::clone(access),
            secret: Arc::clone(secret),
            token: token.clone(),
        }),
        AwsCredential::DefaultChain => {
            let access = std::env::var("AWS_ACCESS_KEY_ID").map_err(|_| HostError)?;
            let secret = std::env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| HostError)?;
            let token = std::env::var("AWS_SESSION_TOKEN").ok().map(Arc::from);
            Ok(SigningCredential {
                access: Arc::from(access),
                secret: Arc::from(secret),
                token,
            })
        }
    }
}

fn sign(
    mut request: HttpRequest,
    url: &Url,
    host: &str,
    binding: &AwsBinding,
    credential: &SigningCredential,
) -> Result<HttpRequest, HostError> {
    let now = chrono::Utc::now();
    let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    request.headers.retain(|(name, _)| {
        !matches!(
            name.to_ascii_lowercase().as_str(),
            "authorization"
                | "x-amz-date"
                | "x-amz-content-sha256"
                | "x-amz-security-token"
                | "host"
        )
    });
    let payload_hash = hex::encode(Sha256::digest(&request.body));
    request.headers.push(("host".into(), host.into()));
    request
        .headers
        .push(("x-amz-date".into(), timestamp.clone()));
    request
        .headers
        .push(("x-amz-content-sha256".into(), payload_hash.clone()));
    if let Some(token) = &credential.token {
        request
            .headers
            .push(("x-amz-security-token".into(), token.to_string()));
    }
    let (canonical_headers, signed_headers) = canonical_headers(&request.headers);
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method.to_ascii_uppercase(),
        canonical_path(url),
        canonical_query(url),
        canonical_headers,
        signed_headers,
        payload_hash
    );
    let scope = format!("{date}/{}/{}/aws4_request", binding.region, binding.service);
    let digest = hex::encode(Sha256::digest(canonical.as_bytes()));
    let string = format!("AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{digest}");
    let signature = hex::encode(signing_key(
        &credential.secret,
        &date,
        &binding.region,
        &binding.service,
        string.as_bytes(),
    )?);
    request.headers.push((
        "authorization".into(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            credential.access, scope, signed_headers, signature
        ),
    ));
    Ok(request)
}

async fn send(
    client: &reqwest::Client,
    url: Url,
    request: HttpRequest,
) -> Result<HttpResponse, HostError> {
    let method = reqwest::Method::from_bytes(request.method.as_bytes()).map_err(|_| HostError)?;
    let mut builder = client.request(method, url).body(request.body);
    for (name, value) in request.headers {
        builder = builder.header(name, value);
    }
    let response = builder.send().await.map_err(|_| HostError)?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect();
    let body = response.bytes().await.map_err(|_| HostError)?.to_vec();
    Ok(HttpResponse {
        status,
        headers,
        body,
        upgrade: false,
        websocket_id: None,
    })
}

fn request_host(url: &Url) -> Option<String> {
    let host = url.host_str()?;
    Some(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

fn canonical_headers(headers: &[(String, String)]) -> (String, String) {
    let mut names = BTreeSet::new();
    for (name, _) in headers {
        names.insert(name.to_ascii_lowercase());
    }
    let mut canonical = String::new();
    for name in &names {
        let values = headers
            .iter()
            .filter(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| normalize(value))
            .collect::<Vec<_>>()
            .join(",");
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(&values);
        canonical.push('\n');
    }
    (canonical, names.into_iter().collect::<Vec<_>>().join(";"))
}

fn canonical_path(url: &Url) -> String {
    let path = url.path();
    if path.is_empty() {
        "/".into()
    } else {
        path.into()
    }
}

fn canonical_query(url: &Url) -> String {
    let mut pairs = url
        .query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(name, value)| format!("{}={}", encode(&name), encode(&value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

fn normalize(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn signing_key(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
    string: &[u8],
) -> Result<Vec<u8>, HostError> {
    let date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let region = hmac(&date, region.as_bytes())?;
    let service = hmac(&region, service.as_bytes())?;
    let signing = hmac(&service, b"aws4_request")?;
    hmac(&signing, string)
}

fn hmac(key: &[u8], value: &[u8]) -> Result<Vec<u8>, HostError> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| HostError)?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}
