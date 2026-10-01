use peren_runtime::HostError;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub(super) struct RedisEndpoint {
    pub(super) address: String,
    pub(super) password: Option<String>,
    pub(super) database: Option<u32>,
}

pub(super) fn endpoint(url: &str) -> Result<RedisEndpoint, HostError> {
    let rest = url.strip_prefix("redis://").ok_or(HostError)?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (credentials, host) = authority
        .rsplit_once('@')
        .map_or((None, authority), |(left, right)| (Some(left), right));
    let password = credentials.and_then(|value| {
        let value = value
            .split_once(':')
            .map_or(value, |(_, password)| password);
        (!value.is_empty()).then(|| value.to_string())
    });
    let database = (!path.is_empty())
        .then(|| path.parse::<u32>())
        .transpose()
        .map_err(|_| HostError)?;
    let address = if host.contains(':') {
        host.to_string()
    } else {
        format!("{host}:6379")
    };
    Ok(RedisEndpoint {
        address,
        password,
        database,
    })
}

async fn redis_connect(url: &str) -> Result<TcpStream, HostError> {
    let endpoint = endpoint(url)?;
    let mut stream = TcpStream::connect(&endpoint.address)
        .await
        .map_err(|_| HostError)?;
    if let Some(password) = endpoint.password {
        redis_command(&mut stream, &[b"AUTH".as_slice(), password.as_bytes()]).await?;
    }
    if let Some(database) = endpoint.database {
        let database = database.to_string();
        redis_command(&mut stream, &[b"SELECT".as_slice(), database.as_bytes()]).await?;
    }
    Ok(stream)
}

pub(crate) async fn get(url: &str, key: &str) -> Result<Option<Vec<u8>>, HostError> {
    let mut stream = redis_connect(url).await?;
    match redis_command(&mut stream, &[b"GET".as_slice(), key.as_bytes()]).await? {
        RedisReply::Bulk(value) => Ok(Some(value)),
        RedisReply::Nil => Ok(None),
        _ => Err(HostError),
    }
}

pub(crate) async fn set(url: &str, key: &str, value: &[u8]) -> Result<(), HostError> {
    let mut stream = redis_connect(url).await?;
    match redis_command(&mut stream, &[b"SET".as_slice(), key.as_bytes(), value]).await? {
        RedisReply::Simple(value) if value == b"OK" => Ok(()),
        _ => Err(HostError),
    }
}

pub(crate) async fn del(url: &str, key: &str) -> Result<bool, HostError> {
    let mut stream = redis_connect(url).await?;
    match redis_command(&mut stream, &[b"DEL".as_slice(), key.as_bytes()]).await? {
        RedisReply::Integer(value) => Ok(value > 0),
        _ => Err(HostError),
    }
}

enum RedisReply {
    Simple(Vec<u8>),
    Bulk(Vec<u8>),
    Array(Vec<RedisReply>),
    Integer(i64),
    Nil,
}

async fn redis_command(stream: &mut TcpStream, parts: &[&[u8]]) -> Result<RedisReply, HostError> {
    let mut command = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        command.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        command.extend_from_slice(part);
        command.extend_from_slice(b"\r\n");
    }
    stream.write_all(&command).await.map_err(|_| HostError)?;
    redis_reply(stream).await
}

async fn redis_reply(stream: &mut TcpStream) -> Result<RedisReply, HostError> {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag).await.map_err(|_| HostError)?;
    match tag[0] {
        b'+' => Ok(RedisReply::Simple(redis_line(stream).await?)),
        b':' => {
            let line = redis_line(stream).await?;
            let text = std::str::from_utf8(&line).map_err(|_| HostError)?;
            Ok(RedisReply::Integer(text.parse().map_err(|_| HostError)?))
        }
        b'*' => {
            let line = redis_line(stream).await?;
            let text = std::str::from_utf8(&line).map_err(|_| HostError)?;
            let len: isize = text.parse().map_err(|_| HostError)?;
            if len < 0 {
                return Ok(RedisReply::Nil);
            }
            let mut values = Vec::with_capacity(usize::try_from(len).map_err(|_| HostError)?);
            for _ in 0..len {
                values.push(Box::pin(redis_reply(stream)).await?);
            }
            Ok(RedisReply::Array(values))
        }
        b'$' => {
            let line = redis_line(stream).await?;
            let text = std::str::from_utf8(&line).map_err(|_| HostError)?;
            let len: isize = text.parse().map_err(|_| HostError)?;
            if len < 0 {
                return Ok(RedisReply::Nil);
            }
            let mut body = vec![0_u8; usize::try_from(len).map_err(|_| HostError)? + 2];
            stream.read_exact(&mut body).await.map_err(|_| HostError)?;
            body.truncate(body.len().saturating_sub(2));
            Ok(RedisReply::Bulk(body))
        }
        _ => Err(HostError),
    }
}

async fn redis_line(stream: &mut TcpStream) -> Result<Vec<u8>, HostError> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await.map_err(|_| HostError)?;
        if byte[0] == b'\n' {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(line);
        }
        line.push(byte[0]);
    }
}

pub(crate) async fn scan(url: &str, pattern: &str) -> Result<Vec<String>, HostError> {
    let mut stream = redis_connect(url).await?;
    let mut cursor = b"0".to_vec();
    let mut keys = Vec::new();
    loop {
        let reply = redis_command(
            &mut stream,
            &[
                b"SCAN".as_slice(),
                &cursor,
                b"MATCH".as_slice(),
                pattern.as_bytes(),
                b"COUNT".as_slice(),
                b"1000".as_slice(),
            ],
        )
        .await?;
        let RedisReply::Array(mut values) = reply else {
            return Err(HostError);
        };
        if values.len() != 2 {
            return Err(HostError);
        }
        let RedisReply::Bulk(next) = values.remove(0) else {
            return Err(HostError);
        };
        let RedisReply::Array(batch) = values.remove(0) else {
            return Err(HostError);
        };
        for item in batch {
            let RedisReply::Bulk(key) = item else {
                return Err(HostError);
            };
            keys.push(String::from_utf8(key).map_err(|_| HostError)?);
        }
        if next == b"0" {
            break;
        }
        cursor = next;
    }
    Ok(keys)
}
