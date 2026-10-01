use crate::Problem;

use super::required;

pub(super) fn cache(config: &crate::FleetConfig, problems: &mut Vec<Problem>) {
    match &config.cache {
        crate::Cache::Kv { namespace } if namespace.trim().is_empty() => {
            problems.push(Problem::new("cache.namespace", "must not be empty"));
        }
        crate::Cache::Memory | crate::Cache::Kv { .. } => {}
        crate::Cache::Redis { url_env } => required(Some(url_env), "cache.url_env", problems),
        crate::Cache::Bucket {
            endpoint,
            bucket,
            access_key_id_env,
            secret_access_key_env,
            ..
        } => {
            required(Some(endpoint), "cache.endpoint", problems);
            required(Some(bucket), "cache.bucket", problems);
            required(Some(access_key_id_env), "cache.access_key_id_env", problems);
            required(
                Some(secret_access_key_env),
                "cache.secret_access_key_env",
                problems,
            );
        }
    }
}

pub(super) fn vector(provider: &crate::VectorProvider, field: &str, problems: &mut Vec<Problem>) {
    match provider {
        crate::VectorProvider::Http { url, .. } if url.trim().is_empty() => {
            missing(format!("{field}.provider.url"), problems);
        }
        crate::VectorProvider::Local | crate::VectorProvider::Http { .. } => {}
        crate::VectorProvider::Qdrant {
            url, collection, ..
        } => {
            if url.trim().is_empty() {
                missing(format!("{field}.provider.url"), problems);
            }
            if collection.trim().is_empty() {
                missing(format!("{field}.provider.collection"), problems);
            }
        }
        crate::VectorProvider::Pinecone {
            url,
            index,
            api_key_env,
            ..
        } => {
            if url.trim().is_empty() {
                missing(format!("{field}.provider.url"), problems);
            }
            if index.trim().is_empty() {
                missing(format!("{field}.provider.index"), problems);
            }
            if api_key_env.trim().is_empty() {
                missing(format!("{field}.provider.api_key_env"), problems);
            }
        }
        crate::VectorProvider::Weaviate {
            url, class_name, ..
        } => {
            if url.trim().is_empty() {
                missing(format!("{field}.provider.url"), problems);
            }
            if class_name.trim().is_empty() {
                missing(format!("{field}.provider.class_name"), problems);
            }
        }
    }
}

pub(super) fn image(provider: &crate::ImageProvider, field: &str, problems: &mut Vec<Problem>) {
    match provider {
        crate::ImageProvider::Http { url, .. } if url.trim().is_empty() => {
            missing(format!("{field}.provider.url"), problems);
        }
        crate::ImageProvider::Local | crate::ImageProvider::Http { .. } => {}
    }
}

pub(super) fn ai(provider: &crate::AiProvider, field: &str, problems: &mut Vec<Problem>) {
    match provider {
        crate::AiProvider::OpenAi {
            api_key_env,
            base_url,
        }
        | crate::AiProvider::Gemini {
            api_key_env,
            base_url,
        } => keyed_ai(api_key_env, base_url, field, problems),
        crate::AiProvider::Anthropic {
            api_key_env,
            base_url,
            version,
        } => {
            keyed_ai(api_key_env, base_url, field, problems);
            if version.trim().is_empty() {
                missing(format!("{field}.provider.version"), problems);
            }
        }
        crate::AiProvider::WorkersAi {
            account_id_env,
            api_token_env,
        } => {
            if account_id_env.trim().is_empty() {
                missing(format!("{field}.provider.account_id_env"), problems);
            }
            if api_token_env.trim().is_empty() {
                missing(format!("{field}.provider.api_token_env"), problems);
            }
        }
        crate::AiProvider::Local { command } if command.trim().is_empty() => {
            missing(format!("{field}.provider.command"), problems);
        }
        crate::AiProvider::Http | crate::AiProvider::Local { .. } => {}
    }
}

fn keyed_ai(api_key_env: &str, base_url: &str, field: &str, problems: &mut Vec<Problem>) {
    if api_key_env.trim().is_empty() {
        missing(format!("{field}.provider.api_key_env"), problems);
    }
    if base_url.trim().is_empty() {
        missing(format!("{field}.provider.base_url"), problems);
    }
}

fn missing(field: String, problems: &mut Vec<Problem>) {
    problems.push(Problem::new(field, "must not be empty"));
}
