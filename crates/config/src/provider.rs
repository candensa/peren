use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ImageProvider {
    #[default]
    Local,
    Http {
        url: String,
        #[serde(default)]
        token_env: Option<String>,
    },
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VectorProvider {
    #[default]
    Local,
    Http {
        url: String,
        #[serde(default)]
        token_env: Option<String>,
    },
    Qdrant {
        url: String,
        collection: String,
        #[serde(default)]
        api_key_env: Option<String>,
    },
    Pinecone {
        url: String,
        index: String,
        api_key_env: String,
        #[serde(default)]
        namespace: Option<String>,
    },
    Weaviate {
        url: String,
        class_name: String,
        #[serde(default)]
        api_key_env: Option<String>,
    },
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AiProvider {
    #[default]
    Http,
    OpenAi {
        api_key_env: String,
        #[serde(default = "openai_base_url")]
        base_url: String,
    },
    Anthropic {
        api_key_env: String,
        #[serde(default = "anthropic_base_url")]
        base_url: String,
        #[serde(default = "anthropic_version")]
        version: String,
    },
    Gemini {
        api_key_env: String,
        #[serde(default = "gemini_base_url")]
        base_url: String,
    },
    WorkersAi {
        account_id_env: String,
        api_token_env: String,
    },
    Local {
        command: String,
    },
}

fn openai_base_url() -> String {
    "https://api.openai.com/v1".to_string()
}

fn anthropic_base_url() -> String {
    "https://api.anthropic.com/v1".to_string()
}

fn anthropic_version() -> String {
    "2023-06-01".to_string()
}

fn gemini_base_url() -> String {
    "https://generativelanguage.googleapis.com/v1beta".to_string()
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KvBackend {
    #[default]
    Native,
    Redis {
        url_env: String,
    },
    Bucket {
        endpoint: String,
        bucket: String,
        #[serde(default)]
        prefix: String,
        access_key_id_env: String,
        secret_access_key_env: String,
        #[serde(default)]
        allow_http: bool,
    },
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum D1Backend {
    #[default]
    NativeSqlite,
    Turso {
        url_env: String,
        token_env: String,
        #[serde(default)]
        replica_path: Option<String>,
    },
    External {
        url_env: String,
        driver: D1Driver,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum D1Driver {
    Postgres,
    MySql,
    Sqlite,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Cache {
    #[default]
    Memory,
    Kv {
        namespace: String,
    },
    Redis {
        url_env: String,
    },
    Bucket {
        endpoint: String,
        bucket: String,
        #[serde(default)]
        prefix: String,
        access_key_id_env: String,
        secret_access_key_env: String,
        #[serde(default)]
        allow_http: bool,
    },
}
