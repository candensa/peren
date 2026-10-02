use crate::{BucketKind, ConfigError, FleetConfig, Problem, ValidatedConfig};
use peren_primitives::ServiceName;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn validate(config: FleetConfig) -> Result<ValidatedConfig, ConfigError> {
    let mut problems = Vec::new();
    let services = validate_services(&config, &mut problems);
    let sockets = validate_sockets(&config, &mut problems);
    let advertise_addr = parse_address(
        &config.node.advertise_addr,
        "node.advertise_addr",
        &mut problems,
    );
    let peer_listen = parse_address(&config.node.listen, "node.listen", &mut problems);
    let console_listen = config
        .console
        .as_ref()
        .and_then(|console| parse_address(&console.listen, "console.listen", &mut problems));
    validate_bucket(&config, &mut problems);
    validate_secrets(&config, &mut problems);
    queue::validate(&config, &mut problems);
    provider::cache(&config, &mut problems);
    binding::services(&config, &mut problems);
    validate_limits(&config, &mut problems);
    validate_queues_limits(&config, &mut problems);
    validate_tenants(&config, &mut problems);
    validate_namespaces(&config, &mut problems);
    validate_telemetry(&config, &mut problems);

    if problems.is_empty() {
        Ok(ValidatedConfig {
            raw: config,
            services,
            advertise_addr: advertise_addr.expect("validated address"),
            peer_listen: peer_listen.expect("validated address"),
            sockets,
            console_listen,
        })
    } else {
        Err(ConfigError::Invalid(problems.len(), problems))
    }
}

fn validate_services(
    config: &FleetConfig,
    problems: &mut Vec<Problem>,
) -> BTreeMap<ServiceName, usize> {
    let mut services = BTreeMap::new();
    for (index, service) in config.services.iter().enumerate() {
        match ServiceName::parse(service.name.as_str()) {
            Ok(name) => {
                if services.insert(name, index).is_some() {
                    problems.push(Problem::new(
                        format!("services[{index}].name"),
                        "service name is duplicated",
                    ));
                }
            }
            Err(error) => problems.push(Problem::new(
                format!("services[{index}].name"),
                error.to_string(),
            )),
        }
        if service.expose_node_id {
            if service.vars.contains_key("PEREN_NODE_ID") {
                problems.push(Problem::new(
                    format!("services[{index}].vars.PEREN_NODE_ID"),
                    "is reserved when expose_node_id is enabled",
                ));
            }
            if service.secrets.contains_key("PEREN_NODE_ID") {
                problems.push(Problem::new(
                    format!("services[{index}].secrets.PEREN_NODE_ID"),
                    "is reserved when expose_node_id is enabled",
                ));
            }
            if service.secrets_store_refs.contains_key("PEREN_NODE_ID") {
                problems.push(Problem::new(
                    format!("services[{index}].secrets_store_refs.PEREN_NODE_ID"),
                    "is reserved when expose_node_id is enabled",
                ));
            }
            for (name, binding) in &service.bindings {
                if name == "PEREN_NODE_ID"
                    && matches!(binding, crate::Binding::SecretsStoreSecret { .. })
                {
                    problems.push(Problem::new(
                        format!("services[{index}].bindings.PEREN_NODE_ID"),
                        "is reserved when expose_node_id is enabled",
                    ));
                }
            }
        }
    }
    if config.services.is_empty() {
        problems.push(Problem::new("services", "at least one service is required"));
    }
    services
}

fn validate_sockets(
    config: &FleetConfig,
    problems: &mut Vec<Problem>,
) -> BTreeMap<String, std::net::SocketAddr> {
    let service_names: BTreeSet<_> = config
        .services
        .iter()
        .map(|service| &service.name)
        .collect();
    let mut socket_names = BTreeSet::new();
    let mut sockets = BTreeMap::new();
    for (index, socket) in config.sockets.iter().enumerate() {
        if !socket_names.insert(&socket.name) {
            problems.push(Problem::new(
                format!("sockets[{index}].name"),
                "socket name is duplicated",
            ));
        }
        if matches!(socket.name.as_str(), "peer" | "console") {
            problems.push(Problem::new(
                format!("sockets[{index}].name"),
                "socket name is reserved",
            ));
        }
        if !service_names.contains(&socket.service) {
            problems.push(Problem::new(
                format!("sockets[{index}].service"),
                format!("unknown service {:?}", socket.service),
            ));
        }
        if let Some(address) = parse_address(
            &socket.listen,
            &format!("sockets[{index}].listen"),
            problems,
        ) {
            sockets.insert(socket.name.clone(), address);
        }
    }
    sockets
}

fn parse_address(
    value: &str,
    field: &str,
    problems: &mut Vec<Problem>,
) -> Option<std::net::SocketAddr> {
    if let Ok(address) = value.parse() {
        Some(address)
    } else {
        problems.push(Problem::new(field, "must be an IP socket address"));
        None
    }
}

fn validate_secrets(config: &FleetConfig, problems: &mut Vec<Problem>) {
    match config.secrets.provider {
        crate::SecretsProvider::Local => {}
        crate::SecretsProvider::AwsSecretsManager => {
            validate_host_owned_secret_credentials(config, problems);
            required(config.secrets.region.as_ref(), "secrets.region", problems);
        }
        crate::SecretsProvider::GcpSecretManager => {
            validate_host_owned_secret_credentials(config, problems);
            required(config.secrets.project.as_ref(), "secrets.project", problems);
        }
        crate::SecretsProvider::AzureKeyVault | crate::SecretsProvider::Vault => {
            validate_host_owned_secret_credentials(config, problems);
            required(
                config.secrets.vault_url.as_ref(),
                "secrets.vault_url",
                problems,
            );
        }
    }
}

fn validate_host_owned_secret_credentials(config: &FleetConfig, problems: &mut Vec<Problem>) {
    if matches!(
        config.secrets.credentials_source,
        crate::CredentialsSource::Configured | crate::CredentialsSource::Environment
    ) {
        problems.push(Problem::new(
            "secrets.credentials_source",
            "external secret managers must use a host-owned identity source",
        ));
    }
}

fn validate_bucket(config: &FleetConfig, problems: &mut Vec<Problem>) {
    match config.bucket.kind {
        BucketKind::Memory if !config.seed_peers.is_empty() => problems.push(Problem::new(
            "bucket.kind",
            "memory storage cannot coordinate multiple processes",
        )),
        BucketKind::File => {
            required(config.bucket.path.as_ref(), "bucket.path", problems);
            if !config.seed_peers.is_empty() {
                problems.push(Problem::new(
                    "bucket.kind",
                    "file storage is local to one host; use S3-compatible storage for a multi-host fleet",
                ));
            }
        }
        BucketKind::S3 => {
            required(config.bucket.endpoint.as_ref(), "bucket.endpoint", problems);
            required(config.bucket.name.as_ref(), "bucket.bucket", problems);
            if matches!(
                config.bucket.credentials_source,
                crate::CredentialsSource::Configured | crate::CredentialsSource::Environment
            ) {
                required(
                    config.bucket.access_key_env.as_ref(),
                    "bucket.access_key_env",
                    problems,
                );
                required(
                    config.bucket.secret_key_env.as_ref(),
                    "bucket.secret_key_env",
                    problems,
                );
            }
            if matches!(
                config.bucket.credentials_source,
                crate::CredentialsSource::Configured | crate::CredentialsSource::Environment
            ) && config.bucket.access_key_env == config.bucket.secret_key_env
            {
                problems.push(Problem::new(
                    "bucket.secret_key_env",
                    "must reference a different environment variable than bucket.access_key_env",
                ));
            }
        }
        BucketKind::AzureBlob => {
            required(config.bucket.name.as_ref(), "bucket.bucket", problems);
            if !matches!(
                config.bucket.credentials_source,
                crate::CredentialsSource::Configured | crate::CredentialsSource::Environment
            ) {
                problems.push(Problem::new(
                    "bucket.credentials_source",
                    "Azure Blob buckets require configured environment credentials",
                ));
            }
            required(
                config.bucket.azure_account_env.as_ref(),
                "bucket.azure_account_env",
                problems,
            );
            required(
                config.bucket.azure_access_key_env.as_ref(),
                "bucket.azure_access_key_env",
                problems,
            );
            if config.bucket.azure_account_env == config.bucket.azure_access_key_env {
                problems.push(Problem::new(
                    "bucket.azure_access_key_env",
                    "must reference a different environment variable than bucket.azure_account_env",
                ));
            }
        }
        BucketKind::Memory => {}
    }
}

mod binding;
mod limits;
mod provider;
mod queue;
use limits::{validate_limits, validate_queues_limits, validate_service_limits};

fn validate_tenants(config: &FleetConfig, problems: &mut Vec<Problem>) {
    let mut tenants = BTreeSet::new();
    for (index, tenant) in config.tenants.iter().enumerate() {
        if !tenants.insert(&tenant.id) {
            problems.push(Problem::new(
                format!("tenants[{index}].id"),
                "tenant id is duplicated",
            ));
        }
    }

    let mut projects = BTreeMap::new();
    for (index, project) in config.projects.iter().enumerate() {
        if !tenants.contains(&project.tenant_id) {
            problems.push(Problem::new(
                format!("projects[{index}].tenant_id"),
                "unknown tenant",
            ));
        }
        if projects
            .insert(project.id.as_str(), project.tenant_id.as_str())
            .is_some()
        {
            problems.push(Problem::new(
                format!("projects[{index}].id"),
                "project id is duplicated",
            ));
        }
    }

    for (index, service) in config.services.iter().enumerate() {
        if service
            .tenant_id
            .as_ref()
            .is_some_and(|id| !tenants.contains(id))
        {
            problems.push(Problem::new(
                format!("services[{index}].tenant_id"),
                "unknown tenant",
            ));
        }
        if let Some(project_id) = service.project_id.as_deref() {
            let Some(project_tenant) = projects.get(project_id) else {
                problems.push(Problem::new(
                    format!("services[{index}].project_id"),
                    "unknown project",
                ));
                continue;
            };
            match service.tenant_id.as_deref() {
                Some(service_tenant) if service_tenant != *project_tenant => {
                    problems.push(Problem::new(
                        format!("services[{index}].project_id"),
                        "project belongs to a different tenant",
                    ));
                }
                None => {
                    problems.push(Problem::new(
                        format!("services[{index}].tenant_id"),
                        "service assigned to a project must declare that project's tenant",
                    ));
                }
                Some(_) => {}
            }
        }
    }
}

fn validate_namespaces(config: &FleetConfig, problems: &mut Vec<Problem>) {
    let mut namespaces = BTreeSet::new();
    for (index, namespace) in config.dispatch_namespaces.iter().enumerate() {
        if !namespaces.insert(&namespace.name) {
            problems.push(Problem::new(
                format!("dispatch_namespaces[{index}].name"),
                "namespace name is duplicated",
            ));
        }
        let mut scripts = BTreeSet::new();
        for (script_index, script) in namespace.scripts.iter().enumerate() {
            if !scripts.insert(&script.name) {
                problems.push(Problem::new(
                    format!("dispatch_namespaces[{index}].scripts[{script_index}].name"),
                    "script name is duplicated",
                ));
            }
            if script.cell_quota == 0 {
                problems.push(Problem::new(
                    format!("dispatch_namespaces[{index}].scripts[{script_index}].cell_quota"),
                    "must be greater than zero",
                ));
            }
        }
    }
    for (service_index, service) in config.services.iter().enumerate() {
        for (name, binding) in &service.bindings {
            if let crate::Binding::Dispatcher { namespace } = binding
                && !namespaces.contains(namespace)
            {
                problems.push(Problem::new(
                    format!("services[{service_index}].bindings.{name}.namespace"),
                    "unknown dispatch namespace",
                ));
            }
        }
    }
}

fn validate_telemetry(config: &FleetConfig, problems: &mut Vec<Problem>) {
    if !(0.0..=1.0).contains(&config.tracing.sampling_ratio) {
        problems.push(Problem::new(
            "tracing.sampling_ratio",
            "must be between 0 and 1",
        ));
    }

    if let Some(otlp) = &config.otlp {
        if otlp.endpoint.trim().is_empty() {
            problems.push(Problem::new("otlp.endpoint", "must not be empty"));
        }
        if !(otlp.endpoint.starts_with("http://") || otlp.endpoint.starts_with("https://")) {
            problems.push(Problem::new(
                "otlp.endpoint",
                "must be an HTTP or HTTPS OTLP endpoint",
            ));
        }
        if otlp.channel_capacity == 0 {
            problems.push(Problem::new("otlp.channel_capacity", "must be at least 1"));
        }
        if otlp.batch_max_spans == 0 {
            problems.push(Problem::new("otlp.batch_max_spans", "must be at least 1"));
        }
        if otlp.flush_interval_secs == 0 {
            problems.push(Problem::new(
                "otlp.flush_interval_secs",
                "must be at least 1",
            ));
        }
        if otlp.request_timeout_secs == 0 {
            problems.push(Problem::new(
                "otlp.request_timeout_secs",
                "must be at least 1",
            ));
        }
    }
    if config.logpush.is_some() {
        problems.push(Problem::new(
            "logpush",
            "Logpush export is not implemented in this build; remove this table until the exporter is available",
        ));
    }
    if config.chronicle_export.is_some() {
        problems.push(Problem::new(
            "chronicle_export",
            "Chronicle export is not part of this release scope",
        ));
    }

    for (field, level) in [
        (
            "logging.internal_level",
            config.logging.internal_level.as_str(),
        ),
        (
            "logging.worker_console_level",
            config.logging.worker_console_level.as_str(),
        ),
    ] {
        if !matches!(level, "trace" | "debug" | "info" | "warn" | "error") {
            problems.push(Problem::new(
                field,
                "must be trace, debug, info, warn, or error",
            ));
        }
    }
}

fn required<T>(value: Option<&T>, field: &str, problems: &mut Vec<Problem>) {
    if value.is_none() {
        problems.push(Problem::new(
            field,
            "field is required for the selected provider",
        ));
    }
}
