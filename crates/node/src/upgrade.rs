use crate::{ConformanceError, ConformanceStorage, Environment, conformance_storage};

#[derive(Debug)]
pub struct Check {
    pub target: Option<String>,
}

#[derive(Debug)]
pub struct CheckReport {
    pub current: String,
    pub target: String,
    pub storage: bool,
    pub services: usize,
    pub sockets: usize,
}

#[derive(Debug)]
pub struct Plan {
    pub target: Option<String>,
}

#[derive(Debug)]
pub struct PlanReport {
    pub check: CheckReport,
    pub steps: Vec<Step>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Step {
    pub order: usize,
    pub action: &'static str,
}

pub async fn check(
    config: peren_config::ValidatedConfig,
    environment: &impl Environment,
    request: Check,
) -> Result<CheckReport, ConformanceError> {
    let services = config.raw.services.len();
    let sockets = config.sockets.len();
    conformance_storage(config, environment, ConformanceStorage).await?;
    let current = env!("CARGO_PKG_VERSION").to_string();
    Ok(CheckReport {
        target: request.target.unwrap_or_else(|| current.clone()),
        current,
        storage: true,
        services,
        sockets,
    })
}

pub async fn plan(
    config: peren_config::ValidatedConfig,
    environment: &impl Environment,
    request: Plan,
) -> Result<PlanReport, ConformanceError> {
    let check = check(
        config,
        environment,
        Check {
            target: request.target,
        },
    )
    .await?;
    Ok(PlanReport {
        check,
        steps: vec![
            Step {
                order: 1,
                action: "backup data directory",
            },
            Step {
                order: 2,
                action: "drain one node at a time",
            },
            Step {
                order: 3,
                action: "replace binary with target version",
            },
            Step {
                order: 4,
                action: "run conformance storage",
            },
            Step {
                order: 5,
                action: "run deploy health",
            },
        ],
    })
}

#[cfg(test)]
mod tests {
    use peren_config::FleetConfig;

    use super::*;

    struct TestEnvironment;

    impl Environment for TestEnvironment {
        fn get(&self, _name: &str) -> Option<String> {
            None
        }
    }

    #[tokio::test]
    async fn upgrade_check_runs_storage_conformance() {
        let report = check(
            config(),
            &TestEnvironment,
            Check {
                target: Some("0.1.1".into()),
            },
        )
        .await
        .unwrap();

        assert_eq!(report.current, env!("CARGO_PKG_VERSION"));
        assert_eq!(report.target, "0.1.1");
        assert!(report.storage);
        assert_eq!(report.services, 1);
    }

    #[tokio::test]
    async fn upgrade_plan_reuses_check_and_orders_steps() {
        let report = plan(config(), &TestEnvironment, Plan { target: None })
            .await
            .unwrap();

        assert_eq!(report.check.target, env!("CARGO_PKG_VERSION"));
        assert_eq!(report.steps[0].order, 1);
        assert_eq!(report.steps[0].action, "backup data directory");
        assert!(
            report
                .steps
                .iter()
                .any(|step| step.action == "run conformance storage")
        );
    }

    fn config() -> peren_config::ValidatedConfig {
        FleetConfig::from_toml(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "worker.js"
compatibility_date = "2026-01-01"
"#,
        )
        .unwrap()
        .validate()
        .unwrap()
    }
}
