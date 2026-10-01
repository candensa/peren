use std::{fs, path::Path};

use peren_config::{FleetConfig, ValidatedConfig};
use peren_node::{
    DeployError, DeployHealth, DeployList, DeployPrune, DeployRecord, DeployRollback, DeployVerify,
    Deployment, Environment,
};
use tempfile::TempDir;
use uuid::Uuid;

struct Env {
    data: TempDir,
}

impl Env {
    fn new() -> Self {
        Self {
            data: tempfile::tempdir().unwrap(),
        }
    }

    fn deploy_file(&self) -> std::path::PathBuf {
        self.data.path().join("deploy/deployments.json")
    }
}

impl Environment for Env {
    fn get(&self, name: &str) -> Option<String> {
        (name == "PEREN_DATA_DIR").then(|| self.data.path().display().to_string())
    }
}

struct Project {
    root: TempDir,
    worker: std::path::PathBuf,
}

impl Project {
    fn new(source: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let worker = root.path().join("worker.js");
        fs::write(&worker, source).unwrap();
        Self { root, worker }
    }

    fn write_worker(&self, source: &str) {
        fs::write(&self.worker, source).unwrap();
    }

    fn map(&self, source: &str) -> std::path::PathBuf {
        let path = self.root.path().join("worker.js.map");
        fs::write(&path, source).unwrap();
        path
    }
}

#[test]
fn deployment_lifecycle_records_lists_health_rolls_back_and_prunes() {
    let project = Project::new("export default { fetch() { return new Response('one') } };");
    let config = config(&project.worker);
    let env = Env::new();
    let deployment = Deployment::new(&config, &env);

    let first = deployment
        .record(&DeployRecord {
            percent: 100,
            preview: false,
        })
        .unwrap()
        .generations
        .remove(0);
    assert!(first.active);

    project.write_worker("export default { fetch() { return new Response('two') } };");
    deployment
        .record(&DeployRecord {
            percent: 100,
            preview: false,
        })
        .unwrap();

    let listed = deployment.list(&DeployList { service: None }).unwrap();
    assert_eq!(listed.generations.len(), 2);
    assert_eq!(
        listed.generations.iter().filter(|item| item.active).count(),
        1
    );

    assert_eq!(
        deployment
            .health(&DeployHealth { service: None })
            .unwrap()
            .services[0]
            .service,
        "api"
    );

    assert!(matches!(
        deployment.rollback(&DeployRollback {
            service: "api".into(),
            digest: Some(first.digest.clone()),
        }),
        Err(DeployError::Drift { service, .. }) if service == "api"
    ));

    let pruned = deployment
        .prune(&DeployPrune {
            service: "api".into(),
            keep: 0,
            dry: false,
        })
        .unwrap();
    assert_eq!(pruned.removed.len(), 1);
    assert_eq!(
        deployment
            .list(&DeployList { service: None })
            .unwrap()
            .generations
            .len(),
        1
    );
}

#[test]
fn deployment_record_is_idempotent_and_audit_stays_metadata_only() {
    let project = Project::new("export default { fetch() { return new Response('same') } };");
    let config = config(&project.worker);
    let env = Env::new();
    let deployment = Deployment::new(&config, &env);

    let first = deployment
        .record(&DeployRecord {
            percent: 25,
            preview: false,
        })
        .unwrap()
        .generations
        .remove(0);
    let second = deployment
        .record(&DeployRecord {
            percent: 75,
            preview: false,
        })
        .unwrap()
        .generations
        .remove(0);

    assert_eq!(first.digest, second.digest);
    assert_eq!(first.created_at_ms, second.created_at_ms);
    assert_eq!(second.percent, 75);

    let registry: serde_json::Value =
        serde_json::from_slice(&fs::read(env.deploy_file()).unwrap()).unwrap();
    let audit = serde_json::to_string(&registry["audit"]).unwrap();
    assert!(!audit.contains("same"));
    assert!(!audit.contains("worker.js"));
    assert!(!audit.contains(&project.worker.display().to_string()));
}

#[test]
fn deployment_verify_rejects_descriptor_drift_and_legacy_records() {
    let project = Project::new("export default { fetch() { return new Response('stable') } };");
    let original = config(&project.worker);
    let mut changed = config(&project.worker);
    changed.raw.services[0]
        .compatibility_flags
        .push("streams_enable_constructors".into());
    let env = Env::new();
    let deployment = Deployment::new(&original, &env);

    let mut generation = deployment
        .record(&DeployRecord {
            percent: 100,
            preview: false,
        })
        .unwrap()
        .generations
        .remove(0);

    assert!(matches!(
        Deployment::new(&changed, &env).health(&DeployHealth { service: None }),
        Err(DeployError::DescriptorDrift { service, .. }) if service == "api"
    ));

    generation.descriptor_digest = None;
    write_registry(&env, &[generation]);
    assert!(matches!(
        deployment.verify(&DeployVerify { service: None }),
        Err(DeployError::DescriptorMissing { service, .. }) if service == "api"
    ));
}

#[test]
fn deployment_verify_rejects_unsupported_descriptor_schema_and_features() {
    let project = Project::new("export default { fetch() { return new Response('gated') } };");
    let config = config(&project.worker);
    let env = Env::new();
    let deployment = Deployment::new(&config, &env);
    let mut generation = deployment
        .record(&DeployRecord {
            percent: 100,
            preview: false,
        })
        .unwrap()
        .generations
        .remove(0);

    generation.descriptor_schema = 99;
    write_registry(&env, std::slice::from_ref(&generation));
    assert!(matches!(
        deployment.verify(&DeployVerify { service: None }),
        Err(DeployError::DescriptorSchema { service, schema: 99, .. }) if service == "api"
    ));

    generation.descriptor_schema = 1;
    generation.required_features = vec!["future-runtime".into()];
    write_registry(&env, &[generation]);
    assert!(matches!(
        deployment.verify(&DeployVerify { service: None }),
        Err(DeployError::DescriptorFeature { service, feature, .. })
            if service == "api" && feature == "future-runtime"
    ));
}

#[test]
fn source_maps_are_deployment_artifacts() {
    let project = Project::new("export default { fetch() { return new Response('mapped') } };");
    let map = project.map(r#"{"version":3,"sources":["worker.ts"],"mappings":""}"#);
    let mut config = config(&project.worker);
    config.raw.services[0]
        .source_maps
        .insert("worker.js.map".into(), map.clone());
    let env = Env::new();
    let deployment = Deployment::new(&config, &env);

    let recorded = deployment
        .record(&DeployRecord {
            percent: 100,
            preview: false,
        })
        .unwrap();
    assert_eq!(recorded.generations[0].source_maps, 1);
    assert!(recorded.generations[0].source_map_digest.is_some());
    assert_eq!(
        deployment
            .verify(&DeployVerify { service: None })
            .unwrap()
            .generations
            .len(),
        1
    );

    fs::write(
        &map,
        r#"{"version":3,"sources":["changed.ts"],"mappings":""}"#,
    )
    .unwrap();
    assert!(matches!(
        deployment.health(&DeployHealth { service: None }),
        Err(DeployError::MapDrift { service, .. }) if service == "api"
    ));
}

fn write_registry(env: &Env, generations: &[peren_node::DeployGeneration]) {
    let path = env.deploy_file();
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let body = serde_json::json!({
        "generations": generations,
        "audit": []
    });
    fs::write(path, serde_json::to_vec_pretty(&body).unwrap()).unwrap();
}

fn config(bundle: &Path) -> ValidatedConfig {
    let text = format!(
        r#"
[node]
node_id = "{}"
advertise_addr = "127.0.0.1:0"
listen = "127.0.0.1:0"

[bucket]
kind = "memory"

[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"

[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2024-01-01"
"#,
        Uuid::new_v4(),
        bundle.display()
    );
    toml::from_str::<FleetConfig>(&text)
        .unwrap()
        .validate()
        .unwrap()
}
