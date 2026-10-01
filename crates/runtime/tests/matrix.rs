use std::collections::{BTreeMap, BTreeSet};

use peren_runtime::{CAPABILITIES, CapabilityKind, CapabilityStatus, release_blockers};

#[test]
fn runtime_capability_fixtures_have_explicit_dispositions() {
    let mut matrix = BTreeMap::new();
    for capability in CAPABILITIES {
        assert!(!capability.name.trim().is_empty());
        assert!(!capability.evidence.trim().is_empty());
        let previous = matrix.insert((capability.kind, capability.name), capability);
        assert!(
            previous.is_none(),
            "duplicate capability {:?} {}",
            capability.kind,
            capability.name
        );
    }

    assert_fixture(
        &matrix,
        CapabilityKind::Binding,
        include_str!("../../../tests/fixtures/current/runtime/bindings.txt"),
    );
    assert_fixture(
        &matrix,
        CapabilityKind::Event,
        include_str!("../../../tests/fixtures/current/runtime/events.txt"),
    );
    assert_fixture(
        &matrix,
        CapabilityKind::Global,
        include_str!("../../../tests/fixtures/current/runtime/globals.txt"),
    );
    assert_fixture(
        &matrix,
        CapabilityKind::Module,
        include_str!("../../../tests/fixtures/current/runtime/modules.txt"),
    );
}

fn assert_fixture(
    matrix: &BTreeMap<(CapabilityKind, &'static str), &peren_runtime::RuntimeCapability>,
    kind: CapabilityKind,
    fixture: &str,
) {
    let expected = fixture
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<BTreeSet<_>>();
    let actual = matrix
        .keys()
        .filter_map(|(capability, name)| (*capability == kind).then_some(*name))
        .collect::<BTreeSet<_>>();

    let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
    let extra = actual.difference(&expected).copied().collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "missing {kind:?} dispositions: {missing:?}"
    );
    assert!(
        extra.is_empty(),
        "extra {kind:?} dispositions absent from fixture: {extra:?}"
    );
}

#[test]
fn bootstrap_global_exports_are_supported_capabilities() {
    let bootstrap = include_str!("../src/bootstrap.js");
    let exposed = bootstrap_global_exports(bootstrap);
    let supported = CAPABILITIES
        .iter()
        .filter(|capability| {
            capability.kind == CapabilityKind::Global
                && capability.status != CapabilityStatus::Unsupported
                && capability.status != CapabilityStatus::Planned
        })
        .map(|capability| capability.name)
        .collect::<BTreeSet<_>>();

    let undocumented = exposed.difference(&supported).copied().collect::<Vec<_>>();

    assert!(
        undocumented.is_empty(),
        "bootstrap exposes globals without supported matrix evidence: {undocumented:?}"
    );
}

fn bootstrap_global_exports(source: &str) -> BTreeSet<&str> {
    let Some((_, after)) = source.split_once("ObjectDefineProperties(globalThis, {") else {
        panic!("bootstrap must define globalThis properties");
    };
    let Some((block, _)) = after.split_once("\n});") else {
        panic!("globalThis property block must terminate");
    };

    block
        .lines()
        .filter_map(|line| {
            let line = line.trim_start();
            let (name, value) = line.split_once(':')?;
            value
                .trim_start()
                .starts_with("core.propNonEnumerable")
                .then_some(name)
        })
        .collect()
}

#[test]
fn builtin_node_modules_have_explicit_dispositions() {
    let source = include_str!("../src/bundle.rs");
    let builtins = builtin_node_modules(source);
    let documented = CAPABILITIES
        .iter()
        .filter(|capability| capability.kind == CapabilityKind::Module)
        .map(|capability| capability.name)
        .collect::<BTreeSet<_>>();

    let missing = builtins
        .difference(&documented)
        .copied()
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "built-in node modules lack runtime matrix dispositions: {missing:?}"
    );
}

fn builtin_node_modules(source: &str) -> BTreeSet<&str> {
    source
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if !line.starts_with("pub(crate) const BUILTIN_") {
                return None;
            }
            let (_, value) = line.split_once("= ")?;
            let value = value.trim().strip_prefix('"')?.strip_suffix("\";")?;
            value.starts_with("node:").then_some(value)
        })
        .collect()
}

#[test]
fn unavailable_capabilities_name_their_release_boundary() {
    let unavailable = CAPABILITIES
        .iter()
        .filter(|capability| {
            matches!(
                capability.status,
                CapabilityStatus::Partial
                    | CapabilityStatus::Unsupported
                    | CapabilityStatus::Planned
            )
        })
        .collect::<Vec<_>>();

    assert!(
        !unavailable.is_empty(),
        "non-supported runtime capabilities should be explicit instead of hidden inside Supported evidence"
    );

    for capability in unavailable {
        let evidence = capability.evidence.to_ascii_lowercase();
        assert!(
            evidence.contains("unsupported")
                || evidence.contains("not enabled")
                || evidence.contains("subset")
                || evidence.contains("gap")
                || evidence.contains("outside")
                || evidence.contains("deliberately")
                || evidence.contains("intentionally")
                || evidence.contains("not exposed")
                || evidence.contains("not implemented"),
            "non-supported capability {:?} {} must state the release boundary in its evidence",
            capability.kind,
            capability.name
        );
    }
}

#[test]
fn runtime_capability_matrix_has_no_planned_release_blockers() {
    let blockers = release_blockers();

    assert!(
        blockers.is_empty(),
        "planned runtime capabilities must be resolved before release: {blockers:?}"
    );
}
