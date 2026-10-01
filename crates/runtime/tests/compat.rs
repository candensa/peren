use peren_runtime::{CompatibilityError, NodeCompatibility, ResolvedCompatibility};

#[test]
fn resolves_only_behavior_the_runtime_can_honor() {
    let resolved =
        ResolvedCompatibility::resolve("2026-01-01", &["no_nodejs_compat".to_string()]).unwrap();
    assert_eq!(resolved.node(), NodeCompatibility::Disabled);

    assert!(matches!(
        ResolvedCompatibility::resolve("2026-01-01", &["nodejs_compat".to_string()]),
        Err(CompatibilityError::Unavailable(flag)) if flag == "nodejs_compat"
    ));
}

#[test]
fn rejects_unknown_duplicate_and_invalid_inputs() {
    assert!(matches!(
        ResolvedCompatibility::resolve("2026-02-30", &[]),
        Err(CompatibilityError::Date(_))
    ));
    assert!(matches!(
        ResolvedCompatibility::resolve("2026-01-01", &["future_flag".to_string()]),
        Err(CompatibilityError::Unknown(_))
    ));
    assert!(matches!(
        ResolvedCompatibility::resolve(
            "2026-01-01",
            &[
                "no_nodejs_compat".to_string(),
                "no_nodejs_compat".to_string()
            ]
        ),
        Err(CompatibilityError::Duplicate(_))
    ));
}
