use peren_runtime::{InvocationBudget, InvocationLimits, LimitError};

#[test]
fn rejects_request_before_execution_when_its_body_exceeds_the_limit() {
    let result = InvocationBudget::begin(InvocationLimits::new(10, 2), 11);
    assert!(matches!(
        result,
        Err(LimitError::RequestBytes {
            used: 11,
            limit: 10
        })
    ));
}

#[test]
fn charges_subrequests_without_mutating_after_refusal() {
    let mut budget = InvocationBudget::begin(InvocationLimits::new(10, 2), 10).unwrap();
    budget.charge_subrequest().unwrap();
    budget.charge_subrequest().unwrap();
    assert!(matches!(
        budget.charge_subrequest(),
        Err(LimitError::Subrequests { used: 3, limit: 2 })
    ));
    assert_eq!(budget.subrequests(), 2);
}

#[test]
fn rejects_cpu_time_after_execution() {
    let limits = InvocationLimits::new(10, 2).with_cpu_time(std::time::Duration::from_millis(5));
    let error = limits
        .validate_cpu_time(std::time::Duration::from_millis(6))
        .unwrap_err();

    assert!(matches!(
        error,
        LimitError::CpuTime { used, limit }
            if used == std::time::Duration::from_millis(6)
                && limit == std::time::Duration::from_millis(5)
    ));
}
