use std::{fmt::Debug, future::Future, time::Duration};

pub async fn eventually<T, F, Fut, P>(
    label: &str,
    within: Duration,
    poll_every: Duration,
    mut check: F,
    mut passes: P,
) -> T
where
    T: Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = T>,
    P: FnMut(&T) -> bool,
{
    let started = tokio::time::Instant::now();
    loop {
        let observed = check().await;
        if passes(&observed) {
            return observed;
        }
        assert!(
            started.elapsed() < within,
            "eventual assertion {label:?} timed out after {within:?}; last observed value: {observed:?}"
        );
        tokio::time::sleep(poll_every).await;
    }
}
