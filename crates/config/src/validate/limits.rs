use crate::{FleetConfig, Problem};

pub(super) fn validate_limits(config: &FleetConfig, problems: &mut Vec<Problem>) {
    if config.limits.heap_bytes < 16 * 1024 * 1024 {
        problems.push(Problem::new(
            "limits.max_heap_bytes",
            "must be at least 16 MiB",
        ));
    }
    if config.limits.execution_time_ms == 0 {
        problems.push(Problem::new(
            "limits.max_execution_time_ms",
            "must be greater than zero",
        ));
    }
    if !(1..=100).contains(&config.limits.isolate_fair_share_percent) {
        problems.push(Problem::new(
            "limits.isolate_fair_share_percent",
            "must be between 1 and 100",
        ));
    }
    if config.limits.isolates == 0 {
        problems.push(Problem::new(
            "limits.max_isolates",
            "must be greater than zero",
        ));
    }
    if config.limits.cron_retry_max_attempts > 0 && config.limits.cron_retry_base_ms == 0 {
        problems.push(Problem::new(
            "limits.cron_retry_base_ms",
            "must be greater than zero when cron retries are enabled",
        ));
    }
}

pub(super) fn validate_service_limits(
    service: &crate::Service,
    index: usize,
    problems: &mut Vec<Problem>,
) {
    if service
        .isolate_fair_share_percent
        .is_some_and(|value| !(1..=100).contains(&value))
    {
        problems.push(Problem::new(
            format!("services[{index}].isolate_fair_share_percent"),
            "must be between 1 and 100",
        ));
    }
    if service
        .max_heap_bytes
        .is_some_and(|bytes| bytes < 16 * 1024 * 1024)
    {
        problems.push(Problem::new(
            format!("services[{index}].max_heap_bytes"),
            "must be at least 16 MiB",
        ));
    }
    if service.max_execution_time_ms == Some(0) {
        problems.push(Problem::new(
            format!("services[{index}].max_execution_time_ms"),
            "must be greater than zero",
        ));
    }
}

pub(super) fn validate_queues_limits(config: &FleetConfig, problems: &mut Vec<Problem>) {
    let Some(queues) = &config.queues else { return };
    validate_queue_values(
        &queues.consumer_defaults,
        "queues.consumer_defaults",
        problems,
    );
    for (service_index, service) in config.services.iter().enumerate() {
        for (queue_index, consumer) in service.consumes_queues.iter().enumerate() {
            if let crate::QueueConsumer::Settings(settings) = consumer {
                validate_queue_overrides(
                    &settings.overrides,
                    &format!("services[{service_index}].consumes_queues[{queue_index}]"),
                    problems,
                );
            }
        }
    }
}

trait QueueValues {
    fn values(&self) -> (u16, u64, u16, u16, u64);
}

impl QueueValues for crate::QueueDefaults {
    fn values(&self) -> (u16, u64, u16, u16, u64) {
        (
            self.max_batch_size,
            self.max_batch_timeout_secs,
            self.max_retries,
            self.max_concurrency,
            self.retry_delay_secs,
        )
    }
}

fn validate_queue_values(values: &impl QueueValues, field: &str, problems: &mut Vec<Problem>) {
    let (batch, timeout, retries, concurrency, delay) = values.values();
    for (valid, suffix, message) in [
        (
            (1..=100).contains(&batch),
            "max_batch_size",
            "must be between 1 and 100",
        ),
        (
            (1..=60).contains(&timeout),
            "max_batch_timeout_secs",
            "must be between 1 and 60",
        ),
        (retries <= 100, "max_retries", "must be at most 100"),
        (
            (1..=250).contains(&concurrency),
            "max_concurrency",
            "must be between 1 and 250",
        ),
        (delay <= 86_400, "retry_delay_secs", "must be at most 86400"),
    ] {
        if !valid {
            problems.push(Problem::new(format!("{field}.{suffix}"), message));
        }
    }
}

fn validate_queue_overrides(
    values: &crate::QueueOverrides,
    field: &str,
    problems: &mut Vec<Problem>,
) {
    for (valid, suffix, message) in [
        (
            values
                .max_batch_size
                .is_none_or(|value| (1..=100).contains(&value)),
            "max_batch_size",
            "must be between 1 and 100",
        ),
        (
            values
                .max_batch_timeout_secs
                .is_none_or(|value| (1..=60).contains(&value)),
            "max_batch_timeout_secs",
            "must be between 1 and 60",
        ),
        (
            values.max_retries.is_none_or(|value| value <= 100),
            "max_retries",
            "must be at most 100",
        ),
        (
            values
                .max_concurrency
                .is_none_or(|value| (1..=250).contains(&value)),
            "max_concurrency",
            "must be between 1 and 250",
        ),
        (
            values.retry_delay_secs.is_none_or(|value| value <= 86_400),
            "retry_delay_secs",
            "must be at most 86400",
        ),
    ] {
        if !valid {
            problems.push(Problem::new(format!("{field}.{suffix}"), message));
        }
    }
}
