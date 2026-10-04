use std::fmt::Write as _;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::admission::Snapshot as AdmissionSnapshot;

const DURATION_BUCKETS_MS: [u64; 8] = [1, 5, 10, 25, 50, 100, 250, 1000];

#[derive(Clone)]
pub(crate) struct Metrics {
    pub(crate) listener: Arc<str>,
    pub(crate) listeners: usize,
    pub(crate) services: usize,
    pub(crate) consumers: usize,
    pub(crate) worker: bool,
    pub(crate) started_at_ms: u64,
    pub(crate) telemetry: Arc<Telemetry>,
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[derive(Default)]
pub(crate) struct Telemetry {
    pub(crate) http_requests: AtomicU64,
    pub(crate) http_errors: AtomicU64,
    pub(crate) http_duration_ms: AtomicU64,
    pub(crate) http_duration: DurationHistogram,
    pub(crate) worker_dispatches: AtomicU64,
    pub(crate) worker_dispatch_errors: AtomicU64,
    pub(crate) worker_dispatch_duration_ms: AtomicU64,
    pub(crate) worker_dispatch_duration: DurationHistogram,
    pub(crate) cell_restores: AtomicU64,
    pub(crate) cell_dispatches: AtomicU64,
    pub(crate) cell_dispatch_errors: AtomicU64,
    pub(crate) cell_commits: AtomicU64,
    pub(crate) cell_releases: AtomicU64,
    pub(crate) cell_release_errors: AtomicU64,
    pub(crate) queue_ticks: AtomicU64,
    pub(crate) queue_leases: AtomicU64,
    pub(crate) queue_messages: AtomicU64,
    pub(crate) queue_dispatches: AtomicU64,
    pub(crate) queue_errors: AtomicU64,
    pub(crate) queue_dispatch_duration_ms: AtomicU64,
    pub(crate) queue_dispatch_duration: DurationHistogram,
    pub(crate) storage_commits: AtomicU64,
    pub(crate) storage_commit_errors: AtomicU64,
    pub(crate) storage_commit_duration_ms: AtomicU64,
    pub(crate) storage_commit_duration: DurationHistogram,
    pub(crate) storage_rollbacks: AtomicU64,
    pub(crate) storage_rollback_errors: AtomicU64,
    pub(crate) queue_sends: AtomicU64,
    pub(crate) queue_send_errors: AtomicU64,
    pub(crate) queue_send_duration_ms: AtomicU64,
    pub(crate) queue_send_duration: DurationHistogram,
    pub(crate) r2_operations: AtomicU64,
    pub(crate) r2_errors: AtomicU64,
    pub(crate) r2_duration_ms: AtomicU64,
    pub(crate) r2_duration: DurationHistogram,
    pub(crate) kv_operations: AtomicU64,
    pub(crate) kv_errors: AtomicU64,
    pub(crate) kv_duration_ms: AtomicU64,
    pub(crate) kv_duration: DurationHistogram,
    pub(crate) d1_queries: AtomicU64,
    pub(crate) d1_errors: AtomicU64,
    pub(crate) d1_duration_ms: AtomicU64,
    pub(crate) d1_duration: DurationHistogram,
    pub(crate) cache_operations: AtomicU64,
    pub(crate) cache_errors: AtomicU64,
    pub(crate) cache_duration_ms: AtomicU64,
    pub(crate) cache_duration: DurationHistogram,
    pub(crate) ai_runs: AtomicU64,
    pub(crate) ai_errors: AtomicU64,
    pub(crate) ai_duration_ms: AtomicU64,
    pub(crate) ai_duration: DurationHistogram,
    pub(crate) service_fetches: AtomicU64,
    pub(crate) service_errors: AtomicU64,
    pub(crate) service_duration_ms: AtomicU64,
    pub(crate) service_duration: DurationHistogram,
    pub(crate) outbound_fetches: AtomicU64,
    pub(crate) outbound_errors: AtomicU64,
    pub(crate) outbound_duration_ms: AtomicU64,
    pub(crate) outbound_duration: DurationHistogram,
    pub(crate) object_fetches: AtomicU64,
    pub(crate) object_errors: AtomicU64,
    pub(crate) object_duration_ms: AtomicU64,
    pub(crate) object_duration: DurationHistogram,
    pub(crate) websocket_sessions: AtomicU64,
    pub(crate) websocket_messages: AtomicU64,
    pub(crate) websocket_closes: AtomicU64,
    pub(crate) websocket_errors: AtomicU64,
}

pub(crate) struct DurationHistogram {
    buckets: [AtomicU64; DURATION_BUCKETS_MS.len()],
    infinite: AtomicU64,
}

impl Default for DurationHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            infinite: AtomicU64::new(0),
        }
    }
}

impl DurationHistogram {
    fn observe(&self, elapsed_ms: u64) {
        for (index, bound) in DURATION_BUCKETS_MS.iter().enumerate() {
            if elapsed_ms <= *bound {
                self.buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.infinite.fetch_add(1, Ordering::Relaxed);
    }

    fn counts(&self) -> ([u64; DURATION_BUCKETS_MS.len()], u64) {
        (
            std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
            self.infinite.load(Ordering::Relaxed),
        )
    }
}

impl Telemetry {
    pub(crate) fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add(counter: &AtomicU64, value: usize) {
        let value = u64::try_from(value).unwrap_or(u64::MAX);
        counter.fetch_add(value, Ordering::Relaxed);
    }

    pub(crate) fn observe(counter: &AtomicU64, histogram: &DurationHistogram, started: Instant) {
        let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        counter.fetch_add(elapsed, Ordering::Relaxed);
        histogram.observe(elapsed);
    }

    fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "Prometheus text exposition is clearer as one ordered snapshot"
)]
pub(crate) fn render(
    ready: bool,
    metrics: &Metrics,
    admission: Option<AdmissionSnapshot>,
    websocket_active: usize,
    placement_weight: u32,
) -> String {
    let telemetry = &metrics.telemetry;
    let admission_capacity = admission.map_or(0, |snapshot| snapshot.capacity);
    let admission_available = admission.map_or(0, |snapshot| snapshot.available);
    let admission_active = admission.map_or(0, |snapshot| snapshot.active);
    let admission_admitted = admission.map_or(0, |snapshot| snapshot.admitted);
    let admission_completed = admission.map_or(0, |snapshot| snapshot.completed);
    let admission_refused = admission.map_or(0, |snapshot| snapshot.refused);
    let placement_available = admission
        .is_some_and(|snapshot| ready && matches!(snapshot.mode, crate::admission::Mode::Serving));
    let uptime_seconds = now_ms().saturating_sub(metrics.started_at_ms) / 1000;
    let websocket_active = u64::try_from(websocket_active).unwrap_or(u64::MAX);
    let listener = label(&metrics.listener);
    let mut output = format!(
        "# HELP peren_build_info Peren build information.\n\
# TYPE peren_build_info gauge\n\
peren_build_info{{version=\"{}\"}} 1\n\
# HELP peren_ready Whether this listener is ready to serve traffic.\n\
# TYPE peren_ready gauge\n\
peren_ready{{listener=\"{}\"}} {}\n\
# HELP peren_listener_worker Whether this listener dispatches Worker traffic.\n\
# TYPE peren_listener_worker gauge\n\
peren_listener_worker{{listener=\"{}\"}} {}\n\
# HELP peren_process_started_time_seconds Unix timestamp when this process started.\n\
# TYPE peren_process_started_time_seconds gauge\n\
peren_process_started_time_seconds {}\n\
# HELP peren_process_uptime_seconds Seconds since this process started.\n\
# TYPE peren_process_uptime_seconds gauge\n\
peren_process_uptime_seconds {}\n\
# HELP peren_configured_listeners Configured listener count for this node.\n\
# TYPE peren_configured_listeners gauge\n\
peren_configured_listeners {}\n\
# HELP peren_configured_services Configured service count for this node.\n\
# TYPE peren_configured_services gauge\n\
peren_configured_services {}\n\
# HELP peren_configured_queue_consumers Configured queue consumer count for this node.\n\
# TYPE peren_configured_queue_consumers gauge\n\
peren_configured_queue_consumers {}\n\
# HELP peren_admission_capacity Concurrent top-level work admitted by this node.\n\
# TYPE peren_admission_capacity gauge\n\
peren_admission_capacity {}\n\
# HELP peren_admission_available Available top-level work capacity on this node.\n\
# TYPE peren_admission_available gauge\n\
peren_admission_available {}\n\
# HELP peren_admission_active Active top-level work currently running on this node.\n\
# TYPE peren_admission_active gauge\n\
peren_admission_active {}\n\
# HELP peren_admission_admitted_total Top-level work admitted by this node.\n\
# TYPE peren_admission_admitted_total counter\n\
peren_admission_admitted_total {}\n\
# HELP peren_admission_completed_total Top-level work completed by this node.\n\
# TYPE peren_admission_completed_total counter\n\
peren_admission_completed_total {}\n\
# HELP peren_admission_refused_total Top-level work refused because the node was saturated.\n\
# TYPE peren_admission_refused_total counter\n\
peren_admission_refused_total {}\n",
        env!("CARGO_PKG_VERSION"),
        listener,
        u8::from(ready),
        listener,
        u8::from(metrics.worker),
        metrics.started_at_ms / 1000,
        uptime_seconds,
        metrics.listeners,
        metrics.services,
        metrics.consumers,
        admission_capacity,
        admission_available,
        admission_active,
        admission_admitted,
        admission_completed,
        admission_refused,
    );
    let _ = write!(
        output,
        "# HELP peren_placement_weight Configured placement weight for this node.\n\
# TYPE peren_placement_weight gauge\n\
peren_placement_weight {}\n\
# HELP peren_placement_available Whether this listener is serving and eligible for new placement.\n\
# TYPE peren_placement_available gauge\n\
peren_placement_available {}\n",
        placement_weight,
        u8::from(placement_available),
    );
    counter(
        &mut output,
        "peren_http_requests_total",
        "HTTP requests observed by this node.",
        Telemetry::get(&telemetry.http_requests),
    );
    counter(
        &mut output,
        "peren_http_errors_total",
        "HTTP requests completed with a 5xx status or dispatch failure.",
        Telemetry::get(&telemetry.http_errors),
    );
    duration(
        &mut output,
        "peren_http_duration_ms",
        "HTTP request wall time in milliseconds.",
        Telemetry::get(&telemetry.http_duration_ms),
        &telemetry.http_duration,
    );
    duration_seconds(
        &mut output,
        "peren_http_request_duration_seconds",
        "HTTP request wall time in seconds.",
        Telemetry::get(&telemetry.http_duration_ms),
        &telemetry.http_duration,
    );
    counter(
        &mut output,
        "peren_worker_dispatches_total",
        "Worker fetch dispatches completed by this node.",
        Telemetry::get(&telemetry.worker_dispatches),
    );
    counter(
        &mut output,
        "peren_worker_dispatch_errors_total",
        "Worker fetch dispatches that returned an error response or failed.",
        Telemetry::get(&telemetry.worker_dispatch_errors),
    );
    duration_seconds(
        &mut output,
        "peren_worker_dispatch_duration_seconds",
        "Worker fetch dispatch wall time in seconds.",
        Telemetry::get(&telemetry.worker_dispatch_duration_ms),
        &telemetry.worker_dispatch_duration,
    );
    counter(
        &mut output,
        "peren_cell_restores_total",
        "Cell restore phases completed before dispatch.",
        Telemetry::get(&telemetry.cell_restores),
    );
    counter(
        &mut output,
        "peren_cell_dispatches_total",
        "Cell dispatch phases completed.",
        Telemetry::get(&telemetry.cell_dispatches),
    );
    counter(
        &mut output,
        "peren_cell_dispatch_errors_total",
        "Cell dispatch phases that returned an error response or failed.",
        Telemetry::get(&telemetry.cell_dispatch_errors),
    );
    counter(
        &mut output,
        "peren_cell_commits_total",
        "Cell dispatches that published a durable storage commit.",
        Telemetry::get(&telemetry.cell_commits),
    );
    counter(
        &mut output,
        "peren_cell_releases_total",
        "Cell release phases completed successfully.",
        Telemetry::get(&telemetry.cell_releases),
    );
    counter(
        &mut output,
        "peren_cell_release_errors_total",
        "Cell release phases that failed.",
        Telemetry::get(&telemetry.cell_release_errors),
    );
    counter(
        &mut output,
        "peren_queue_ticks_total",
        "Queue consumer polling ticks.",
        Telemetry::get(&telemetry.queue_ticks),
    );
    counter(
        &mut output,
        "peren_queue_leases_total",
        "Queue lease batches acquired.",
        Telemetry::get(&telemetry.queue_leases),
    );
    counter(
        &mut output,
        "peren_queue_messages_total",
        "Queue messages leased for dispatch.",
        Telemetry::get(&telemetry.queue_messages),
    );
    counter(
        &mut output,
        "peren_queue_dispatches_total",
        "Queue batches dispatched to Workers.",
        Telemetry::get(&telemetry.queue_dispatches),
    );
    counter(
        &mut output,
        "peren_queue_errors_total",
        "Queue lease or dispatch failures.",
        Telemetry::get(&telemetry.queue_errors),
    );
    duration(
        &mut output,
        "peren_queue_dispatch_duration_ms",
        "Queue dispatch wall time in milliseconds.",
        Telemetry::get(&telemetry.queue_dispatch_duration_ms),
        &telemetry.queue_dispatch_duration,
    );
    duration_seconds(
        &mut output,
        "peren_queue_dispatch_duration_seconds",
        "Queue dispatch wall time in seconds.",
        Telemetry::get(&telemetry.queue_dispatch_duration_ms),
        &telemetry.queue_dispatch_duration,
    );
    counter(
        &mut output,
        "peren_storage_commits_total",
        "Durable storage commits completed through the node host.",
        Telemetry::get(&telemetry.storage_commits),
    );
    counter(
        &mut output,
        "peren_storage_commit_errors_total",
        "Durable storage commit failures through the node host.",
        Telemetry::get(&telemetry.storage_commit_errors),
    );
    duration(
        &mut output,
        "peren_storage_commit_duration_ms",
        "Durable storage commit wall time in milliseconds.",
        Telemetry::get(&telemetry.storage_commit_duration_ms),
        &telemetry.storage_commit_duration,
    );
    duration_seconds(
        &mut output,
        "peren_storage_commit_duration_seconds",
        "Durable storage commit wall time in seconds.",
        Telemetry::get(&telemetry.storage_commit_duration_ms),
        &telemetry.storage_commit_duration,
    );
    counter(
        &mut output,
        "peren_storage_rollbacks_total",
        "Durable storage rollbacks completed through the node host.",
        Telemetry::get(&telemetry.storage_rollbacks),
    );
    counter(
        &mut output,
        "peren_storage_rollback_errors_total",
        "Durable storage rollback failures through the node host.",
        Telemetry::get(&telemetry.storage_rollback_errors),
    );
    counter(
        &mut output,
        "peren_queue_sends_total",
        "Worker queue sends completed through bindings.",
        Telemetry::get(&telemetry.queue_sends),
    );
    counter(
        &mut output,
        "peren_queue_send_errors_total",
        "Worker queue send failures.",
        Telemetry::get(&telemetry.queue_send_errors),
    );
    duration(
        &mut output,
        "peren_queue_send_duration_ms",
        "Worker queue send wall time in milliseconds.",
        Telemetry::get(&telemetry.queue_send_duration_ms),
        &telemetry.queue_send_duration,
    );
    duration_seconds(
        &mut output,
        "peren_queue_send_duration_seconds",
        "Worker queue send wall time in seconds.",
        Telemetry::get(&telemetry.queue_send_duration_ms),
        &telemetry.queue_send_duration,
    );
    counter(
        &mut output,
        "peren_r2_operations_total",
        "R2 binding operations completed through the node host.",
        Telemetry::get(&telemetry.r2_operations),
    );
    counter(
        &mut output,
        "peren_r2_errors_total",
        "R2 binding operation failures.",
        Telemetry::get(&telemetry.r2_errors),
    );
    duration(
        &mut output,
        "peren_r2_duration_ms",
        "R2 binding operation wall time in milliseconds.",
        Telemetry::get(&telemetry.r2_duration_ms),
        &telemetry.r2_duration,
    );
    duration_seconds(
        &mut output,
        "peren_r2_duration_seconds",
        "R2 binding operation wall time in seconds.",
        Telemetry::get(&telemetry.r2_duration_ms),
        &telemetry.r2_duration,
    );
    counter(
        &mut output,
        "peren_kv_operations_total",
        "KV binding operations completed through the node host.",
        Telemetry::get(&telemetry.kv_operations),
    );
    counter(
        &mut output,
        "peren_kv_errors_total",
        "KV binding operation failures.",
        Telemetry::get(&telemetry.kv_errors),
    );
    duration(
        &mut output,
        "peren_kv_duration_ms",
        "KV binding operation wall time in milliseconds.",
        Telemetry::get(&telemetry.kv_duration_ms),
        &telemetry.kv_duration,
    );
    duration_seconds(
        &mut output,
        "peren_kv_duration_seconds",
        "KV binding operation wall time in seconds.",
        Telemetry::get(&telemetry.kv_duration_ms),
        &telemetry.kv_duration,
    );
    counter(
        &mut output,
        "peren_d1_queries_total",
        "D1 SQL queries completed through the node host.",
        Telemetry::get(&telemetry.d1_queries),
    );
    counter(
        &mut output,
        "peren_d1_errors_total",
        "D1 SQL query failures.",
        Telemetry::get(&telemetry.d1_errors),
    );
    duration(
        &mut output,
        "peren_d1_duration_ms",
        "D1 SQL query wall time in milliseconds.",
        Telemetry::get(&telemetry.d1_duration_ms),
        &telemetry.d1_duration,
    );
    duration_seconds(
        &mut output,
        "peren_d1_duration_seconds",
        "D1 SQL query wall time in seconds.",
        Telemetry::get(&telemetry.d1_duration_ms),
        &telemetry.d1_duration,
    );
    counter(
        &mut output,
        "peren_cache_operations_total",
        "Cache binding operations completed through the node host.",
        Telemetry::get(&telemetry.cache_operations),
    );
    counter(
        &mut output,
        "peren_cache_errors_total",
        "Cache binding operation failures.",
        Telemetry::get(&telemetry.cache_errors),
    );
    duration(
        &mut output,
        "peren_cache_duration_ms",
        "Cache binding operation wall time in milliseconds.",
        Telemetry::get(&telemetry.cache_duration_ms),
        &telemetry.cache_duration,
    );
    duration_seconds(
        &mut output,
        "peren_cache_duration_seconds",
        "Cache binding operation wall time in seconds.",
        Telemetry::get(&telemetry.cache_duration_ms),
        &telemetry.cache_duration,
    );
    counter(
        &mut output,
        "peren_ai_runs_total",
        "AI binding calls completed through the node host.",
        Telemetry::get(&telemetry.ai_runs),
    );
    counter(
        &mut output,
        "peren_ai_errors_total",
        "AI binding call failures.",
        Telemetry::get(&telemetry.ai_errors),
    );
    duration(
        &mut output,
        "peren_ai_duration_ms",
        "AI binding call wall time in milliseconds.",
        Telemetry::get(&telemetry.ai_duration_ms),
        &telemetry.ai_duration,
    );
    duration_seconds(
        &mut output,
        "peren_ai_duration_seconds",
        "AI binding call wall time in seconds.",
        Telemetry::get(&telemetry.ai_duration_ms),
        &telemetry.ai_duration,
    );
    counter(
        &mut output,
        "peren_service_fetches_total",
        "Service binding fetches completed.",
        Telemetry::get(&telemetry.service_fetches),
    );
    counter(
        &mut output,
        "peren_service_errors_total",
        "Service binding fetch failures.",
        Telemetry::get(&telemetry.service_errors),
    );
    duration(
        &mut output,
        "peren_service_duration_ms",
        "Service binding fetch wall time in milliseconds.",
        Telemetry::get(&telemetry.service_duration_ms),
        &telemetry.service_duration,
    );
    duration_seconds(
        &mut output,
        "peren_service_duration_seconds",
        "Service binding fetch wall time in seconds.",
        Telemetry::get(&telemetry.service_duration_ms),
        &telemetry.service_duration,
    );
    counter(
        &mut output,
        "peren_outbound_fetches_total",
        "Host-gated outbound fetches completed.",
        Telemetry::get(&telemetry.outbound_fetches),
    );
    counter(
        &mut output,
        "peren_outbound_errors_total",
        "Host-gated outbound fetch failures.",
        Telemetry::get(&telemetry.outbound_errors),
    );
    duration(
        &mut output,
        "peren_outbound_duration_ms",
        "Host-gated outbound fetch wall time in milliseconds.",
        Telemetry::get(&telemetry.outbound_duration_ms),
        &telemetry.outbound_duration,
    );
    duration_seconds(
        &mut output,
        "peren_outbound_duration_seconds",
        "Host-gated outbound fetch wall time in seconds.",
        Telemetry::get(&telemetry.outbound_duration_ms),
        &telemetry.outbound_duration,
    );
    counter(
        &mut output,
        "peren_object_fetches_total",
        "Durable Object binding fetches completed.",
        Telemetry::get(&telemetry.object_fetches),
    );
    counter(
        &mut output,
        "peren_object_errors_total",
        "Durable Object binding fetch failures.",
        Telemetry::get(&telemetry.object_errors),
    );
    duration(
        &mut output,
        "peren_object_duration_ms",
        "Durable Object binding fetch wall time in milliseconds.",
        Telemetry::get(&telemetry.object_duration_ms),
        &telemetry.object_duration,
    );
    duration_seconds(
        &mut output,
        "peren_object_duration_seconds",
        "Durable Object binding fetch wall time in seconds.",
        Telemetry::get(&telemetry.object_duration_ms),
        &telemetry.object_duration,
    );
    counter(
        &mut output,
        "peren_websocket_sessions_total",
        "WebSocket upgrade sessions accepted by Workers.",
        Telemetry::get(&telemetry.websocket_sessions),
    );
    let _ = write!(
        output,
        "# HELP peren_websocket_sessions_registered WebSocket sessions tracked by the host registry.\n\
# TYPE peren_websocket_sessions_registered gauge\n\
peren_websocket_sessions_registered {websocket_active}\n"
    );
    counter(
        &mut output,
        "peren_websocket_messages_total",
        "WebSocket messages received from clients and dispatched to Workers.",
        Telemetry::get(&telemetry.websocket_messages),
    );
    counter(
        &mut output,
        "peren_websocket_closes_total",
        "WebSocket close events observed by the host bridge.",
        Telemetry::get(&telemetry.websocket_closes),
    );
    counter(
        &mut output,
        "peren_websocket_errors_total",
        "WebSocket bridge or dispatch failures.",
        Telemetry::get(&telemetry.websocket_errors),
    );
    output
}

fn counter(output: &mut String, name: &str, help: &str, value: u64) {
    let _ = write!(
        output,
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
    );
}

fn duration(output: &mut String, name: &str, help: &str, sum: u64, histogram: &DurationHistogram) {
    counter(
        output,
        &format!("{name}_total"),
        &format!("Total {help}"),
        sum,
    );
    let _ = write!(output, "# HELP {name} {help}\n# TYPE {name} histogram\n");
    let (buckets, infinite) = histogram.counts();
    for (index, bound) in DURATION_BUCKETS_MS.iter().enumerate() {
        let count = buckets[index];
        let _ = writeln!(output, "{name}_bucket{{le=\"{bound}\"}} {count}");
    }
    let _ = writeln!(output, "{name}_bucket{{le=\"+Inf\"}} {infinite}");
    let _ = write!(output, "{name}_sum {sum}\n{name}_count {infinite}\n");
}

fn duration_seconds(
    output: &mut String,
    name: &str,
    help: &str,
    sum_ms: u64,
    histogram: &DurationHistogram,
) {
    let _ = write!(output, "# HELP {name} {help}\n# TYPE {name} histogram\n");
    let (buckets, infinite) = histogram.counts();
    for (index, bound) in DURATION_BUCKETS_MS.iter().enumerate() {
        let count = buckets[index];
        let seconds = Duration::from_millis(*bound).as_secs_f64();
        let _ = writeln!(output, "{name}_bucket{{le=\"{seconds}\"}} {count}");
    }
    let sum = Duration::from_millis(sum_ms).as_secs_f64();
    let _ = writeln!(output, "{name}_bucket{{le=\"+Inf\"}} {infinite}");
    let _ = write!(output, "{name}_sum {sum}\n{name}_count {infinite}\n");
}

fn label(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::Ordering};

    use super::{Metrics, Telemetry, render};

    #[test]
    fn prometheus_output_includes_first_class_provider_counters() {
        let telemetry = Arc::new(Telemetry::default());
        telemetry.kv_operations.store(2, Ordering::Relaxed);
        telemetry.kv_errors.store(1, Ordering::Relaxed);
        telemetry.kv_duration_ms.store(7, Ordering::Relaxed);
        telemetry.kv_duration.observe(7);
        telemetry.d1_queries.store(5, Ordering::Relaxed);
        telemetry.d1_errors.store(1, Ordering::Relaxed);
        telemetry.d1_duration_ms.store(13, Ordering::Relaxed);
        telemetry.d1_duration.observe(13);
        telemetry.outbound_fetches.store(3, Ordering::Relaxed);
        telemetry.outbound_errors.store(1, Ordering::Relaxed);
        telemetry.outbound_duration_ms.store(11, Ordering::Relaxed);
        telemetry.outbound_duration.observe(11);
        telemetry.worker_dispatches.store(4, Ordering::Relaxed);
        telemetry
            .worker_dispatch_duration_ms
            .store(21, Ordering::Relaxed);
        telemetry.worker_dispatch_duration.observe(21);
        telemetry.cell_restores.store(4, Ordering::Relaxed);
        telemetry.cell_dispatches.store(4, Ordering::Relaxed);
        telemetry.cell_commits.store(3, Ordering::Relaxed);
        telemetry.cell_releases.store(4, Ordering::Relaxed);
        telemetry.storage_commit_errors.store(1, Ordering::Relaxed);
        telemetry.websocket_messages.store(6, Ordering::Relaxed);
        telemetry.websocket_closes.store(2, Ordering::Relaxed);
        telemetry.websocket_errors.store(1, Ordering::Relaxed);
        let metrics = Metrics {
            listener: Arc::from("public"),
            listeners: 1,
            services: 1,
            consumers: 0,
            worker: true,
            started_at_ms: 1_000,
            telemetry,
        };

        let output = render(true, &metrics, None, 0, 100);

        assert!(output.contains("peren_kv_operations_total 2"));
        assert!(output.contains("peren_kv_errors_total 1"));
        assert!(output.contains("peren_kv_duration_ms_total 7"));
        assert!(output.contains("peren_kv_duration_ms_bucket{le=\"10\"} 1"));
        assert!(output.contains("peren_kv_duration_ms_count 1"));
        assert!(output.contains("peren_d1_queries_total 5"));
        assert!(output.contains("peren_d1_errors_total 1"));
        assert!(output.contains("peren_d1_duration_ms_total 13"));
        assert!(output.contains("peren_outbound_fetches_total 3"));
        assert!(output.contains("peren_outbound_errors_total 1"));
        assert!(output.contains("peren_outbound_duration_ms_total 11"));
        assert!(output.contains("peren_placement_weight 100"));
        assert!(output.contains("peren_worker_dispatches_total 4"));
        assert!(output.contains("peren_worker_dispatch_duration_seconds_bucket{le=\"0.025\"} 1"));
        assert!(output.contains("peren_worker_dispatch_duration_seconds_count 1"));
        assert!(output.contains("peren_cell_restores_total 4"));
        assert!(output.contains("peren_cell_dispatches_total 4"));
        assert!(output.contains("peren_cell_commits_total 3"));
        assert!(output.contains("peren_cell_releases_total 4"));
        assert!(output.contains("peren_storage_commit_errors_total 1"));
        assert!(output.contains("peren_websocket_messages_total 6"));
        assert!(output.contains("peren_websocket_closes_total 2"));
        assert!(output.contains("peren_websocket_errors_total 1"));
    }
}
