use std::{cell::RefCell, rc::Rc};

use deno_core::{OpState, op2};

use crate::wire::{WorkerLogEvent, WorkerLogLevel};

#[derive(Default)]
pub(crate) struct ConsoleEvents {
    events: Vec<WorkerLogEvent>,
}

impl ConsoleEvents {
    pub(crate) fn clear(&mut self) {
        self.events.clear();
    }

    pub(crate) fn take(&mut self) -> Vec<WorkerLogEvent> {
        std::mem::take(&mut self.events)
    }
}

#[op2(fast)]
#[allow(
    clippy::needless_pass_by_value,
    reason = "deno_core::op2 requires OpState to use the Rc<RefCell<OpState>> ABI"
)]
pub fn op_console_log(
    state: Rc<RefCell<OpState>>,
    #[string] level: &str,
    #[string] message: String,
) {
    let level = match level {
        "debug" => WorkerLogLevel::Debug,
        "warn" => WorkerLogLevel::Warn,
        "error" => WorkerLogLevel::Error,
        _ => WorkerLogLevel::Info,
    };
    let timestamp_ms = chrono::Utc::now().timestamp_millis();
    state
        .borrow_mut()
        .borrow_mut::<ConsoleEvents>()
        .events
        .push(WorkerLogEvent {
            level,
            message,
            timestamp_ms,
        });
}
