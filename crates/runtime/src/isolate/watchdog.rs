use super::EngineError;
use deno_core::JsRuntime;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;

pub(crate) struct Watchdog {
    state: Arc<(Mutex<bool>, Condvar)>,
    expired: Arc<AtomicBool>,
    thread: JoinHandle<()>,
}

impl Watchdog {
    pub(crate) fn start(runtime: &mut JsRuntime, limit: std::time::Duration) -> Self {
        let state = Arc::new((Mutex::new(false), Condvar::new()));
        let expired = Arc::new(AtomicBool::new(false));
        let handle = runtime.v8_isolate().thread_safe_handle();
        let thread_state = Arc::clone(&state);
        let thread_expired = Arc::clone(&expired);
        let thread = std::thread::spawn(move || {
            let (lock, wake) = &*thread_state;
            let finished = lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (finished, result) = wake
                .wait_timeout_while(finished, limit, |finished| !*finished)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !*finished && result.timed_out() {
                thread_expired.store(true, Ordering::Release);
                handle.terminate_execution();
            }
        });
        Self {
            state,
            expired,
            thread,
        }
    }

    pub(crate) fn finish(
        self,
        runtime: &mut JsRuntime,
        heap_exceeded: &AtomicBool,
    ) -> Result<(), EngineError> {
        let (lock, wake) = &*self.state;
        *lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        wake.notify_one();
        self.thread.join().map_err(|_| EngineError::Watchdog)?;

        if heap_exceeded.swap(false, Ordering::AcqRel) {
            runtime.v8_isolate().cancel_terminate_execution();
            return Err(EngineError::HeapLimit);
        }
        if self.expired.load(Ordering::Acquire) {
            runtime.v8_isolate().cancel_terminate_execution();
            return Err(EngineError::ExecutionTime);
        }
        Ok(())
    }
}
