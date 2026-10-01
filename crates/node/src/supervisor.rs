use std::{future::Future, sync::Arc, time::Duration};

use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Phase {
    Building,
    Ready,
    Draining,
    Stopped,
    Failed,
}

#[derive(Clone)]
pub struct Shutdown {
    receiver: watch::Receiver<bool>,
}

impl Shutdown {
    #[must_use]
    pub fn requested(&self) -> bool {
        *self.receiver.borrow()
    }

    pub async fn wait(&mut self) {
        while !*self.receiver.borrow_and_update() {
            if self.receiver.changed().await.is_err() {
                break;
            }
        }
    }
}

pub struct Supervisor {
    phase: Phase,
    shutdown: watch::Sender<bool>,
    events: mpsc::UnboundedReceiver<TaskEvent>,
    sender: mpsc::UnboundedSender<TaskEvent>,
    tasks: Vec<Task>,
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervisor {
    #[must_use]
    pub fn new() -> Self {
        let (shutdown, _) = watch::channel(false);
        let (sender, events) = mpsc::unbounded_channel();
        Self {
            phase: Phase::Building,
            shutdown,
            events,
            sender,
            tasks: Vec::new(),
        }
    }

    pub fn spawn<F, Fut>(&mut self, name: impl Into<Arc<str>>, required: bool, task: F)
    where
        F: FnOnce(Shutdown) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        let name = name.into();
        let event_name = name.clone();
        let token = Shutdown {
            receiver: self.shutdown.subscribe(),
        };
        let sender = self.sender.clone();
        let handle = tokio::spawn(async move {
            let result = task(token).await;
            let _ = sender.send(TaskEvent {
                name: event_name,
                required,
                failed: result.is_err(),
            });
            result
        });
        self.tasks.push(Task { name, handle });
    }

    pub fn ready(&mut self) -> Result<(), SupervisorError> {
        if self.phase != Phase::Building {
            return Err(SupervisorError::Transition {
                from: self.phase,
                to: Phase::Ready,
            });
        }
        self.check()?;
        self.phase = Phase::Ready;
        Ok(())
    }

    pub fn check(&mut self) -> Result<(), SupervisorError> {
        while let Ok(event) = self.events.try_recv() {
            if event.required {
                self.phase = Phase::Failed;
                return Err(SupervisorError::TaskStopped {
                    name: event.name,
                    failed: event.failed,
                });
            }
        }
        Ok(())
    }

    #[must_use]
    pub const fn phase(&self) -> Phase {
        self.phase
    }

    #[must_use]
    pub const fn live(&self) -> bool {
        !matches!(self.phase, Phase::Stopped | Phase::Failed)
    }

    pub fn accepting(&mut self) -> bool {
        self.check().is_ok() && self.phase == Phase::Ready
    }

    pub async fn shutdown(mut self, deadline: Duration) -> Result<(), SupervisorError> {
        if matches!(self.phase, Phase::Stopped) {
            return Ok(());
        }
        self.phase = Phase::Draining;
        self.shutdown.send_replace(true);
        let stop = async {
            for task in self.tasks {
                match task.handle.await {
                    Ok(Ok(())) => {}
                    Ok(Err(source)) => {
                        return Err(SupervisorError::Task {
                            name: task.name,
                            source,
                        });
                    }
                    Err(source) => {
                        return Err(SupervisorError::Join {
                            name: task.name,
                            source,
                        });
                    }
                }
            }
            Ok(())
        };
        match tokio::time::timeout(deadline, stop).await {
            Ok(result) => result,
            Err(_) => Err(SupervisorError::Deadline),
        }
    }
}

struct Task {
    name: Arc<str>,
    handle: JoinHandle<Result<(), TaskError>>,
}

struct TaskEvent {
    name: Arc<str>,
    required: bool,
    failed: bool,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct TaskError {
    message: Arc<str>,
}

impl TaskError {
    #[must_use]
    pub fn new(message: impl Into<Arc<str>>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("invalid supervisor transition from {from:?} to {to:?}")]
    Transition { from: Phase, to: Phase },
    #[error("required task {name:?} stopped (failed: {failed})")]
    TaskStopped { name: Arc<str>, failed: bool },
    #[error("task {name:?} failed")]
    Task {
        name: Arc<str>,
        #[source]
        source: TaskError,
    },
    #[error("task {name:?} could not be joined")]
    Join {
        name: Arc<str>,
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("shutdown deadline elapsed")]
    Deadline,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn readiness_ends_before_tasks_are_stopped() {
        let mut supervisor = Supervisor::new();
        supervisor.spawn("listener", true, |mut shutdown| async move {
            shutdown.wait().await;
            Ok(())
        });
        supervisor.ready().unwrap();
        assert!(supervisor.accepting());
        supervisor.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn an_early_required_task_failure_removes_readiness() {
        let mut supervisor = Supervisor::new();
        supervisor.spawn("listener", true, |_| async {
            Err(TaskError::new("bind failed"))
        });
        tokio::task::yield_now().await;
        assert!(!supervisor.accepting());
        assert_eq!(supervisor.phase(), Phase::Failed);
    }

    #[tokio::test]
    async fn shutdown_has_a_real_deadline() {
        let mut supervisor = Supervisor::new();
        supervisor.spawn("stuck", true, |_| async {
            std::future::pending::<()>().await;
            Ok(())
        });
        supervisor.ready().unwrap();
        let error = supervisor
            .shutdown(Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(matches!(error, SupervisorError::Deadline));
    }
}
