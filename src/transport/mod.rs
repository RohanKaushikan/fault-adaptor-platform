use std::fmt;

use async_trait::async_trait;

use crate::domain::{PoolId, Task, TaskId};
use crate::worker::WorkerId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskAssignment {
    pub task: Task,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRenewal {
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub attempt: u32,
    pub ownership_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskOutcome {
    Succeeded(Vec<u8>),
    Failed { message: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskResult {
    pub task_id: TaskId,
    pub attempt: u32,
    pub ownership_epoch: u64,
    pub outcome: TaskOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrchestratorClientError {
    pub message: String,
}

impl OrchestratorClientError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for OrchestratorClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for OrchestratorClientError {}

#[async_trait]
pub trait OrchestratorClient: Send + Sync {
    async fn request_task(
        &self,
        worker_id: &WorkerId,
        pool_id: &PoolId,
    ) -> Result<Option<TaskAssignment>, OrchestratorClientError>;

    async fn renew_lease(&self, renewal: LeaseRenewal) -> Result<(), OrchestratorClientError>;

    async fn report_result(&self, result: TaskResult) -> Result<(), OrchestratorClientError>;
}
