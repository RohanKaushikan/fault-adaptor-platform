use std::time::{Duration, SystemTime};

use super::{Lease, PoolId, TenantId};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ActivityType(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkflowId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryMetadata {
    pub retry_count: u32,
    pub max_retries: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttemptMetadata {
    pub attempts_started: u32,
    pub last_attempt_at: Option<SystemTime>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskLimits {
    pub max_execution_time: Duration,
    pub deadline: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskStatus {
    Pending {
        expires_at: SystemTime,
    },
    Leased(Lease),
    Completed {
        completed_at: SystemTime,
    },
    Failed {
        failed_at: SystemTime,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Task {
    pub id: TaskId,
    pub tenant_id: TenantId,
    pub pool_id: PoolId,
    pub activity_type: ActivityType,
    pub input: Vec<u8>,
    pub workflow_id: Option<WorkflowId>,
    pub priority: i32,
    pub created_at: SystemTime,
    pub limits: TaskLimits,
    pub retry: RetryMetadata,
    pub attempt: AttemptMetadata,
    pub status: TaskStatus,
}
