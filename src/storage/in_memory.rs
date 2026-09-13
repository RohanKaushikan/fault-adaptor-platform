use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::SystemTime;

use crate::domain::{Lease, LeaseOwnerId, PoolId, Task, TaskId, TaskStatus};
use crate::worker::WorkerId;

pub struct InMemoryTaskStore {
    tasks: Mutex<HashMap<TaskId, Task>>,
}

impl InMemoryTaskStore {
    pub fn new() -> Self {
        Self {
            tasks: Mutex::new(HashMap::new()),
        }
    }

    pub fn submit(&self, task: Task) -> Result<(), TaskStoreError> {
        if !matches!(task.status, TaskStatus::Pending { .. }) {
            return Err(TaskStoreError::InvalidTask(
                "submitted tasks must be pending".into(),
            ));
        }

        if task.limits.max_execution_time.is_zero() {
            return Err(TaskStoreError::InvalidTask(
                "max execution time must be greater than zero".into(),
            ));
        }

        let mut tasks = self.tasks.lock().map_err(|_| TaskStoreError::Unavailable)?;
        if tasks.contains_key(&task.id) {
            return Err(TaskStoreError::DuplicateTask(task.id));
        }

        tasks.insert(task.id.clone(), task);
        Ok(())
    }

    pub fn get(&self, task_id: &TaskId) -> Option<Task> {
        self.tasks.lock().ok()?.get(task_id).cloned()
    }

    pub fn lease_next(
        &self,
        pool_id: &PoolId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
        now: SystemTime,
    ) -> Result<Option<Task>, TaskStoreError> {
        let mut tasks = self.tasks.lock().map_err(|_| TaskStoreError::Unavailable)?;
        Self::recover_expired_locked(&mut tasks, now);

        let task_id = tasks
            .values()
            .filter(|task| Self::is_available(task, pool_id, now))
            .min_by(|left, right| {
                right
                    .priority
                    .cmp(&left.priority)
                    .then_with(|| left.created_at.cmp(&right.created_at))
                    .then_with(|| left.id.cmp(&right.id))
            })
            .map(|task| task.id.clone());

        let Some(task_id) = task_id else {
            return Ok(None);
        };

        let task = tasks
            .get_mut(&task_id)
            .expect("selected task must still be present");
        task.attempt.attempts_started = task.attempt.attempts_started.saturating_add(1);
        task.attempt.last_attempt_at = Some(now);
        task.status = TaskStatus::Leased(Lease {
            owner_id: LeaseOwnerId(worker_id.0.clone()),
            ownership_epoch,
            leased_at: now,
            expires_at: Self::lease_expiry(task, now),
        });

        Ok(Some(task.clone()))
    }

    pub fn renew(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
        now: SystemTime,
    ) -> Result<Task, TaskStoreError> {
        let mut tasks = self.tasks.lock().map_err(|_| TaskStoreError::Unavailable)?;
        let task = Self::task_mut(&mut tasks, task_id)?;
        let lease = Self::active_lease(task, worker_id, ownership_epoch, now)?;

        task.status = TaskStatus::Leased(Lease {
            expires_at: Self::lease_expiry(task, now),
            ..lease
        });
        Ok(task.clone())
    }

    pub fn complete(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
        now: SystemTime,
    ) -> Result<Task, TaskStoreError> {
        let mut tasks = self.tasks.lock().map_err(|_| TaskStoreError::Unavailable)?;
        let task = Self::task_mut(&mut tasks, task_id)?;
        Self::active_lease(task, worker_id, ownership_epoch, now)?;
        task.status = TaskStatus::Completed { completed_at: now };
        Ok(task.clone())
    }

    pub fn retry(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
        now: SystemTime,
        message: impl Into<String>,
    ) -> Result<Task, TaskStoreError> {
        let mut tasks = self.tasks.lock().map_err(|_| TaskStoreError::Unavailable)?;
        let task = Self::task_mut(&mut tasks, task_id)?;
        Self::active_lease(task, worker_id, ownership_epoch, now)?;
        Self::retry_or_fail(task, now, message.into());
        Ok(task.clone())
    }

    pub fn fail(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
        now: SystemTime,
        message: impl Into<String>,
    ) -> Result<Task, TaskStoreError> {
        let mut tasks = self.tasks.lock().map_err(|_| TaskStoreError::Unavailable)?;
        let task = Self::task_mut(&mut tasks, task_id)?;
        Self::active_lease(task, worker_id, ownership_epoch, now)?;
        task.status = TaskStatus::Failed {
            failed_at: now,
            message: message.into(),
        };
        Ok(task.clone())
    }

    pub fn recover_expired(&self, now: SystemTime) -> Result<Vec<Task>, TaskStoreError> {
        let mut tasks = self.tasks.lock().map_err(|_| TaskStoreError::Unavailable)?;
        Ok(Self::recover_expired_locked(&mut tasks, now))
    }

    fn task_mut<'a>(
        tasks: &'a mut HashMap<TaskId, Task>,
        task_id: &TaskId,
    ) -> Result<&'a mut Task, TaskStoreError> {
        tasks
            .get_mut(task_id)
            .ok_or_else(|| TaskStoreError::TaskNotFound(task_id.clone()))
    }

    fn active_lease(
        task: &Task,
        worker_id: &WorkerId,
        ownership_epoch: u64,
        now: SystemTime,
    ) -> Result<Lease, TaskStoreError> {
        let TaskStatus::Leased(lease) = &task.status else {
            return Err(TaskStoreError::InvalidLease("task is not leased".into()));
        };

        if lease.owner_id.0 != worker_id.0 || lease.ownership_epoch != ownership_epoch {
            return Err(TaskStoreError::InvalidLease(
                "lease belongs to another worker or epoch".into(),
            ));
        }

        if now >= lease.expires_at || now >= task.limits.deadline {
            return Err(TaskStoreError::LeaseExpired);
        }

        Ok(lease.clone())
    }

    fn is_available(task: &Task, pool_id: &PoolId, now: SystemTime) -> bool {
        task.pool_id == *pool_id
            && now < task.limits.deadline
            && matches!(task.status, TaskStatus::Pending { expires_at } if now < expires_at)
    }

    fn lease_expiry(task: &Task, now: SystemTime) -> SystemTime {
        now.checked_add(task.limits.max_execution_time)
            .unwrap_or(task.limits.deadline)
            .min(task.limits.deadline)
    }

    fn recover_expired_locked(tasks: &mut HashMap<TaskId, Task>, now: SystemTime) -> Vec<Task> {
        let task_ids: Vec<TaskId> = tasks.keys().cloned().collect();
        let mut changed = Vec::new();

        for task_id in task_ids {
            let task = tasks
                .get_mut(&task_id)
                .expect("task ID came from the task store");
            let expired = match &task.status {
                TaskStatus::Pending { expires_at } => {
                    now >= *expires_at || now >= task.limits.deadline
                }
                TaskStatus::Leased(lease) => now >= lease.expires_at || now >= task.limits.deadline,
                TaskStatus::Completed { .. } | TaskStatus::Failed { .. } => false,
            };

            if !expired {
                continue;
            }

            match task.status {
                TaskStatus::Leased(_) => {
                    Self::retry_or_fail(task, now, "lease expired".into());
                }
                TaskStatus::Pending { .. } => {
                    task.status = TaskStatus::Failed {
                        failed_at: now,
                        message: "task expired before it was leased".into(),
                    };
                }
                TaskStatus::Completed { .. } | TaskStatus::Failed { .. } => continue,
            }
            changed.push(task.clone());
        }

        changed
    }

    fn retry_or_fail(task: &mut Task, now: SystemTime, message: String) {
        if now >= task.limits.deadline || task.retry.retry_count >= task.retry.max_retries {
            task.status = TaskStatus::Failed {
                failed_at: now,
                message,
            };
            return;
        }

        task.retry.retry_count = task.retry.retry_count.saturating_add(1);
        task.status = TaskStatus::Pending {
            expires_at: task.limits.deadline,
        };
    }
}

impl Default for InMemoryTaskStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskStoreError {
    DuplicateTask(TaskId),
    TaskNotFound(TaskId),
    InvalidTask(String),
    InvalidLease(String),
    LeaseExpired,
    Unavailable,
}

impl fmt::Display for TaskStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateTask(task_id) => write!(formatter, "task already exists: {}", task_id.0),
            Self::TaskNotFound(task_id) => write!(formatter, "task not found: {}", task_id.0),
            Self::InvalidTask(message) | Self::InvalidLease(message) => {
                formatter.write_str(message)
            }
            Self::LeaseExpired => formatter.write_str("lease has expired"),
            Self::Unavailable => formatter.write_str("task store is unavailable"),
        }
    }
}

impl std::error::Error for TaskStoreError {}
