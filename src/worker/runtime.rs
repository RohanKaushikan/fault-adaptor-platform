use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::{interval_at, sleep, Instant};

use crate::domain::{Lease, PoolId, TaskStatus};
use crate::transport::{
    LeaseRenewal, OrchestratorClient, OrchestratorClientError, TaskAssignment, TaskOutcome,
    TaskResult,
};

use super::{ActivityError, ActivityRegistry};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkerId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerConfig {
    pub max_concurrency: usize,
    pub idle_poll_interval: Duration,
    pub lease_renewal_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 1,
            idle_poll_interval: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_secs(10),
        }
    }
}

#[derive(Clone)]
pub struct Worker {
    pub id: WorkerId,
    pub pool: PoolId,
    pub activities: ActivityRegistry,
    pub max_concurrency: usize,
    orchestrator: Arc<dyn OrchestratorClient>,
    idle_poll_interval: Duration,
    lease_renewal_interval: Duration,
}

impl Worker {
    pub fn new(
        id: WorkerId,
        pool: PoolId,
        activities: ActivityRegistry,
        orchestrator: Arc<dyn OrchestratorClient>,
        config: WorkerConfig,
    ) -> Result<Self, WorkerError> {
        if config.max_concurrency == 0 {
            return Err(WorkerError::InvalidConfiguration(
                "max_concurrency must be greater than zero".into(),
            ));
        }

        if config.idle_poll_interval.is_zero() || config.lease_renewal_interval.is_zero() {
            return Err(WorkerError::InvalidConfiguration(
                "poll and lease renewal intervals must be greater than zero".into(),
            ));
        }

        Ok(Self {
            id,
            pool,
            activities,
            max_concurrency: config.max_concurrency,
            orchestrator,
            idle_poll_interval: config.idle_poll_interval,
            lease_renewal_interval: config.lease_renewal_interval,
        })
    }

    pub async fn run(&self) -> Result<(), WorkerError> {
        let mut slots = JoinSet::new();

        for _ in 0..self.max_concurrency {
            let worker = self.clone();
            slots.spawn(async move { worker.run_slot().await });
        }

        match slots.join_next().await {
            Some(Ok(result)) => result,
            Some(Err(error)) => Err(WorkerError::WorkerLoop(error.to_string())),
            None => Ok(()),
        }
    }

    pub async fn run_once(&self) -> Result<bool, WorkerError> {
        let assignment = self
            .orchestrator
            .request_task(&self.id, &self.pool)
            .await
            .map_err(WorkerError::Orchestrator)?;

        let Some(assignment) = assignment else {
            return Ok(false);
        };

        self.execute_assignment(assignment).await?;
        Ok(true)
    }

    async fn run_slot(&self) -> Result<(), WorkerError> {
        loop {
            if !self.run_once().await? {
                sleep(self.idle_poll_interval).await;
            }
        }
    }

    async fn execute_assignment(&self, assignment: TaskAssignment) -> Result<(), WorkerError> {
        let lease = match &assignment.task.status {
            TaskStatus::Leased(lease) => lease.clone(),
            _ => {
                return Err(WorkerError::InvalidAssignment(
                    "orchestrator returned a task that was not leased".into(),
                ));
            }
        };

        if lease.owner_id.0 != self.id.0 {
            return Err(WorkerError::InvalidAssignment(
                "task lease belongs to another worker".into(),
            ));
        }

        if assignment.task.pool_id != self.pool {
            return Err(WorkerError::InvalidAssignment(
                "task belongs to another pool".into(),
            ));
        }

        let attempt = assignment.task.attempt.attempts_started;
        let outcome = match self.activities.get(&assignment.task.activity_type) {
            Some(activity) => {
                match self
                    .execute_with_renewal(&assignment, &lease, activity)
                    .await?
                {
                    Ok(output) => TaskOutcome::Succeeded(output),
                    Err(error) => TaskOutcome::Failed {
                        message: error.message,
                    },
                }
            }
            None => TaskOutcome::Failed {
                message: format!(
                    "activity type is not registered: {}",
                    assignment.task.activity_type.0
                ),
            },
        };

        self.orchestrator
            .report_result(TaskResult {
                task_id: assignment.task.id,
                attempt,
                ownership_epoch: lease.ownership_epoch,
                outcome,
            })
            .await
            .map_err(WorkerError::Orchestrator)
    }

    async fn execute_with_renewal(
        &self,
        assignment: &TaskAssignment,
        lease: &Lease,
        activity: Arc<dyn super::Activity>,
    ) -> Result<Result<Vec<u8>, ActivityError>, WorkerError> {
        let execution = activity.execute(&assignment.task.input);
        tokio::pin!(execution);

        let next_renewal = Instant::now() + self.lease_renewal_interval;
        let mut renewal_timer = interval_at(next_renewal, self.lease_renewal_interval);

        loop {
            tokio::select! {
                result = &mut execution => return Ok(result),
                _ = renewal_timer.tick() => {
                    self.orchestrator
                        .renew_lease(LeaseRenewal {
                            task_id: assignment.task.id.clone(),
                            worker_id: self.id.clone(),
                            attempt: assignment.task.attempt.attempts_started,
                            ownership_epoch: lease.ownership_epoch,
                        })
                        .await
                        .map_err(WorkerError::Orchestrator)?;
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum WorkerError {
    InvalidConfiguration(String),
    InvalidAssignment(String),
    Orchestrator(OrchestratorClientError),
    WorkerLoop(String),
}

impl fmt::Display for WorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message)
            | Self::InvalidAssignment(message)
            | Self::WorkerLoop(message) => formatter.write_str(message),
            Self::Orchestrator(error) => write!(formatter, "orchestrator request failed: {error}"),
        }
    }
}

impl std::error::Error for WorkerError {}
