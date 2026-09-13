use std::fmt;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use tokio_postgres::{Client, NoTls, Transaction};
use uuid::Uuid;

use crate::domain::{
    ActivityType, AttemptMetadata, Lease, LeaseOwnerId, PoolId, RetryMetadata, Task, TaskId,
    TaskLimits, TaskStatus, TenantId, WorkflowId,
};
use crate::worker::WorkerId;

pub struct PostgresTaskStore {
    database_url: String,
}

impl PostgresTaskStore {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
        }
    }

    pub async fn connect(database_url: impl Into<String>) -> Result<Self, PostgresTaskStoreError> {
        let store = Self::new(database_url);
        let client = store.open().await?;
        client.simple_query("SELECT 1").await?;
        Ok(store)
    }

    pub async fn submit(&self, task: &Task) -> Result<(), PostgresTaskStoreError> {
        let TaskStatus::Pending { .. } = task.status else {
            return Err(PostgresTaskStoreError::InvalidTask(
                "submitted tasks must be pending".into(),
            ));
        };
        let workflow_id = task.workflow_id.as_ref().ok_or_else(|| {
            PostgresTaskStoreError::InvalidTask("PostgreSQL tasks require a workflow ID".into())
        })?;
        let execution_timeout_ms = i64::try_from(task.limits.max_execution_time.as_millis())
            .map_err(|_| {
                PostgresTaskStoreError::InvalidTask("execution timeout is too large".into())
            })?;
        if execution_timeout_ms == 0 {
            return Err(PostgresTaskStoreError::InvalidTask(
                "max execution time must be greater than zero".into(),
            ));
        }

        let retry_policy = retry_policy(task.retry.max_retries, task.retry.retry_count);
        let client = self.open().await?;
        client
            .execute(
                "INSERT INTO platform.tasks (
                    id, workflow_id, activity_type, input, state, pool_id, priority,
                    available_at, execution_timeout_ms, retry_policy, current_attempt,
                    created_at, deadline
                ) VALUES ($1, $2, $3, $4, 'ready', $5, $6, $7, $8, $9, $10, $11, $12)",
                &[
                    &task.id.0,
                    &workflow_id.0,
                    &task.activity_type.0,
                    &task.input,
                    &task.pool_id.0,
                    &task.priority,
                    &db_time(task.created_at),
                    &execution_timeout_ms,
                    &retry_policy,
                    &(task.attempt.attempts_started as i32),
                    &db_time(task.created_at),
                    &db_time(task.limits.deadline),
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn lease_next(
        &self,
        pool_id: &PoolId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
    ) -> Result<Option<Task>, PostgresTaskStoreError> {
        let mut client = self.open().await?;
        let transaction = client.transaction().await?;
        let row = transaction
            .query_opt(
                "SELECT
                    tasks.id, tasks.workflow_id, workflows.tenant_id, tasks.activity_type,
                    tasks.input, tasks.pool_id, tasks.priority, tasks.created_at,
                    tasks.deadline, tasks.execution_timeout_ms, tasks.retry_policy,
                    tasks.current_attempt
                 FROM platform.tasks AS tasks
                 JOIN platform.workflows AS workflows ON workflows.id = tasks.workflow_id
                 WHERE tasks.pool_id = $1
                   AND tasks.state = 'ready'
                   AND tasks.available_at <= NOW()
                   AND tasks.deadline > NOW()
                 ORDER BY tasks.priority DESC, tasks.created_at ASC, tasks.id ASC
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1",
                &[&pool_id.0],
            )
            .await?;

        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };

        let now = SystemTime::now();
        let deadline = system_time(row.get::<_, DateTime<Utc>>("deadline"));
        let execution_timeout_ms: i64 = row.get("execution_timeout_ms");
        let lease_expiry = capped_expiry(now, execution_timeout_ms, deadline)?;
        let current_attempt: i32 = row.get("current_attempt");
        let attempt_number = current_attempt.checked_add(1).ok_or_else(|| {
            PostgresTaskStoreError::InvalidTask("attempt count overflowed".into())
        })?;

        transaction
            .execute(
                "UPDATE platform.tasks
                 SET state = 'leased', current_attempt = $2
                 WHERE id = $1",
                &[&row.get::<_, String>("id"), &attempt_number],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO platform.task_attempts (
                    id, task_id, attempt_number, worker_id, ownership_epoch,
                    lease_expiry, started_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    &Uuid::new_v4().to_string(),
                    &row.get::<_, String>("id"),
                    &attempt_number,
                    &worker_id.0,
                    &(ownership_epoch as i64),
                    &db_time(lease_expiry),
                    &db_time(now),
                ],
            )
            .await?;
        write_event(
            &transaction,
            &row.get::<_, String>("id"),
            "leased",
            now,
            json!({ "worker_id": worker_id.0, "ownership_epoch": ownership_epoch }),
        )
        .await?;

        let task = task_from_row(
            &row,
            TaskStatus::Leased(Lease {
                owner_id: LeaseOwnerId(worker_id.0.clone()),
                ownership_epoch,
                leased_at: now,
                expires_at: lease_expiry,
            }),
            attempt_number as u32,
        )?;
        transaction.commit().await?;
        Ok(Some(task))
    }

    pub async fn renew(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
    ) -> Result<(), PostgresTaskStoreError> {
        let mut client = self.open().await?;
        let transaction = client.transaction().await?;
        let active = active_attempt(&transaction, task_id, worker_id, ownership_epoch).await?;
        let now = SystemTime::now();
        ensure_active(&active, now)?;
        let lease_expiry = capped_expiry(now, active.execution_timeout_ms, active.deadline)?;

        transaction
            .execute(
                "UPDATE platform.task_attempts
                 SET lease_expiry = $4
                 WHERE task_id = $1
                   AND worker_id = $2
                   AND ownership_epoch = $3
                   AND finished_at IS NULL",
                &[
                    &task_id.0,
                    &worker_id.0,
                    &(ownership_epoch as i64),
                    &db_time(lease_expiry),
                ],
            )
            .await?;
        write_event(
            &transaction,
            &task_id.0,
            "lease_renewed",
            now,
            json!({ "ownership_epoch": ownership_epoch }),
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn complete(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
    ) -> Result<(), PostgresTaskStoreError> {
        self.finish(task_id, worker_id, ownership_epoch, "completed", None)
            .await
    }

    pub async fn fail(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
        message: impl Into<String>,
    ) -> Result<(), PostgresTaskStoreError> {
        self.finish(
            task_id,
            worker_id,
            ownership_epoch,
            "failed",
            Some(message.into()),
        )
        .await
    }

    pub async fn retry(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
        message: impl Into<String>,
    ) -> Result<(), PostgresTaskStoreError> {
        let mut client = self.open().await?;
        let transaction = client.transaction().await?;
        let active = active_attempt(&transaction, task_id, worker_id, ownership_epoch).await?;
        let now = SystemTime::now();
        ensure_active(&active, now)?;
        transition_to_retry_or_failure(&transaction, task_id, &active, now, message.into()).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn recover_expired(&self) -> Result<usize, PostgresTaskStoreError> {
        let mut client = self.open().await?;
        let transaction = client.transaction().await?;
        let now = SystemTime::now();
        let rows = transaction
            .query(
                "SELECT tasks.id, tasks.deadline, tasks.execution_timeout_ms, tasks.retry_policy,
                        attempts.lease_expiry
                 FROM platform.tasks AS tasks
                 JOIN platform.task_attempts AS attempts ON attempts.task_id = tasks.id
                 WHERE tasks.state = 'leased'
                   AND attempts.finished_at IS NULL
                   AND (attempts.lease_expiry <= $1 OR tasks.deadline <= $1)
                 FOR UPDATE SKIP LOCKED",
                &[&db_time(now)],
            )
            .await?;

        for row in &rows {
            let task_id = TaskId(row.get::<_, String>("id"));
            let active = ActiveAttempt {
                deadline: system_time(row.get::<_, DateTime<Utc>>("deadline")),
                lease_expiry: system_time(row.get::<_, DateTime<Utc>>("lease_expiry")),
                execution_timeout_ms: row.get("execution_timeout_ms"),
                retry_policy: row.get("retry_policy"),
            };
            transition_to_retry_or_failure(
                &transaction,
                &task_id,
                &active,
                now,
                "lease expired".into(),
            )
            .await?;
        }

        let pending_failed = transaction
            .execute(
                "UPDATE platform.tasks
                 SET state = 'failed'
                 WHERE state = 'ready' AND deadline <= $1",
                &[&db_time(now)],
            )
            .await?;
        transaction.commit().await?;
        Ok(rows.len() + pending_failed as usize)
    }

    async fn finish(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        ownership_epoch: u64,
        state: &str,
        error: Option<String>,
    ) -> Result<(), PostgresTaskStoreError> {
        let mut client = self.open().await?;
        let transaction = client.transaction().await?;
        let active = active_attempt(&transaction, task_id, worker_id, ownership_epoch).await?;
        let now = SystemTime::now();
        ensure_active(&active, now)?;
        transaction
            .execute(
                "UPDATE platform.tasks SET state = $2 WHERE id = $1",
                &[&task_id.0, &state],
            )
            .await?;
        transaction
            .execute(
                "UPDATE platform.task_attempts
                 SET finished_at = $4, outcome = $5, error = $6
                 WHERE task_id = $1
                   AND worker_id = $2
                   AND ownership_epoch = $3
                   AND finished_at IS NULL",
                &[
                    &task_id.0,
                    &worker_id.0,
                    &(ownership_epoch as i64),
                    &db_time(now),
                    &state,
                    &error,
                ],
            )
            .await?;
        write_event(
            &transaction,
            &task_id.0,
            state,
            now,
            json!({ "error": error }),
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn open(&self) -> Result<Client, PostgresTaskStoreError> {
        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }
}

struct ActiveAttempt {
    deadline: SystemTime,
    lease_expiry: SystemTime,
    execution_timeout_ms: i64,
    retry_policy: Value,
}

async fn active_attempt(
    transaction: &Transaction<'_>,
    task_id: &TaskId,
    worker_id: &WorkerId,
    ownership_epoch: u64,
) -> Result<ActiveAttempt, PostgresTaskStoreError> {
    let row = transaction
        .query_opt(
            "SELECT tasks.deadline, tasks.execution_timeout_ms, tasks.retry_policy,
                    attempts.lease_expiry
             FROM platform.tasks AS tasks
             JOIN platform.task_attempts AS attempts ON attempts.task_id = tasks.id
             WHERE tasks.id = $1
               AND tasks.state = 'leased'
               AND attempts.worker_id = $2
               AND attempts.ownership_epoch = $3
               AND attempts.finished_at IS NULL
             FOR UPDATE",
            &[&task_id.0, &worker_id.0, &(ownership_epoch as i64)],
        )
        .await?
        .ok_or(PostgresTaskStoreError::InvalidLease)?;

    Ok(ActiveAttempt {
        deadline: system_time(row.get::<_, DateTime<Utc>>("deadline")),
        lease_expiry: system_time(row.get::<_, DateTime<Utc>>("lease_expiry")),
        execution_timeout_ms: row.get("execution_timeout_ms"),
        retry_policy: row.get("retry_policy"),
    })
}

fn ensure_active(active: &ActiveAttempt, now: SystemTime) -> Result<(), PostgresTaskStoreError> {
    if now >= active.lease_expiry || now >= active.deadline {
        return Err(PostgresTaskStoreError::LeaseExpired);
    }
    Ok(())
}

async fn transition_to_retry_or_failure(
    transaction: &Transaction<'_>,
    task_id: &TaskId,
    active: &ActiveAttempt,
    now: SystemTime,
    message: String,
) -> Result<(), PostgresTaskStoreError> {
    let (max_retries, retry_count) = retry_values(&active.retry_policy)?;
    let failed = now >= active.deadline || retry_count >= max_retries;
    let state = if failed { "failed" } else { "ready" };
    let next_policy = retry_policy(max_retries, retry_count.saturating_add(u32::from(!failed)));

    transaction
        .execute(
            "UPDATE platform.tasks
             SET state = $2, available_at = $3, retry_policy = $4
             WHERE id = $1",
            &[&task_id.0, &state, &db_time(now), &next_policy],
        )
        .await?;
    transaction
        .execute(
            "UPDATE platform.task_attempts
             SET finished_at = $2, outcome = $3, error = $4
             WHERE task_id = $1 AND finished_at IS NULL",
            &[&task_id.0, &db_time(now), &state, &message],
        )
        .await?;
    write_event(
        transaction,
        &task_id.0,
        state,
        now,
        json!({ "message": message, "retry_count": next_policy["retry_count"] }),
    )
    .await
}

async fn write_event(
    transaction: &Transaction<'_>,
    task_id: &str,
    event_type: &str,
    now: SystemTime,
    details: Value,
) -> Result<(), PostgresTaskStoreError> {
    transaction
        .execute(
            "INSERT INTO platform.task_events (id, task_id, event_type, timestamp, details)
             VALUES ($1, $2, $3, $4, $5)",
            &[
                &Uuid::new_v4().to_string(),
                &task_id,
                &event_type,
                &db_time(now),
                &details,
            ],
        )
        .await?;
    Ok(())
}

fn task_from_row(
    row: &tokio_postgres::Row,
    status: TaskStatus,
    attempts_started: u32,
) -> Result<Task, PostgresTaskStoreError> {
    let (max_retries, retry_count) = retry_values(&row.get("retry_policy"))?;
    let execution_timeout_ms: i64 = row.get("execution_timeout_ms");
    let deadline = system_time(row.get::<_, DateTime<Utc>>("deadline"));

    Ok(Task {
        id: TaskId(row.get("id")),
        tenant_id: TenantId(row.get("tenant_id")),
        pool_id: PoolId(row.get("pool_id")),
        activity_type: ActivityType(row.get("activity_type")),
        input: row.get("input"),
        workflow_id: Some(WorkflowId(row.get("workflow_id"))),
        priority: row.get("priority"),
        created_at: system_time(row.get::<_, DateTime<Utc>>("created_at")),
        limits: TaskLimits {
            max_execution_time: duration_from_ms(execution_timeout_ms)?,
            deadline,
        },
        retry: RetryMetadata {
            retry_count,
            max_retries,
        },
        attempt: AttemptMetadata {
            attempts_started,
            last_attempt_at: Some(SystemTime::now()),
        },
        status,
    })
}

fn retry_values(policy: &Value) -> Result<(u32, u32), PostgresTaskStoreError> {
    let max_retries = policy
        .get("max_retries")
        .and_then(Value::as_u64)
        .ok_or_else(|| PostgresTaskStoreError::InvalidTask("retry policy is invalid".into()))?;
    let retry_count = policy
        .get("retry_count")
        .and_then(Value::as_u64)
        .ok_or_else(|| PostgresTaskStoreError::InvalidTask("retry policy is invalid".into()))?;
    Ok((max_retries as u32, retry_count as u32))
}

fn retry_policy(max_retries: u32, retry_count: u32) -> Value {
    json!({ "max_retries": max_retries, "retry_count": retry_count })
}

fn capped_expiry(
    now: SystemTime,
    execution_timeout_ms: i64,
    deadline: SystemTime,
) -> Result<SystemTime, PostgresTaskStoreError> {
    let duration = duration_from_ms(execution_timeout_ms)?;
    Ok(now.checked_add(duration).unwrap_or(deadline).min(deadline))
}

fn duration_from_ms(milliseconds: i64) -> Result<Duration, PostgresTaskStoreError> {
    u64::try_from(milliseconds)
        .map(Duration::from_millis)
        .map_err(|_| PostgresTaskStoreError::InvalidTask("execution timeout is invalid".into()))
}

fn db_time(value: SystemTime) -> DateTime<Utc> {
    value.into()
}

fn system_time(value: DateTime<Utc>) -> SystemTime {
    value.into()
}

#[derive(Debug)]
pub enum PostgresTaskStoreError {
    Database(tokio_postgres::Error),
    InvalidTask(String),
    InvalidLease,
    LeaseExpired,
}

impl fmt::Display for PostgresTaskStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::InvalidTask(message) => formatter.write_str(message),
            Self::InvalidLease => formatter.write_str("lease is not current"),
            Self::LeaseExpired => formatter.write_str("lease has expired"),
        }
    }
}

impl std::error::Error for PostgresTaskStoreError {}

impl From<tokio_postgres::Error> for PostgresTaskStoreError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Database(error)
    }
}
