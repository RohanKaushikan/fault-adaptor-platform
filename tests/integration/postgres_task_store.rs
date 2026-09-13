use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use platform_09_26::domain::{
    ActivityType, AttemptMetadata, PoolId, RetryMetadata, Task, TaskId, TaskLimits, TaskStatus,
    TenantId, WorkflowId,
};
use platform_09_26::storage::PostgresTaskStore;
use platform_09_26::worker::WorkerId;
use tokio_postgres::NoTls;

fn database_url() -> String {
    env::var("DATABASE_URL").expect("DATABASE_URL must point to the local PostgreSQL database")
}

fn pending_task(id: &str, now: SystemTime, max_retries: u32, timeout: Duration) -> Task {
    Task {
        id: TaskId(id.into()),
        tenant_id: TenantId("tenant-1".into()),
        pool_id: PoolId("pool-1".into()),
        activity_type: ActivityType("echo".into()),
        input: b"hello".to_vec(),
        workflow_id: Some(WorkflowId("workflow-1".into())),
        priority: 10,
        created_at: now,
        limits: TaskLimits {
            max_execution_time: timeout,
            deadline: now + Duration::from_secs(60),
        },
        retry: RetryMetadata {
            retry_count: 0,
            max_retries,
        },
        attempt: AttemptMetadata {
            attempts_started: 0,
            last_attempt_at: None,
        },
        status: TaskStatus::Pending {
            expires_at: now + Duration::from_secs(60),
        },
    }
}

async fn reset_database(database_url: &str) {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "TRUNCATE platform.task_events, platform.task_attempts, platform.tasks,
                      platform.workflows, platform.workers, platform.tenants,
                      platform.worker_pools CASCADE;
             INSERT INTO platform.worker_pools (id, name, status)
             VALUES ('pool-1', 'shared', 'active');
             INSERT INTO platform.tenants
                 (id, name, concurrency_limit, current_pool_id, ownership_epoch)
             VALUES ('tenant-1', 'tenant', 10, 'pool-1', 1);
             INSERT INTO platform.workers
                 (id, pool_id, status, max_concurrency, last_heartbeat)
             VALUES
                 ('worker-1', 'pool-1', 'active', 1, NOW()),
                 ('worker-2', 'pool-1', 'active', 1, NOW());
             INSERT INTO platform.workflows (id, tenant_id, state, created_at, deadline)
             VALUES ('workflow-1', 'tenant-1', 'running', NOW(), NOW() + INTERVAL '1 hour');",
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires Docker Compose PostgreSQL"]
async fn postgres_leases_one_task_to_exactly_one_worker() {
    let database_url = database_url();
    reset_database(&database_url).await;
    let store = Arc::new(PostgresTaskStore::connect(&database_url).await.unwrap());
    store
        .submit(&pending_task(
            "task-1",
            SystemTime::now(),
            3,
            Duration::from_secs(30),
        ))
        .await
        .unwrap();

    let first_store = store.clone();
    let second_store = store.clone();
    let first = tokio::spawn(async move {
        first_store
            .lease_next(&PoolId("pool-1".into()), &WorkerId("worker-1".into()), 1)
            .await
            .unwrap()
    });
    let second = tokio::spawn(async move {
        second_store
            .lease_next(&PoolId("pool-1".into()), &WorkerId("worker-2".into()), 1)
            .await
            .unwrap()
    });

    let leases = [first.await.unwrap(), second.await.unwrap()];
    assert_eq!(leases.iter().flatten().count(), 1);
}

#[tokio::test]
#[ignore = "requires Docker Compose PostgreSQL"]
async fn postgres_retries_expired_leases_then_fails_at_the_limit() {
    let database_url = database_url();
    reset_database(&database_url).await;
    let store = PostgresTaskStore::connect(&database_url).await.unwrap();
    store
        .submit(&pending_task(
            "task-1",
            SystemTime::now(),
            1,
            Duration::from_millis(1),
        ))
        .await
        .unwrap();

    store
        .lease_next(&PoolId("pool-1".into()), &WorkerId("worker-1".into()), 1)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    store.recover_expired().await.unwrap();

    store
        .lease_next(&PoolId("pool-1".into()), &WorkerId("worker-2".into()), 1)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    store.recover_expired().await.unwrap();

    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT state, retry_policy FROM platform.tasks WHERE id = 'task-1'",
            &[],
        )
        .await
        .unwrap();
    let retry_policy: serde_json::Value = row.get("retry_policy");
    assert_eq!(row.get::<_, String>("state"), "failed");
    assert_eq!(retry_policy["retry_count"], 1);
}
