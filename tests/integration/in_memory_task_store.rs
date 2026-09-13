use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime};

use platform_09_26::domain::{
    ActivityType, AttemptMetadata, PoolId, RetryMetadata, Task, TaskId, TaskLimits, TaskStatus,
    TenantId,
};
use platform_09_26::storage::InMemoryTaskStore;
use platform_09_26::worker::WorkerId;

fn pending_task(
    id: &str,
    pool_id: &str,
    now: SystemTime,
    max_retries: u32,
    max_execution_time: Duration,
    deadline: SystemTime,
) -> Task {
    Task {
        id: TaskId(id.into()),
        tenant_id: TenantId("tenant-1".into()),
        pool_id: PoolId(pool_id.into()),
        activity_type: ActivityType("echo".into()),
        input: b"hello".to_vec(),
        workflow_id: None,
        priority: 10,
        created_at: now,
        limits: TaskLimits {
            max_execution_time,
            deadline,
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
            expires_at: deadline,
        },
    }
}

#[test]
fn a_task_is_leased_to_exactly_one_worker() {
    let now = SystemTime::now();
    let store = Arc::new(InMemoryTaskStore::new());
    store
        .submit(pending_task(
            "task-1",
            "shared",
            now,
            3,
            Duration::from_secs(30),
            now + Duration::from_secs(60),
        ))
        .unwrap();

    let start = Arc::new(Barrier::new(2));
    let first_store = store.clone();
    let first_start = start.clone();
    let first = thread::spawn(move || {
        first_start.wait();
        first_store
            .lease_next(
                &PoolId("shared".into()),
                &WorkerId("worker-1".into()),
                1,
                now,
            )
            .unwrap()
    });
    let second_store = store.clone();
    let second = thread::spawn(move || {
        start.wait();
        second_store
            .lease_next(
                &PoolId("shared".into()),
                &WorkerId("worker-2".into()),
                1,
                now,
            )
            .unwrap()
    });

    let leases = [first.join().unwrap(), second.join().unwrap()];
    assert_eq!(leases.iter().flatten().count(), 1);
}

#[test]
fn workers_in_one_pool_lease_different_tasks() {
    let now = SystemTime::now();
    let store = InMemoryTaskStore::new();
    for id in ["task-1", "task-2"] {
        store
            .submit(pending_task(
                id,
                "shared",
                now,
                3,
                Duration::from_secs(30),
                now + Duration::from_secs(60),
            ))
            .unwrap();
    }

    let first = store
        .lease_next(
            &PoolId("shared".into()),
            &WorkerId("worker-1".into()),
            1,
            now,
        )
        .unwrap()
        .unwrap();
    let second = store
        .lease_next(
            &PoolId("shared".into()),
            &WorkerId("worker-2".into()),
            1,
            now,
        )
        .unwrap()
        .unwrap();

    assert_ne!(first.id, second.id);
    assert!(matches!(
        first.status,
        TaskStatus::Leased(ref lease) if lease.owner_id.0 == "worker-1"
    ));
    assert!(matches!(
        second.status,
        TaskStatus::Leased(ref lease) if lease.owner_id.0 == "worker-2"
    ));
}

#[test]
fn an_expired_lease_retries_then_can_complete() {
    let now = SystemTime::now();
    let store = InMemoryTaskStore::new();
    store
        .submit(pending_task(
            "task-1",
            "shared",
            now,
            2,
            Duration::from_secs(10),
            now + Duration::from_secs(60),
        ))
        .unwrap();

    let first_lease = store
        .lease_next(
            &PoolId("shared".into()),
            &WorkerId("worker-1".into()),
            1,
            now,
        )
        .unwrap()
        .unwrap();
    let lease_expiry = match first_lease.status {
        TaskStatus::Leased(lease) => lease.expires_at,
        _ => panic!("task should be leased"),
    };

    store.recover_expired(lease_expiry).unwrap();
    let retried = store.get(&TaskId("task-1".into())).unwrap();
    assert_eq!(retried.retry.retry_count, 1);
    assert!(matches!(retried.status, TaskStatus::Pending { .. }));

    let second_lease = store
        .lease_next(
            &PoolId("shared".into()),
            &WorkerId("worker-2".into()),
            1,
            lease_expiry,
        )
        .unwrap()
        .unwrap();
    store
        .complete(
            &second_lease.id,
            &WorkerId("worker-2".into()),
            1,
            lease_expiry,
        )
        .unwrap();

    assert!(matches!(
        store.get(&TaskId("task-1".into())).unwrap().status,
        TaskStatus::Completed { .. }
    ));
}

#[test]
fn a_task_fails_after_exhausting_its_retries() {
    let now = SystemTime::now();
    let store = InMemoryTaskStore::new();
    store
        .submit(pending_task(
            "task-1",
            "shared",
            now,
            1,
            Duration::from_secs(10),
            now + Duration::from_secs(60),
        ))
        .unwrap();

    let first = store
        .lease_next(
            &PoolId("shared".into()),
            &WorkerId("worker-1".into()),
            1,
            now,
        )
        .unwrap()
        .unwrap();
    let first_expiry = match first.status {
        TaskStatus::Leased(lease) => lease.expires_at,
        _ => panic!("task should be leased"),
    };
    store.recover_expired(first_expiry).unwrap();

    let second = store
        .lease_next(
            &PoolId("shared".into()),
            &WorkerId("worker-2".into()),
            1,
            first_expiry,
        )
        .unwrap()
        .unwrap();
    let second_expiry = match second.status {
        TaskStatus::Leased(lease) => lease.expires_at,
        _ => panic!("task should be leased"),
    };
    store.recover_expired(second_expiry).unwrap();

    assert!(matches!(
        store.get(&TaskId("task-1".into())).unwrap().status,
        TaskStatus::Failed { .. }
    ));
}

#[test]
fn a_renewal_never_extends_past_the_total_task_deadline() {
    let now = SystemTime::now();
    let deadline = now + Duration::from_secs(5);
    let store = InMemoryTaskStore::new();
    store
        .submit(pending_task(
            "task-1",
            "shared",
            now,
            3,
            Duration::from_secs(4),
            deadline,
        ))
        .unwrap();

    let leased = store
        .lease_next(
            &PoolId("shared".into()),
            &WorkerId("worker-1".into()),
            1,
            now,
        )
        .unwrap()
        .unwrap();
    let initial_expiry = match leased.status {
        TaskStatus::Leased(lease) => lease.expires_at,
        _ => panic!("task should be leased"),
    };
    assert_eq!(initial_expiry, now + Duration::from_secs(4));

    let renewed = store
        .renew(
            &TaskId("task-1".into()),
            &WorkerId("worker-1".into()),
            1,
            now + Duration::from_secs(3),
        )
        .unwrap();
    assert!(matches!(
        renewed.status,
        TaskStatus::Leased(ref lease) if lease.expires_at == deadline
    ));
}

#[test]
fn reaching_the_total_task_deadline_causes_final_failure() {
    let now = SystemTime::now();
    let deadline = now + Duration::from_secs(5);
    let store = InMemoryTaskStore::new();
    store
        .submit(pending_task(
            "task-1",
            "shared",
            now,
            3,
            Duration::from_secs(30),
            deadline,
        ))
        .unwrap();

    store
        .lease_next(
            &PoolId("shared".into()),
            &WorkerId("worker-1".into()),
            1,
            now,
        )
        .unwrap()
        .unwrap();
    store.recover_expired(deadline).unwrap();

    assert!(matches!(
        store.get(&TaskId("task-1".into())).unwrap().status,
        TaskStatus::Failed { .. }
    ));
}
