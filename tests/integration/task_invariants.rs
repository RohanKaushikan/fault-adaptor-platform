use std::time::{Duration, SystemTime};

use platform_09_26::domain::{
    ActivityType, AttemptMetadata, Lease, LeaseOwnerId, PoolId, RetryMetadata, Task, TaskId,
    TaskStatus, TenantId, WorkflowId,
};

fn task_with_status(status: TaskStatus) -> Task {
    Task {
        id: TaskId("task-1".into()),
        tenant_id: TenantId("tenant-1".into()),
        pool_id: PoolId("shared".into()),
        activity_type: ActivityType("send-email".into()),
        input: br#"{"recipient":"user@example.com"}"#.to_vec(),
        workflow_id: Some(WorkflowId("workflow-1".into())),
        priority: 10,
        created_at: SystemTime::UNIX_EPOCH,
        retry: RetryMetadata {
            retry_count: 1,
            max_retries: 3,
        },
        attempt: AttemptMetadata {
            attempts_started: 2,
            last_attempt_at: Some(SystemTime::UNIX_EPOCH),
        },
        status,
    }
}

#[test]
fn pending_task_has_one_tenant_and_an_expiry() {
    let expires_at = SystemTime::UNIX_EPOCH + Duration::from_secs(60);
    let task = task_with_status(TaskStatus::Pending { expires_at });

    assert_eq!(task.tenant_id, TenantId("tenant-1".into()));
    assert!(matches!(
        task.status,
        TaskStatus::Pending { expires_at: value } if value == expires_at
    ));
}

#[test]
fn leased_task_has_one_owner_and_consistent_attempt_metadata() {
    let leased_at = SystemTime::UNIX_EPOCH;
    let expires_at = leased_at + Duration::from_secs(30);
    let task = task_with_status(TaskStatus::Leased(Lease {
        owner_id: LeaseOwnerId("worker-1".into()),
        ownership_epoch: 4,
        leased_at,
        expires_at,
    }));

    assert_eq!(task.attempt.attempts_started, 2);
    assert_eq!(task.retry.retry_count, 1);
    assert!(matches!(
        task.status,
        TaskStatus::Leased(Lease { owner_id, expires_at: value, .. })
            if owner_id == LeaseOwnerId("worker-1".into()) && value == expires_at
    ));
}

#[test]
fn completed_task_records_when_it_completed() {
    let completed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(90);
    let task = task_with_status(TaskStatus::Completed { completed_at });

    assert!(matches!(
        task.status,
        TaskStatus::Completed { completed_at: value } if value == completed_at
    ));
}
