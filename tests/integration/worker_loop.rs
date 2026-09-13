use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use platform_09_26::domain::{
    ActivityType, AttemptMetadata, Lease, LeaseOwnerId, PoolId, RetryMetadata, Task, TaskId,
    TaskStatus, TenantId,
};
use platform_09_26::transport::{
    LeaseRenewal, OrchestratorClient, OrchestratorClientError, TaskAssignment, TaskOutcome,
    TaskResult,
};
use platform_09_26::worker::{
    Activity, ActivityError, ActivityRegistry, Worker, WorkerConfig, WorkerId,
};

struct EchoActivity;

#[async_trait]
impl Activity for EchoActivity {
    async fn execute(&self, input: &[u8]) -> Result<Vec<u8>, ActivityError> {
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(input.to_vec())
    }
}

#[derive(Default)]
struct FakeOrchestrator {
    assignment: Mutex<Option<TaskAssignment>>,
    renewals: Mutex<Vec<LeaseRenewal>>,
    results: Mutex<Vec<TaskResult>>,
}

#[async_trait]
impl OrchestratorClient for FakeOrchestrator {
    async fn request_task(
        &self,
        _worker_id: &WorkerId,
        _pool_id: &PoolId,
    ) -> Result<Option<TaskAssignment>, OrchestratorClientError> {
        Ok(self.assignment.lock().unwrap().take())
    }

    async fn renew_lease(&self, renewal: LeaseRenewal) -> Result<(), OrchestratorClientError> {
        self.renewals.lock().unwrap().push(renewal);
        Ok(())
    }

    async fn report_result(&self, result: TaskResult) -> Result<(), OrchestratorClientError> {
        self.results.lock().unwrap().push(result);
        Ok(())
    }
}

fn leased_task() -> Task {
    Task {
        id: TaskId("task-1".into()),
        tenant_id: TenantId("tenant-1".into()),
        pool_id: PoolId("shared".into()),
        activity_type: ActivityType("echo".into()),
        input: b"hello".to_vec(),
        workflow_id: None,
        priority: 10,
        created_at: SystemTime::UNIX_EPOCH,
        retry: RetryMetadata {
            retry_count: 0,
            max_retries: 3,
        },
        attempt: AttemptMetadata {
            attempts_started: 1,
            last_attempt_at: Some(SystemTime::UNIX_EPOCH),
        },
        status: TaskStatus::Leased(Lease {
            owner_id: LeaseOwnerId("worker-1".into()),
            ownership_epoch: 7,
            leased_at: SystemTime::UNIX_EPOCH,
            expires_at: SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        }),
    }
}

#[tokio::test(start_paused = true)]
async fn worker_renews_the_lease_and_reports_the_result() {
    let orchestrator = Arc::new(FakeOrchestrator {
        assignment: Mutex::new(Some(TaskAssignment {
            task: leased_task(),
        })),
        ..FakeOrchestrator::default()
    });

    let mut activities = ActivityRegistry::default();
    activities.register(ActivityType("echo".into()), EchoActivity);

    let worker = Worker::new(
        WorkerId("worker-1".into()),
        PoolId("shared".into()),
        activities,
        orchestrator.clone(),
        WorkerConfig {
            max_concurrency: 1,
            idle_poll_interval: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_secs(1),
        },
    )
    .unwrap();

    assert!(worker.run_once().await.unwrap());

    let renewals = orchestrator.renewals.lock().unwrap();
    assert!(!renewals.is_empty());
    assert!(renewals.iter().all(|renewal| {
        renewal.task_id == TaskId("task-1".into())
            && renewal.worker_id == WorkerId("worker-1".into())
            && renewal.attempt == 1
            && renewal.ownership_epoch == 7
    }));

    let results = orchestrator.results.lock().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].task_id, TaskId("task-1".into()));
    assert_eq!(results[0].attempt, 1);
    assert_eq!(results[0].ownership_epoch, 7);
    assert_eq!(
        results[0].outcome,
        TaskOutcome::Succeeded(b"hello".to_vec())
    );
}
