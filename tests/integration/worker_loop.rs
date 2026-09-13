use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use platform_09_26::domain::{
    ActivityType, AttemptMetadata, Lease, LeaseOwnerId, PoolId, RetryMetadata, Task, TaskId,
    TaskLimits, TaskStatus, TenantId,
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

struct FinishingActivity;

#[async_trait]
impl Activity for FinishingActivity {
    async fn execute(&self, input: &[u8]) -> Result<Vec<u8>, ActivityError> {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(input.to_vec())
    }
}

struct FailingActivity;

#[async_trait]
impl Activity for FailingActivity {
    async fn execute(&self, _input: &[u8]) -> Result<Vec<u8>, ActivityError> {
        Err(ActivityError::new("activity failed"))
    }
}

struct SlowActivity {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

#[async_trait]
impl Activity for SlowActivity {
    async fn execute(&self, input: &[u8]) -> Result<Vec<u8>, ActivityError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(3)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(input.to_vec())
    }
}

#[derive(Default)]
struct FakeOrchestrator {
    assignments: Mutex<VecDeque<TaskAssignment>>,
    renewals: Mutex<Vec<LeaseRenewal>>,
    results: Mutex<Vec<TaskResult>>,
    fail_renewal: bool,
}

#[async_trait]
impl OrchestratorClient for FakeOrchestrator {
    async fn request_task(
        &self,
        _worker_id: &WorkerId,
        _pool_id: &PoolId,
    ) -> Result<Option<TaskAssignment>, OrchestratorClientError> {
        Ok(self.assignments.lock().unwrap().pop_front())
    }

    async fn renew_lease(&self, renewal: LeaseRenewal) -> Result<(), OrchestratorClientError> {
        self.renewals.lock().unwrap().push(renewal);

        if self.fail_renewal {
            return Err(OrchestratorClientError::new("lease renewal failed"));
        }

        Ok(())
    }

    async fn report_result(&self, result: TaskResult) -> Result<(), OrchestratorClientError> {
        self.results.lock().unwrap().push(result);
        Ok(())
    }
}

fn leased_task(activity_type: &str) -> Task {
    leased_task_with_expiry(
        activity_type,
        SystemTime::UNIX_EPOCH + Duration::from_secs(10),
    )
}

fn leased_task_with_expiry(activity_type: &str, expires_at: SystemTime) -> Task {
    Task {
        id: TaskId("task-1".into()),
        tenant_id: TenantId("tenant-1".into()),
        pool_id: PoolId("shared".into()),
        activity_type: ActivityType(activity_type.into()),
        input: b"hello".to_vec(),
        workflow_id: None,
        priority: 10,
        created_at: SystemTime::UNIX_EPOCH,
        limits: TaskLimits {
            max_execution_time: Duration::from_secs(30),
            deadline: expires_at + Duration::from_secs(60),
        },
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
            expires_at,
        }),
    }
}

#[tokio::test(start_paused = true)]
async fn worker_renews_the_lease_and_reports_the_result() {
    let orchestrator = Arc::new(FakeOrchestrator {
        assignments: Mutex::new(VecDeque::from([TaskAssignment {
            task: leased_task("echo"),
        }])),
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

#[tokio::test(start_paused = true)]
async fn worker_reports_an_activity_failure() {
    let orchestrator = Arc::new(FakeOrchestrator {
        assignments: Mutex::new(VecDeque::from([TaskAssignment {
            task: leased_task("failing"),
        }])),
        ..FakeOrchestrator::default()
    });
    let mut activities = ActivityRegistry::default();
    activities.register(ActivityType("failing".into()), FailingActivity);
    let worker = Worker::new(
        WorkerId("worker-1".into()),
        PoolId("shared".into()),
        activities,
        orchestrator.clone(),
        WorkerConfig::default(),
    )
    .unwrap();

    assert!(worker.run_once().await.unwrap());
    assert_eq!(
        orchestrator.results.lock().unwrap()[0].outcome,
        TaskOutcome::Failed {
            message: "activity failed".into()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn worker_reports_an_unregistered_activity() {
    let orchestrator = Arc::new(FakeOrchestrator {
        assignments: Mutex::new(VecDeque::from([TaskAssignment {
            task: leased_task("missing"),
        }])),
        ..FakeOrchestrator::default()
    });
    let worker = Worker::new(
        WorkerId("worker-1".into()),
        PoolId("shared".into()),
        ActivityRegistry::default(),
        orchestrator.clone(),
        WorkerConfig::default(),
    )
    .unwrap();

    assert!(worker.run_once().await.unwrap());
    assert!(matches!(
        &orchestrator.results.lock().unwrap()[0].outcome,
        TaskOutcome::Failed { message } if message.contains("not registered")
    ));
}

#[tokio::test]
async fn worker_reports_completion_after_a_failed_renewal() {
    let orchestrator = Arc::new(FakeOrchestrator {
        assignments: Mutex::new(VecDeque::from([TaskAssignment {
            task: leased_task_with_expiry(
                "finishing",
                SystemTime::now() + Duration::from_millis(100),
            ),
        }])),
        fail_renewal: true,
        ..FakeOrchestrator::default()
    });
    let mut activities = ActivityRegistry::default();
    activities.register(ActivityType("finishing".into()), FinishingActivity);
    let worker = Worker::new(
        WorkerId("worker-1".into()),
        PoolId("shared".into()),
        activities,
        orchestrator.clone(),
        WorkerConfig {
            max_concurrency: 1,
            idle_poll_interval: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_millis(5),
        },
    )
    .unwrap();

    assert!(worker.run_once().await.unwrap());
    assert_eq!(orchestrator.renewals.lock().unwrap().len(), 1);
    assert_eq!(
        orchestrator.results.lock().unwrap()[0].outcome,
        TaskOutcome::Succeeded(b"hello".to_vec())
    );
}

#[tokio::test]
async fn worker_requests_a_retry_when_a_failed_renewal_reaches_expiry() {
    let orchestrator = Arc::new(FakeOrchestrator {
        assignments: Mutex::new(VecDeque::from([TaskAssignment {
            task: leased_task_with_expiry("echo", SystemTime::now() + Duration::from_millis(50)),
        }])),
        fail_renewal: true,
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
            lease_renewal_interval: Duration::from_millis(5),
        },
    )
    .unwrap();

    assert!(worker.run_once().await.unwrap());
    assert_eq!(orchestrator.renewals.lock().unwrap().len(), 1);
    assert_eq!(
        orchestrator.results.lock().unwrap()[0].outcome,
        TaskOutcome::Retry {
            retry_count: 1,
            message: "lease expired after a failed renewal".into(),
        }
    );
}

#[tokio::test]
async fn worker_runs_two_long_activities_concurrently() {
    let orchestrator = Arc::new(FakeOrchestrator {
        assignments: Mutex::new(VecDeque::from([
            TaskAssignment {
                task: leased_task("slow"),
            },
            TaskAssignment {
                task: Task {
                    id: TaskId("task-2".into()),
                    ..leased_task("slow")
                },
            },
        ])),
        ..FakeOrchestrator::default()
    });
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut activities = ActivityRegistry::default();
    activities.register(
        ActivityType("slow".into()),
        SlowActivity {
            active: active.clone(),
            peak: peak.clone(),
        },
    );
    let worker = Worker::new(
        WorkerId("worker-1".into()),
        PoolId("shared".into()),
        activities,
        orchestrator,
        WorkerConfig {
            max_concurrency: 2,
            idle_poll_interval: Duration::from_millis(10),
            lease_renewal_interval: Duration::from_secs(1),
        },
    )
    .unwrap();

    let worker_task = tokio::spawn(async move { worker.run().await });
    let overlapped = tokio::time::timeout(Duration::from_secs(1), async {
        while peak.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    worker_task.abort();
    assert!(
        overlapped.is_ok(),
        "both worker slots should execute at once"
    );
}
