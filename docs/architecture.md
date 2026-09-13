# Architecture

- Rust Cargo workspace implementing a Temporal-inspired, multi-tenant distributed executor.
- Registered activity types only; arbitrary tenant code is outside v1.
- Sequential workflows initially; DAG workflows deferred.
- Orchestrator owns scheduling, retries, leases, fairness, priority, and pool migration.
- Workers poll pools, execute activities, heartbeat leases, and report results.
- Postgres is the authoritative state store and initial queue.
- Start with one shared worker pool; add multiple pools and fabricated tenants.
- Automatically isolate noisy tenants using ownership epochs and graceful live handoff.
- Reintegrate tenants only after sustained healthy behavior plus cooldown.
- Adversarial tenants have ordinary API permissions but generate hostile legal workloads.
- External fault injector targets documented process, network, and storage boundaries; Byzantine workers are optional later.
- Use scripted scenarios plus seeded randomized exploration.
- OpenTelemetry provides instrumentation/traces; Prometheus stores and queries metrics.
- CLI first; frontend and GitHub demo after the system works.
- Single restartable orchestrator initially; replicated control plane, scheduler details, isolation thresholds, DLQ, and full fault catalog remain TBD.
