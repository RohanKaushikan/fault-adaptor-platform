# Invariants

- Activities execute at least once; duplicate execution is possible.
- Each task has one current lease; expired or stale-epoch leases cannot commit.
- Durable completion is committed once; external side effects require idempotency keys.
- Accepted tasks are never silently lost and eventually complete, retry, or enter a terminal state.
- Retry counts and ownership epochs increase monotonically.
- A tenant has exactly one authoritative pool and ownership epoch.
- Graceful migration preserves queued and in-flight work across crashes.
- Sequential workflow activities start only after predecessors complete durably.
- Per-tenant concurrency limits and bounded priorities are enforced.
- No eligible tenant is permanently starved.
- Noisy tenants cannot prevent healthy tenants from making progress.
- Acknowledged state survives worker and orchestrator restarts, assuming Postgres remains durable.
- Fault scenarios are deterministic, documented, replayable, and eventually minimizable.
