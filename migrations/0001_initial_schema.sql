CREATE SCHEMA IF NOT EXISTS platform;

CREATE TABLE IF NOT EXISTS platform.worker_pools (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('active', 'draining', 'disabled'))
);

CREATE TABLE IF NOT EXISTS platform.tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    concurrency_limit INTEGER NOT NULL CHECK (concurrency_limit > 0),
    current_pool_id TEXT REFERENCES platform.worker_pools(id),
    ownership_epoch BIGINT NOT NULL DEFAULT 0 CHECK (ownership_epoch >= 0)
);

CREATE TABLE IF NOT EXISTS platform.workers (
    id TEXT PRIMARY KEY,
    pool_id TEXT NOT NULL REFERENCES platform.worker_pools(id),
    status TEXT NOT NULL CHECK (status IN ('active', 'draining', 'offline')),
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency > 0),
    last_heartbeat TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS platform.workflows (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform.tenants(id),
    state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    deadline TIMESTAMPTZ NOT NULL,
    CHECK (deadline > created_at)
);

CREATE TABLE IF NOT EXISTS platform.tasks (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES platform.workflows(id),
    activity_type TEXT NOT NULL,
    input BYTEA NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ready', 'leased', 'completed', 'failed')),
    pool_id TEXT NOT NULL REFERENCES platform.worker_pools(id),
    priority INTEGER NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    execution_timeout_ms BIGINT NOT NULL CHECK (execution_timeout_ms > 0),
    retry_policy JSONB NOT NULL,
    current_attempt INTEGER NOT NULL DEFAULT 0 CHECK (current_attempt >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    deadline TIMESTAMPTZ NOT NULL,
    CHECK (deadline > created_at)
);

CREATE TABLE IF NOT EXISTS platform.task_attempts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES platform.tasks(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    worker_id TEXT NOT NULL REFERENCES platform.workers(id),
    ownership_epoch BIGINT NOT NULL CHECK (ownership_epoch >= 0),
    lease_expiry TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    outcome TEXT,
    error TEXT,
    UNIQUE (task_id, attempt_number),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS task_attempts_one_active_lease
    ON platform.task_attempts (task_id)
    WHERE finished_at IS NULL;

CREATE TABLE IF NOT EXISTS platform.task_events (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES platform.tasks(id),
    event_type TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS tasks_ready_for_pool
    ON platform.tasks (pool_id, priority DESC, created_at ASC, id ASC)
    WHERE state = 'ready';

CREATE INDEX IF NOT EXISTS task_events_by_task
    ON platform.task_events (task_id, timestamp ASC);
