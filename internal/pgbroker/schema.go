package pgbroker

const createSchemaSQL = `
CREATE TABLE IF NOT EXISTS asynq_queues (
    qname TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS asynq_tasks (
    queue    TEXT NOT NULL,
    id       TEXT NOT NULL,
    msg      BYTEA NOT NULL,
    state    TEXT NOT NULL DEFAULT 'pending',
    unique_key    TEXT NOT NULL DEFAULT '',
    group_key     TEXT NOT NULL DEFAULT '',
    result        BYTEA,

    pending_since  BIGINT NOT NULL DEFAULT 0,
    lease_expire   BIGINT NOT NULL DEFAULT 0,
    score          BIGINT NOT NULL DEFAULT 0,
    completed_at   BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (queue, id)
);

CREATE INDEX IF NOT EXISTS idx_asynq_tasks_state ON asynq_tasks (queue, state);
CREATE INDEX IF NOT EXISTS idx_asynq_tasks_score ON asynq_tasks (queue, state, score);
CREATE INDEX IF NOT EXISTS idx_asynq_tasks_lease ON asynq_tasks (queue, lease_expire) WHERE state = 'active';

CREATE TABLE IF NOT EXISTS asynq_unique_locks (
    key        TEXT PRIMARY KEY,
    task_id    TEXT NOT NULL,
    expire_at  BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS asynq_groups (
    queue  TEXT NOT NULL,
    gname  TEXT NOT NULL,
    PRIMARY KEY (queue, gname)
);

CREATE TABLE IF NOT EXISTS asynq_aggregation_sets (
    queue       TEXT NOT NULL,
    gname       TEXT NOT NULL,
    set_id      TEXT NOT NULL,
    task_id     TEXT NOT NULL,
    msg         BYTEA NOT NULL,
    score       BIGINT NOT NULL DEFAULT 0,
    expire_at   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, gname, set_id, task_id)
);

CREATE INDEX IF NOT EXISTS idx_asynq_agg_expire ON asynq_aggregation_sets (queue, expire_at);

CREATE TABLE IF NOT EXISTS asynq_stats_daily (
    queue      TEXT NOT NULL,
    date       TEXT NOT NULL,
    processed  BIGINT NOT NULL DEFAULT 0,
    failed     BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, date)
);

CREATE TABLE IF NOT EXISTS asynq_stats_total (
    queue      TEXT PRIMARY KEY,
    processed  BIGINT NOT NULL DEFAULT 0,
    failed     BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS asynq_servers (
    key       TEXT PRIMARY KEY,
    data      BYTEA NOT NULL,
    expire_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS asynq_workers (
    server_key TEXT NOT NULL,
    worker_id  TEXT NOT NULL,
    data       BYTEA NOT NULL,
    expire_at  BIGINT NOT NULL,
    PRIMARY KEY (server_key, worker_id)
);
`
