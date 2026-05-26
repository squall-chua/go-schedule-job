package postgres

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    queue         TEXT NOT NULL,
    name          TEXT NOT NULL,
    payload       BYTEA,
    codec_name    TEXT NOT NULL DEFAULT '',
    priority      SMALLINT NOT NULL DEFAULT 1,
    run_at        TIMESTAMPTZ NOT NULL,
    attempt       INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 3,
    state         SMALLINT NOT NULL DEFAULT 0,
    timeout_ns    BIGINT NOT NULL DEFAULT 0,
    locked_by     TEXT NOT NULL DEFAULT '',
    locked_until  TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00+00',
    last_error    TEXT NOT NULL DEFAULT '',
    recurring_id  TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00+00',
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00+00'
);

CREATE INDEX IF NOT EXISTS idx_jobs_claim
    ON jobs(queue, state, priority DESC, run_at);

CREATE TABLE IF NOT EXISTS recurring (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    queue         TEXT NOT NULL,
    payload       BYTEA,
    codec_name    TEXT NOT NULL DEFAULT '',
    priority      SMALLINT NOT NULL DEFAULT 1,
    timeout_ns    BIGINT NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 3,
    cron          TEXT NOT NULL DEFAULT '',
    every_ns      BIGINT NOT NULL DEFAULT 0,
    catchup       BOOLEAN NOT NULL DEFAULT FALSE,
    next_run_at   TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00+00',
    last_fire_at  TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00+00',
    lease_until   TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00+00',
    leased_by     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id      TEXT PRIMARY KEY,
    last_heartbeat TIMESTAMPTZ NOT NULL
);
`
