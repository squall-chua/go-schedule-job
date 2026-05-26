package sqlite

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    queue         TEXT NOT NULL,
    name          TEXT NOT NULL,
    payload       BLOB,
    codec_name    TEXT NOT NULL DEFAULT '',
    priority      INTEGER NOT NULL DEFAULT 1,
    run_at        INTEGER NOT NULL,
    attempt       INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 3,
    state         INTEGER NOT NULL DEFAULT 0,
    timeout_ns    INTEGER NOT NULL DEFAULT 0,
    locked_by     TEXT NOT NULL DEFAULT '',
    locked_until  INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT NOT NULL DEFAULT '',
    recurring_id  TEXT NOT NULL DEFAULT '',
    created_at    INTEGER NOT NULL DEFAULT 0,
    updated_at    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_jobs_claim
    ON jobs(queue, state, priority DESC, run_at);

CREATE TABLE IF NOT EXISTS recurring (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    queue         TEXT NOT NULL,
    payload       BLOB,
    codec_name    TEXT NOT NULL DEFAULT '',
    priority      INTEGER NOT NULL DEFAULT 1,
    timeout_ns    INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 3,
    cron          TEXT NOT NULL DEFAULT '',
    every_ns      INTEGER NOT NULL DEFAULT 0,
    catchup       INTEGER NOT NULL DEFAULT 0,
    next_run_at   INTEGER NOT NULL DEFAULT 0,
    last_fire_at  INTEGER NOT NULL DEFAULT 0,
    lease_until   INTEGER NOT NULL DEFAULT 0,
    leased_by     TEXT NOT NULL DEFAULT ''
);
`
