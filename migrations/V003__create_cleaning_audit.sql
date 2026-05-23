-- V003__create_cleaning_audit.sql
-- Persistent, queryable version of the per-run cleaning_report.json.
-- Long form (one row per drop reason) rather than wide (one column per reason)
-- so we can add a new reason without a schema migration.

CREATE TABLE IF NOT EXISTS cleaning_audit (
    audit_id        BIGSERIAL    PRIMARY KEY,
    run_id          BIGINT       NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    drop_reason     VARCHAR(50)  NOT NULL,
    rows_dropped    INTEGER      NOT NULL,
    recorded_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, drop_reason)
);

CREATE INDEX IF NOT EXISTS idx_cleaning_audit_run_id
    ON cleaning_audit (run_id);

COMMENT ON TABLE cleaning_audit IS
  'Per-reason drop counts for each pipeline run. Used for trend analysis ("are duplicates getting worse?") and incident triage.';
COMMENT ON COLUMN cleaning_audit.drop_reason IS
  'Free-text reason name. Examples: future_timestamp, duplicate_event_id, null_user_id, bot_user, outside_window. Kept as VARCHAR rather than enum so new reasons can be added without migration.';
