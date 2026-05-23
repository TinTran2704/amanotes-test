-- V002__create_pipeline_runs.sql
-- One row per pipeline invocation. The PK doubles as a foreign key target
-- for cleaning_audit, dq_check_results, and daily_user_metrics so we can
-- always answer "which run produced this number?".

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          BIGSERIAL    PRIMARY KEY,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(20)  NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    input_file      VARCHAR(500) NOT NULL,
    input_file_sha  VARCHAR(64),
    rows_loaded     INTEGER,
    rows_final      INTEGER,
    window_start    DATE,
    window_end      DATE,
    git_commit      VARCHAR(40),
    triggered_by    VARCHAR(100) NOT NULL DEFAULT CURRENT_USER,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
    ON pipeline_runs (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON pipeline_runs (status)
    WHERE status IN ('RUNNING', 'FAILED');

COMMENT ON TABLE pipeline_runs IS
  'Audit log of pipeline invocations. Used for "did today run?", "which run produced this number?", and stale-RUNNING detection.';
COMMENT ON COLUMN pipeline_runs.input_file_sha IS
  'SHA256 of the input file. Lets us detect re-runs on the same input vs. genuinely new data.';
