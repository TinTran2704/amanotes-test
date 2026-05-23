-- V004__create_dq_check_results.sql
-- Every DQ check writes a row here every run. This is the source of truth for
-- "has the dirty rate been creeping up over the last month?" — questions you
-- can't answer from a single run's exit code.

CREATE TABLE IF NOT EXISTS dq_check_results (
    check_id        BIGSERIAL    PRIMARY KEY,
    run_id          BIGINT       NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    check_name      VARCHAR(100) NOT NULL,
    metric_value    NUMERIC(10,4),
    threshold       NUMERIC(10,4),
    passed          BOOLEAN      NOT NULL,
    message         TEXT,
    recorded_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_check_results_run_id
    ON dq_check_results (run_id);

CREATE INDEX IF NOT EXISTS idx_dq_check_results_check_name
    ON dq_check_results (check_name, recorded_at DESC);

COMMENT ON TABLE dq_check_results IS
  'Every DQ check writes one row here per run. Supports trend dashboards and early-warning alerts on metrics that are still passing but moving the wrong way.';
