-- V006__create_daily_user_metrics.sql
-- The output of the pipeline, persisted. The CSV is kept too (spec asked for
-- one) but this table is the source of truth — it carries the run_id back-
-- reference so we can answer "which run produced this number?" forever.
--
-- The (event_date, run_id) PK is intentional: it lets us keep history of
-- re-runs (e.g. backfills) on the same date for audit, rather than silently
-- overwriting.

CREATE TABLE IF NOT EXISTS daily_user_metrics (
    event_date          DATE          NOT NULL,
    run_id              BIGINT        NOT NULL REFERENCES pipeline_runs(run_id),
    dau                 INTEGER       NOT NULL,
    sessions            INTEGER       NOT NULL,
    events_per_session  NUMERIC(10,2) NOT NULL,
    new_users           INTEGER       NOT NULL DEFAULT 0,
    recorded_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_date, run_id),

    -- Invariants — surface bad aggregation at write time, not read time.
    CONSTRAINT chk_dau_positive          CHECK (dau >= 0),
    CONSTRAINT chk_sessions_positive     CHECK (sessions >= 0),
    CONSTRAINT chk_eps_at_least_one      CHECK (events_per_session >= 1),
    CONSTRAINT chk_new_users_le_dau      CHECK (new_users <= dau)
);

CREATE INDEX IF NOT EXISTS idx_daily_user_metrics_event_date
    ON daily_user_metrics (event_date DESC);

-- Convenience view: the latest run's numbers per date. Most dashboards
-- want this, not the full re-run history.
CREATE OR REPLACE VIEW daily_user_metrics_latest AS
SELECT DISTINCT ON (event_date)
    event_date, run_id, dau, sessions, events_per_session, new_users, recorded_at
FROM daily_user_metrics
ORDER BY event_date DESC, run_id DESC;

COMMENT ON TABLE daily_user_metrics IS
  'Persisted daily aggregates. Keyed on (event_date, run_id) to preserve re-run history. Query daily_user_metrics_latest for the current view.';
