-- V001__create_schema_migrations.sql
-- Tracks which migrations have been applied. Mirrors Flyway's flyway_schema_history.
-- Must be the first migration, since the runner uses this table to decide what else to run.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version         VARCHAR(20)   PRIMARY KEY,
    description     VARCHAR(255)  NOT NULL,
    script_filename VARCHAR(255)  NOT NULL,
    checksum        VARCHAR(64)   NOT NULL,
    applied_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    applied_by      VARCHAR(100)  NOT NULL DEFAULT CURRENT_USER,
    execution_time_ms INTEGER     NOT NULL,
    success         BOOLEAN       NOT NULL
);

COMMENT ON TABLE schema_migrations IS
  'Migration audit log. Rows are inserted by scripts/migrate.py after a migration runs.';
