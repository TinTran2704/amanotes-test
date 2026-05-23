-- V005__create_bot_users_blocklist.sql
-- Bots stay bots. Once we've decided a user_id is a bot, we want to remember
-- that across runs rather than re-discovering it from scratch every morning.
--
-- The first_detected_run_id lets us reconstruct "when did this bot first appear?"
-- without scanning all of cleaning_audit.

CREATE TABLE IF NOT EXISTS bot_users_blocklist (
    user_id              VARCHAR(100) PRIMARY KEY,
    first_detected_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    first_detected_run_id BIGINT      NOT NULL REFERENCES pipeline_runs(run_id),
    last_seen_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    detection_reason     VARCHAR(100) NOT NULL,
    peak_daily_events    INTEGER,
    reviewed             BOOLEAN      NOT NULL DEFAULT FALSE,
    notes                TEXT
);

CREATE INDEX IF NOT EXISTS idx_bot_users_unreviewed
    ON bot_users_blocklist (reviewed)
    WHERE reviewed = FALSE;

COMMENT ON TABLE bot_users_blocklist IS
  'Persisted bot list. The pipeline filters by this on every run, and updates last_seen_at for each bot that shows up again. reviewed=FALSE rows are the analyst queue for human verification.';
COMMENT ON COLUMN bot_users_blocklist.reviewed IS
  'Set to TRUE manually after an analyst confirms (or rejects) the detection. Rejections are deleted, not flipped.';
