"""
metadata_store.py — writes pipeline state into the Postgres metadata DB.

This module is intentionally optional: daily_metrics.py works with or
without it. If Postgres isn't reachable (or --no-db is set), the
pipeline runs and writes the CSV exactly as before. With Postgres
available, every run is also recorded for audit and trend analysis.

The tables this writes to are created by migrations/V001..V006
(see scripts/migrate.py).
"""

from __future__ import annotations

import hashlib
import logging
import os
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable

import psycopg

log = logging.getLogger("metadata_store")


def _dsn() -> str:
    return (
        f"host={os.environ.get('PGHOST', 'localhost')} "
        f"port={os.environ.get('PGPORT', '5432')} "
        f"dbname={os.environ.get('PGDATABASE', 'amanotes')} "
        f"user={os.environ.get('PGUSER', 'postgres')} "
        f"password={os.environ.get('PGPASSWORD', 'postgres')}"
    )


@contextmanager
def connect():
    """Yields a psycopg connection. Caller decides commit/rollback boundaries."""
    with psycopg.connect(_dsn()) as conn:
        yield conn


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_commit() -> str | None:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL, text=True
        ).strip()
        return out[:40] if out else None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


# --------------------------------------------------------------------------
# Pipeline lifecycle: start_run → ... → finish_run (success or failure)
# --------------------------------------------------------------------------

def start_run(conn, input_file: Path) -> int:
    """Insert a RUNNING row, return run_id. Must be called before any other
    metadata write so child rows have a FK target."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_runs
                (status, input_file, input_file_sha, git_commit)
            VALUES ('RUNNING', %s, %s, %s)
            RETURNING run_id
        """, (str(input_file), _file_sha256(input_file), _git_commit()))
        run_id = cur.fetchone()[0]
    conn.commit()
    log.info("Started run_id=%d (input=%s)", run_id, input_file.name)
    return run_id


def finish_run_success(conn, run_id: int, report) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_runs
            SET finished_at = NOW(),
                status      = 'SUCCESS',
                rows_loaded = %s,
                rows_final  = %s,
                window_start = %s,
                window_end   = %s
            WHERE run_id = %s
        """, (report.rows_loaded, report.rows_final,
              report.window_start, report.window_end, run_id))
    conn.commit()
    log.info("Finished run_id=%d (SUCCESS)", run_id)


def finish_run_failure(conn, run_id: int, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_runs
            SET finished_at   = NOW(),
                status        = 'FAILED',
                error_message = %s
            WHERE run_id = %s
        """, (error_message[:2000], run_id))
    conn.commit()
    log.info("Finished run_id=%d (FAILED)", run_id)


# --------------------------------------------------------------------------
# Per-run audit writes
# --------------------------------------------------------------------------

def record_cleaning_audit(conn, run_id: int, report) -> None:
    """One row per drop reason. Zero-count reasons are skipped — keeps the
    table tidy and makes 'SELECT * WHERE run_id = N' immediately readable."""
    rows: list[tuple[int, str, int]] = []
    reason_fields = [
        ("unparseable_json",         report.rows_unparseable_json),
        ("missing_required_field",   report.rows_missing_required_field),
        ("bad_timestamp",            report.rows_bad_timestamp),
        ("future_timestamp",         report.rows_future_timestamp),
        ("outside_window",           report.rows_outside_window),
        ("null_user_id",             report.rows_null_user_id),
        ("null_event_name",          report.rows_null_event_name),
        ("invalid_event_name",       report.rows_invalid_event_name),
        ("duplicate_event_id",       report.rows_duplicate_event_id),
        ("bot_user",                 report.rows_bot_user),
    ]
    for reason, count in reason_fields:
        if count > 0:
            rows.append((run_id, reason, count))

    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO cleaning_audit (run_id, drop_reason, rows_dropped) "
            "VALUES (%s, %s, %s)",
            rows,
        )
    conn.commit()


def record_dq_check(
    conn, run_id: int, check_name: str,
    metric_value: float | None, threshold: float | None,
    passed: bool, message: str = "",
) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dq_check_results
                (run_id, check_name, metric_value, threshold, passed, message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (run_id, check_name, metric_value, threshold, passed, message))
    conn.commit()


# --------------------------------------------------------------------------
# Output: persist daily_user_metrics
# --------------------------------------------------------------------------

def write_daily_metrics(conn, run_id: int, daily_df) -> None:
    """Write each row of the daily_user_metrics DataFrame. The CSV file is
    still produced by the caller — this is the durable copy."""
    rows = [
        (row.event_date, run_id, int(row.dau), int(row.sessions),
         float(row.events_per_session), int(row.new_users))
        for row in daily_df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO daily_user_metrics
                (event_date, run_id, dau, sessions, events_per_session, new_users)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, rows)
    conn.commit()


# --------------------------------------------------------------------------
# Bot blocklist — read at the start, update at the end
# --------------------------------------------------------------------------

def fetch_known_bots(conn) -> set[str]:
    """Return user_ids previously flagged as bots. The pipeline filters by
    this set before its own detection runs, so a bot that goes quiet for a
    day still stays excluded."""
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM bot_users_blocklist")
        return {row[0] for row in cur.fetchall()}


def upsert_bots(
    conn, run_id: int, bots_seen_today: Iterable[tuple[str, int]],
    detection_reason: str,
) -> None:
    """For each bot detected this run, either insert a new row or update
    last_seen_at / peak_daily_events. Postgres' ON CONFLICT makes this one
    statement instead of a SELECT-then-INSERT race."""
    rows = [(uid, run_id, detection_reason, peak) for uid, peak in bots_seen_today]
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO bot_users_blocklist
                (user_id, first_detected_run_id, detection_reason, peak_daily_events)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
                SET last_seen_at      = NOW(),
                    peak_daily_events = GREATEST(
                        bot_users_blocklist.peak_daily_events,
                        EXCLUDED.peak_daily_events
                    )
        """, rows)
    conn.commit()
