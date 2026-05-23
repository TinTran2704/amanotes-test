"""
daily_metrics.py — produce daily_user_metrics from events.jsonl.

Usage:
    python src/daily_metrics.py [--input PATH] [--output PATH] [--no-db]

Defaults read data/events.jsonl and write output/daily_user_metrics.csv.

Pipeline:
    load → profile → clean → aggregate → data-quality check → write

By default the run is also recorded in Postgres (see migrations/ and
src/metadata_store.py): a pipeline_runs row, per-reason cleaning_audit,
per-check dq_check_results, the persistent bot blocklist, and a copy of
daily_user_metrics. Pass --no-db to skip Postgres entirely — the CSV
output is identical either way.

If anything looks wrong-shaped (zero rows, schema drift, too many drops),
the run fails loudly with a non-zero exit code so an Airflow task would
catch it and the metadata row is finalised as FAILED.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("daily_metrics")


# --------------------------------------------------------------------------
# Config — kept at top so it's the first thing a reader sees.
# Threshold choices are documented in README §3.
# --------------------------------------------------------------------------

REQUIRED_COLUMNS = {
    "event_id", "user_id", "event_name", "event_timestamp",
    "session_id", "app_version", "country", "device_platform",
}
VALID_EVENT_NAMES = {
    "app_open", "level_start", "level_complete",
    "ad_impression", "iap_purchase",
}

# Bot detection threshold. Set deliberately well above p99 of the current
# user-event-count distribution (which sits around ~40/day) so this is a
# *safety net* for future bot waves, not a tuned classifier. A real bot
# attack tends to produce hundreds-to-thousands of events per id per day;
# anything organic stays comfortably below 500. Lower this if a future
# anomaly review shows real bots slipping under.
BOT_EVENT_THRESHOLD = 500

MAX_DIRTY_FRACTION = 0.10        # if >10% of in-window rows are dirty, fail
MAX_BOT_FRACTION = 0.25          # if >25% of in-window rows are bots, fail
EXPECTED_MIN_DAYS = 5            # we expect at least this many days of data

# Timezone-format split check. The current data carries two ISO-8601
# variants — UTC ('Z') and Vietnam offset ('+07:00') — at roughly 50/50.
# If the ratio drifts past this threshold, it usually means a client SDK
# release has changed how timestamps are serialized. The pipeline still
# parses both correctly (utc=True normalises), but a sudden shift is
# worth surfacing because it's the kind of upstream change that quietly
# breaks downstream session-attribution joins on the local-time side.
MAX_TIMEZONE_SKEW = 0.30         # |ratio − 0.5| > this → warn      # we expect at least this many days of data


@dataclass
class CleaningReport:
    """Tracks how many rows we touched at each step. Surfaces in logs, in
    a sidecar JSON, and (if --no-db isn't set) in cleaning_audit."""
    rows_loaded: int = 0
    rows_unparseable_json: int = 0
    rows_missing_required_field: int = 0
    rows_bad_timestamp: int = 0
    rows_future_timestamp: int = 0
    rows_outside_window: int = 0
    rows_null_user_id: int = 0
    rows_null_event_name: int = 0
    rows_invalid_event_name: int = 0
    rows_duplicate_event_id: int = 0
    rows_bot_user: int = 0
    rows_final: int = 0
    bot_users_detected: list = field(default_factory=list)
    bot_peaks: dict = field(default_factory=dict)  # user_id -> peak daily events
    sessions_crossing_midnight: int = 0
    tz_utc_count: int = 0        # raw timestamps ending in 'Z'
    tz_offset_count: int = 0     # raw timestamps with explicit non-UTC offset
    window_start: str | None = None
    window_end: str | None = None


@dataclass
class DqCheckResult:
    name: str
    metric_value: float | None
    threshold: float | None
    passed: bool
    message: str = ""


# --------------------------------------------------------------------------
# 1. Load — robust to malformed lines
# --------------------------------------------------------------------------

def load_events(path: Path, report: CleaningReport) -> pd.DataFrame:
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                report.rows_unparseable_json += 1
                log.warning("Skipping unparseable JSON at line %d: %s", line_num, e)
    df = pd.DataFrame(records)
    report.rows_loaded = len(df)
    log.info("Loaded %d events from %s", len(df), path.name)
    return df


# --------------------------------------------------------------------------
# 2. Profile — what's actually in the data
# --------------------------------------------------------------------------

def profile(df: pd.DataFrame) -> None:
    log.info("--- input profile ---")
    log.info("rows=%d, cols=%s", len(df), sorted(df.columns))
    log.info("unique event_id=%d (%.1f%% duplicates)",
             df["event_id"].nunique(),
             100 * (1 - df["event_id"].nunique() / len(df)))
    log.info("null user_id=%d", df["user_id"].isna().sum() + (df["user_id"] == "").sum())
    log.info("null event_name=%d",
             df["event_name"].isna().sum() + (df["event_name"] == "").sum())
    log.info("event_name distribution:\n%s",
             df["event_name"].value_counts(dropna=False).to_string())
    log.info("---------------------")


# --------------------------------------------------------------------------
# 3. Clean — each step logged, each step idempotent
# --------------------------------------------------------------------------

def _parse_timestamp(value: object) -> datetime | None:
    """Handle the timestamp-format zoo. Always return UTC-aware datetime, else None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return pd.to_datetime(value, utc=True, errors="raise").to_pydatetime()
    except (ValueError, TypeError):
        return None


def clean(
    df: pd.DataFrame,
    report: CleaningReport,
    known_bots: set[str] | None = None,
) -> pd.DataFrame:
    """Clean the input. known_bots, if provided, is the persisted blocklist —
    those users are dropped before fresh bot detection runs, so re-detection
    counts only new bots."""
    known_bots = known_bots or set()

    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Input missing required columns: {missing_cols}")

    before = len(df)
    df = df[df["event_id"].notna() & (df["event_id"] != "")]
    report.rows_missing_required_field += before - len(df)

    df = df.copy()
    raw_ts = df["event_timestamp"].astype(str)
    report.tz_utc_count = int(raw_ts.str.endswith("Z").sum())
    report.tz_offset_count = int(raw_ts.str.contains(r"[+-]\d{2}:\d{2}$", regex=True).sum())

    df["event_timestamp"] = df["event_timestamp"].apply(_parse_timestamp)
    bad_ts_mask = df["event_timestamp"].isna()
    report.rows_bad_timestamp = int(bad_ts_mask.sum())
    df = df[~bad_ts_mask]

    now = datetime.now(timezone.utc)
    future_cutoff = pd.Timestamp(now) + pd.Timedelta(days=1)
    future_mask = df["event_timestamp"] > future_cutoff
    report.rows_future_timestamp = int(future_mask.sum())
    if future_mask.any():
        log.warning("Dropping %d events with future timestamps (>%s)",
                    future_mask.sum(), future_cutoff.isoformat())
    df = df[~future_mask]

    df["event_date"] = df["event_timestamp"].dt.date
    max_date = df["event_date"].max()
    min_date = (pd.Timestamp(max_date) - pd.Timedelta(days=6)).date()
    in_window = (df["event_date"] >= min_date) & (df["event_date"] <= max_date)
    report.rows_outside_window = int((~in_window).sum())
    report.window_start = str(min_date)
    report.window_end = str(max_date)
    if (~in_window).any():
        log.warning("Dropping %d events outside [%s, %s]",
                    (~in_window).sum(), min_date, max_date)
    df = df[in_window]

    null_uid_mask = df["user_id"].isna() | (df["user_id"] == "")
    report.rows_null_user_id = int(null_uid_mask.sum())
    df = df[~null_uid_mask]

    null_name_mask = df["event_name"].isna() | (df["event_name"] == "")
    report.rows_null_event_name = int(null_name_mask.sum())
    df = df[~null_name_mask]

    invalid_name_mask = ~df["event_name"].isin(VALID_EVENT_NAMES)
    report.rows_invalid_event_name = int(invalid_name_mask.sum())
    if invalid_name_mask.any():
        unknown = df.loc[invalid_name_mask, "event_name"].value_counts().to_dict()
        log.warning("Dropping events with unknown event_name: %s", unknown)
    df = df[~invalid_name_mask]

    before = len(df)
    df = df.drop_duplicates(subset="event_id", keep="first")
    report.rows_duplicate_event_id = before - len(df)

    # Bot handling, two stages:
    #   (a) Drop anything from the persisted blocklist — those are known bots.
    #   (b) Detect *new* bots by daily-event-count threshold from what remains.
    blocklist_mask = df["user_id"].isin(known_bots)
    blocklist_drop = int(blocklist_mask.sum())
    df = df[~blocklist_mask]

    daily_counts = df.groupby(["user_id", "event_date"]).size().reset_index(name="n")
    bot_pairs = daily_counts[daily_counts["n"] > BOT_EVENT_THRESHOLD]
    new_bots = bot_pairs["user_id"].unique().tolist()
    # Peak daily event count per detected bot, used when we upsert the blocklist.
    bot_peaks = bot_pairs.groupby("user_id")["n"].max().to_dict()
    if new_bots:
        bot_mask = df["user_id"].isin(new_bots)
        detected_drop = int(bot_mask.sum())
        log.warning("New bots detected (>%d events/day): %s",
                    BOT_EVENT_THRESHOLD, new_bots)
        df = df[~bot_mask]
    else:
        detected_drop = 0

    report.rows_bot_user = blocklist_drop + detected_drop
    report.bot_users_detected = new_bots
    report.bot_peaks = {uid: int(n) for uid, n in bot_peaks.items()}

    # Surface sessions that straddle a UTC date boundary. Each event is still
    # attributed to its own date, so these sessions contribute to two days'
    # session counts. Documented in the README; logged so a reader can see
    # the count without re-running the diagnostic.
    session_date_spans = df.groupby("session_id")["event_date"].nunique()
    report.sessions_crossing_midnight = int((session_date_spans > 1).sum())
    if report.sessions_crossing_midnight:
        log.info("%d session(s) span >1 UTC date — counted in both days",
                 report.sessions_crossing_midnight)

    report.rows_final = len(df)
    return df


# --------------------------------------------------------------------------
# 4. Aggregate — produce the required table
# --------------------------------------------------------------------------

def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """Build daily_user_metrics: event_date, dau, sessions, events_per_session, new_users."""
    daily = df.groupby("event_date").agg(
        dau=("user_id", "nunique"),
        sessions=("session_id", "nunique"),
        events=("event_id", "count"),
    ).reset_index()
    daily["events_per_session"] = (daily["events"] / daily["sessions"]).round(2)
    daily = daily.drop(columns=["events"])

    first_seen = df.groupby("user_id")["event_date"].min().reset_index()
    new_users = first_seen.groupby("event_date").size().reset_index(name="new_users")
    daily = daily.merge(new_users, on="event_date", how="left")
    daily["new_users"] = daily["new_users"].fillna(0).astype(int)

    return daily.sort_values("event_date").reset_index(drop=True)


# --------------------------------------------------------------------------
# 5. Data-quality checks
# --------------------------------------------------------------------------

def run_dq_checks(daily: pd.DataFrame, report: CleaningReport) -> list[DqCheckResult]:
    """Return a list of DqCheckResult, one per check. Caller decides whether
    to record them in the DB; caller also decides whether a failed check
    raises (currently: yes, at the bottom of main)."""
    results = []
    rows_in_window = report.rows_loaded - report.rows_outside_window
    rows_truly_dirty = (
        report.rows_missing_required_field
        + report.rows_bad_timestamp
        + report.rows_future_timestamp
        + report.rows_null_user_id
        + report.rows_null_event_name
        + report.rows_invalid_event_name
        + report.rows_duplicate_event_id
    )
    dirty_ratio = rows_truly_dirty / rows_in_window if rows_in_window else 1.0
    bot_ratio = report.rows_bot_user / rows_in_window if rows_in_window else 0.0

    results.append(DqCheckResult(
        name="dirty_ratio",
        metric_value=round(dirty_ratio, 4),
        threshold=MAX_DIRTY_FRACTION,
        passed=dirty_ratio <= MAX_DIRTY_FRACTION,
        message=f"{rows_truly_dirty}/{rows_in_window} in-window rows dirty",
    ))
    results.append(DqCheckResult(
        name="bot_ratio",
        metric_value=round(bot_ratio, 4),
        threshold=MAX_BOT_FRACTION,
        passed=bot_ratio <= MAX_BOT_FRACTION,
        message=f"{report.rows_bot_user}/{rows_in_window} in-window rows from bots",
    ))
    results.append(DqCheckResult(
        name="day_coverage",
        metric_value=float(len(daily)),
        threshold=float(EXPECTED_MIN_DAYS),
        passed=len(daily) >= EXPECTED_MIN_DAYS,
        message=f"{len(daily)} days in output",
    ))
    eps_ok = not (daily["events_per_session"] < 1).any()
    results.append(DqCheckResult(
        name="events_per_session_invariant",
        metric_value=float(daily["events_per_session"].min()) if len(daily) else None,
        threshold=1.0,
        passed=eps_ok,
        message="events_per_session must be >= 1 for every day",
    ))

    # Timezone-format skew. Hard-fail-worthy only if the split has moved
    # massively from its baseline — the pipeline parses both variants
    # correctly, so this is an upstream-alert, not a correctness alert.
    tz_total = report.tz_utc_count + report.tz_offset_count
    if tz_total > 0:
        utc_share = report.tz_utc_count / tz_total
        skew = abs(utc_share - 0.5)
        results.append(DqCheckResult(
            name="timezone_format_skew",
            metric_value=round(skew, 4),
            threshold=MAX_TIMEZONE_SKEW,
            passed=skew <= MAX_TIMEZONE_SKEW,
            message=f"UTC share={utc_share:.2%} "
                    f"(Z={report.tz_utc_count}, offset={report.tz_offset_count})",
        ))

    # Tail-day sparsity check. A "last day" with far fewer events than the
    # rolling median usually means data was cut at file-export time, not a
    # real DAU collapse. Warning-only — we don't fail the run, but the
    # number lands in dq_check_results for the trend dashboard.
    if len(daily) >= 3:
        median_daily_events = (daily["sessions"] * daily["events_per_session"]).median()
        tail = daily.iloc[-1]
        tail_events = tail["sessions"] * tail["events_per_session"]
        tail_ratio = tail_events / median_daily_events if median_daily_events else 1.0
        results.append(DqCheckResult(
            name="tail_day_completeness",
            metric_value=round(tail_ratio, 4),
            threshold=0.20,
            passed=tail_ratio >= 0.20,
            message=f"tail day ({tail['event_date']}) has "
                    f"{tail_events:.0f} events vs median {median_daily_events:.0f}",
        ))

    for r in results:
        verdict = "OK" if r.passed else "FAIL"
        log.info("DQ %s: %s (value=%s, threshold=%s) — %s",
                 verdict, r.name, r.metric_value, r.threshold, r.message)
    return results


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    here = Path(__file__).parent.parent
    parser.add_argument("--input", type=Path, default=here / "data" / "events.jsonl")
    parser.add_argument("--output", type=Path,
                        default=here / "output" / "daily_user_metrics.csv")
    parser.add_argument("--report", type=Path,
                        default=here / "output" / "cleaning_report.json")
    parser.add_argument("--no-db", action="store_true",
                        help="Skip all Postgres writes. CSV output is unchanged.")
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Optional metadata layer. Imported lazily so --no-db works on machines
    # without psycopg installed.
    use_db = not args.no_db
    meta = None
    conn = None
    run_id: int | None = None
    if use_db:
        try:
            from src import metadata_store as meta  # type: ignore
        except ImportError:
            import metadata_store as meta  # type: ignore
        try:
            conn_ctx = meta.connect()
            conn = conn_ctx.__enter__()
            run_id = meta.start_run(conn, args.input)
        except Exception as e:
            log.warning("Could not connect to metadata DB: %s. Continuing with --no-db semantics.", e)
            use_db = False
            conn = None
            meta = None

    report = CleaningReport()
    try:
        df_raw = load_events(args.input, report)
        profile(df_raw)

        known_bots = meta.fetch_known_bots(conn) if use_db else set()
        if known_bots:
            log.info("Pre-filtering %d known bots from blocklist", len(known_bots))

        df_clean = clean(df_raw, report, known_bots=known_bots)
        daily = aggregate(df_clean)
        dq_results = run_dq_checks(daily, report)

        # Always write the CSV + JSON, even if a DQ check fails — having the
        # artefacts on disk helps debugging.
        daily.to_csv(args.output, index=False)
        log.info("Wrote %s", args.output)
        args.report.write_text(json.dumps(report.__dict__, indent=2, default=str))
        log.info("Wrote %s", args.report)

        # Persist metadata for this run.
        if use_db:
            meta.record_cleaning_audit(conn, run_id, report)
            for r in dq_results:
                meta.record_dq_check(conn, run_id, r.name, r.metric_value,
                                     r.threshold, r.passed, r.message)
            if report.bot_users_detected:
                meta.upsert_bots(
                    conn, run_id,
                    [(uid, report.bot_peaks.get(uid, 0))
                     for uid in report.bot_users_detected],
                    detection_reason=f">{BOT_EVENT_THRESHOLD} events/day",
                )
            meta.write_daily_metrics(conn, run_id, daily)

        # Fail the process only on hard-failure checks. Warnings (tail-day
        # sparsity, timezone skew) still land in dq_check_results — the
        # trend dashboard catches them — but they don't block today's run.
        WARNING_ONLY_CHECKS = {"tail_day_completeness", "timezone_format_skew"}
        hard_failed = [r for r in dq_results
                       if not r.passed and r.name not in WARNING_ONLY_CHECKS]
        warned = [r for r in dq_results
                  if not r.passed and r.name in WARNING_ONLY_CHECKS]
        for r in warned:
            log.warning("DQ WARN: %s = %s (threshold %s) — %s",
                        r.name, r.metric_value, r.threshold, r.message)
        if hard_failed:
            messages = "; ".join(f"{r.name}={r.metric_value} vs {r.threshold}"
                                 for r in hard_failed)
            raise AssertionError(f"DQ FAIL: {messages}")

        if use_db:
            meta.finish_run_success(conn, run_id, report)

        print()
        print("daily_user_metrics:")
        print(daily.to_string(index=False))

    except Exception as e:
        if use_db and run_id is not None:
            try:
                meta.finish_run_failure(conn, run_id, str(e))
            except Exception as inner:
                log.error("Could not record failure to metadata DB: %s", inner)
        if isinstance(e, AssertionError):
            log.error("%s", e)
            sys.exit(1)
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
