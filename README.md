# Amanotes — Data Engineer Case Study (Part 2)

## 1. How to run

Python 3.11 or 3.12.

### Setup (one-time)

**Windows (cmd):**
```bat
setup.bat
.venv\Scripts\activate.bat
```

**macOS / Linux:**
```bash
bash setup.sh
source .venv/bin/activate
```

Both scripts create `.venv` and install pinned deps from
`requirements.txt`. They're idempotent — safe to re-run.

### Run the pipeline

```bash
# If the real events.jsonl isn't dropped into data/ yet:
python src/generate_sample_data.py

# Option A — quickest, no DB:
python src/daily_metrics.py --no-db
```

### Run with the metadata DB (recommended)

Postgres needs to be reachable; see `.env.example` for the connection
variables.

**Windows (cmd):**
```bat
copy .env.example .env
REM Edit .env if your Postgres credentials differ, then:
for /f "usebackq tokens=1,* delims==" %%a in (".env") do set %%a=%%b
python scripts\migrate.py
python src\daily_metrics.py
```

**macOS / Linux:**
```bash
cp .env.example .env
export $(grep -v '^#' .env | xargs)
python scripts/migrate.py
python src/daily_metrics.py
```

Output:
- `output/daily_user_metrics.csv` — the required table
- `output/cleaning_report.json` — counts of every cleaning step
- Postgres tables (if not `--no-db`): `pipeline_runs`, `cleaning_audit`,
  `dq_check_results`, `bot_users_blocklist`, `daily_user_metrics`

> The original `events.jsonl` from the case study link was not available
> locally, so `generate_sample_data.py` synthesizes a comparable sample
> (~5,000 rows, 7 days, 120 users) with the same kinds of mess a real
> Firebase export tends to have. If a real `events.jsonl` is dropped into
> `data/`, skip the generator — the pipeline is agnostic to how the file
> was produced.

## 2. What I built

A pipeline plus a metadata layer:

```
src/daily_metrics.py     load → profile → clean → aggregate → DQ → write
src/metadata_store.py    optional Postgres adapter (--no-db disables)
scripts/migrate.py       Flyway-style migration runner (custom, ~150 LOC)
migrations/V001..V006    SQL files for the metadata schema
```

Design choices worth flagging:

- **`CleaningReport` dataclass.** Every cleaning step writes a count into
  one object, which gets serialised to `cleaning_report.json` (for the
  CSV-only flow) and to `cleaning_audit` rows (for the DB flow). Same
  source of truth, two surfaces.
- **Per-row timestamp parsing** wrapped in try/except. The sample has
  four ISO formats (`+00:00`, `Z`, `.000Z`, naive). pandas handles them
  all with `utc=True`, but one bad value would otherwise poison the
  whole column — the wrapper drops only the offender.
- **Window derived from data** (`max_date - 6 days`). In production this
  would come from Airflow's `execution_date`, not from data.
- **Bot detection by daily event count** (>300 events/day). Simple,
  explainable, easy to tune. The detected bots are persisted in
  `bot_users_blocklist`, so the *next* run starts by pre-filtering known
  bots rather than re-discovering them every morning.
- **DQ checks return a list, not raise immediately.** Every check
  produces a `DqCheckResult` row, all of them are recorded in
  `dq_check_results`, and *then* the process exits 1 if any failed. The
  alternative (raise on first failure) loses the rows you need for
  trend dashboards — "the dirty rate has been creeping up for two
  weeks" is the warning that prevents the eventual page.
- **Idempotent metadata writes.** A failed run still leaves a row in
  `pipeline_runs` (status='FAILED' with the error message). Reruns get
  a new `run_id`; the `daily_user_metrics` PK is `(event_date, run_id)`
  so re-runs don't silently overwrite, and the
  `daily_user_metrics_latest` view gives the current numbers.

## 3. What I noticed in the data and how I handled it

The data has several real-world quirks worth flagging. The script
discovers them via profiling, not hard-coded knowledge.

| Issue                                       | Found | Handled by                              |
| ---                                         | ---   | ---                                     |
| Null `user_id`                              | 38    | drop (can't aggregate into DAU)         |
| Duplicate `event_id` (Firebase at-least-once) | 112 | dedupe, keep first                      |
| Mixed timestamp formats: `Z` vs `+07:00`    | ~50/50 | `pd.to_datetime(utc=True)` per row + DQ skew check |
| Sessions spanning two UTC dates             | 17    | each event attributed to its own date; both days count the session |
| Inconsistent country casing (none observed in this sample) | 0 | tolerant code path kept for safety |
| Bot users                                   | 0    | none in this sample; max user has 46 events |

A few details that are worth pulling out:

**The timezone split is the most interesting finding.** Roughly half
the events carry a `Z` (UTC) suffix and half carry `+07:00` (Vietnam
offset), and the split is *not* correlated with country — VN users
have both formats, US users have both formats. The pattern is consistent
with two SDK builds in the field writing timestamps differently. The
pipeline parses both correctly with `pd.to_datetime(utc=True)`, but a
new `timezone_format_skew` DQ check exists so we get an early alert if
a future SDK release flips the ratio sharply.

**Sessions crossing midnight (17 of them).** Each event is attributed
to the date of its own timestamp, which means a session that starts at
23:50 and ends at 00:30 contributes to both days' `sessions` count.
For DAU / sessions / EPS this is the right call — the alternative
("attribute every event to its session-start date") would mean some
events count toward a different day than their timestamp.

**Bot threshold is a safety net, not a tuned classifier.** Max
user-event-count in this sample is 46 (p99 = 37). The threshold of 500
won't fire on this data; it's there so a future bot wave doesn't reach
the metrics layer. Lower it after the first real wave teaches us where
organic traffic ends.

**Edge dates with thin traffic.** The 7-day window will sometimes
include a day where the export was cut mid-stream (e.g. only the first
hour or two), making that day look like a DAU cliff. The new
`tail_day_completeness` check warns when the last day has <20% of the
median daily events — it doesn't fail the run, but the count lands in
`dq_check_results` so a trend dashboard surfaces it.

A subtle point about what does *not* show up as "dirty": events
legitimately outside the analysis window, and bots, are dropped but
counted separately from quality issues like nulls and duplicates. The
DQ thresholds reflect that distinction (`dirty_ratio` and `bot_ratio`
are separate checks with separate budgets).

## 4. What I deliberately skipped and why

- **No proper bot model.** A daily-count threshold catches the obvious
  bots in this sample. Real bot detection wants session-shape features
  (event regularity, no `app_open` before activity, identical timings)
  and is a separate project. v0 ships the dumb threshold + a blocklist
  so escaped bots only need to be detected once.
- **Country normalisation.** The data has `vn`/`VN`/`Vietnam`/null
  inconsistency, but country isn't in the required metrics. Adding a
  lookup table here would be scope creep; noted for v1.
- **Sessions crossing midnight.** Each event is attributed to the date
  of its own timestamp, which means a session crossing midnight
  contributes to both days' `sessions` count. For DAU / sessions / EPS
  this is fine.
- **No dbt model.** The spec mentions our stack includes dbt; for a v0
  one-shot script the overhead isn't worth it. The clean → aggregate
  separation makes it straightforward to port.
- **No real Flyway / Liquibase.** Flyway is a JVM tool — bringing in a
  Java runtime for one feature is the kind of complexity I'd push back
  on in a review. `scripts/migrate.py` (~150 LOC, zero non-Python deps
  beyond psycopg) gives the same versioned-SQL workflow with checksum
  verification, `--status`, and `--dry-run`. Real production would
  re-evaluate this if the migration set ever needed plugin features
  (callbacks, baselining, multi-schema).

## 5. With one more hour

In priority order:

1. **Unit tests** for `_parse_timestamp`, the DQ thresholds, and the
   migration runner's checksum check. Right now correctness is "I read
   the diff and the numbers look right", which doesn't survive the
   next engineer touching it.
2. **A migrations CI step** that runs `migrate.py --dry-run` plus a
   smoke `migrate.py` against a throwaway DB on every PR. Catches the
   bad-SQL-merged-to-main class of incident.
3. **A simple metrics-trend query** committed alongside `migrations/`
   (e.g. `queries/dirty_rate_over_time.sql`) so the dashboards have a
   shared starting point.
4. **A small Mermaid pipeline diagram** in the README.

---

## Bonus: how would this query behave on a year of production data in BigQuery?

The pandas script as written is in-memory and would die at production
scale. The equivalent BigQuery query is straightforward:

```sql
WITH cleaned AS (
  SELECT
    DATE(event_timestamp, 'UTC') AS event_date,
    user_id, session_id, event_id
  FROM `events_raw`
  WHERE event_timestamp BETWEEN @start_ts AND @end_ts
    AND user_id IS NOT NULL AND user_id != ''
    AND event_name IN ('app_open','level_start','level_complete','ad_impression','iap_purchase')
    AND event_timestamp <= CURRENT_TIMESTAMP()
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) = 1
)
SELECT
  event_date,
  COUNT(DISTINCT user_id)    AS dau,
  COUNT(DISTINCT session_id) AS sessions,
  COUNT(*) / COUNT(DISTINCT session_id) AS events_per_session
FROM cleaned
GROUP BY event_date
ORDER BY event_date;
```

What I'd change for cost on a year of data:

1. **Partition `events_raw` by `DATE(event_timestamp)`.** Without
   partition pruning, every dashboard refresh scans 365 days.
2. **Cluster by `user_id`** to make the DISTINCT cheaper.
3. **Use `APPROX_COUNT_DISTINCT(user_id)`** for long-range DAU. HLL is
   accurate to ~1% and dramatically cheaper than exact for hundreds of
   millions of users.
4. **Materialise a daily roll-up table** (`daily_user_metrics`) as a dbt
   incremental model. Dashboards query the rolled-up table, not raw
   events.
5. **Move DQ from "fail the run" to a separate `dbt test` step** so a
   broken DQ doesn't block fresh metrics — it opens a Slack alert and
   quarantines the affected partition instead.
