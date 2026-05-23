<!-- Two versions below: English first, Vietnamese second. -->

# Amanotes — Data Engineer Case Study (Part 2)

## Prerequisites
 
Everything the pipeline needs to run, and where to get it:
 
| Requirement | Version | Notes |
| --- | --- | --- |
| **Python** | 3.11 or 3.12 | [python.org/downloads](https://www.python.org/downloads/) — tick "Add to PATH" on Windows installer |
| **pip** | bundled with Python | Upgrade with `python -m pip install --upgrade pip` if needed |
| **PostgreSQL** | 14+ | [postgresql.org/download](https://www.postgresql.org/download/) — only needed if running with metadata DB (without `--no-db`) |
| **psql** | bundled with Postgres | Must be on PATH — the `prepare.bat` / `prepare.sh` script uses it to bootstrap the database |
| **Git** | any | To clone the repo |
 
**Python packages** are installed automatically by `setup.bat` / `setup.sh`:
 
| Package | Version | Purpose |
| --- | --- | --- |
| `pandas` | 2.2.3 | data loading, cleaning, aggregation |
| `psycopg[binary]` | 3.2.3 | Postgres driver for the metadata layer |
 
**Postgres is optional.** Run with `--no-db` to skip it entirely — only Python and pip are needed.
**Data preparation required.** Place the `events.jsonl` file in the `data/` folder and prepare your `.env` file (or copy `.env.example` to keep default local settings).
## 1. How to run

Python 3.11 or 3.12. Three commands from a fresh clone:

**Windows:**
```bat
setup.bat
prepare.bat
python src\daily_metrics.py --no-db
```

**macOS / Linux:**
```bash
bash setup.sh && source .venv/bin/activate
python src/daily_metrics.py --no-db
```

To also write metadata to Postgres, copy `.env.example` → `.env`, then
run `prepare.bat` (Windows) or `export $(grep -v '^#' .env | xargs) && python scripts/migrate.py` (Unix) before running the pipeline without `--no-db`.

Output:
- `output/daily_user_metrics.csv` — the required table
- `output/cleaning_report.json` — per-step drop counts
- Postgres (if not `--no-db`): `pipeline_runs`, `cleaning_audit`, `dq_check_results`, `bot_users_blocklist`, `daily_user_metrics`

## 2. What I built

```
src/daily_metrics.py     load → profile → clean → aggregate → DQ → write
src/metadata_store.py    optional Postgres adapter (--no-db disables it)
scripts/migrate.py       Flyway-style migration runner (~150 LOC, no JVM)
migrations/V001–V006     schema for the metadata layer
db/init.sql              idempotent database bootstrap
```

Key decisions:

- **`CleaningReport` dataclass** — every drop step writes a count into one object, serialised to both `cleaning_report.json` and `cleaning_audit` rows. One source of truth, two surfaces.
- **Per-row timestamp parsing** — data carries two ISO-8601 variants (`Z` and `+07:00`). Wrapping `pd.to_datetime(utc=True)` per row means one bad value drops only itself, not the whole column.
- **Analysis window = `max_date − 6 days`** — derived from data. In production this would come from Airflow's `execution_date`.
- **DQ checks collect first, raise after** — all results land in `dq_check_results` before the process exits, so trend dashboards see every check on every run, not just the first failure.
- **`(event_date, run_id)` PK on `daily_user_metrics`** — re-runs don't overwrite; `daily_user_metrics_latest` view gives the current numbers.

## 3. What I noticed in the data

| Issue | Count | Handled by |
| --- | --- | --- |
| Null `user_id` | 38 | drop |
| Duplicate `event_id, user_id` | 112 | dedupe, keep first |
| Mixed timestamps: `Z` vs `+07:00` | \~50 / 50 | `pd.to_datetime(utc=True)` + timezone skew DQ check |
| Sessions spanning two UTC dates | 17 | each event attributed to its own date |

**Timezone split** is the most interesting finding. Both formats appear across all countries — not just VN — suggesting two SDK builds in the field writing timestamps differently. The pipeline handles both correctly; the `timezone_format_skew` DQ check alerts if the ratio shifts sharply in a future release.

**Bot threshold (500 events/day)** won't fire on this data (max user = 46 events). It's a safety net for future bot waves, not a tuned classifier.

**Tail-day sparsity** — 2025-04-08 has only 8 sessions, well below the week's median. The `tail_day_completeness` check warns (does not fail) when the last day looks like a cut export rather than a real DAU cliff.

## 4. What I skipped

- **No bot ML model.** Threshold + persistent blocklist is sufficient for v0; session-shape features (event regularity, missing `app_open`) are a v1 item.
- **No country normalisation.** Country isn't in the required metrics; noted for v1.
- **No dbt model.** Clean → aggregate separation makes porting straightforward when needed

## 5. With more time

1. **Unit tests** for `_parse_timestamp` and the DQ thresholds.
2. **CI step**: `migrate.py --dry-run` + smoke migration against a throwaway DB on every PR.
3. **Incremental pipeline design** — the current script re-processes the full 7-day window every run. For production the right shape is:

```
events_raw (partitioned by date)
    │
    ▼  dbt incremental model — runs daily, processes only new partition
daily_user_metrics_incremental
    │  strategy: merge on (event_date)
    │  unique_key: event_date
    │  on_schema_change: append_new_columns
    ▼
daily_user_metrics_latest   ← dashboard reads here
```

The incremental model would look like this in dbt:

```sql
-- models/marts/daily_user_metrics.sql
{{ config(
    materialized = 'incremental',
    unique_key    = 'event_date',
    partition_by  = {'field': 'event_date', 'data_type': 'date'},
    cluster_by    = ['event_date']
) }}

with deduped as (
    select *
    from {{ source('firebase', 'events_raw') }}
    where user_id is not null
      and event_name in (
          'app_open','level_start','level_complete',
          'ad_impression','iap_purchase'
      )
    -- Incremental filter: only load today's partition on scheduled runs.
    -- On full-refresh (backfill), this clause is omitted automatically.
    {% if is_incremental() %}
      and date(event_timestamp) >= date_sub(current_date(), interval 2 day)
    {% endif %}
    qualify row_number() over (
        partition by event_id order by event_timestamp
    ) = 1
),

aggregated as (
    select
        date(event_timestamp, 'UTC')        as event_date,
        count(distinct user_id)             as dau,
        count(distinct session_id)          as sessions,
        count(*) / count(distinct session_id) as events_per_session,
        countif(is_new_user)                as new_users
    from deduped
    -- Join against a users dim to get first_seen_date for new_user flag.
    -- Left join so unknown users (null user_id already filtered) don't drop.
    left join {{ ref('dim_users') }} using (user_id)
    group by 1
)

select * from aggregated
```

Two-day lookback (`interval 2 day`) instead of one handles the main real-world edge case from this data: **all 112 duplicates land on the same UTC date as their original**, so deduping within a single-day window is safe here. If cross-date duplicates ever appear, widen the lookback to 3 days.

---

## Bonus: BigQuery at production scale

Numbers below are extrapolated from the actual sample (3,869 events, \~265 bytes/row, \~130 DAU) scaled to Amanotes's stated \~5M DAU.

**Estimated production volume:**

| Metric | Value |
| --- | --- |
| Daily events | \~21M |
| Yearly events | \~7.8B |
| Yearly raw data | \~2 TB |

**Cost without optimisation** — a single `SELECT *` over a year of unpartitioned data scans ~2 TB → **\~$10 per query** at BigQuery's $5/TB on-demand rate. A dashboard that auto-refreshes every 5 minutes costs \~$86k/day in scan alone.

**What to change, in priority order:**

**1. Partition `events_raw` by `DATE(event_timestamp)`.**
The most impactful change. A daily pipeline only needs yesterday's partition → scan drops from 2 TB to ~5.6 GB (\~$0.03/run). Partition pruning requires the `WHERE` clause to filter on the partition column directly — not inside a function — which is already the case in the query above.

**2. Cluster by `user_id`.**
`COUNT(DISTINCT user_id)` for DAU touches every row in the partition. Clustering means BigQuery co-locates rows with the same `user_id` on the same storage blocks, reducing the bytes read for the distinct scan by 30–60% depending on cardinality.

**3. Use `APPROX_COUNT_DISTINCT` for DAU on long date ranges.**
For single-day DAU the exact count is fine. For weekly/monthly rollups across hundreds of millions of users, HyperLogLog++ (`APPROX_COUNT_DISTINCT`) is accurate to \~1% and can be 5–10× cheaper than the exact version. On the current sample (max DAU = 216) the difference is invisible; at 5M DAU it becomes the difference between a query that finishes in seconds and one that times out.

**4. Materialise `daily_user_metrics` as a dbt incremental model.**
Dashboards query the roll-up table (7–365 rows), not `events_raw` (billions of rows). The incremental model runs once per day, merges on `event_date`, and costs \~$0.03. Every subsequent dashboard load costs $0.

**5. Handle duplicates correctly within the incremental window.**
From the data: all 112 duplicate `event_id` pairs fall on the **same UTC date** — no cross-partition duplicates. `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ...)` within a single-day partition therefore catches 100% of duplicates without needing a cross-partition scan. If this changes (e.g. a client SDK starts retrying across midnight), widen the dedup window in the incremental lookback from 1 to 3 days and add a DQ check that alerts when cross-date dup rate exceeds 0%.

**Query used in the incremental model above:**

```sql
-- Daily run: partition_date = CURRENT_DATE() - 1
-- Scans: ~5.6 GB (one day's partition, clustered by user_id)
-- Cost:  ~$0.03

with deduped as (
    select
        date(event_timestamp, 'UTC')  as event_date,
        user_id,
        session_id,
        event_id
    from `project.dataset.events_raw`
    where date(event_timestamp) = @partition_date   -- partition pruning
      and user_id is not null
      and event_name in (
          'app_open','level_start','level_complete',
          'ad_impression','iap_purchase'
      )
    qualify row_number() over (
        partition by event_id
        order by event_timestamp
    ) = 1                                            -- dedup within partition
)

select
    event_date,
    count(distinct user_id)                           as dau,
    count(distinct session_id)                        as sessions,
    safe_divide(
        count(*), count(distinct session_id)
    )                                                 as events_per_session
from deduped
group by event_date
```

`SAFE_DIVIDE` instead of `/` — on the current data sessions is never 0 after cleaning, but a future bug that produces zero-session days should return `NULL` rather than a division error that kills the whole run.

---
---

<!-- VIETNAMESE VERSION -->

# Amanotes — Data Engineer Case Study (Phần 2)

 
## Yêu cầu cài đặt
 
Những thứ cần có trước khi chạy pipeline:
 
| Yêu cầu | Phiên bản | Ghi chú |
| --- | --- | --- |
| **Python** | 3.11 hoặc 3.12 | [python.org/downloads](https://www.python.org/downloads/) — Windows nhớ tick "Add to PATH" khi cài |
| **pip** | đi kèm Python | Upgrade nếu cần: `python -m pip install --upgrade pip` |
| **PostgreSQL** | 14+ | [postgresql.org/download](https://www.postgresql.org/download/) — chỉ cần nếu chạy có metadata DB (không dùng `--no-db`) |
| **psql** | đi kèm Postgres | Phải có trong PATH — `prepare.bat` dùng để khởi tạo database |
| **Git** | bất kỳ | Để clone repo |
 
**Python packages** được cài tự động bởi `setup.bat` / `setup.sh`:
 
| Package | Phiên bản | Mục đích |
| --- | --- | --- |
| `pandas` | 2.2.3 | load, clean, aggregate data |
| `psycopg[binary]` | 3.2.3 | Postgres driver cho metadata layer |
 
**Postgres là tuỳ chọn.** Chạy với `--no-db` để bỏ qua — chỉ cần Python và pip.
**Files cần chuẩn bị.** Đặt `events.jsonl` file vào thư mục `data/` và chuẩn bị file `.env` file (hoặc copy `.env.example` giữ cài đặt local).
## 1. Cách chạy

Python 3.11 hoặc 3.12. Ba lệnh từ fresh clone:

**Windows:**
```bat
setup.bat
prepare.bat
python src\daily_metrics.py --no-db
```

**macOS / Linux:**
```bash
bash setup.sh && source .venv/bin/activate
python src/daily_metrics.py --no-db
```

Để ghi metadata vào Postgres, copy `.env.example` → `.env`, rồi chạy `prepare.bat` (Windows) hoặc `export $(grep -v '^#' .env | xargs) && python scripts/migrate.py` (Unix) trước khi chạy pipeline không có `--no-db`.

Output:
- `output/daily_user_metrics.csv` — bảng kết quả theo yêu cầu đề
- `output/cleaning_report.json` — số rows dropped ở từng bước
- Postgres (nếu không dùng `--no-db`): `pipeline_runs`, `cleaning_audit`, `dq_check_results`, `bot_users_blocklist`, `daily_user_metrics`

## 2. Những gì tôi đã xây dựng

```
src/daily_metrics.py     load → profile → clean → aggregate → DQ → write
src/metadata_store.py    Postgres adapter tuỳ chọn (--no-db sẽ tắt)
scripts/migrate.py       Migration runner kiểu Flyway (~150 LOC, không cần JVM)
migrations/V001–V006     Schema cho metadata layer
db/init.sql              Khởi tạo database idempotent
```

Các quyết định thiết kế chính:

- **`CleaningReport` dataclass** — mỗi bước drop ghi count vào một object, được serialize ra cả `cleaning_report.json` và các row trong `cleaning_audit`. Một nguồn sự thật, hai nơi hiển thị.
- **Parse timestamp từng row** — data có hai dạng ISO-8601 (`Z` và `+07:00`). Wrap `pd.to_datetime(utc=True)` theo từng row để một giá trị xấu chỉ drop mình nó, không làm hỏng cả cột.
- **Analysis window = `max_date − 6 ngày`** — lấy từ data. Trong production sẽ lấy từ `execution_date` của Airflow.
- **DQ checks collect trước, raise sau** — tất cả kết quả ghi vào `dq_check_results` trước khi process exit, giúp trend dashboard thấy đủ mọi check trên mọi lần chạy.
- **PK `(event_date, run_id)` trên `daily_user_metrics`** — re-run không ghi đè; view `daily_user_metrics_latest` cho số mới nhất.

## 3. Những điều phát hiện trong data

| Vấn đề | Số lượng | Xử lý |
| --- | --- | --- |
| `user_id` null | 38 | drop |
| `event_id` trùng (Firebase at-least-once) | 112 | dedupe, giữ lần đầu |
| Timestamp format lẫn lộn: `Z` vs `+07:00` | \~50 / 50 | `pd.to_datetime(utc=True)` + DQ check timezone skew |
| Session vắt qua 2 ngày UTC | 17 | mỗi event tính theo ngày của timestamp của chính nó |

**Timezone split** là phát hiện thú vị nhất. Cả hai format xuất hiện ở mọi country, không riêng VN — cho thấy đang có hai SDK build trong thực tế ghi timestamp khác nhau. Pipeline xử lý được cả hai; DQ check `timezone_format_skew` sẽ cảnh báo nếu tỉ lệ thay đổi đột ngột ở release tiếp theo.

**Bot threshold (500 events/ngày)** sẽ không trigger với data này (user nhiều nhất = 46 events). Đây là safety net cho bot wave trong tương lai, không phải classifier được tinh chỉnh.

**Tail-day sparsity** — 2025-04-08 chỉ có 8 sessions, thấp hơn nhiều so với median cả tuần. Check `tail_day_completeness` sẽ WARN (không FAIL) khi ngày cuối trông như export bị cắt ngang thay vì DAU thật sự giảm.

## 4. Những gì tôi bỏ qua

- **Không có bot ML model.** Threshold + persistent blocklist là đủ cho v0; session-shape features là v1.
- **Không chuẩn hoá country.** Country không có trong required metrics; ghi chú cho v1.
- **Không có dbt model.** Cấu trúc clean → aggregate dễ port sang dbt khi cần.

## 5. Nếu có thêm thời gian

1. **Unit tests** cho `_parse_timestamp` và các DQ threshold.
2. **CI step**: `migrate.py --dry-run` + smoke migration chạy trên throwaway DB mỗi PR.
3. **Incremental pipeline** — script hiện tại reprocess toàn bộ 7-day window mỗi lần chạy. Với production, shape đúng là:

```
events_raw (partitioned by date)
    │
    ▼  dbt incremental model — chạy daily, chỉ xử lý partition mới
daily_user_metrics_incremental
    │  strategy: merge on (event_date)
    │  unique_key: event_date
    │  on_schema_change: append_new_columns
    ▼
daily_user_metrics_latest   ← dashboard đọc ở đây
```

Model dbt trông như thế này:

```sql
-- models/marts/daily_user_metrics.sql
{{ config(
    materialized = 'incremental',
    unique_key    = 'event_date',
    partition_by  = {'field': 'event_date', 'data_type': 'date'},
    cluster_by    = ['event_date']
) }}

with deduped as (
    select *
    from {{ source('firebase', 'events_raw') }}
    where user_id is not null
      and event_name in (
          'app_open','level_start','level_complete',
          'ad_impression','iap_purchase'
      )
    {% if is_incremental() %}
      and date(event_timestamp) >= date_sub(current_date(), interval 2 day)
    {% endif %}
    qualify row_number() over (
        partition by event_id order by event_timestamp
    ) = 1
),

aggregated as (
    select
        date(event_timestamp, 'UTC')          as event_date,
        count(distinct user_id)               as dau,
        count(distinct session_id)            as sessions,
        count(*) / count(distinct session_id) as events_per_session,
        countif(is_new_user)                  as new_users
    from deduped
    left join {{ ref('dim_users') }} using (user_id)
    group by 1
)

select * from aggregated
```

Lookback 2 ngày (`interval 2 day`) thay vì 1 ngày để xử lý edge case: **toàn bộ 112 duplicate trong data đều nằm cùng UTC date với bản gốc** — không có cross-partition duplicate. Nếu sau này SDK retry qua midnight, mở rộng lookback lên 3 ngày và thêm DQ check cảnh báo khi tỉ lệ cross-date dup > 0%.

---

## Bonus: BigQuery ở production scale

Các con số dưới đây được extrapolate từ data thực tế (3,869 events, \~265 bytes/row, \~130 DAU) scale lên \~5M DAU của Amanotes.

**Ước tính volume production:**

| Metric | Giá trị |
| --- | --- |
| Events mỗi ngày | ~21M |
| Events mỗi năm | ~7.8B |
| Dung lượng raw data / năm | ~2 TB |

**Chi phí nếu không tối ưu** — một câu `SELECT *` quét toàn bộ 1 năm không partition sẽ scan \~2 TB → **\~$10/query** theo on-demand rate $5/TB của BigQuery. Dashboard auto-refresh 5 phút một lần sẽ tốn \~$86k/ngày chỉ tiền scan.

**Những gì cần thay đổi, theo thứ tự ưu tiên:**

**1. Partition `events_raw` theo `DATE(event_timestamp)`.**
Thay đổi có impact lớn nhất. Pipeline chạy daily chỉ cần đọc partition hôm qua → scan giảm từ 2 TB xuống \~5.6 GB (\~$0.03/lần chạy). Partition pruning yêu cầu mệnh đề `WHERE` filter trực tiếp trên cột partition, không bọc trong function — đã đáp ứng trong query trên.

**2. Cluster theo `user_id`.**
`COUNT(DISTINCT user_id)` cho DAU phải đọc mọi row trong partition. Clustering giúp BigQuery co-locate các row cùng `user_id` vào cùng storage block, giảm bytes đọc cho distinct scan 30–60% tuỳ cardinality.

**3. Dùng `APPROX_COUNT_DISTINCT` cho DAU trên date range dài.**
Cho DAU ngày đơn, exact count là đủ. Cho weekly/monthly rollup qua hàng trăm triệu user, HyperLogLog++ (`APPROX_COUNT_DISTINCT`) chính xác đến \~1% và có thể nhanh hơn 5–10× so với exact version. Trên data hiện tại (max DAU = 216) không thấy khác biệt; ở 5M DAU nó là ranh giới giữa query chạy trong vài giây và query timeout.

**4. Materialise `daily_user_metrics` thành dbt incremental model.**
Dashboard query bảng roll-up (7–365 rows), không query `events_raw` (hàng tỷ rows). Incremental model chạy 1 lần/ngày, merge theo `event_date`, tốn \~$0.03. Mọi lần load dashboard sau đó tốn $0.

**5. Xử lý duplicate đúng trong incremental window.**
Từ data thực tế: toàn bộ 112 cặp `event_id` trùng đều nằm **cùng UTC date** — không có cross-partition duplicate. `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ...)` trong một partition đơn ngày đã catch 100% duplicate mà không cần cross-partition scan.

**Query dùng trong incremental model:**

```sql
-- Daily run: partition_date = CURRENT_DATE() - 1
-- Scan:  ~5.6 GB (1 ngày partition, clustered by user_id)
-- Cost:  ~$0.03

with deduped as (
    select
        date(event_timestamp, 'UTC')  as event_date,
        user_id,
        session_id,
        event_id
    from `project.dataset.events_raw`
    where date(event_timestamp) = @partition_date   -- partition pruning
      and user_id is not null
      and event_name in (
          'app_open','level_start','level_complete',
          'ad_impression','iap_purchase'
      )
    qualify row_number() over (
        partition by event_id
        order by event_timestamp
    ) = 1
)

select
    event_date,
    count(distinct user_id)                           as dau,
    count(distinct session_id)                        as sessions,
    safe_divide(
        count(*), count(distinct session_id)
    )                                                 as events_per_session
from deduped
group by event_date
```

`SAFE_DIVIDE` thay vì `/` — trên data hiện tại sessions không bao giờ = 0 sau cleaning, nhưng một bug tương lai tạo ra zero-session day nên trả về `NULL` thay vì division error làm chết cả run.
