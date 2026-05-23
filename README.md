<!-- Two versions below: English first, Vietnamese second. -->

# Amanotes — Data Engineer Case Study (Part 2)

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
| Duplicate `event_id` (Firebase at-least-once) | 112 | dedupe, keep first |
| Mixed timestamps: `Z` vs `+07:00` | ~50 / 50 | `pd.to_datetime(utc=True)` + timezone skew DQ check |
| Sessions spanning two UTC dates | 17 | each event attributed to its own date |

**Timezone split** is the most interesting finding. Both formats appear across all countries — not just VN — suggesting two SDK builds in the field writing timestamps differently. The pipeline handles both correctly; the `timezone_format_skew` DQ check alerts if the ratio shifts sharply in a future release.

**Bot threshold (500 events/day)** won't fire on this data (max user = 46 events). It's a safety net for future bot waves, not a tuned classifier.

**Tail-day sparsity** — 2025-04-08 has only 8 sessions, well below the week's median. The `tail_day_completeness` check warns (does not fail) when the last day looks like a cut export rather than a real DAU cliff.

## 4. What I deliberately skipped

- **No bot ML model.** Threshold + persistent blocklist is sufficient for v0; session-shape features (event regularity, missing `app_open`) are a v1 item.
- **No country normalisation.** Country isn't in the required metrics; noted for v1.
- **No dbt model.** Clean → aggregate separation makes porting straightforward when needed.
- **No real Flyway.** Flyway requires a JVM. The custom runner gives the same versioned-SQL workflow (checksum verification, `--status`, `--dry-run`) with zero extra runtime.

## 5. With one more hour

1. Unit tests for `_parse_timestamp` and the DQ thresholds.
2. CI step: `migrate.py --dry-run` + smoke migration against a throwaway DB on every PR.
3. `queries/dirty_rate_over_time.sql` as a starter dashboard query.

---

## Bonus: BigQuery at production scale

```sql
WITH cleaned AS (
  SELECT DATE(event_timestamp, 'UTC') AS event_date, user_id, session_id, event_id
  FROM `events_raw`
  WHERE event_timestamp BETWEEN @start_ts AND @end_ts
    AND user_id IS NOT NULL
    AND event_name IN ('app_open','level_start','level_complete','ad_impression','iap_purchase')
    AND event_timestamp <= CURRENT_TIMESTAMP()
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) = 1
)
SELECT event_date,
       COUNT(DISTINCT user_id)                        AS dau,
       COUNT(DISTINCT session_id)                     AS sessions,
       COUNT(*) / COUNT(DISTINCT session_id)          AS events_per_session
FROM cleaned GROUP BY event_date ORDER BY event_date;
```

Changes for a year of data: partition `events_raw` by date; cluster by `user_id`; use `APPROX_COUNT_DISTINCT` for DAU; materialise a dbt incremental roll-up so dashboards query rows, not billions of raw events.

---
---

<!-- VIETNAMESE VERSION -->

# Amanotes — Data Engineer Case Study (Phần 2)

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
| Timestamp format lẫn lộn: `Z` vs `+07:00` | ~50 / 50 | `pd.to_datetime(utc=True)` + DQ check timezone skew |
| Session vắt qua 2 ngày UTC | 17 | mỗi event tính theo ngày của timestamp của chính nó |

**Timezone split** là phát hiện thú vị nhất. Cả hai format xuất hiện ở mọi country, không riêng VN — cho thấy đang có hai SDK build trong thực tế ghi timestamp khác nhau. Pipeline xử lý được cả hai; DQ check `timezone_format_skew` sẽ cảnh báo nếu tỉ lệ thay đổi đột ngột ở release tiếp theo.

**Bot threshold (500 events/ngày)** sẽ không trigger với data này (user nhiều nhất = 46 events). Đây là safety net cho bot wave trong tương lai, không phải classifier được tinh chỉnh.

**Tail-day sparsity** — 2025-04-08 chỉ có 8 sessions, thấp hơn nhiều so với median cả tuần. Check `tail_day_completeness` sẽ WARN (không FAIL) khi ngày cuối trông như export bị cắt ngang thay vì DAU thật sự giảm.

## 4. Những gì tôi cố tình bỏ qua

- **Không có bot ML model.** Threshold + persistent blocklist là đủ cho v0; session-shape features là v1.
- **Không chuẩn hoá country.** Country không có trong required metrics; ghi chú cho v1.
- **Không có dbt model.** Cấu trúc clean → aggregate dễ port sang dbt khi cần.
- **Không dùng Flyway thật.** Flyway cần JVM. Custom runner cung cấp đủ workflow versioned-SQL (checksum verification, `--status`, `--dry-run`) không cần runtime thêm.

## 5. Nếu có thêm một tiếng

1. Unit tests cho `_parse_timestamp` và các DQ threshold.
2. CI step: `migrate.py --dry-run` + smoke migration chạy trên throwaway DB mỗi PR.
3. `queries/dirty_rate_over_time.sql` làm starter query cho dashboard.

---

## Bonus: BigQuery ở production scale

```sql
WITH cleaned AS (
  SELECT DATE(event_timestamp, 'UTC') AS event_date, user_id, session_id, event_id
  FROM `events_raw`
  WHERE event_timestamp BETWEEN @start_ts AND @end_ts
    AND user_id IS NOT NULL
    AND event_name IN ('app_open','level_start','level_complete','ad_impression','iap_purchase')
    AND event_timestamp <= CURRENT_TIMESTAMP()
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) = 1
)
SELECT event_date,
       COUNT(DISTINCT user_id)                        AS dau,
       COUNT(DISTINCT session_id)                     AS sessions,
       COUNT(*) / COUNT(DISTINCT session_id)          AS events_per_session
FROM cleaned GROUP BY event_date ORDER BY event_date;
```

Thay đổi cho một năm data: partition `events_raw` theo date; cluster theo `user_id`; dùng `APPROX_COUNT_DISTINCT` cho DAU; materialise dbt incremental roll-up để dashboard query rows thay vì hàng tỷ raw events.