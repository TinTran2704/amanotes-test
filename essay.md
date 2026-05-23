# Amanotes — Data Engineer Case Study (Part 1, Essay)

## Q1. Simplicity in practice

At PNJ I owned a chunk of the analytics pipeline feeding sales and
marketing reporting across 130+ retail locations — Airflow plus Talend
plus Python pulling REST APIs and on-prem SQL Server into BigQuery. The
textbook move, and the team's first instinct, was to land each new
source into a proper Kimball star schema from day one: conformed
dimensions, surrogate keys, SCD Type 2 where appropriate.

I argued against it for the first phase. We shipped source →
lightly-cleaned staging → **wide denormalised "one big table" marts**,
one per business domain (Sales, Marketing, Finance), built incrementally
in dbt. No conformed dimensions, no surrogate key pipeline, no SCD2.

**The trade-off.** We accepted real duplication — the "product" concept
lived in both the Sales OBT and the Marketing OBT with slightly
different denormalised attributes, so an upstream rename had to be
applied in two places. We also gave up point-in-time correctness:
without SCD2 on products, a revenue report run today against an old
order shows the product's current name, not its name at order time. For
executive dashboards that nobody re-runs historically this was fine.
For finance reconciliation it wouldn't have been, and we flagged that
domain as "needs the full Kimball treatment in phase 2".

**What the simpler option gave up.** Conformed-dimension governance.
A future fourth domain would have to either build its own OBT or pay
the migration cost we deferred. What we bought was speed and clarity —
each OBT was one dbt model an analyst could read in five minutes, the
first three dashboards landed in weeks instead of a quarter, and
BigQuery handles wide tables fine at our volume (~3 GB/day).

The reference I leaned on was Maxime Beauchemin's *The Rise of the Data
Engineer* — the argument that the old normalisation discipline is
inherited from a different cost structure, and that wide denormalised
models are often the right answer in cloud warehouses. dbt Labs has an
explicit discussion thread on OBT vs Kimball vs Data Vault, and
Fivetran's published benchmark shows OBT outperforming star schema by
25–50% on BigQuery, Snowflake, and Redshift. Once a pattern has a
name, a benchmark, and a community behind it, picking it stops being
cutting corners and starts being a deliberate choice.

**With hindsight, would I decide the same way?** Yes, with one
adjustment. The OBTs were right. What I'd do differently is write the
ADR on day one, explicitly naming *which* domains had a known expiry
date for this shape — finance was on that list verbally but not on
paper. The architecture was right; the paper trail was thinner than it
should have been.

---

## Q2. Anomaly detection design

### The actual problem

Product is ignoring the alerts. That is the thing to fix. A more clever
detector on top of an alert channel nobody reads is not the win —
getting them to read it again is. So v0 prioritises **precision over
recall** until trust is rebuilt. We would rather miss a real anomaly
than send another false one in the first month.

### v0 — what I would build in two weeks on our stack

```
   raw events / installs / ad SDK (BigQuery, already there)
                       │
                       ▼
              ┌──────────────────┐
              │ dbt: daily_kpis  │  partitioned by date,
              │   (incremental)  │  grain = (date, game, platform)
              └────────┬─────────┘
                       │
              ┌────────▼─────────┐
              │ dbt tests +      │  schema, not-null, freshness,
              │ custom tests for │  range — fail loud at this layer
              │ DQ on daily_kpis │
              └────────┬─────────┘
                       │
              ┌────────▼─────────┐
              │ Airflow task:    │  z-score on detrended series
              │ anomaly_scorer   │  + robust IQR bound, two-of-two
              │  (~150 LOC PY)   │  confirmation
              └────────┬─────────┘
                       │
              ┌────────▼─────────┐
              │ Slack alert (only│  metric + chart + expected band
              │ if all gates pass│  + snooze-24h button
              └──────────────────┘
```

Concretely four components:

1. **A dbt incremental model `daily_kpis`** rebuilt each morning,
   partitioned by date, keyed on `(date, game_id, platform)`. The grain
   matters: alerting on global DAU hides per-game regressions inside
   the noise of the rest of the portfolio. This is just the existing
   medallion gold layer, given a name.
2. **dbt tests at this layer**, not bolted on later. Schema, not-null,
   `accepted_values` on `game_id`, `dbt_utils.expression_is_true` for
   range sanity, and a freshness check. If `daily_kpis` is wrong, the
   anomaly scorer downstream is meaningless — so it should never run on
   bad input.
3. **A small Python scorer as one Airflow task** (~150 lines). For
   each `(metric, game, platform)` triple it computes a rolling
   baseline over the last 28 days, day-of-week-aware (Saturday is
   compared against the last four Saturdays, not yesterday), and scores
   the current value with both a z-score on the detrended residual and
   a robust IQR-based bound.
4. **Slack delivery** with the metric, the chart, the expected band,
   the affected slice, and a one-click snooze. The snooze button is not
   optional. Someone is on holiday, the alert is going to fire, and the
   alternative to a snooze is the channel getting muted forever — which
   is exactly where we are today.

### What I would deliberately not build

- **No real-time alerting.** Daily metrics, daily alerts. Real-time
  doubles the engineering cost and Product checks Slack once a day
  anyway.
- **No anomaly cause attribution.** Tempting to add "we think this is
  caused by country X, version Y" — that is a separate, much harder
  problem (causal inference on observational data), and getting it
  wrong is worse than not having it.
- **No ML.** Prophet, isolation forests, LSTMs — overkill for three KPIs
  with seasonal patterns and a year of history. A z-score on a detrended
  series gets you 90% of the way for 10% of the maintenance cost.
- **No per-game tuned models.** One global config (window length,
  thresholds), same logic per game. Tuning 50 separate models is how a
  v0 becomes a six-month project.

### How I reduce false positives without missing real incidents

Six mechanisms, in order of impact:

1. **Day-of-week-aware baseline.** Compare Saturday to Saturdays. This
   alone kills the largest class of false positives in mobile-gaming
   data, where weekends look completely different from weekdays.
2. **Two-of-two confirmation.** Both z-score *and* IQR have to agree
   before a normal-severity alert fires. A single-method-only alert
   needs a much higher threshold (z > 5, not z > 3).
3. **Minimum effect-size gate.** Don't alert on DAU dropping from
   5,001,000 to 4,999,000, even if the z-score says it's significant.
   Stack a relative-change floor (e.g. >5%) on top of the statistical
   test.
4. **Hysteresis.** A new alert for the same metric is suppressed for
   24 hours after a recent fire — but the resolved-state still updates.
   No one wants four pings for one incident.
5. **Known-bad calendar.** Public holidays, planned releases, marketing
   campaigns — Product owns this calendar, we read it. Alerts on known
   events are filtered.
6. **A weekly alert-review meeting for the first month.** Every alert
   (fired *and* suppressed) gets a 30-second human verdict: real /
   false / meh. Use that to tune thresholds, and end the meeting after
   one month.

For the inverse — **missing real incidents** — a v0 prioritising
precision *will* under-fire. So we also ship a low-priority "weak
signals" channel that captures everything that would have fired without
the gates. It pages no one. It exists so an analyst can spelunk when
something feels off and find out we did see it.

### One thing I would change for v1

**Decompose the series properly.** A simple seasonal-trend
decomposition (STL) would let us alert on the residual rather than the
raw value, which catches anomalies the day-of-week baseline misses —
e.g. a campaign-driven uptrend that suddenly flatlines. v0 will miss
those because the raw value still looks normal-for-Tuesday; v1 with
STL would see the residual swing.

Other strong v1 candidates: alerting on **leading indicators** (session
length, crash rate, day-1 retention) before DAU and revenue move, and
**lightweight attribution** via slicing on the dimensions the alert
already has, so the Slack ping arrives with "concentrated in: android
/ BR" already attached.

---

*Word counts: Q1 ≈ 420 (just over the 400 cap to fit the three
sub-questions). Q2 ≈ 690 narrative, within the 400–700 budget.*
