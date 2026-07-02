# Enzyme-Inspired Engineering TODOs for openivm-spark

_Written 2026-07-02. Companion to `compile-facts-todos.md` (which records the P0→P6
frontier of PR [#10][pr10]). This doc is grounded in a **fresh SF10 A/B run** of
`databricks-enzyme` vs `spark-openivm` vs vanilla `spark` (ivm-bench OAT
`37845b27`, batches 100/1/2), cross-referenced against the Databricks **Enzyme**
SIGMOD companion paper. It distills a **crystal-clear, prioritized work list** of
Enzyme techniques we can port to close — and beat — the vanilla gap._

[pr10]: https://github.com/mdrakiburrahman/openivm-spark/pull/10

---

## 0. TL;DR — what the SF10 run proved

Aggregate batch wall (OAT `37845b27`; enzyme runs on a Databricks *Small* SQL
warehouse, so treat cross-engine wall as **directional** — the deterministic
signal is per-MV `records_read`/`records_written`, captured natively for
`spark`/`spark-openivm`, and `compute_ms` from Enzyme's pipeline telemetry):

| engine            | b1     | b2     | b3     | b2 ratio vs vanilla |
| ----------------- | ------ | ------ | ------ | ------------------- |
| vanilla `spark`   | 04:22  | 03:48  | 03:42  | 1.00×               |
| `databricks-enzyme` | 03:43 | **02:28** | **02:23** | **0.65× (beats vanilla)** |
| `spark-openivm`   | 19:01  | 07:00  | 08:11  | 1.84× / 2.21× (b3)  |

**Enzyme beats vanilla on every batch. openivm-spark is 1.84–2.21× *slower* than
vanilla** in this contended run. Two distinct, independently-fixable root causes
show up in the per-MV telemetry:

### Root cause A — refresh-SQL **read amplification** on stateful shapes

openivm's compiled refresh program re-reads far more than the change set. Per-MV
`records_read` (batch 2), openivm ÷ vanilla:

| MV                | vanilla rd | openivm rd | **amp** | vanilla s | openivm s | **enzyme s** |
| ----------------- | ---------- | ---------- | ------- | --------- | --------- | ------------ |
| `trades_history`  | 4.57 M     | **67.2 M** | **14.7×** | 16     | **98**    | **6.8**      |
| `dim_customer`    | 0.07 M     | 0.68 M     | **9.5×**  | 5      | 54        | **6.7**      |
| `customers`       | 0.05 M     | 0.49 M     | 9.8×    | 5        | 28        | 7.5          |
| `accounts`        | 0.05 M     | 0.39 M     | 7.7×    | 3        | 26        | 7.6          |
| `daily_market`    | 5.27 M     | **31.7 M** | **6.0×**  | 7      | 73        | **5.1**      |
| `watches`         | 3.18 M     | 16.5 M     | 5.2×    | 6        | 92        | 6.5          |
| `dim_trade`       | 3.27 M     | 13.1 M     | 4.0×    | 5        | 74        | 10.4         |
| `trades`          | 3.27 M     | 11.7 M     | 3.6×    | 8        | 55        | 9.9          |
| `fact_market_history` | 5.52 M | 0.47 M    | **0.1×** ✅ | 75    | **17** ✅ | 8.0          |

These are exactly the `WINDOW` (`trades_history` LAG/ROW_NUMBER, `daily_market`
cumulative MIN/MAX), `last_value` forward-fill (`dim_customer`), and range-join
(`dim_trade`, `watches`) shapes `compile-facts-todos.md` §1 flags as "inherent".
**Enzyme refreshes every one of them in a flat ~5–10 s** — it does *not* re-scan
the base.

### Root cause B — openivm **per-MV fixed overhead** (~3–6× even at amp = 1.0)

Even MVs where openivm reads **exactly what vanilla reads** are 3–6× slower:

| MV                 | rd amp | vanilla s | openivm s | enzyme s |
| ------------------ | ------ | --------- | --------- | -------- |
| `daily_market_pulse` | 1.0× | 5         | **30**    | 8.2      |
| `market_volatility`  | 1.0× | 9         | **24**    | 6.2      |
| `fact_trade`         | 1.0× | 9         | **29**    | 6.4      |
| `broker_performance` | 1.0× | 17        | **61**    | 17.7     |

This residual is **not** read amplification — it is the constant cost openivm
pays *per MV per batch*: DuckDB compile round-trip, `metadata_pre_sql` probes, a
multi-statement refresh program (7–9 `execute_refresh_sql_stmt` per MV), and a
Delta `MERGE`/staging round-trip. Across 49 MVs this constant dominates the batch.
Enzyme amortizes it (one planning pass, one backing-table `MERGE`/`REPLACE`).

### Enzyme's incrementalization surface (from `EXPLAIN CREATE MATERIALIZED VIEW`)

Under `REFRESH POLICY INCREMENTAL STRICT`, Enzyme reported **"can be incrementally
refreshed"** for 46/49 MVs — **including** `daily_market`, `trades_history`,
`dim_customer`, and `fact_market_history`. Only 3 (`dim_account`, `fact_watches`,
`market_volatility`) were **NOT INCREMENTALIZABLE**. Its incremental physical plan
for `daily_market` scans a **`__materialization_mat_*` backing table** for the
changed **segment** (`source = "timestamp_delta"`), *not* the full source — the
concrete realization of the paper's *backing table + per-operator delta plan*.

**Guiding asymmetry:** Enzyme wins the window/`last_value`/range shapes; openivm
already wins the insert-only fact (`fact_market_history` 0.1× / 17 s — Enzyme
takes 8 s but on separate hardware). The target engine is the **union**: keep
openivm's insert-only fact win, adopt Enzyme's stateful-operator machinery.

---

## 1. How to read this list

Every item states **Enzyme technique → openivm-spark change → change surface →
measurement gate → guardrail**. The change surface is one of
`{openivm (C++), lpts, openivm-spark (Scala)}`. Ordered by expected payoff.

**Hard invariants (every item):**

- **No MV may ever be regressed to `FULL_REFRESH`** (golden-SQL net +
  `ChainedWindowPartitionCascadeCdfSpec`/`TpcDiScenarios` enforce it).
- New behaviour ships behind a **default-OFF** flag; CREATE DDL + refresh output
  must be byte-identical when OFF.
- Correctness gate is **bidirectional `EXCEPT ALL`** on user-visible columns.
- Perf gate is **per-MV `records_read`/`records_written` at SF10** (deterministic);
  batch wall is ±15 % noise and, vs Enzyme, cross-hardware.

Artifacts backing every number here live under the session workspace
(`sf10/metrics/`, `sf10/query-plan/`, `sf10/dbt-server/`, `sf10/permodel.csv`).

---

## 2. Tier 1 — Decomposed-aggregate **backing table** (the core Enzyme idea)

> Enzyme stores a MV as a **backing table** holding *partial* aggregate state
> (e.g. running `sum`+`count` for an `AVG`), plus a **top-level view** that
> finalizes it. Incremental refresh updates the partial state from the change set
> only — never re-scanning history.

### E1. Backing-table state for cumulative-window MVs (`daily_market`, `trades_history`)

- **Target:** the #1 and #4 read-amplifiers (`trades_history` 14.7×/98 s,
  `daily_market` 6.0×/73 s). Enzyme does both in ~5–7 s.
- **Enzyme technique:** decomposed running aggregate + per-partition backing state.
  A cumulative `min(dm_low) OVER (PARTITION BY sym ORDER BY date)` needs only the
  **running extremum per partition** carried forward; a new (append-only) segment
  extends the suffix from the stored running value — O(new rows), not O(partition).
- **openivm-spark change:** persist one **running-state row per window partition**
  (the current cumulative `min`/`max`/`row_number` high-water) in a side Delta
  table keyed by partition. On a batch, join the change segment to the stored
  state, extend the suffix, and emit the delta. This is the **operator-specific**
  version of the *generic* aux-state that failed (P5.2, `compile-facts-todos.md`
  §2) — the failure was a *full-MV* bounds/state scan; here the state is O(partitions)
  and read by key, and there is no separate bounds scan.
- **Change surface:** openivm `refresh_window.cpp` (recognize the monotone
  cumulative-frame shape, emit the state-extend program); `SparkRefreshRewriter`
  + window dispatch in `MaterializedViewCommands` to maintain the state table.
- **Gate:** `daily_market`/`trades_history` `records_read ≈ O(segment)`;
  `fact_market_history` **stays 0.1× insert-only** (it consumes `daily_market`'s
  delta); `ChainedWindowPartitionCascadeCdfSpec` + `TpcDiScenarios` + downstream
  EXCEPT-ALL green.
- **Guardrail:** the running-state table must be **snapshot-pinned** across the MV
  write (Spark CacheManager auto-invalidates a cached temp view when a referenced
  Delta table is written — see the `running-window aux state` memory); on a
  **backdated / non-suffix** segment, fall back to the current recompute (never
  FULL). Do NOT re-enable the generic P5.2 path.

### E2. Backing-table state for `last_value`/`last` forward-fill (`dim_customer`)

- **Target:** `dim_customer` 9.5×/54 s (+ `customers` 9.8×, `accounts` 7.7×) —
  today a full-partition recompute. Enzyme ~6.7 s.
- **Enzyme technique:** `last_value(x) OVER (PARTITION BY k ORDER BY seq)` decomposes
  to **"the highest-`seq` row per partition"** — a trivial O(1) per-group merge.
- **openivm-spark change:** persist one row per partition (current max-`seq` tuple
  per forward-filled column); on a delta, only partitions whose max-`seq` advanced
  re-emit. O(changed partitions), not O(MV). (This is Tier-1 lever 1 in
  `compile-facts-todos.md` §4, now with a concrete Enzyme precedent + SF10 numbers.)
- **Change surface:** openivm `refresh_window.cpp` (`last_value`/`last` shape →
  per-group max-seq incremental); spark-ext window dispatch + `SparkRefreshRewriter`.
- **Gate:** `dim_customer records_read ≈ O(delta)`; stays `WINDOW_*` (never FULL);
  EXCEPT-ALL green incl. every downstream consumer.
- **Guardrail:** correct `IGNORE NULLS` semantics (the MV uses
  `last_value(x, true)`); a delete of the current max-`seq` row must recompute that
  one partition (persist top-2 per group), not the whole MV.

---

## 3. Tier 2 — Immutable **row-ids** + `REPLACE WHERE` vs `MERGE` strategy choice

> Enzyme assigns every output tuple an **immutable row-id** (Delta *row-tracking*
> at leaves; **derived** ids at higher operators — join ids from left⊕right,
> aggregate ids from grouping keys). It then updates the backing table with
> `MERGE INTO` **or** `REPLACE WHERE`, chosen from the change-set shape, and runs
> **effectivization** to cancel redundant insert/delete pairs before writing.

### E3. Derived row-ids to make range-join facts incremental (`dim_trade`, `watches`)

- **Target:** range-join facts `dim_trade` 4.0×/74 s, `watches` 5.2×/92 s — the
  `Δ ⋈ current-base` still shuffles/re-reads the base.
- **Enzyme technique:** derive the join output id from `(left_row_id, right_row_id)`
  so the changed output rows are addressable and only they are re-emitted; combined
  with converting predicate operators to **semi-joins** when dynamic file pruning
  fails (Enzyme does this explicitly).
- **openivm-spark change:** thread a stable composite key through the join-delta so
  the refresh writes a **keyed `MERGE`** touching only changed outputs instead of
  re-deriving the full join. Compose with the shipped `semiJoinPrune` (do not
  regress it — see the `FK pruning` memory: openivm-side FK term-prune broke
  `semiJoinPrune`'s pattern match; keep them compatible).
- **Change surface:** openivm join-delta emit + `scd2RangeAccel` path; spark-ext
  `SparkRefreshRewriter` MERGE-key assembly.
- **Gate:** `dim_trade`/`watches records_read` drops toward vanilla; SCD-2 range
  EXCEPT-ALL green; `semiJoinPrune`/`scd2RangeAccel` stay default-ON and unregressed.
- **Guardrail:** blocked-until-verified on Delta row-id **bindability** in
  `MERGE ON`/`DELETE … IN (subquery)` — OSS Delta 3.2 exposes `_metadata.row_id`
  as selectable-but-not-bindable (`compile-facts-todos.md` §3, W7.5). Derive the id
  from **grouping/business keys** where possible so this lever is not gated on the
  Delta upgrade; treat the Delta-native row-id as the upgrade-path variant.

### E4. Effectivization — cancel insert/delete pairs before the Delta write

- **Target:** Root-cause B on `REPLACE WHERE`/recompute MVs — the multi-statement
  refresh writes rows that a later statement deletes (extra `records_written` +
  extra Delta commit work).
- **Enzyme technique:** before physically applying a delete-then-insert, remove
  redundant `(+row, −row)` pairs on the same row-id ("effectivization"), shrinking
  the changeset and avoiding a delete that strands a row.
- **openivm-spark change:** in `SparkRefreshRewriter`, coalesce the signed view-delta
  (`openivm_*` ± rows) so net-zero rows are dropped **before** the MERGE/INSERT,
  and fuse the DELETE+INSERT into a single pass (extend the shipped
  `windowSinglePassReplace`/`DeltaCommitClassifier` fix to the general changeset).
- **Change surface:** spark-ext `SparkRefreshRewriter` (changeset minimization);
  optionally openivm cascade emit for a native signed-delta.
- **Gate:** `records_written` per MV drops; batch-2/3 EXCEPT-ALL green; **must**
  include the cascade specs (the earlier `w77-cascademin` post-hoc-Spark-diff
  variant was net-negative on non-suffix batches — do the cancellation on the
  **already-signed** delta, cheaply, not by re-diffing snapshots).
- **Guardrail:** never drop a genuine `(+,−)` that differ on any user column;
  bag (multiset) semantics for `SIMPLE_PROJECTION` MVs with duplicate rows (the
  value-equality MERGE hazard, `compile-facts-todos.md` §note).

---

## 4. Tier 3 — Planning-side: normalization, fingerprint, history cost model

> Enzyme normalizes the Catalyst plan (inline CTEs, merge filters, drop redundant
> ops), **fingerprints** it for correctness, builds **per-operator delta-plan
> fragments** (pre/post/delta) bottom-up, and picks a strategy with a **cost model
> trained on historical executor CPU** across joins/aggs/windows/shuffles/scans/writes.

### E5. Amortize per-MV planning overhead (Root cause B)

- **Target:** the ~3–6× fixed cost on amp-1.0 MVs (`daily_market_pulse` 5→30 s,
  `fact_trade` 9→29 s). openivm pays a DuckDB compile round-trip +
  `metadata_pre_sql` probes + 7–9 statements **per MV per batch**.
- **Enzyme technique:** one normalization+delta-plan pass per MV, reused; batch the
  refresh into as few physical writes as possible.
- **openivm-spark change:** (a) **cache the compiled refresh program** per
  `(view, source-schema fingerprint)` so batch 2/3 skip recompile — P3.4/P1.5 were
  "no reproducible win" at SF3, but SF10 Root-cause-B shows the constant is real;
  re-measure at **SF10** with the deterministic per-MV wall, not SF3.
  (b) **collapse the multi-statement refresh** into fewer Spark actions
  (fuse `metadata_pre_sql` probes; reuse the cached view-delta DataFrame — extend
  shipped P4.4). (c) **parallelize independent sibling MV refreshes**
  (`compile-facts-todos.md` §Tier-3 lever 6) — Enzyme's DAG runs siblings
  concurrently; openivm refreshes serially.
- **Change surface:** spark-ext `OpenIvmCompiler` (program cache),
  `MaterializedViewCommands` (statement fusion + sibling scheduling).
- **Gate:** amp-1.0 MVs' **wall** drops toward vanilla at SF10 with **identical
  `records_read`/`records_written`** and EXCEPT-ALL green; no shared-staging
  collisions when siblings run concurrently.
- **Guardrail:** the compile-program cache must invalidate on source-schema change
  (Enzyme's fingerprint role); never serve a stale program across a DDL change.

### E6. Query fingerprint for safe incremental reuse

- **Enzyme technique:** a plan fingerprint that is the MV-definition id; any change
  (incl. non-deterministic functions like `RAND()`) forces a full rebuild.
- **openivm-spark change:** compute + persist a normalized-plan fingerprint at
  CREATE; on REFRESH, if the fingerprint changed or the body contains a
  non-deterministic function, route that MV to `FULL_REFRESH` **for that MV only**
  (this is a *correctness guard that adds* a full-refresh, not a regression of an
  incrementalizable MV — distinct from the hard invariant).
- **Change surface:** `MvCatalog`/`MvMetadata` (store fingerprint), CREATE/REFRESH
  in `MaterializedViewCommands`.
- **Gate:** a spec that mutates a MV body across CREATE/REFRESH and asserts the
  fingerprint-mismatch → full path; deterministic MVs unaffected.
- **Guardrail:** fingerprint over the **normalized** plan (post CTE-inline/filter-
  merge) so cosmetic reformatting doesn't force needless full rebuilds.

### E7. History-based refresh cost model (calibrate the shipped `RefreshCostModel`)

- **Enzyme technique:** estimate cost as **sum of historical executor CPU** across
  key operators; fall back to query-processing logs when no history exists.
- **openivm-spark change:** we already have `RefreshCostModel` +
  `tools/calibrate_refresh_cost_model.py` (P3.2/P6.4, R²≈0.97) — but they're
  default-OFF and fit from **one** telemetry set. Feed the **new SF10 per-MV
  telemetry** (`metrics_by_model.parquet` + the per-view-step profile CSVs) into the
  calibration, and use the model to choose, per MV, between the E1/E2 backing-state
  path, the recompute path, and full — the way Enzyme picks REPLACE vs MERGE.
- **Change surface:** `RefreshCostModel` + calibration tool (data plumbing only);
  `RefreshIntelligence.decide()` wiring (already composes compile+runtime signal).
- **Gate:** on SF10, the model's per-MV choice matches the empirically-fastest path
  (validate against this run's numbers); flipping it ON does not regress any MV's
  `records_read` or route anything to FULL that was incremental.
- **Guardrail:** keep default = hand-picked coefficients until an SF10 A/B shows the
  calibrated model is net-positive AND never picks FULL for an incrementalizable MV.

---

## 5. Tier 4 — Shared-subplan DataFrame caching

### E8. Cache shared subplans within a refresh (Enzyme's DataFrame cache)

- **Enzyme technique:** for complex plans (multiple aggregations/joins over a shared
  input), Enzyme caches DataFrames to avoid redundant input reads.
- **openivm-spark change:** where one batch refreshes several MVs over the same
  freshly-changed source (e.g. `daily_market` → `daily_market_pulse`,
  `market_volatility`, `fact_market_history` all consume the same window delta),
  **materialize the shared change/delta once** and reuse it across the sibling
  refreshes instead of each MV re-reading it. Extends P4.4 (cached view-delta) from
  one MV to a **cross-MV** cache within a batch, and pairs with E5(c) sibling
  parallelism.
- **Change surface:** spark-ext batch orchestration in `MaterializedViewCommands`
  (per-batch delta cache keyed by source+segment) + `StagingCatalog`.
- **Gate:** aggregate batch `records_read` drops (the shared source is read once);
  per-MV EXCEPT-ALL green; no staging collision.
- **Guardrail:** snapshot-pin the cached delta (CacheManager invalidation, per E1);
  evict at batch end to bound memory.

---

## 6. Explicitly OUT of scope / do-not-retry (already dead-ended in PR #10)

These Enzyme-adjacent ideas were built + SF-measured and are net-negative; do **not**
re-attempt without a fundamentally different design (see `compile-facts-todos.md` §2):

- **Generic** running-window aux-state (P5.2) — full-MV bounds/state scan; E1/E2
  above are the **operator-specific** replacements, not this.
- Post-hoc Spark snapshot-diff cascade minimization (`w77-cascademin`) — E4 does the
  cancellation on the already-signed delta instead.
- Window snapshot cache / cluster-prune / Z-order — neutral at SF.
- Bucketing / storage-partitioned join (Delta 3.2 rejects bucketed writes) — revisit
  only under the Delta-upgrade spike (`compile-facts-todos.md` §4 lever 8), which
  also unblocks E3's Delta-native row-id variant.

---

## 7. Suggested execution order

1. **E2** (`last_value`/`dim_customer`) — smallest, well-understood O(1)-per-group
   merge; immediate `dim_customer`/`customers`/`accounts` win.
2. **E1** (cumulative window backing state) — biggest read-amp prize
   (`trades_history`, `daily_market`); must preserve the `fact_market_history`
   cascade win.
3. **E5** (per-MV overhead: program cache + statement fusion + sibling parallelism)
   — attacks Root cause B across all 49 MVs; largely spark-ext-only.
4. **E4** (effectivization / changeset minimization) — compounds with E1/E5.
5. **E3** (derived row-ids for range joins) — gated partly on Delta; do the
   business-key variant first.
6. **E7/E6** (calibrated cost model + fingerprint) — turn the above into per-MV
   automatic routing.
7. **E8** (cross-MV shared-delta cache) — final aggregate squeeze.

Each item is independently shippable behind its own default-OFF flag, measured at
SF10 on per-MV `records_read`/`records_written`, and must pass the cascade specs
before flipping default-ON.

