# Beating Vanilla Spark — the open frontier

_Last rewritten 2026-07-02. This replaces the old CompileFacts P0→P6 roadmap, which is
now fully executed (shipped, dead-ended, or blocked). This doc records **only what is
still true and still actionable**: the honest baseline, the levers we already proved do
NOT work (so nobody re-runs them), the one item blocked on an upstream dependency, and
the concrete open work that can still close the gap to vanilla-Spark full refresh._

**All the code we depend on now lives on a single branch per fork:**

- openivm  → `mdrakiburrahman:dev/mdrrahman/perf-tuning` (PR [ila/openivm#6][pr6]).
- lpts     → `mdrakiburrahman:dev/mdrrahman/perf-tuning`.
- openivm-spark → `dev/mdrrahman/perf-tuning` (PR [#10][pr10]); pins in
  `spark-ext/dev/pins.env`.

[pr6]: https://github.com/ila/openivm/pull/6
[pr10]: https://github.com/mdrakiburrahman/openivm-spark/pull/10

---

## 1. Honest baseline (where we actually are)

At **SF10, aggregate batch totals**, incremental openivm-spark is **~1.28–1.38× vanilla
Spark** in batches 2 and 3 (openivm B2 ≈ 212–218 s vs vanilla ≈ 158–166 s). We **beat our
own batch 1** comfortably, and we **beat vanilla on the cheap reference/bronze/dim MVs**,
but we **do not yet beat vanilla on the aggregate** — that is the one remaining goal.

The residual is **concentrated and largely inherent** to the view-delta cascade:

| MV                     | openivm B2         | vanilla B2      | why                                                      |
| ---------------------- | ------------------ | --------------- | ------------------------------------------------------- |
| `fact_market_history`  | ~16 s / 0.06 M rd  | ~32 s / 1.34 M  | **the genuine win** — insert-only, ~38× fewer reads     |
| `daily_market`         | ~24 s              | ~4.7 s (1 CTAS) | window recompute + cascade needs old+new snapshots      |
| `dim_customer`         | ~53 s (**worst**)  | tail            | `last_value` forces a full-partition recompute          |
| range-join facts       | `watches` ~30 s, `dim_trade` ~23 s | tail | `Δ ⋈ current-base` still shuffles the base       |

**Why it is hard, stated plainly:** `daily_market` feeds `fact_market_history` (our 38×
insert-only win), `daily_market_pulse`, and `market_volatility`, so it **must** emit a
view-delta (old+new snapshot) — we cannot simply full-refresh it without destroying the
downstream win. Vanilla does the same window in a single CTAS. So the frontier is: make
the window emit a **minimal** delta as cheaply as vanilla emits a full snapshot.

### Wins we keep (shipped, default-ON — do not regress)

`windowSinglePassReplace` (single-pass window `INSERT … REPLACE WHERE <collected literal>`
+ the `DeltaCommitClassifier` predicate-scoped-Overwrite = `Mutating` fix), `semiJoinPrune`,
`scd2RangeAccel`, `windowSuffixSkip`, `runtimeFilter`, `adaptiveBroadcast`. **Hard
invariant:** no MV may ever be regressed to `FULL_REFRESH`; the golden-SQL net + the
`ChainedWindowPartitionCascadeCdfSpec`/`TpcDiScenarios` specs enforce it.

---

## 2. Tried and measured NEGATIVE — do not re-run

Each of these was built, correctness-validated, and SF-measured. All are net-neutral or
net-negative. They are kept default-OFF (or the branch was dropped). **Do not spend time
re-attempting them without a fundamentally different design.**

- **Running-window auxiliary state (P5.2 / P5.2a `p52-auxstate`).** Persisted per-partition
  `bounds`/`state` table. Removed the bounds/state scans (`daily_market` 20.5 M → 12.9 M
  reads) but **wall ROSE 74 → 99 s** (Delta aux-table maintenance costs more than the scan
  it saves) **and introduced a multi-refresh B3 correctness bug** (`daily_market diff=1`).
  Reads still 10× vanilla because `openivm_new`/cascade scans are untouched. **Excluded from
  THE BRANCH.**
- **Cascade-delta minimization (`w77-cascademin`).** Minimizing the old/new snapshot diff to
  changed rows only. Correct, but net-negative on TPC-DI because the batches are **not
  pure-suffix**, so the minimized cascade still carries −1 rows. **Excluded from THE BRANCH.**
- **Window snapshot cache (`windowSnapshotCache`).** Neutral at SF.
- **Window cluster pruning / Z-order clustering (`windowClusterPrune`).** Neutral.
- **Bucketing / storage-partitioned join (P4.6 / W7.4 SPJ half).** Delta 3.2 **rejects
  bucketed writes** on the MV/staging tables, so the zero-shuffle SPJ path can't be built.
  Dead-end on Delta 3.2 (see §3 for the upgrade path that could revive it).
- **Bloom-only Δ⋈base (P4.6 bloom half).** Neutral on `local[*]`.

---

## 3. Blocked on an upstream dependency

- **`fact_holdings` incremental via Delta `enableRowTracking` (W7.5).** openivm's
  rowid-keyed self-join delete needs a stable row id in a `MERGE ON` / `DELETE … WHERE id IN
  (subquery)` predicate. Delta 3.2 exposes `_metadata.row_id` as **selectable but NOT
  bindable** in those predicates, so `fact_holdings` stays `FULL_REFRESH`. **Unblock:** a
  Delta release that lifts the restriction — tracked as part of the Delta-upgrade spike (§4,
  lever 8).

---

## 4. Open levers — what CAN be done to beat vanilla

Ordered by expected payoff. Every lever states **target · hypothesis · files · gate**. The
correctness gate is always **bidirectional `EXCEPT ALL`** on the user-visible columns; the
perf gate is always **SF `records_read` per MV** (deterministic; wall is ±15 % noisy) plus
the aggregate B2/B3 total. **No lever may regress any MV to `FULL_REFRESH`.**

### Tier 1 — the two residual hotspots (deep; openivm-C++ + spark-ext)

1. **⭐ Incremental `last_value` / `last` window operator for `dim_customer`.**
   - **Target:** the ~53 s #1 residual — a full-partition recompute today.
   - **Hypothesis:** unlike the failed *generic* aux-state, `last_value(x) OVER (PARTITION BY
     k ORDER BY seq)` has a **trivial O(1) per-group update**: the answer is just the
     highest-`seq` row per partition. Persist one row per partition (the current max-`seq`
     tuple); on a delta, only partitions whose max-`seq` moved need re-emit. This is
     O(changed-partitions), not O(MV).
   - **Files:** openivm `refresh_window.cpp` (recognise the `last_value`/`last` shape and emit
     the per-group max-seq incremental), spark-ext window dispatch + `SparkRefreshRewriter`.
   - **Gate:** `dim_customer` EXCEPT-ALL green + every downstream consumer; SF
     `dim_customer records_read ≈ O(delta)`; `dim_customer` stays `WINDOW_*` (never FULL).

2. **⭐ Native minimal row-level view-delta for the window cascade.**
   - **Target:** `daily_market` ~24 s → ≈ vanilla's ~4.7 s, WITHOUT losing the downstream
     `fact_market_history` 38× win.
   - **Hypothesis:** emit only the genuinely-changed rows as signed ±1 **computed inside
     openivm** (hash / deletion-vector diff of the recomputed partition against the prior
     snapshot), rather than materialising a full old-snapshot + new-snapshot and diffing them
     in Spark. This is the *native* version of the `w77-cascademin` idea that failed as a
     post-hoc Spark diff. Combine with `windowSinglePassReplace` so the DATA update is one
     clean pass and the CASCADE emit is one minimal-delta pass.
   - **Files:** openivm `refresh_window.cpp` / `refresh_compiler_aux.cpp` (recompute + cascade
     emit), spark-ext window dispatch.
   - **Gate:** `fact_market_history` stays 38× insert-only; `daily_market records_read ≈
     vanilla`; window + all downstream EXCEPT-ALL green (MUST include
     `ChainedWindowPartitionCascadeCdfSpec` + `TpcDiScenarios`).

### Tier 2 — harvest the foundations already built into THE BRANCH (cheap; spark-ext + pin)

These openivm/lpts capabilities are **already merged onto `dev/mdrrahman/perf-tuning`** but
are **not yet consumed** by the spark-ext rewriter. Each is a bounded spark-ext task + an
SF measurement.

3. **Wire `COUNT(DISTINCT)` → incremental (P5.7).** openivm now emits a
   `COUNT_DISTINCT_INCREMENTAL` refresh program (from `p57-stateful`). Teach
   `SparkRefreshRewriter` to translate it (persisted per-group distinct-value aux-state
   instead of `GROUP_RECOMPUTE`). **Gate:** `Distinct*Spec` parity + any SF MV that uses
   `COUNT(DISTINCT)` drops to O(delta).

4. **Wire LPTS `/*+ BROADCAST */` hints (P6.3).** openivm (`emit_spark_hints`) + lpts
   (`67f058a`) now render Spark optimizer hints into the compiled refresh SQL. spark-ext must
   **preserve the SQL comment** through its re-parse (Spark's parser drops block comments, so
   the hint has to survive the openivm-dialect → Spark-dialect rewrite). **Gate:** parity +
   an SF range-join MV where the broadcast plan is chosen (watch the join in the physical
   plan) and B2 drops.

5. **SF-measure `scd2RangeJoinAccel` (P5.4).** Shipped default-OFF, never measured at SF.
   openivm adds delta timestamp-domain filters for SCD-2 range joins. Run an SF10 A/B on the
   SCD-2 range-join MVs (`dim_trade`, `watches`); if net-positive, flip default-ON with a
   confirming full verify. **Gate:** SF B2 drop + EXCEPT-ALL; else leave opt-in.

### Tier 3 — orchestration + structural (medium → large)

6. **Parallel / pipelined cascade refresh.** Independent downstream MVs
   (`daily_market_pulse`, `market_volatility`, `fact_market_history`) currently refresh
   **serially** after `daily_market`. They have no mutual dependency — refresh them
   concurrently. This is a **spark-ext orchestration** change (schedule the refresh DAG by
   depth, fan out siblings), not an openivm change, and it lowers wall even when total work is
   constant. **Gate:** identical results (EXCEPT-ALL) + B2/B3 wall drop; no shared-staging
   collisions.

7. **Broadcast / shuffle A/B for the range joins.** `watches` (~30 s) and `dim_trade`
   (~23 s) still shuffle the current base. Tune `selectiveBroadcast` / `adaptiveBroadcast`
   thresholds so the small `Δ` side broadcasts and the base scan stays local. **Gate:** SF B2
   drop + EXCEPT-ALL.

8. **Delta-upgrade spike (revives two dead/blocked structural levers).** A newer Delta may
   (a) make `_metadata.row_id` **bindable** in `MERGE ON` / `DELETE … IN (subquery)`,
   unblocking `fact_holdings` (§3 / W7.5), and (b) accept **bucketed writes**, unblocking the
   zero-shuffle co-bucketed `Δ ⋈ base` join (W7.4). This is a dependency bump: bump
   Delta/Spark pins in `pins.env` + `project/Dependencies.scala` + the matching SHAs, get
   full verify green, THEN re-attempt W7.4/W7.5 behind their flags. **Gate:** verify green on
   the new Delta before any re-attempt.

---

## 5. Remaining stateful operators (P5.7 follow-on)

Only `COUNT(DISTINCT)` has an incremental (aux-state) implementation so far. The same
persisted-aux-state pattern still owes:

- **`DISTINCT`** (bag → set with deletes).
- **`SEMI` / `ANTI` join** with base deletes.
- **`MIN` / `MAX` with deletes** (the classic "recompute on delete of the extremum" problem —
  persist the top-2 per group so a delete of the max is O(1) unless it empties the runner-up).

Each needs its openivm operator + a `SparkRefreshRewriter` translation + a parity spec, and is
only worth doing if an SF MV actually exercises that shape on the critical path.

---

## 6. How to work these

- **Validate openivm fast:** `GEN=ninja make -j8` in the `.temp/openivm` worktree, stage the
  built `duckdb` + `openivm.duckdb_extension` under `spark-ext/target/openivm-*/`, and run the
  spark-ext specs / SF harness with `-e OPENIVM_CLI_PATH=… -e OPENIVM_EXTENSION_PATH=…`.
- **Measure with `records_read` / `records_written`** per MV from ivm-bench's
  `mount/metrics/3/processed/metrics_by_model.parquet` — deterministic and decisive. Wall is
  ±15 % noise; never gate on a single wall number. Use the ivm-bench telemetry diff server to
  compare `spark` vs `spark-openivm` per-MV and find the bottleneck.
- **Serialize SF** — never run two SF harnesses (or an SF and a full verify) at once.
- **Every window/cascade change MUST run `ChainedWindowPartitionCascadeCdfSpec` +
  `TpcDiScenarios`** before merge — a flat single-MV spec will NOT catch a downstream
  `FULL_REFRESH` regression.

