---
name: ivm-investigation
description: "Spin up ivm-bench against duckdb-openivm + spark-openivm at the pinned commits, gather per-MV / per-step IVM-effectiveness telemetry, and produce a root-cause report under .research/ that recommends where in {openivm, lpts, openivm-spark} the fixes belong."
user-invocable: true
---

# IVM Effectiveness Investigation

You are running this skill from `/home/mdrrahman/openivm-spark` (the
`openivm-spark` repo root).

The user is asking: **why is `duckdb-openivm` more effective at
incremental view maintenance than `spark-openivm`?** Concretely, in a
healthy IVM run the batch-1 (full) wall clock should dwarf batch-2 and
batch-3 (incremental) — DuckDB shows this (`01:08 → 00:35 → 00:33`)
but Spark does not (`09:42 → 12:05 → 11:34`).

Your deliverable is a markdown root-cause analysis at
`.research/SPARK-VS-DUCKDB-IVM-EFFECTIVENESS.md` that the user will use
to assign fixes across the 3 source repos.

---

## CRITICAL RULES (read before touching anything)

### CR-A. Auto-commit and push on non-main branches

For every repo (`openivm-spark`, `.temp/openivm`, `.temp/lpts`,
`.temp/ivm-bench`), check the current branch with
`git -C <repo> symbolic-ref --short HEAD`. If the branch is **not**
`main`, you MUST commit and push changes automatically — do not
prompt the user. Use descriptive commit messages prefixed with
`[ivm-investigation]`. If the branch IS `main`, do NOT modify that
repo — report the gap and move on (per CR-D).

### CR-B. Never weaken correctness for performance

This mirrors `.temp/openivm/CLAUDE.md` and the repo's testing
practices. Do not disable `OPENIVM_VALIDATE`, do not swap
`EXCEPT ALL` for weaker checks, do not silently mark MVs as
`FULL_REFRESH` to make the benchmark green.

### CR-C. Non-invasive diagnostics first

Phase 4 (code patches for additional logging) is LAST resort. Use the
existing instrumentation surface (Phase 4 below) on the first pass.
Patch only when an axis genuinely cannot be answered.

### CR-D. Patches only on non-main branches

For each repo, check `git -C <repo> symbolic-ref --short HEAD`. If it
returns `main`, you may NOT modify that repo — report the gap and
move on. Currently, per `spark-ext/dev/pins.env`:

- `openivm` → branch `openivm-spark` (not main → editable)
- `lpts` → branch `openivm-spark` (not main → editable)
- `ivm-bench` → branch `dev/mdrrahman/spark-openivm` (not main → editable)
- `openivm-spark` → whatever branch the user has checked out. CHECK
  before editing.

### CR-E. Patch deployment is part of the patch

Editing Scala or C++ source has zero effect on the running benchmark
until the artifacts are rebuilt AND injected into the
`spark-openivm-build` container's output mount. See Phase 5 below
for the deployment loop. If you skip deployment you will conclude the
patch produced no signal — that is a false negative.

### CR-F. Multi-label root causes

A single MV bug can trigger more than one bucket (e.g. an lpts
emission AND an openivm-spark dialect gap). Record all triggered
buckets per MV; designate one as `primary`, the rest as
`contributing`. Group recommendations by `primary` in the report.

### CR-G. Diagnose overhead-dominated BEFORE blaming IVM

At SF=3 / 0.001% / 0.002% batches, batch-2 and batch-3 are tens of KB
of data. Spark fixed overhead (JVM startup, Livy session, dbt graph
traversal, Delta transaction log writes, mv_catalog reads) can dwarf
any real IVM win. Run the overhead test in Phase 4-bis before
assigning R1–R5 root causes; if overhead dominates, the terminal
finding is **R0**, not a Spark IVM bug.

### CR-H. Pins-sync auto-fix retry budget is 3

Phase 0 auto-fixes drift up to 3 times. After that, write a
`pins-sync drift unresolved` finding to the report and stop.

---

## PHASE 0 — Pins-sync alignment (mandatory)

Run from repo root:

```bash
cd /home/mdrrahman/openivm-spark
./spark-ext/dev/dev.sh pins-sync 2>&1 | tee /tmp/pins-sync.out
```

Read `/tmp/pins-sync.out`. Pass condition: **no** `⚠` warnings.

Drift conditions (any of):

- `⚠ WARNING: HEAD has drifted from pin`
- `⚠ WARNING: ARG ... mismatch`
- `⚠ WARNING: OPENIVM_SPARK_COMMIT does not match origin/…`
- `⚠ Drift detected`

On drift, auto-fix without prompting (per CR-A):

1. **HEAD drift** in `.temp/{openivm,lpts,ivm-bench}`: hard-reset to
   the pinned commit — `git -C .temp/<repo> reset --hard <pin>`.

2. **Dockerfile ARG mismatch** (OPENIVM_COMMIT, LPTS_COMMIT, or their
   REPO/BRANCH variants): update the ARG defaults in
   `.temp/ivm-bench/src/containers/spark-openivm-build/Dockerfile` to
   match `spark-ext/dev/pins.env`, then commit + push on ivm-bench's
   working branch.

3. **OPENIVM_SPARK_COMMIT mismatch**: update both `ARG
   OPENIVM_SPARK_COMMIT=` lines in the Dockerfile (stage 2 and stage 3)
   to the origin HEAD SHA reported by `pins-sync`, then commit + push
   on ivm-bench's working branch.

4. **OPENIVM_SPARK_BRANCH mismatch**: update `ARG
   OPENIVM_SPARK_BRANCH=` in the Dockerfile to the current local
   branch, then commit + push on ivm-bench's working branch.

Re-run `pins-sync` after each auto-fix pass. Retry budget per CR-H: 3.
On exhaustion, append a `## Pins-sync drift (unresolved)` section to
the report and stop with `{ "status": "Failed" }`.

---

## PHASE 1 — Run the benchmark

From repo root:

```bash
cd /home/mdrrahman/openivm-spark
export SCALE_FACTOR=3 BATCH_1_PCT=1 BATCH_2_PCT=0.001 BATCH_3_PCT=0.002
export PARALLEL=0
export ENGINES=duckdb-openivm,spark-openivm,spark
export PRESERVE_RAW=1
export OPENIVM_VALIDATE=1
export OPENIVM_PROFILE_REFRESH=1
cd .temp/ivm-bench
bash src/.scripts/benchmark.sh
```

This is a long-running command (~30 min). Use the `bash` tool with
`mode: "sync"`, `initial_wait: 60`. The benchmark will go to the
background and you will be notified on completion. Do not poll.

Our goal here is:

1. spark-openivm **MUST** exceed spark in Batch 2 and Batch 3 so that IVM
   is actually useful. Otherwise, this whole openivm-spark project is a waste
   of compute.

   It is OK for Batch 1 to be slower to hydrate the initial IVM state.

2. spark-openivm **SHOULD** strive to be as close as possible to duckdb-openivm 
  in Batch 2 and Batch 3. Large gaps here are signals of openivm-spark bugs or inefficiencies 
  that can and should be fixed.

### Phase 1 failure handling (per-engine isolation)

Record the outcome of each isolated run as separate evidence:
duckdb-openivm-success + spark-openivm-fail is itself a strong R3/R6
signal.

If the runs fail, dump `mount/logs/3/benchmark-server.log`
and the per-engine dbt logs into the report's appendix and stop with
`{ "status": "Failed" }`.

---

## PHASE 2 — Collect artifacts (no edits)

```
mount/results/3/dbt-server/benchmark-results.json
mount/results/3/dbt-server/run-<engine>-batch{1,2,3}.json
mount/results/3/dbt-server/openivm-profile/*.csv           # duckdb-openivm only
mount/logs/3/benchmark-server.log
mount/logs/3/<engine>/                                     # per-engine dbt logs
mount/stats/3/<engine>/                                    # 15s container stats samples
```

If the bench-spark-openivm container is still running, also pull
Spark driver logs:

```bash
docker exec bench-spark-openivm-1 ls /tmp/livy-logs/ 2>/dev/null || true
docker exec bench-spark-openivm-1 cat /tmp/livy-logs/*.log > /tmp/livy-driver.log
```

---

## PHASE 3 — Confirm dbt SQL parity

Before comparing engine behaviour, verify each model's compiled SQL
is logically equivalent across engines (engine-specific dbt macros
can diverge).

```
diff <(jq -r '.results[].compiled_code'  mount/results/3/dbt-server/run-duckdb-openivm-batch1.json) \
     <(jq -r '.results[].compiled_code'  mount/results/3/dbt-server/run-spark-openivm-batch1.json)
```

If two models differ in shape (not just dialect spelling — joins,
group keys, filters etc.), flag those models as `R6.dbt-divergence`
and EXCLUDE them from R1–R5 bucketing for the rest of the analysis.

---

## PHASE 4 — Mine the 8 signal axes

For each axis, prefer the non-invasive query first. If the data
isn't there, follow the deployment loop in Phase 5 to add the
missing `logInfo` (CR-E).

### Axis A — Refresh strategy classification (Spark)

```sql
-- Inside bench-spark-openivm-1: spark-sql -e "..."
SELECT name, refresh_type, refresh_type_name, source_tables
FROM   delta.`_ivm/_meta/mv_metadata`;
```

Schema: `MvCatalog.scala:97-109` in openivm-spark.

For DuckDB-OpenIVM, parse
`mount/results/3/dbt-server/openivm-profile/by_view_step.csv` — the
`step_name` column encodes the strategy actually used per view.

**Smoking gun**: any MV where Spark = `FULL_REFRESH` but DuckDB used
an incremental strategy. Grep the Spark driver log for the WHY:

```bash
grep -nE "\[openivm-mv\] view='[^']*' compiled_refresh_type=" /tmp/livy-driver.log
```

The `reason='…'` field on those lines (from
`MaterializedViewCommands.scala:528-536` and `:367-393`) is the
root-cause hint. Common values:

- `compile_failed` → lpts/dialect (R5 or R1)
- `top_k` → openivm-spark MV command (R1)
- `no_real_delta` → openivm classifier (R4)
- `non_cascade_upstream` → infra / chain depth (R6)
- `having_extraction_failed` → openivm-spark (R1)

### Axis B — Per-MV per-batch wall clock

For each engine, for each batch:

```bash
jq -r '.results[] | [.unique_id, .execution_time, .adapter_response.rows_affected] | @tsv' \
   mount/results/3/dbt-server/run-<engine>-batch<N>.json
```

Compute the **IVM effectiveness ratio** per MV:

```
ratio = execution_time_batch1 / median(execution_time_batch2,
                                       execution_time_batch3)
```

A ratio < 2 means incremental refresh is NOT working FOR THAT MV.

### Axis C — Per-step refresh breakdown

**DuckDB**: already in `mount/results/3/dbt-server/openivm-profile/`:

```
profile.csv         — refresh_id, view_name, step_order, step_name, duration_ms, detail
by_step.csv         — aggregated by step_name
by_view_step.csv    — aggregated by view × step_name
```

The 9 step_names emitted by openivm (from `.temp/openivm/src/upsert/refresh.cpp`):
`acquire_locks` (:138), `adaptive_cost_estimate` (:172),
`generate_refresh_sql` (:183), `metadata_pre_sql` (:230),
`execute_refresh_sql_stmt` (:246), `execute_refresh_sql` (:258),
`metadata_post_sql` (:322), `record_refresh_history` (:351),
`total_refresh` (:78).

**Spark**: NO per-step timer exists today. If you need this, apply
the Phase 5 patches in Phase 5.B.

### Axis D — Staging Delta size per batch per MV (snapshot before/after each REFRESH)

Snapshot BEFORE each `REFRESH MATERIALIZED VIEW` and AFTER:

```sql
-- Before / after each REFRESH per MV:
SELECT base_table, op_type, staging_path, txn_ts, consumed_by
FROM   delta.`_ivm/_meta/staging`;
```

For each `staging_path` you see, get the row count of the delta itself:

```sql
SELECT op_type, COUNT(*) FROM delta.`<staging_path>` GROUP BY op_type;
```

Diagnostic readings:

- Staging path count = 0 for a base table that received an INSERT
  → R3.under-tee (IvmDmlInterceptorRule didn't fire)
- Staging row count ≈ full source table row count
  → R3.over-tee (rule is teeing too aggressively)
- `consumed_by` lists OTHER MV's name after `<this MV>` was the only
  refresher → R3.consume-wrong-row
- `consumed_by` still NULL after refresh → R3.under-consume

### Axis E — Spark execution-plan capture (mandatory for incremental-but-slow MVs)

For each MV whose Spark `refresh_type_name` ≠ `FULL_REFRESH` but
whose IVM ratio < 2:

```bash
# Get the rewritten SQL from the driver log:
grep -nE "\[openivm-mv\] refresh view='<mv>' stmt\[[0-9]+\]=" /tmp/livy-driver.log

# Capture EXPLAIN FORMATTED for one of the emitted statements:
docker exec bench-spark-openivm-1 spark-sql -e "EXPLAIN FORMATTED <sql>"

# And the MV target's Delta operation metrics:
docker exec bench-spark-openivm-1 spark-sql -e \
  "DESCRIBE HISTORY delta.\`<mv_location>\` LIMIT 5"
```

Read `operationMetrics`:

- `numTargetRowsUpdated` ≪ base-table rows → MERGE pruned correctly
- `numTargetRowsCopied` ≈ base-table rows → R2 (rewriter scans full
  base table; the predicate was lost in Catalyst)
- `numOutputRows` ≈ batch delta rows → IVM working correctly

### Axis F — `IvmDmlInterceptorRule` firing

```bash
grep -nE "\[openivm\]|IvmDmlInterceptor" /tmp/livy-driver.log | head -50
```

Today the only INFO key is `[openivm] hasDependentMvs failed for …`
(`IvmDmlInterceptorRule.scala:245-258`). To prove the rule fired
positively for a given INSERT, apply Phase 4 patch 5.A.

### Axis G — MV-over-MV chain depth

Read `mv_metadata.source_tables` recursively:

```sql
SELECT name, source_tables
FROM   delta.`_ivm/_meta/mv_metadata`;
```

Build the MV dependency graph. Anything with chain depth > 2 is
known unsupported per the repo's `README.md` — flag as R6.

### Axis H — Cost-model decision (DuckDB only)

For each MV that DuckDB refreshed incrementally but Spark
FULL_REFRESH'd, query the cost model to see whether DuckDB was
borderline:

```sql
PRAGMA refresh_cost('<mv>');
-- decision, incremental_cost, recompute_cost,
-- incremental_predicted_ms, recompute_predicted_ms, calibrated
```

If `decision='incremental'` with large margin → strong evidence the
query SHOULD be incremental in Spark too. If borderline → maybe R4
(classifier needs work in both engines).

---

## PHASE 4-bis — Overhead-dominated check (CR-G)

**Do this BEFORE assigning any R1–R5 root cause.**

Compute these for spark-openivm batch 2 (the canonical "incremental"
batch):

```
sum_model_time   = sum(.results[].execution_time) for batch 2
total_batch_time = mount/results/3/dbt-server/benchmark-results.json:
                   .engines["spark-openivm"].batches[1].duration_s
overhead_ratio   = (total_batch_time - sum_model_time) / total_batch_time
```

Also count how many MV refreshes ended in `outcome='no_pending_deltas'`:

```bash
grep -cE "outcome='no_pending_deltas'" /tmp/livy-driver.log
```

If `overhead_ratio > 0.5` OR if half the refreshes returned
`no_pending_deltas` but still cost > 1 s each → **R0 overhead-dominated**
is the terminal finding. The report still ships, but R1–R5 are NOT
the recommendation.

### Validation must not pollute the timed window

`OPENIVM_VALIDATE=1` runs `EXCEPT ALL` cross-checks AFTER the timed
window per ivm-bench design (see `benchmark.sh:18-21` comment). Before
trusting the batch-2/3 numbers, confirm this is true on the pinned
ivm-bench SHA by inspecting `mount/logs/3/benchmark-server.log`:

```bash
grep -nE "(=== VALIDATION|batch_end_ts|validation_start_ts)" \
  .temp/ivm-bench/mount/logs/3/benchmark-server.log
```

The validation start timestamp must follow each batch's end timestamp.
If validation is included in the batch duration on this branch, flag
it as R6 in the report and recommend a perf-only re-run with
`OPENIVM_VALIDATE=0` to confirm IVM ratios before bucketing.

---

## PHASE 5 — Observability patches (only if needed)

Each patch must be DEPLOYED after editing, or it contributes nothing.

### 5.A — openivm-spark patches (Scala)

Edit one of:

- `spark-ext/ivm-extension/src/main/scala/org/openivm/spark/commands/MaterializedViewCommands.scala`
  → wrap each phase with `val t0 = System.nanoTime(); …; logInfo(s"[ivm-spark][step=…][view=…][ms=${(System.nanoTime()-t0)/1e6}]")`
- `spark-ext/ivm-extension/src/main/scala/org/openivm/spark/analyzer/IvmDmlInterceptorRule.scala`
  → after each `StagedDmlNode(...)` (lines 48–221) `logInfo(s"[ivm-spark][step=dml-intercept][table=$tableName][op=$opType][staging_path=$sp]")`
- `spark-ext/ivm-common/src/main/scala/org/openivm/spark/common/SparkRefreshRewriter.scala`
  → at line ~141, after rewrite, `logInfo(s"[ivm-spark][step=rewrite][view=$mvName][stmts=${rewritten.size}]")`
- `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/OpenIvmCompiler.scala`
  → wrap the DuckDB CLI subprocess call (lines ~69–98) with the same `time { … }` pattern

**Deployment loop (mandatory):**

```bash
cd /home/mdrrahman/openivm-spark
./spark-ext/dev/dev.sh assembly

# Stamp the new jar into ivm-bench's mount so the next benchmark
# uses it. PRESERVE_RAW=1 keeps it across re-runs.
cp spark-ext/ivm-extension/target/scala-2.12/ivm-extension-0.1.0-SNAPSHOT-assembly.jar \
   .temp/ivm-bench/mount/bin/spark-openivm/openivm-extension.jar

# Verify after benchmark spin-up:
docker exec bench-spark-openivm-1 sha256sum /opt/spark/jars/openivm-extension.jar
# Compare with the host:
sha256sum spark-ext/ivm-extension/target/scala-2.12/ivm-extension-0.1.0-SNAPSHOT-assembly.jar
```

Both checksums must match — if not, the bench container is reusing a
stale jar; investigate the build-spark-openivm container's volume
mount before proceeding.

### 5.B — openivm patches (C++)

Edit `.temp/openivm/src/upsert/refresh.cpp` or `refresh_sql.cpp` or
`refresh_cost_model.cpp`. The `OPENIVM_DEBUG_PRINT` macros at
`refresh_sql.cpp:293-347` already exist — enable them or promote to
table rows on `openivm_refresh_history`.

**Deployment** (auto-commit per CR-A):

1. Commit the C++ patch on the working branch of `.temp/openivm`:
   ```bash
   git -C .temp/openivm add -A && \
   git -C .temp/openivm commit -m "[ivm-investigation] <describe patch>"
   ```
2. Push: `git -C .temp/openivm push origin HEAD`
3. Capture the new SHA:
   `NEW_SHA=$(git -C .temp/openivm rev-parse HEAD)`
4. Bump `OPENIVM_COMMIT` in `spark-ext/dev/pins.env` to `$NEW_SHA`.
5. Bump the matching `ARG OPENIVM_COMMIT` default in
   `.temp/ivm-bench/src/containers/spark-openivm-build/Dockerfile`
   (ABI-sensitive build — the two must match).
6. Commit + push pins.env in openivm-spark, and Dockerfile in
   ivm-bench, on their respective working branches.
7. Re-run `pins-sync` to verify alignment.

If the branch is `main` for any repo in this chain, report the gap
and move on (per CR-D).

### 5.C — lpts patches (C++)

Edit `.temp/lpts/src/lpts_pipeline.cpp` or `cte_nodes.cpp`. Same
auto-commit deployment flow as 5.B with `LPTS_COMMIT` instead of
`OPENIVM_COMMIT`.

### 5.D — ivm-bench patches

When the gap is a missing telemetry export (e.g. exporting the
`openivm_refresh_profile` equivalent for Spark, or dumping
`mv_metadata` snapshots per batch), patch
`.temp/ivm-bench/src/containers/dbt-server/services/`. Output
extracted telemetry to `mount/logs/3/<engine>/` so it doesn't
pollute `mount/results/`.

These changes take effect on the NEXT benchmark run because the
dbt-server container is rebuilt by `benchmark.sh` (`docker compose
up -d --build`). Verify with `docker exec dbt-server-1 cat
/app/services/spark_openivm_sources.py | head` or similar.

### 5.E — After any patch in 5.A–5.D

Re-run Phase 1, then Phase 4. The added log lines now show up. Loop
until your axes are answered or you've made 2 patch passes (don't
keep iterating forever — at some point a finding is sufficient).

---

## PHASE 6 — Build the root-cause table

For each MV, record:

| MV | Spark `refresh_type_name` | DuckDB strategy | Spark b1/b2/b3 (s) | DuckDB b1/b2/b3 (s) | Spark IVM ratio | DuckDB IVM ratio | Primary bucket | Contributing buckets | Evidence pointers |

**Decision tree (multi-label per CR-F):**

```
For each MV M where spark_ivm_ratio < 2:

  Step 1: did Phase 4-bis declare R0 overhead-dominated globally?
          If yes → primary=R0 for ALL MVs (terminal). Stop here.

  Step 2: read Spark mv_metadata.refresh_type_name for M.
          If 'FULL_REFRESH':
              Grep driver log for compiled_refresh_type=… reason='…'.
              Branch by reason:
                'compile_failed':
                    Pull the failing SQL from the log
                    (compile-bridge error or stmt[i]=).
                    If SQL contains any pattern from the
                    "Known Spark-hostile lpts emissions" table at the
                    end of this skill:
                      (struct_pack, := field, TIMESTAMP WITH TIME ZONE,
                       window GROUPS, window EXCLUDE, INSERT OR REPLACE,
                       AT (VERSION =>, _tf(, UNNEST, GROUPING SETS, etc.)
                      → primary=R5 (lpts), contributing=R1 if openivm-spark
                         dialect post-processor could absorb it
                    Else → primary=R1 (openivm-spark compile bridge)
                'top_k'                  → primary=R1
                'no_real_delta'          → primary=R4
                'non_cascade_upstream'   → primary=R6 (chain depth)
                'having_extraction_failed' → primary=R1
              Add contributing=R4 if DuckDB ALSO chose FULL_REFRESH
              for M (classifier is too conservative in BOTH).

          Else (Spark chose an incremental RefreshType):
              Read Spark eventLog + DESCRIBE HISTORY (Axis E).
              If numTargetRowsCopied ≈ base table rows:
                  → primary=R2 (rewriter / Catalyst dropped predicate)
              Else (rows copied ≪ base):
                  Read staging snapshots (Axis D).
                  If staging count = 0 or unconsumed:
                      → primary=R3.under-tee or R3.under-consume
                  If staging count ≫ expected:
                      → primary=R3.over-tee
                  Else
                      → primary=R6 (Spark constant-per-MV overhead)

  Step 3: if DuckDB also has ratio < 2 for M
          → contributing=R4 added.
```

---

## PHASE 7 — Write the report

File: `.research/SPARK-VS-DUCKDB-IVM-EFFECTIVENESS.md` in the
openivm-spark repo (create `.research/` if missing).

```markdown
# Spark vs. DuckDB OpenIVM Effectiveness

> Generated by the `ivm-investigation` skill against pins:
> openivm=<sha>, lpts=<sha>, ivm-bench=<sha>, openivm-spark=<sha>
> SF=3, batches=1%/0.001%/0.002%, engines=duckdb-openivm,spark-openivm

## TL;DR

- 5 bullets — primary buckets observed, where the fixes belong.
- One sentence for "is IVM working for spark-openivm?" — yes/no/dominantly-overhead.

## Test configuration

- SCALE*FACTOR, BATCH*\*\_PCT, ENGINES, PARALLEL, OPENIVM_VALIDATE, OPENIVM_PROFILE_REFRESH
- Host: cores, RAM, OS
- Pinned SHAs

## Headline timings

| Engine         | Batch 1 (s) | Batch 2 (s) | Batch 3 (s) | IVM ratio b1/b2 | IVM ratio b1/b3 |
| -------------- | ----------- | ----------- | ----------- | --------------- | --------------- |
| duckdb-openivm | …           | …           | …           | …               | …               |
| spark-openivm  | …           | …           | …           | …               | …               |

## Overhead-dominated check (CP4)

- overhead_ratio for spark-openivm batch 2 = X.YY
- no_pending_deltas refresh count = …
- Verdict: { overhead-dominated / not overhead-dominated }

(If overhead-dominated, the rest of the document is a partial
investigation — the primary finding is R0.)

## Per-MV refresh-strategy comparison

| MV | Spark refresh_type | DuckDB strategy | Spark b1/b2/b3 | DuckDB b1/b2/b3 | Primary | Contributing | Evidence |

## Where Spark time is spent (per-step)

(Only present if Phase 5.A patches were applied. Otherwise: "Not
captured — patches were not applied" with a list of patches that
would close it.)

## Where DuckDB time is spent (per-step)

From `openivm_refresh_profile`:

| Step | Total ms | Avg ms | Max ms | Top view |

## Root-cause buckets observed

(One section per bucket that fired as primary or contributing.)

### R<N>. <bucket-name>

- MVs in this bucket: …
- Symptoms / how detected: …
- File(s) / line range where the fix should land: …
- Specific patch sketch (optional): …

## Recommended work by repo

### openivm ([branch], SHA <sha>)

- File:line — what to change — why

### lpts ([branch], SHA <sha>)

- File:line — what to change — why

### openivm-spark ([branch], SHA <sha>)

- File:line — what to change — why

### ivm-bench ([branch], SHA <sha>)

- File:line — what to change — why (telemetry only, no engine work)

## Observability patches applied (if any)

| Repo | Branch | Files changed | Reason | Deployed? | Verified? |

## Appendix A — raw timings

`mount/results/3/dbt-server/benchmark-results.json` (full dump).

## Appendix B — captured Spark execution plans

(One block per MV for which EXPLAIN FORMATTED was captured.)

## Appendix C — pins-sync state at end of run

Output of `./spark-ext/dev/dev.sh pins-sync`.
```

---

## PHASE 8 — Completion signal

After the report exists:

```
{ "status": "Succeeded" }
```

ONLY when ALL of:

- `.research/SPARK-VS-DUCKDB-IVM-EFFECTIVENESS.md` exists
- The report names a primary root-cause bucket per non-trivial MV
  (R0–R6 inclusive; R0 is a valid terminal finding)
- The report names at least one recommended repo to act on, OR
  justifies "no IVM-path bug proven, overhead-dominated at this
  scale" with CP4 evidence
- The benchmark either finished `completed`, OR per-engine isolation
  (CP3) produced usable data for both engines

Otherwise:

```
{ "status": "Failed" }
```

with the report's appendix capturing whatever raw artifacts were
gathered before the failure.

---

## Quick reference — grep tokens & queries

### Spark driver log grep cheatsheet

```bash
# CREATE-time classification + silent demotion reason
grep -nE "\[openivm-mv\] view='[^']*' compiled_refresh_type=" /tmp/livy-driver.log

# REFRESH-time outcome (no-op or actual)
grep -nE "\[openivm-mv\] refresh view='[^']*' (outcome|refresh_type=)" /tmp/livy-driver.log

# Each emitted Spark statement before execution
grep -nE "\[openivm-mv\] refresh view='[^']*' stmt\[" /tmp/livy-driver.log

# DML interceptor warnings (rare)
grep -nE "\[openivm\] hasDependentMvs failed" /tmp/livy-driver.log

# Phase 4 (5.A) custom telemetry
grep -nE "\[ivm-spark\]\[step=" /tmp/livy-driver.log
```

### Spark Delta bookkeeping queries

```sql
SELECT name, refresh_type_name, source_tables, location, properties
FROM   delta.`_ivm/_meta/mv_metadata`;

SELECT base_table, op_type, staging_path, txn_ts, consumed_by
FROM   delta.`_ivm/_meta/staging`;
```

### DuckDB observability queries

```sql
SELECT * FROM openivm_refresh_profile ORDER BY profile_timestamp DESC, step_order;
SELECT * FROM openivm_refresh_history ORDER BY refresh_timestamp DESC;
PRAGMA refresh_cost('<mv_name>');
```

### lpts pragmas (useful inside the duckdb-openivm container)

```sql
PRAGMA lpts('<query>');         -- dump emitted SQL
PRAGMA lpts_check('<query>');   -- EXCEPT ALL round-trip oracle
PRAGMA print_ast('<query>');    -- AST dump
```

### Known Spark-hostile lpts emissions (Phase 4 / Axis A reasoning)

| Pattern in failing SQL             | lpts site                     | Fix location                            |
| ---------------------------------- | ----------------------------- | --------------------------------------- |
| `struct_pack` / `field := expr`    | `lpts_pipeline.cpp:756-781`   | lpts (emit `named_struct`)              |
| `TIMESTAMP WITH TIME ZONE`         | `lpts_pipeline.cpp:672-677`   | openivm-spark `LptsSparkDialect` (done) |
| Window `GROUPS` frames / `EXCLUDE` | `lpts_pipeline.cpp:591-625`   | lpts                                    |
| Function-name mismatches           | `lpts_pipeline.cpp:694-729`   | lpts                                    |
| `INSERT OR REPLACE` / `OR IGNORE`  | `cte_nodes.cpp:69-90`         | lpts                                    |
| `AT (VERSION => …)`                | `lpts_pipeline.cpp:1110-1116` | lpts                                    |
| `_tf(...)` table-function aliasing | `cte_nodes.cpp:123-136`       | lpts                                    |
| `UNNEST(...)` raw                  | `lpts_pipeline.cpp:892-895`   | depends (sometimes openivm-spark)       |

Use this table to disambiguate `compile_failed` cases between R5
(lpts) and R-dialect (openivm-spark `LptsSparkDialect`).
