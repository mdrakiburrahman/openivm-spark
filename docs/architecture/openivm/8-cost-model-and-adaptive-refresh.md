# 8. Question D6: cost model and adaptive refresh
Question D6 asks a simple operational question:
```text
When is FULL_REFRESH faster than incremental refresh?
```
The short answer is:
```text
when |ΔD| is large relative to |D|
```
`D` is the base data.
`ΔD` is the pending delta.
`|D|` is the size of the base.
`|ΔD|` is the size of the change set.
Incremental refresh exists because many real workloads have:
```text
|ΔD| << |D|
```
In that case, maintaining the MV from the delta is much cheaper than running
the original view query again.
But the advantage is not permanent.
If a batch rewrites a large fraction of the base table, incremental maintenance
can become slower than full recompute.
A useful first heuristic is:
```text
FULL_REFRESH becomes competitive around |ΔD| / |D| = 0.30 to 0.50
```
That 30-50% range is only a heuristic.
The real crossover is workload-dependent.
It depends on view shape, join fanout, touched groups, touched partitions,
file layout, cache state, merge cost, and MV write amplification.
## 8.1 The core cost comparison
Every refresh has two conceptual strategies.
The first strategy is incremental refresh.
It applies the OpenIVM delta program to the existing MV.
The second strategy is full refresh.
It recomputes the MV from the stored view query.
The rough comparison is:
```text
incremental ≈ cost(Q over ΔD) + cost(apply result to MV)
full        ≈ cost(Q over D)  + cost(replace MV)
```
For a narrow projection, incremental cost usually scales with changed rows.
For a grouped aggregate, it usually scales with changed rows and touched
groups.
For a join, it scales with delta rows multiplied through the other inputs.
For a window query, it often scales with changed rows and touched partitions.
So `|ΔD| / |D|` is the starting point.
It is not the whole model.
A small delta with high join fanout can be expensive.
A large append-only delta for a projection can still be cheap.
## 8.2 Classification is structural
OpenIVM's classifier answers a structural question:
```text
Can this query be maintained incrementally, and if so by which refresh type?
```
It does not, by itself, answer:
```text
Is the incremental path cheaper for this particular refresh batch?
```
That distinction matters.
A view can be structurally classified as `SIMPLE_PROJECTION` even if today's
batch changes 80% of the table.
A view can be structurally classified as `AGGREGATE_GROUP` even if the delta
touches every group.
A join can be incrementalizable even if a dense delta makes the join expansion
more expensive than full recompute.
The classifier is based on plan features.
It looks at operators, aggregates, joins, windows, DISTINCT, unsupported
constructs, and rewriteability.
It is not a runtime sampling pass.
It does not inspect Spark Delta file sizes.
It does not estimate pending staging cardinality in openivm-spark.
It does not rerun because workload heuristics changed.
The structural answer remains the same even if the next batch is huge.
## 8.3 Verified upstream knobs and pragmas
The checked upstream source has an experimental refresh-time cost model.
The relevant source files are:
- `src/openivm_extension.cpp`
- `src/upsert/refresh.cpp`
- `src/upsert/refresh_sql.cpp`
- `src/upsert/refresh_cost_model.cpp`
- `src/include/upsert/refresh_cost_model.hpp`
The registered settings include:
```sql
SET openivm_adaptive_refresh = true;
SET openivm_cost_decay = 0.9;
SET openivm_refresh_mode = 'full';
```
The registered cost-related pragmas include:
```sql
PRAGMA refresh_cost('view_name');
PRAGMA refresh_history('view_name');
```
The ordinary refresh pragmas are:
```sql
PRAGMA refresh('view_name');
PRAGMA compile_refresh('view_name');
```
`openivm_adaptive_refresh` is the main adaptive switch.
It is registered as an experimental boolean option.
Its default is `false`.
When it is off, the native refresh path uses the normal IVM strategy unless
other full-refresh conditions apply.
`openivm_cost_decay` is a learned-model setting.
It controls decay for regression over refresh history.
The default in the checked source is `0.9`.
`PRAGMA refresh_cost` returns an estimate for one view.
`PRAGMA refresh_history` returns history used for later calibration.
`openivm_refresh_mode = 'full'` is the force-full mechanism in the checked
source.
## 8.4 What was not found
The source search did **not** find a setting or PRAGMA named:
```sql
openivm_force_full_refresh
```
The force-full spelling in the checked code is instead:
```sql
SET openivm_refresh_mode = 'full';
PRAGMA refresh('view_name');
```
The source search also did **not** find a user-facing adaptive setting named:
```sql
cost_threshold
```
There are internal threshold constants for unrelated rewrite and SQL-size
cases.
Those are not a general delta/base crossover policy.
So the important correction is:
```text
force full:      openivm_refresh_mode = 'full'
not found:       openivm_force_full_refresh
not found:       cost_threshold
adaptive switch: openivm_adaptive_refresh
```
## 8.5 What the adaptive path does
When `openivm_adaptive_refresh` is true, native OpenIVM refresh performs a
cost-estimation step before generating final refresh SQL.
It parses the stored view query.
It plans and optimizes that query.
It calls `EstimateRefreshCost`.
It estimates the incremental path.
It estimates full recompute.
It chooses recompute if:
```text
recompute_predicted_ms < incremental_predicted_ms
```
The estimate has static components:
- incremental compute;
- incremental upsert;
- recompute compute;
- recompute replace.
It also has calibrated predictions:
- incremental predicted milliseconds;
- recompute predicted milliseconds.
Calibration uses refresh history when enough samples exist.
The source records history after refresh execution.
The recorded method can be `incremental`, `full`, `group_recompute`, or
`window_partition` depending on the path.
This means current upstream OpenIVM does have an experimental adaptive model.
It is not the same thing as the structural classifier.
It is also not a stable universal policy knob.
## 8.6 What openivm-spark should assume
openivm-spark uses upstream OpenIVM primarily as a compiler.
The Spark bridge calls `PRAGMA compile_refresh` through the DuckDB CLI.
It receives a classified refresh type and a SQL program.
Then `SparkRefreshRewriter` translates that program to Spark-executable SQL.
That compile path is not the same as native `PRAGMA refresh` execution.
A Spark refresh should therefore be treated as structurally selected unless
openivm-spark adds its own cost decision around execution.
The Spark side does not currently have a general automatic rule such as:
```text
if pending_delta_bytes > 40% of base_bytes, use full refresh
```
It does not sample staging Delta files to estimate cardinality before choosing
between incremental SQL and initial-load SQL.
It does not automatically rerun the classifier when workload heuristics change.
Users still need external policy for large deltas.
## 8.7 What OpenIVM still does not provide as complete policy
Even with the upstream experimental adaptive path, OpenIVM should not be viewed
as a complete workload manager.
The remaining gaps are:
- no stable user-facing `cost_threshold` knob;
- no discovered `openivm_force_full_refresh` PRAGMA;
- no Spark-side automatic delta/base crossover decision;
- no Spark-side cardinality estimator for staged Delta rows;
- no automatic orchestration for cadence, compaction, and full rebuilds;
- no create-time classifier rerun that would change a structural refresh type
  because the current delta is large.
The user, scheduler, or platform controller still decides when to force full
rebuilds.
That decision is usually based on data-volume signals and profile history.
## 8.8 Cost-model thought experiment
This table is a mental model, not a contract.
It shows which variable matters for each refresh family.
| RefreshType | Cost roughly scales with | Best when |
| --- | --- | --- |
| `FULL_REFRESH` | `O(|D|)` | `|ΔD| / |D| > 0.5` or incremental is unsupported |
| `SIMPLE_PROJECTION` | `O(|ΔD|)` | `|ΔD| << |D|` |
| `SIMPLE_AGGREGATE` | `O(|ΔD|)` plus a small MV update | usually, unless the full plan changes |
| `AGGREGATE_GROUP` | `O(|ΔD|)` plus touched-group merge | `|ΔD groups| << |all groups|` |
| `JOIN_INCREMENTAL (n=2)` | `O(|ΔR| × |S| + |R| × |ΔS|)` | one side is small or one delta is sparse |
| `JOIN_INCREMENTAL (n=k)` | up to `O(2^k - 1)` terms | small `k`, sparse deltas, pruning works |
| `WINDOW_PARTITION` single source | `O(|ΔD| + |touched partitions|)` | touched partitions are few |
| `WINDOW_PARTITION` multi source | approximately `O(|D|)` recompute behavior | never beneficial; treat as FULL_REFRESH cost |
The join rows are the danger zone.
For `k` joined inputs, the standard delta expansion can grow toward
`2^k - 1` terms.
Empty-delta skipping, FK pruning, and DuckLake N-term telescoping can reduce
that cost.
But dense deltas still erase much of the incremental advantage.
## 8.9 Why large deltas defeat incremental refresh
Incremental refresh replaces one full query with delta computation plus MV
writes.
That is cheaper only while the delta computation and MV writes stay small.
For projection, the bad case is a bulk update or overwrite.
The refresh must process most rows and modify most MV rows.
For grouped aggregates, the bad case is a delta that touches most groups.
The merge becomes close to a full-MV rewrite.
For joins, the bad case is high fanout.
A small base-table delta can produce a large joined delta.
For windows, the bad case is partition spread.
One changed row in every partition can force nearly all partitions to be
recomputed.
So the better rule is:
```text
Do not look only at changed rows.
Look at changed rows after the refresh type amplifies them.
```
## 8.10 Adaptive strategy A: monitor staging Delta size
A practical openivm-spark controller can monitor staged Delta files.
The relevant data is in `_ivm` storage and Delta transaction logs.
For example, it can watch add-file sizes under paths like:
```text
_ivm/views/.../_delta_log/
```
The controller estimates pending delta bytes.
It compares them to base-table bytes.
A simple policy is:
```text
if pending_delta_bytes > X% of base_table_bytes:
    drop and recreate the MV
else:
    run incremental refresh
```
Start with `X = 30` to `50`.
Lower it for joins and group-heavy aggregates.
Raise it for append-only projections.
Then replace the heuristic with measured profile data.
## 8.11 Adaptive strategy B: drop and recreate
When the delta is too large, drop-and-recreate is often cleaner than forcing a
large incremental replay.
In openivm-spark, recreate uses the initial-load path.
When upstream `compile_refresh` provides `initialLoadSql`, Spark stores that SQL
in MV metadata.
Create-time execution can use `initialLoadSql` instead of the original user SQL
for refresh types that need hidden columns or adjusted state.
For `FULL_REFRESH` or missing initial-load SQL, Spark uses the original query.
Drop-and-recreate is useful after:
- large backfills;
- partition overwrites;
- schema-compatible reloads;
- migration batches;
- updates that touch most keys;
- compaction cycles that rewrite most files.
The tradeoff is obvious.
A recreate is disruptive and scans the base.
But it avoids expensive per-row or per-key replay of a massive delta.
## 8.12 Adaptive strategy C: profile and throttle
Upstream OpenIVM has refresh profiling.
The relevant setting is:
```sql
SET openivm_profile_refresh = true;
```
Profile rows are written to:
```text
openivm_refresh_profile
```
Chapter 9 covers profiling in detail.
For this chapter, the key point is that profile data turns guesses into
thresholds.
If profile rows show that merge statements dominate, lower the delta threshold
for that MV.
If SQL generation dominates tiny refreshes, batch refreshes less frequently.
If full recompute is consistently cheap, schedule it more often.
If one RefreshType is always slow on a workload, build a per-type policy.
`PRAGMA refresh_cost` and `PRAGMA refresh_history` provide native OpenIVM cost
and history diagnostics.
`openivm_refresh_profile` provides step-level post-mortem timing.
Together, those are the ingredients for an external cost model.
## 8.13 Adaptive strategy D: alternate cadence
Bursty workloads often need mixed cadence.
A common schedule is:
```text
hourly incremental refresh
nightly full rebuild
```
Another schedule is:
```text
incremental during normal traffic
full rebuild after the bulk ingest window
```
A third schedule is threshold-driven:
```text
incremental while delta/base is small
full rebuild once delta/base crosses the threshold
```
This is not a workaround.
It is often better than a purely local optimizer because the scheduler knows
business cycles.
The SQL compiler does not know that a midnight load rewrites half the fact
table.
The orchestration layer does.
## 8.14 Worked back-of-envelope
Assume a base table with:
```text
|D| = 100,000,000 rows
```
Assume a pending delta with:
```text
|ΔD| = 100,000 rows
```
The ratio is:
```text
|ΔD| / |D| = 0.001 = 0.1%
```
For `SIMPLE_PROJECTION`, this is the ideal case.
A rough intuition might be:
```text
incremental projection ≈ 10 ms
full refresh           ≈ 5 s
```
The exact numbers are illustrative.
The shape is the lesson.
A 0.1% delta should not require scanning 100 million rows.
Now increase the delta.
At 5 million rows, the delta ratio is 5%.
For the toy numbers above, this can already be a plausible crossover.
At 10 million rows, the delta ratio is 10%.
The crossover is very plausible if MERGE/write amplification is high.
At 30 million to 50 million rows, the ratio is 30-50%.
Full refresh becomes a serious contender.
For merge-heavy joins and aggregates, the crossover may happen earlier.
For append-only projections, it may happen later.
## 8.15 Future work: a real Spark-side cost decision
A fuller openivm-spark design would add a cost decision immediately before
refresh execution.
The decision would estimate:
- pending delta rows;
- pending delta bytes;
- base rows or bytes;
- MV rows or bytes;
- touched partitions;
- touched grouping keys;
- join fanout risk;
- previous refresh timings.
For Spark, many inputs should come from Delta metadata and Parquet footers
rather than full `COUNT(*)` scans.
The bridge could then choose:
```text
execute incremental SparkRefreshRewriter output
```
or:
```text
execute full initialLoadSql / recompute SQL
```
A possible upstream-facing API would be:
```sql
PRAGMA openivm_cost_decide('view_name');
```
Another option is extending `PRAGMA refresh_cost` so a bridge can supply
external cardinality estimates.
The result should include:
```text
strategy = incremental | full
reason   = structural_full | user_forced | delta_ratio | learned_history
```
The classifier would remain structural.
The cost model would be runtime-adaptive.
Those are separate responsibilities.
## 8.16 Why rerunning the classifier is not the answer
A classifier rerun asks the wrong question.
It asks:
```text
Can this query be incrementalized?
```
The large-delta problem asks:
```text
Should this valid incremental program be used for this batch?
```
A `SIMPLE_PROJECTION` remains a `SIMPLE_PROJECTION` when the delta grows.
An additive grouped aggregate remains additive when the delta touches every
group.
An incremental join remains algebraically incremental when the join fanout is
expensive.
The structural type does not change.
Only the economics change.
That is why the cost decision belongs at refresh time.
## 8.17 Practical policy template
A simple external policy can track these values per MV:
```text
refresh_type
base_bytes
mv_bytes
pending_delta_bytes
touched_group_estimate
touched_partition_estimate
last_incremental_ms
last_full_ms
```
Then compute:
```text
delta_ratio = pending_delta_bytes / max(base_bytes, 1)
```
Initial thresholds can be:
```text
SIMPLE_PROJECTION     full if delta_ratio > 0.50
SIMPLE_AGGREGATE      full if delta_ratio > 0.70
AGGREGATE_GROUP       full if touched_group_ratio > 0.40
JOIN_INCREMENTAL      full if delta_ratio > 0.10 to 0.30
WINDOW_PARTITION      full if touched_partition_ratio > 0.40
FULL_REFRESH          always full
```
These numbers should not be hard-coded forever.
They are bootstrapping values.
Replace them with profile-driven thresholds after observing real refreshes.
## 8.18 Final answer
OpenIVM's structural classifier does not decide cost.
It decides which refresh program is valid.
The checked upstream source also contains an experimental native adaptive
refresh model behind `openivm_adaptive_refresh`.
That model can compare estimated incremental cost with full recompute cost and
choose recompute when predicted recompute time is lower.
It exposes diagnostics through `PRAGMA refresh_cost` and history through
`PRAGMA refresh_history`.
The checked source does not contain `openivm_force_full_refresh` or a general
`cost_threshold` PRAGMA.
Force full recompute with:
```sql
SET openivm_refresh_mode = 'full';
PRAGMA refresh('view_name');
```
For openivm-spark, users should still implement external adaptive policy.
Monitor pending staging size.
Compare it with base size.
Use profile data from chapter 9.
Choose incremental refresh for small sparse deltas.
Choose full refresh or drop-and-recreate for large dense deltas.
The core rule remains:
```text
incremental wins when |ΔD| is small relative to |D|;
FULL_REFRESH wins when the delta is large enough that replaying it costs as
much as recomputing the view.
```
