# 12. Parity gap forensics
Question D4: how do we study the case where OpenIVM DuckDB performs IVM,
but OpenIVM Spark falls back to `FULL_REFRESH`, and how do we fix Spark to
increase parity?
This chapter is a contributor's guide.
It is written for the person closing a parity gap.
The desired end state is always the same:
- the same materialized-view body receives the same OpenIVM refresh type in
  DuckDB and in openivm-spark;
- the Spark refresh path produces a materialized view that is bag-equal to the
  live Spark query;
- the fix is narrow enough that it does not widen a demotion path;
- the new case is covered by a parity spec.
Do not start by changing the demotion code.
Start by proving where the gap is.
Then fix the smallest layer that is actually responsible.
## Cross-links
Read these chapters together with this one:
- Chapter 4, the compile bridge.
- Chapter 5, `LptsSparkDialect`.
- Chapter 6, `SparkRefreshRewriter`.
- Chapter 11, demotion and `FULL_REFRESH` fallback.
This chapter assumes those terms.
It focuses on the forensics loop.
## The parity model
OpenIVM DuckDB is the reference implementation.
openivm-spark is a Spark execution backend for the same OpenIVM classification.
The goal is not merely that the final rows are correct.
The goal is also that the same MV body gets the same `RefreshType` whenever the
Spark runtime can faithfully execute that refresh strategy.
A view that DuckDB classifies as incremental should not quietly become
`FULL_REFRESH` in Spark unless a documented Spark-side limitation makes that the
only correct behavior.
That distinction matters because `FULL_REFRESH` hides missing coverage.
It also hides performance regressions.
A parity gap is present when all of these are true:
1. the MV body is valid in the shared Spark/DuckDB SQL subset;
2. upstream OpenIVM DuckDB compiles it to an incremental refresh type;
3. openivm-spark either records `FULL_REFRESH`, emits no executable refresh SQL,
   or executes refresh SQL that fails or produces the wrong bag;
4. the failure is not an intentional limitation already documented in this
   repository.
A correctness bug is present when Spark records an incremental type but the MV is
not bag-equal to the live query after a staged DML plus `REFRESH`.
A classification bug is present when Spark records `FULL_REFRESH` while the
DuckDB reference classifies the same body as an incremental refresh type.
Both bugs are parity work.
They just land in different layers.
## Where gaps usually happen
There are four common gap classes.
The first is a DuckDB SQL idiom that leaks into the compiled program.
OpenIVM compiles through DuckDB and LPTS.
The resulting program can contain DuckDB spellings such as postfix casts,
`memory.main.<table>` qualifiers, interval helper functions, `count_star()`, or
future function names.
`LptsSparkDialect.translate` is responsible for post-processing those strings
into Spark SQL.
If the translator misses an idiom, Spark may reject the compiled program even
though OpenIVM DuckDB did real IVM.
The second is a refresh-statement shape that the Spark rewriter does not
recognize.
`SparkRefreshRewriter` classifies each compiled statement into a `StatementKind`.
If OpenIVM emits a new or slightly different statement variant, the rewriter may
classify it as `Unknown`.
An unknown statement can be dropped, retained in a form Spark cannot run, or
fail later when a downstream assembler expects a known shape.
The third is a Spark-only optimizer or analyzer edge case.
The canonical example is Spark 3.5's `Union.rewriteConstraints`
`NoSuchElementException` for `EXCEPT ALL` over a scalar correlated subquery.
That is not a DuckDB classification failure.
It is not a staging failure.
It is a Spark plan-construction bug that affects the test oracle or one refresh
assembly shape.
The fourth is a missing row identity in Spark.
`SIMPLE_PROJECTION` currently uses value-equality `MERGE` logic.
If two source rows are byte-identical, Spark cannot distinguish which copy was
deleted.
A delete of one copy can match and delete both copies.
That is a real Spark-side limitation because this project intentionally does not
use Delta CDF.
## What parity does not mean
Parity does not mean accepting a Spark-only rewrite that changes user-visible
semantics.
Parity does not mean forcing `RefreshTypeCode` to match while silently executing
a full recompute.
Parity does not mean deleting a failing `EXCEPT ALL` assertion and replacing it
with `COUNT(*)`.
Parity does not mean logging debug output from test code.
The repo principle still applies: tests should only emit framework status.
Use the `.logs/test-<timestamp>/fork-*.log` files for debug output.
## The standard diagnostic walk
The historical checklist is sometimes called the "five step" walk.
In practice there are six checks.
Do all six unless the root cause is already conclusive.
The order matters.
Start with what OpenIVM compiled.
Then check whether Spark deliberately demoted.
Then check the bag difference.
Then check staging.
Then check dialect translation.
Finally, rerun with debug logs when the raw DuckDB CLI transcript is needed.
## Step 1: read `_ivm_compiled_sql`
The first question is: what did the DuckDB CLI compile bridge return at
`CREATE MATERIALIZED VIEW` time?
The compile result is cached in MV metadata under `_ivm_compiled_sql`.
In code, the key is `MvMetadata.CompiledSqlKey`.
Read it before rerunning the test.
The cached SQL answers three important questions:
- what `RefreshType` was recorded;
- which statements survived the bridge;
- which DuckDB or `memory.main.*` spellings still appear in the program.
In a parity spec, the minimal inspection helper is:
```scala
val id = spark.sessionState.sqlParser.parseTableIdentifier("mv_name")
val meta = MvCatalog.lookup(spark, id).getOrElse(fail("missing MV metadata"))
println(meta.refreshTypeName)
println(meta.properties.getOrElse(MvMetadata.CompiledSqlKey, "<no compiled sql>"))
```
Do not commit the `println` calls.
Use them locally, or inspect metadata in a debugger, or temporarily fail with a
small clue while reproducing.
For a PR, paste the compiled SQL summary into the PR description instead.
When `_ivm_compiled_sql` is absent, there are only a few explanations:
- the view was recorded as `FULL_REFRESH`;
- the compile bridge failed and the demotion path stored an empty compile result;
- the MV was created before compile-result caching existed;
- a test dropped and recreated the view but is reading stale metadata.
If the key is present, split the program into statements and inspect the shape.
Look for statements that begin with these patterns:
- `INSERT INTO openivm_delta_...`;
- `MERGE INTO ...`;
- `UPDATE ... SET ...`;
- `DELETE FROM ...`;
- `CREATE TEMP TABLE openivm_...`;
- `DROP TABLE ...`.
The statement pattern usually points directly at the relevant rewriter branch.
If the statements still contain `memory.main.<table>`, note that for Step 5.
If the statements contain a DuckDB function that Spark does not have, note that
for Gap E.
If the compiled SQL is empty, skip to Step 2.
## Step 2: read the demotion reason
Next confirm that Spark is not intentionally in `FULL_REFRESH`.
Chapter 11 is the full guide for demotion.
This step is only a gate in the parity workflow.
Inspect the MV metadata and the create-time log.
On branches that expose a `MvMetadata.demotionReason` field or property, read
that first.
On the current implementation, the equivalent signals are:
- `meta.refreshTypeName == "FULL_REFRESH"`;
- missing `_ivm_compiled_sql` for a newly-created view;
- the `[openivm-mv]` create-time log line with
  `effective_refresh_type='FULL_REFRESH'`;
- a reason such as `compile_failed`, unsafe HAVING assembly, or unsupported
  initial-load validation.
A compile failure currently logs a line shaped like this:
```text
[openivm-mv] view='...' compiled_refresh_type='COMPILE_FAILED' effective_refresh_type='FULL_REFRESH' reason='compile_failed' cause=...
```
If the demotion reason is expected and documented, the case may not be a parity
gap.
If the demotion reason is `compile_failed` and DuckDB upstream can compile the
same body, continue.
That usually means the Spark bridge sent DuckDB SQL that it could not bind.
Common causes are missing Spark function shims and Spark-only syntax that was not
normalized before `openivm_compile_with_facts`.
If there is no demotion reason but `refreshTypeName` is `FULL_REFRESH`, treat
that as a bug in observability and route the details through Chapter 11.
Do not widen `FULL_REFRESH` to make a failing parity spec pass.
That is the opposite of closing a parity gap.
## Step 3: compare both sides of `EXCEPT ALL`
Once Spark records an incremental refresh type, correctness is a bag comparison.
Use bidirectional `EXCEPT ALL`.
This is the standard oracle in `ivm-it`.
The first direction is:
```scala
mvProj.exceptAll(expectedProj).count()
```
If that count is non-zero, the MV has extra rows.
Extra rows usually mean stale data.
Typical stale-row causes include:
- a delete delta was not staged;
- a delete delta was staged but not consumed;
- a `MERGE` matched the wrong key;
- a cleanup statement was skipped;
- a group recompute delete did not cover every affected group.
The second direction is:
```scala
expectedProj.exceptAll(mvProj).count()
```
If that count is non-zero, the MV is missing rows.
Missing rows usually mean the refresh failed to insert new materialized output.
Typical missing-row causes include:
- an insert delta was not staged;
- the rewriter dropped a compiled insert statement;
- a dialect rewrite changed a predicate;
- a partition-scoped insert used the wrong affected-key relation;
- a value-equality `MERGE` deleted too much.
Never replace this with only `COUNT(*)`.
Counts can match while row multiplicities differ.
Counts can match while one group is stale and another group is duplicated.
The parity suite exists to catch bag-level differences.
If the `EXCEPT ALL` itself crashes in Spark, see Gap C.
Do not weaken the oracle.
Use the collect-based multiset workaround only for the known Spark optimizer
edge case.
## Step 4: inspect the staging column family
If the bag comparison says stale or missing rows, inspect staging next.
The DML interceptor should tee every tracked base-table DML into a staging Delta
path.
The staging record is stored in the RocksDB `staging` column family for the base
table.
The record carries:
- `baseTable`;
- `opType`;
- `stagingPath`;
- `txnTs`;
- consumed state in the per-MV `consumed` column family.
For local forensics inside a spec, call `StagingCatalog.collectFor` before the
refresh consumes the delta:
```scala
val id = spark.sessionState.sqlParser.parseTableIdentifier("mv_name")
val meta = MvCatalog.lookup(spark, id).getOrElse(fail("missing MV metadata"))
val deltas = StagingCatalog.collectFor(
  spark,
  meta.name.table,
  meta.sourceTables,
  meta.sourceWatermarks
)
deltas.map(d => (d.baseTable, d.opType, d.stagingPath, d.txnTs))
```
Do not commit this as verbose test output.
Use it while narrowing the failure.
Interpret the result by operation type:
- `INSERT`, `OVERWRITE`, and `UPDATE_AFTER` synthesize
  `openivm_multiplicity = +1`;
- `DELETE` and `UPDATE_BEFORE` synthesize `openivm_multiplicity = -1`;
- `MV_VIEW_DELTA` preserves an upstream MV's signed multiplicity column;
- `MERGE_SRC` is not a normal source-delta input for refresh assembly.
If there is no staging record after a DML, the failure is probably in
`IvmDmlInterceptorRule` or table-name matching.
If staging exists but the refresh sees no pending deltas, check source table
names and watermarks.
Remember the known sharp edge: metadata may store `db.table`, while a db-less
CREATE produces a db-less `TableIdentifier`.
Match by the trailing short-name segment where that is the intended behavior.
If staging exists, is pending, and has the right operation type, move to the
rewriter and assembler.
## Step 5: check `LptsSparkDialect` output
Now compare the raw compiled SQL with the translated SQL that Spark sees.
`LptsSparkDialect.translate` is pure string post-processing.
It handles dialect leaks that come from DuckDB and LPTS.
It should be idempotent.
For local diagnosis, evaluate:
```scala
val raw = meta.properties(MvMetadata.CompiledSqlKey)
val translated = org.openivm.spark.compiler.LptsSparkDialect.translate(raw)
```
Then search `translated` for dialect leaks.
The highest-signal leak is `memory.main.`.
If `memory.main.<table>` survives into executable Spark SQL, Spark may resolve
the wrong identifier or reject the statement.
Other common leak patterns are:
- postfix casts such as `x::TIMESTAMP`;
- DuckDB interval constructors such as `to_days(CAST(1 AS DOUBLE))`;
- `count_star()`;
- DuckDB `error(...)` instead of Spark `raise_error(...)`;
- double-quoted identifiers that should be backtick quoted;
- `strptime` or `strftime` after a Spark function shim should have been
  reversed.
If the raw SQL is wrong before translation, inspect the compile bridge.
If the raw SQL is right but translated SQL is wrong, fix
`LptsSparkDialect.translate` or the shared shim scanner.
If translated SQL is right but execution is wrong, inspect
`SparkRefreshRewriter` and its statement-specific assemblers.
## Step 6: rerun with debug logs and read the DuckDB CLI transcript
When Steps 1 through 5 do not isolate the issue, rerun the minimal spec with
debug logging enabled.
Use the dev wrapper so fork logs go under `.logs/`:
```bash
OPENIVM_LOG_LEVEL=DEBUG ./spark-ext/dev/dev.sh test 'testOnly org.openivm.spark.parity.YourGapSpec'
```
Then inspect the newest test log directory:
```bash
grep -R "openivm_compile_with_facts\|duckdb\|raw stdout\|raw stderr" .logs/test-*/fork-*.log
```
The goal is to find the DuckDB CLI script, raw stdout, and raw stderr.
The compile bridge builds a script shaped like this:
```sql
LOAD '.../openivm.duckdb_extension';
SET openivm_minmax_incremental=false;
SET openivm_files_path='...';
CREATE TABLE base (...);
CREATE OR REPLACE MACRO ...;
CREATE OR REPLACE MATERIALIZED VIEW mv AS <normalized user SQL>;
SELECT * FROM openivm_compile_with_facts(
  'mv',
  '{"target_dialect":"spark","compile_only":true,"force_view_delta_cascade":true}'
);
```
Compare that script with the hand-run DuckDB reproduction from the general
workflow below.
If DuckDB stderr says a function does not exist, go to Gap D.
If stdout contains a refresh program that Spark cannot parse, go to Gap E.
If stdout is incremental but Spark metadata records `FULL_REFRESH`, go to
Chapter 11 and inspect demotion.
## Known parity gaps and fix recipes
The following gaps are known patterns.
Use them as diagnosis templates, not as permission to skip a minimal repro.
Each fix should still land with a parity spec.
## Gap A: AVG-on-DECIMAL ULP drift
Symptom:
- an MV with `AVG(decimal_col)` differs from recomputed `AVG(decimal_col)`;
- the difference is tiny, usually one or two units in the last place;
- the refresh type is incremental;
- the bag comparison fails only for the average column.
Typical MV body:
```sql
SELECT k, AVG(amount) AS avg_amount
FROM sales
GROUP BY k
```
The root cause is how OpenIVM decomposes `AVG`.
OpenIVM maintains enough state to update average incrementally.
That usually means maintaining `SUM` and `COUNT`, then dividing.
Spark's literal `AVG(decimal_col)` has its own decimal precision and rounding
rules.
A recomposed `SUM / COUNT` expression can round differently from Spark's native
aggregate.
This is not a stale-row failure.
It is numeric representation drift.
Upstream documents the limitation in `openivm/docs/limitations.md:78-79`.
Do not hide it by casting everything to `DOUBLE` in the test.
A plausible Spark-side fix is structural.
Store the maintained `SUM` and `COUNT` in an internal data table.
Expose the user-facing `AVG` as a Spark-computed column in a view.
That mirrors the `AGGREGATE_HAVING` pattern, where `<mv>__ivm_data` stores the
maintenance state and the user-facing object applies the final projection or
predicate.
The shape would be:
```sql
-- Internal data table
SELECT k, SUM(amount) AS openivm_avg_sum, COUNT(amount) AS openivm_avg_count
FROM sales
GROUP BY k
```
Then expose:
```sql
CREATE OR REPLACE VIEW mv AS
SELECT k, openivm_avg_sum / openivm_avg_count AS avg_amount
FROM mv__ivm_data
```
That is only a sketch.
The real implementation must match Spark's native decimal AVG type coercion.
It must also handle null input rows.
It must handle count zero after deletes.
It must preserve user-visible column names.
It must integrate with the refresh type that OpenIVM already selected.
A good PR for this gap includes decimal cases with insert and delete.
It should include at least one case where the current implementation drifts.
It should not relax the `EXCEPT ALL` oracle.
## Gap B: `SIMPLE_PROJECTION` with byte-identical duplicates
Symptom:
```sql
CREATE TABLE base (id INT, label STRING) USING DELTA;
INSERT INTO base VALUES (1, 'a'), (1, 'a');
CREATE MATERIALIZED VIEW mv AS SELECT id, label FROM base;
DELETE FROM base WHERE id = 1 AND label = 'a';
REFRESH MATERIALIZED VIEW mv;
```
A source delete of one logical row can remove both materialized rows.
The MV is then missing one row.
The failure appears in the second `EXCEPT ALL` direction:
```text
<expected> EXCEPT ALL mv (MV missing rows)
```
The root cause is value-equality matching.
For simple projections, Spark does not have a row identity column from the base
Delta table.
The refresh `MERGE` matches on projected values.
Two byte-identical rows are indistinguishable.
When the delete side says "remove one `(1, 'a')`", the value-equality match can
hit both copies.
A user-level workaround is to add a surrogate value to the MV body:
```sql
SELECT RANDOM_UUID() AS openivm_row_id, id, label
FROM base
```
That disambiguates rows.
It also defeats the optimization.
The random surrogate changes the MV semantics and makes stable incremental
maintenance impossible for the original projection.
Do not recommend it as a real parity fix.
The real fix would require Spark-side row-level tracking.
Delta CDF would provide a source row identity/change stream.
This project explicitly implements OpenIVM without Delta CDF.
Therefore full support for byte-identical duplicate deletes is out of scope
unless a different row-identity mechanism is introduced.
When you hit this gap in a parity port, mark the blocked case explicitly.
Use `ignore("...") /* TODO: ... */` with the reason.
Do not silently demote `SIMPLE_PROJECTION` to `FULL_REFRESH`.
Do not weaken the assertion to counts.
## Gap C: Spark 3.5 `Union.rewriteConstraints` NPE on `EXCEPT ALL`
Symptom:
- the MV refresh may be correct;
- the correctness assertion crashes while building or optimizing the
  `EXCEPT ALL` plan;
- the stack trace includes `Union.rewriteConstraints` and
  `NoSuchElementException`;
- the view body includes a scalar correlated subquery.
This is a Spark 3.5 Catalyst bug.
It is not an OpenIVM classification issue.
It is not a Spark refresh rewriter issue unless the refresh itself uses the same
plan shape.
`LateralSpec.assertMvCorrectCollect` is the accepted workaround for the test
oracle.
The workaround preserves bag semantics by collecting both sides and comparing
Scala multisets:
```scala
private def assertMvCorrectCollect(mvName: String, expectedSql: String): Unit = {
  import scala.collection.mutable
  val expected: DataFrame = spark.sql(expectedSql)
  val userCols            = expected.columns.toSeq
  val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)

  def bag(df: DataFrame): mutable.HashMap[Seq[Any], Int] = {
    val m = mutable.HashMap.empty[Seq[Any], Int]
    df.collect().foreach { r =>
      val key = (0 until r.length).map(r.get)
      m.update(key, m.getOrElse(key, 0) + 1)
    }
    m
  }
  val mvBag       = bag(mv)
  val expectedBag = bag(expected)
  withClue(s"$mvName multiset != <expected> multiset: ") {
    mvBag shouldBe expectedBag
  }
}
```
Use this only for the known Spark bug.
Keep the dataset tiny.
The workaround collects to the driver.
It is acceptable for a focused parity spec.
It is not acceptable as a general replacement for `EXCEPT ALL`.
If there is a public Spark JIRA for this exact Catalyst failure, reference it in
the spec comment and PR description.
If there is no known JIRA, say so plainly and include the stack signature.
Do not file the issue against openivm-spark unless the refresh path itself is
wrong.
## Gap D: missing `__sparkfn_*` shim
Symptom:
A user writes a Spark function that DuckDB cannot parse or bind.
Example:
```sql
SELECT unix_timestamp(order_date, 'yyyy-MM-dd') AS order_epoch
FROM orders
```
Spark accepts the function.
DuckDB does not have Spark's `unix_timestamp(string, format)` signature.
The DuckDB CLI errors during `openivm_compile_with_facts`.
openivm-spark records `compile_failed` and demotes to `FULL_REFRESH`.
This is a bridge gap.
The bridge must let DuckDB bind a compile-only equivalent while preserving Spark
runtime semantics.
The pattern has three pieces:
1. pre-rename the Spark spelling to a collision-free `__sparkfn_*` function name
   before DuckDB sees the MV body;
2. register a DuckDB macro with a type-correct body in
   `OpenIvmCompiler.sparkFunctionShimsPrologue`;
3. post-rewrite the inlined DuckDB macro body back to the original Spark spelling
   in `LptsSparkDialect` / `SparkFunctionShimSql`.
A concrete diff for the example looks like this:
```diff
diff --git a/spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/SparkFunctionShimSql.scala b/spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/SparkFunctionShimSql.scala
@@
      "date_format" -> RenameRule(
        Map(
          1 -> "__sparkfn_date_format"
        )
      ),
+     "unix_timestamp" -> RenameRule(
+       Map(
+         1 -> "__sparkfn_unix_timestamp"
+       )
+     ),
      "last_value" -> RenameRule(
        Map(
          1 -> "__sparkfn_last_value"
        )
      )
diff --git a/spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/OpenIvmCompiler.scala b/spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/OpenIvmCompiler.scala
@@
       "CREATE OR REPLACE MACRO __sparkfn_to_timestamp(s, fmt) AS strptime(s, fmt);",
       "CREATE OR REPLACE MACRO __sparkfn_date_format(d, fmt) AS strftime(d, fmt);",
+      "CREATE OR REPLACE MACRO __sparkfn_unix_timestamp(s, fmt) AS epoch(strptime(s, fmt));",
       // Fallback for non-literal Spark `last_value(expr, ignoreNulls)` calls.
diff --git a/spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/SparkFunctionShimSql.scala b/spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/SparkFunctionShimSql.scala
@@
         .orElse(parseFunctionRewrite(sql, i, "strptime", "to_timestamp", Some(OneArgToTimestampLiteral)))
         .orElse(parseFunctionRewrite(sql, i, "strftime", "date_format"))
+        .orElse(parseFunctionRewrite(sql, i, "epoch", "unix_timestamp"))
         .orElse(parseLastValueIgnoreNullsRewrite(sql, i))
```
The final hunk is illustrative.
In practice, `epoch(strptime(s, fmt))` is a nested shape, not a simple two-arg
function call.
Implement the scanner case that exactly matches the inlined macro body.
Do not use a regex that rewrites inside comments or string literals.
Prefer the existing quote/comment-aware scanner in `SparkFunctionShimSql`.
Add tests for all arities you rename.
If a Spark function has one-arg and two-arg forms, use separate `__sparkfn_*`
names where DuckDB macros cannot overload by arity.
If the macro body only needs to bind and will never execute in DuckDB, it may be
a compile-only placeholder.
The placeholder still must return the right type.
It must also retain enough of the original argument text for the post-pass to
recover Spark semantics.
After rebuilding the assembly, recreate the MV.
Old metadata has already cached either an empty compile result or the old
compiled SQL.
`REFRESH` alone will not test the new bridge for an existing MV.
## Gap E: new LPTS dialect rewrite needed
Symptom:
OpenIVM DuckDB compiles the MV to an incremental type.
`_ivm_compiled_sql` is non-empty.
The raw compiled program contains a SQL construct Spark does not understand.
Examples might include a new DuckDB struct function, list function, array
constructor, interval helper, cast spelling, or identifier spelling emitted by a
future DuckDB/LPTS release.
Spark fails when parsing or analyzing the translated refresh SQL.
This is a dialect gap.
The fix belongs in `LptsSparkDialect.translate` when the rewrite is a pure SQL
spelling conversion.
The rewrite should be:
- local;
- idempotent;
- quote/comment aware when it touches function calls;
- covered by compiler-unit tests if such tests exist;
- covered by an `ivm-it` parity spec that exercises the full bridge.
A simple future example is a DuckDB list constructor that Spark should read as an
array constructor:
```diff
diff --git a/spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/LptsSparkDialect.scala b/spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/LptsSparkDialect.scala
@@
   private val CountStarRe = """(?i)\bcount_star\s*\(\s*\)""".r
+  private val ListValueRe = """(?i)\blist_value\s*\(""".r
@@
       rewriteCountStar(
+        rewriteListValue(
         rewriteIntervalLiterals(
@@
+        )
@@
+  private[compiler] def rewriteListValue(sql: String): String =
+    ListValueRe.replaceAllIn(sql, "array(")
```
That diff is intentionally small.
It is also only safe for a function-name rewrite where the argument semantics are
identical.
If argument order changes, use a parser-style helper.
If nested calls matter, use the balanced-parenthesis scanner pattern already in
`LptsSparkDialect` and `SparkFunctionShimSql`.
If a rewrite can change user semantics, do not put it in the dialect layer.
Route it through the compiler bridge or rewriter where refresh type and context
are available.
## Rewriter-layer gaps
Not every parity gap is a dialect rewrite.
If Spark can parse the translated SQL but the refresh assembler drops or
misorders a statement, inspect `SparkRefreshRewriter`.
The rewriter classifies statements into `StatementKind` variants such as:
- `ViewDeltaInsert`;
- `ViewDeltaCompanion`;
- `MvMerge`;
- `SimpleProjectionDataInsert`;
- `ScalarUpdate`;
- `ScalarDeleteMv`;
- `ScalarFullRecomputeInsert`;
- `PartitionScopedDelete`;
- `PartitionScopedInsert`;
- `GroupRecomputeAffectedCreate`;
- `OldSnapshotCreate`;
- `NewSnapshotCreate`;
- `SnapshotDataInsert`;
- `SnapshotDrop`.
A new OpenIVM statement shape should not be handled by broadening a regex until
it catches unrelated SQL.
Add the narrow classifier case.
Then add or adjust the assembler for the relevant refresh type.
Finally add a parity spec that proves the assembled Spark SQL is correct after a
real staged DML.
If the new shape appears only for one `RefreshTypeCode`, constrain the change to
that type.
Avoid global rewrites that affect every compiled program.
## General fix workflow
Use this workflow for every parity-gap PR.
1. Reproduce the gap with a minimal parity spec.
   Use `AggregateSumSpec` as a template for the test harness.
   Keep the spec small.
   Use a unique table and MV prefix.
   Use a UUID warehouse directory.
   Call `MvCatalog.ensureTables(spark)` and `StagingCatalog.ensureTables(spark)`
   in `beforeAll`.
   Use bidirectional `EXCEPT ALL` as the oracle unless Gap C applies.
2. Confirm that DuckDB does IVM.
   Run a tiny DuckDB session with `openivm_compile_with_facts`.
   The script should register only the minimal source DDL and MV body.
   Verify that the returned refresh type is not `FULL_REFRESH`.
   Save the refresh type and a compact SQL excerpt for the PR description.
   A skeleton script is:
   ```sql
   LOAD '/opt/openivm/openivm.duckdb_extension';
   CREATE TABLE base(id INTEGER, amount DECIMAL(10,2));
   CREATE OR REPLACE MATERIALIZED VIEW mv AS
   SELECT id, SUM(amount) AS total FROM base GROUP BY id;
   SELECT * FROM openivm_compile_with_facts(
     'mv',
     '{"target_dialect":"spark","compile_only":true,"force_view_delta_cascade":true}'
   );
   ```
3. Identify the failure layer.
   Use the six-step diagnostic walk.
   The likely layers are:
   - bridge: DuckDB cannot parse or bind the Spark body;
   - dialect: DuckDB compiled SQL contains a Spark-incompatible spelling;
   - rewriter: `StatementKind` classification or ordering is wrong;
   - assembler: a known statement kind is converted to incorrect Spark SQL;
   - staging: the DML tee did not create the expected signed delta;
   - Spark engine: Catalyst or Delta has a version-specific bug.
4. Add the shim, rewrite, classifier, or assembler case.
   Make the smallest change that handles the reproduced shape.
   Do not add a catch-all demotion.
   Do not turn a failed incremental path into `FULL_REFRESH` unless Chapter 11's
   demotion criteria explicitly apply.
   Do not change unrelated refresh types.
5. Add or update a parity spec under `spark-ext/ivm-it/src/test/scala/org/openivm/spark/parity/`.
   Name the spec after the refresh path it exercises.
   Keep each spec around ten `it(...)` cases or fewer.
   Use a unique prefix for every table and MV.
   Use the same helper style as existing specs.
   Drop hidden `openivm_*` columns before comparing.
6. Run the targeted spec.
   Use:
   ```bash
   ./spark-ext/dev/dev.sh test 'testOnly org.openivm.spark.parity.YourGapSpec'
   ```
   Iterate until the targeted spec passes.
   If the failure is in compile or assembly, inspect `.logs/test-<timestamp>/`.
   Do not commit debug prints.
7. Run full verification.
   Use:
   ```bash
   ./spark-ext/dev/dev.sh verify
   ```
   On smaller hosts, cap forks:
   ```bash
   ./spark-ext/dev/dev.sh verify -- -Dopenivm.test.forks=8
   ```
   Fix regressions before opening the PR.
## Filing checklist for a parity-gap PR
Include this checklist in the PR description.
- [ ] Repro spec exists.
- [ ] DuckDB-side IVM verified with `openivm_compile_with_facts`.
- [ ] Refresh type screenshot or text excerpt is attached.
- [ ] Fix is minimally invasive.
- [ ] No demotion path was widened.
- [ ] All relevant parity specs pass.
- [ ] Full `./spark-ext/dev/dev.sh verify` passes, or any infrastructure failure
      is clearly unrelated and documented.
- [ ] `_ivm_compiled_sql` cache for the new spec is summarized in the PR
      description.
- [ ] No verbose logging was added to test code.
- [ ] Any ignored test has an explicit `TODO` and a real engine-gap reason.
## Reading `_ivm_compiled_sql` in a PR description
Do not paste a massive refresh program without context.
Summarize it.
A useful summary includes:
- the MV body;
- DuckDB reference refresh type;
- Spark recorded refresh type;
- the first two statement kinds in the compiled program;
- the dialect idiom or statement shape that was missing;
- the exact fix layer.
Example:
```text
MV body: SELECT d, unix_timestamp(ds, 'yyyy-MM-dd') AS ts FROM gapd_orders
DuckDB: SIMPLE_PROJECTION
Spark before: FULL_REFRESH, reason=compile_failed, DuckDB binder missing unix_timestamp(VARCHAR,VARCHAR)
Spark after: SIMPLE_PROJECTION
Compiled SQL cache: INSERT openivm_delta_mv..., MERGE INTO mv...
Fix layer: Spark function shim + post-pass recovery
```
That gives reviewers enough signal without drowning the PR in generated SQL.
## Anti-patterns
Do not add a blanket `case _ => FULL_REFRESH` around a failing path.
Do not make `StatementKind.Unknown` execute raw DuckDB SQL in Spark.
Do not rewrite every occurrence of a word with `String.replace` if it can appear
inside a string literal.
Do not add a Spark function shim whose DuckDB macro returns the wrong type.
Do not forget that DuckDB macros inline into the compiled SQL.
Do not fix a decimal ULP case by weakening the expected query.
Do not use `COUNT(*)` as a proxy for bag equality.
Do not leave `println` or logger calls in a parity spec.
Do not share `.temp/` paths or scratch forks in committed docs or tests.
Do not assume a db-less table name and a qualified table name compare equal.
## Layer decision table
Use this table when the failure is ambiguous.
| Observation | Likely layer | First file to inspect |
| --- | --- | --- |
| DuckDB CLI says function not found | bridge shim | `SparkFunctionShimSql.scala`, `OpenIvmCompiler.scala` |
| `_ivm_compiled_sql` has DuckDB syntax Spark cannot parse | dialect | `LptsSparkDialect.scala` |
| Translated SQL parses, but a statement is missing | rewriter classifier | `SparkRefreshRewriter.scala` |
| Statement is classified but output MERGE is wrong | assembler | `SparkRefreshRewriter.scala` |
| No staged delta exists after DML | DML tee | `IvmDmlInterceptorRule` and `StagingCatalog` |
| Staged delta exists but is already consumed | staging metadata | `StagingCatalog` consumed CF |
| `EXCEPT ALL` crashes before comparing rows | Spark engine | `LateralSpec.assertMvCorrectCollect` pattern |
| Decimal average differs by ULP | numeric exposure design | AVG data-table/view split |
| Duplicate simple projection delete removes both rows | row identity limitation | documented out of scope |
## Minimal spec template notes
`AggregateSumSpec` is a good template because it shows the core harness.
Copy the structure, not the table names.
The important pieces are:
```scala
class YourGapSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-your-gap-spec-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("openivm-spark-YourGapSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }
}
```
Keep the helper local to the spec unless multiple specs already share it.
Select only user-visible columns from the MV.
If arrays are involved, follow existing specs and JSON-serialize both sides
before `EXCEPT ALL`.
If the case is blocked by a known engine gap, use `ignore` with a `TODO`.
## How to decide whether to fix Spark or document a limitation
Fix Spark when:
- DuckDB reference does incremental IVM;
- Spark can represent the same result without changing user-visible semantics;
- the missing piece is a parser spelling, function shim, statement classifier,
  or assembler case;
- the test can use the standard bag-equality oracle.
Document or ignore when:
- Spark lacks row identity required for correct multiplicity handling;
- Spark Catalyst cannot build the comparison plan and only the test oracle is
  affected;
- Spark and DuckDB have irreconcilable numeric semantics without a larger data
  model change;
- a required feature would amount to implementing Delta CDF.
Even when documenting a limitation, keep the ignored spec.
An ignored spec is executable documentation.
It prevents future contributors from rediscovering the same edge case without
context.
## Final review questions
Before you call a parity gap closed, ask:
1. Did I prove the DuckDB reference refresh type?
2. Did I prove Spark now records the same refresh type?
3. Did I exercise at least one INSERT and, where relevant, DELETE or UPDATE?
4. Did I compare bags in both directions?
5. Did I inspect staging when the failure looked like stale or missing rows?
6. Did I choose the lowest responsible layer?
7. Did I avoid widening demotion?
8. Did I avoid verbose test logging?
9. Did I document the compiled SQL cache in the PR?
10. Did full verification pass?
If any answer is no, the gap is not closed yet.
Parity work is done only when the reference classification, Spark
classification, Spark execution, and regression test all agree.
