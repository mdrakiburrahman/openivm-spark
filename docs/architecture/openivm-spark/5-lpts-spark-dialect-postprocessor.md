# 5. `LptsSparkDialect`: the SQL post-processor

> Upstream side of the contract: see
> [../lpts/8-integration-with-openivm-and-spark.md](../lpts/8-integration-with-openivm-and-spark.md).

## Role

OpenIVM emits refresh SQL through LPTS with `openivm_target_dialect='spark'`.
That "LPTS-Spark" dialect is close to Spark SQL, but it is not directly
executable by Spark 3.5 / Delta Lake 3.2.

The remaining gaps are small but fatal: DuckDB postfix casts, DuckDB helper
functions, macro bodies inlined by OpenIVM, SQL-standard timestamp suffixes,
quoted interval spellings, and DuckDB identifier quoting.

`LptsSparkDialect` is the final token-level SQL post-processor. Without it,
Spark fails with parser errors, unresolved functions, missing type names, or
wrong semantics before the refresh rewriter can execute the maintenance plan.

Design notes often call this "~13 rewrites". The current `translate()` source
contains fourteen top-level passes, plus one helper pass for parenthesized casts
inside `rewritePostfixCasts`.

## Flow

```mermaid
flowchart LR
  A[openivm DuckDB CLI stdout] --> B[JSON parse]
  B --> C[raw sql]
  C --> D[LptsSparkDialect.translate<br/>(13 rewrite passes)]
  D --> E[Spark-executable SQL]
  E --> F[SparkRefreshRewriter]
```

The diagram is the conceptual contract. In the refresh implementation,
`LptsSparkDialect.translate` is supplied as `postProcess` to
`SparkRefreshRewriter.rewrite` (`MaterializedViewCommands.scala:983-1003`) and
is applied to each kept statement at the end of that rewriter
(`SparkRefreshRewriter.scala:173-175`). Initial-load SQL is translated directly
(`OpenIvmCompiler.scala:344-348`, `MaterializedViewCommands.scala:398-415`).

## Compile bridge context

`OpenIvmCompiler` builds a DuckDB CLI script that loads the OpenIVM extension,
sets `openivm_target_dialect='spark'`, creates empty source tables, creates a
DuckDB materialized view, and runs `PRAGMA compile_refresh`
(`OpenIvmCompiler.scala:144-196`).

The CLI result is read from JSON-lines stdout and decoded into
`CompiledRefresh.sql` (`OpenIvmCompiler.scala:301-370`). That string is a
multi-statement maintenance program, not the user's original query.

Compiled SQL is cached in MV metadata as `_ivm_compiled_sql`
(`MvCatalog.scala:89-101`). Refresh reuses the cached program when present and
recompiles only on a miss (`MaterializedViewCommands.scala:908-941`). Because
cached SQL may live across releases, `translate()` must stay idempotent and
backward-compatible with the emitted shapes it already handles.

## `translate()` pipeline

Source: `LptsSparkDialect.scala:94-131`.

Current call order:

1. `rewriteNowTimestamp`
2. `rewriteToTimestampDoubleCast`
3. `rewriteSparkFunctionInlinings`
4. `rewriteTimestampWithTimeZone`
5. `rewriteStructExtract`
6. `rewritePostfixCasts`
7. `rewriteBareVarcharCast`
8. `rewriteBareHugeIntCast`
9. `rewriteGenerateSeries`
10. `rewriteToTemporalUnit`
11. `rewriteIntervalLiterals`
12. `rewriteCountStar`
13. `rewriteErrorFn`
14. `rewriteDoubleQuotedIdentifiers`

Order matters. `rewriteNowTimestamp` must run before generic postfix casts so
`now()::timestamp` becomes `current_timestamp()`, not `CAST(now() AS TIMESTAMP)`
(`LptsSparkDialect.scala:94-99`). Double-quoted identifier rewriting runs last
so earlier passes can still match DuckDB-shaped syntax.

## Rewrite reference

### 1. `rewriteNowTimestamp`

- Source: `LptsSparkDialect.scala:349-350`.
- Input: `now()::timestamp`.
- Output: `current_timestamp()`.
- Why: OpenIVM/DuckDB emits a postfix cast form; Spark's canonical current-time
  function for the refresh program is `current_timestamp()`.

### 2. `rewriteToTimestampDoubleCast`

- Source: `LptsSparkDialect.scala:138-142`.
- Input: `to_timestamp(CAST('2024-01-01 00:00:00' AS DOUBLE))`.
- Output: `to_timestamp('2024-01-01 00:00:00')`.
- Why: DuckDB binds Spark-looking string timestamp calls through a `DOUBLE`
  overload. Spark would cast the date string to `DOUBLE` as `NULL`, changing the
  result. Numeric or column epoch arguments are not rewritten.

### 3. `rewriteSparkFunctionInlinings`

- Source: `LptsSparkDialect.scala:165-174`.
- Detailed scanner: `SparkFunctionShimSql.scala:87-109`.
- Inputs and outputs:
  - `regexp_matches(s, p)` -> `regexp_like(s, p)`.
  - `CAST(strptime(s, '%Y-%m-%d') AS DATE)` -> `to_date(s)`.
  - `CAST(strptime(s, fmt) AS DATE)` -> `to_date(s, fmt)`.
  - `strptime(s, '%Y-%m-%d %H:%M:%S')` -> `to_timestamp(s)`.
  - `strptime(s, fmt)` -> `to_timestamp(s, fmt)`.
  - `strftime(d, fmt)` -> `date_format(d, fmt)`.
  - `last_value(expr IGNORE NULLS) OVER (...)` ->
    `last_value(expr, true) OVER (...)`.
  - `last(expr) OVER (...)` -> `last_value(expr) OVER (...)`.
- Why: the compiler bridge registers Spark-function shims before DuckDB binds
  the MV body (`OpenIvmCompiler.scala:479-533`). OpenIVM serializes the macro
  body, so the post-pass must recover Spark's original function names.
- Safety: the shared scanner treats single-quoted strings, double-quoted
  identifiers, and comments as opaque (`SparkFunctionShimSql.scala:5-12`).

### 4. `rewriteTimestampWithTimeZone`

- Source: `LptsSparkDialect.scala:278-285`.
- Input: `CAST(action_ts AS TIMESTAMP WITH TIME ZONE)`.
- Output: `CAST(action_ts AS TIMESTAMP)`.
- Also: `TIMESTAMP WITHOUT TIME ZONE` -> `TIMESTAMP`.
- Why: Spark 3.5 does not parse those SQL-standard suffixes in the emitted cast
  contexts. Spark's usable target type here is `TIMESTAMP`.

### 5. `rewriteStructExtract`

- Source: `LptsSparkDialect.scala:305-344`.
- Input: `struct_extract(s, 'k')`.
- Output: `s.k`.
- Nested input: `struct_extract(struct_extract(s, 'a'), 'b')`.
- Nested output: `s.a.b`.
- Why: DuckDB serializes struct field access as `struct_extract`; Spark SQL
  expects dot notation.
- Iteration requirement: the regex matches an innermost call
  (`LptsSparkDialect.scala:315`), then the loop repeats until no more matches
  remain (`LptsSparkDialect.scala:325-342`). This bottom-up loop is required for
  nested calls.
- Quoting: `struct_extract(s, 'a b')` becomes `s.\`a b\`` for non-simple fields
  (`LptsSparkDialect.scala:317-338`).

### 6. `rewritePostfixCasts`

- Source: `LptsSparkDialect.scala:363-421`.
- Helper: `rewriteParenthesisedCasts`, `LptsSparkDialect.scala:433-476`.
- Inputs and outputs:
  - `'2024-01-01'::TIMESTAMP` -> `CAST('2024-01-01' AS TIMESTAMP)`.
  - `mv.openivm_timestamp::DATE` -> `CAST(mv.openivm_timestamp AS DATE)`.
  - `123::BIGINT` -> `CAST(123 AS BIGINT)`.
  - `amount::DECIMAL(10,2)` -> `CAST(amount AS DECIMAL(10,2))`.
  - `COALESCE(v.sum + d.sum, v.sum, d.sum)::DOUBLE` ->
    `CAST(COALESCE(v.sum + d.sum, v.sum, d.sum) AS DOUBLE)`.
- Why: DuckDB/PostgreSQL postfix casts are not Spark cast syntax.
- Safety: single-quoted string literals are placeholder-protected before regex
  replacement, so `'x::TIMESTAMP'` remains literal text.
- Type normalization in this pass: `VARCHAR`/`CHAR`/`TEXT` -> `STRING`, and
  `HUGEINT`/`UHUGEINT` -> `BIGINT`.

### 7. `rewriteBareVarcharCast`

- Source: `LptsSparkDialect.scala:540-543`.
- Input: `CAST(name AS VARCHAR)`, `CAST(code AS CHAR)`, `CAST(c AS TEXT)`.
- Output: `CAST(name AS STRING)`, `CAST(code AS STRING)`, `CAST(c AS STRING)`.
- Why: Spark rejects bare `VARCHAR`/`CHAR` without a size. `STRING` is Spark's
  unbounded text type.
- Preserved: `CAST(name AS VARCHAR(32))`.

### 8. `rewriteBareHugeIntCast`

- Source: `LptsSparkDialect.scala:551-554`.
- Input: `CAST(sum_x AS HUGEINT)`, `CAST(sum_y AS UHUGEINT)`.
- Output: `CAST(sum_x AS BIGINT)`, `CAST(sum_y AS BIGINT)`.
- Why: DuckDB can promote integer aggregates to 128-bit types that Spark does
  not recognize. The Spark-compatible target for the emitted SQL is `BIGINT`.

### 9. `rewriteGenerateSeries`

- Source: `LptsSparkDialect.scala:353-354`.
- Input: `generate_series(1, openivm_multiplicity::BIGINT)`.
- Output after the full pipeline:
  `sequence(1, CAST(openivm_multiplicity AS BIGINT))`.
- Why: DuckDB uses `generate_series`; Spark's array constructor is `sequence`.
  This appears in simple-projection refresh programs.
- Safety: the regex is word-boundary anchored, so `my_generate_series(...)` is
  not rewritten (`LptsSparkDialect.scala:26-29`).

### 10. `rewriteToTemporalUnit`

- Source: `LptsSparkDialect.scala:205-263`.
- Inputs and outputs:
  - `to_milliseconds(CAST(1 AS DOUBLE))` ->
    `((CAST(1 AS DOUBLE)) * INTERVAL 1 MILLISECOND)`.
  - `to_days(7)` -> `((7) * INTERVAL 1 DAY)`.
  - `to_months(CAST(3 AS DOUBLE))` ->
    `((CAST(3 AS DOUBLE)) * INTERVAL 1 MONTH)`.
- Why: DuckDB serializes some intervals as `to_<unit>(...)` helpers. Spark does
  not have those functions, and Spark 3.5 requires literal quantities in
  `INTERVAL <n> UNIT`; multiplying by `INTERVAL 1 UNIT` preserves casted numeric
  expressions (`LptsSparkDialect.scala:249-255`).
- Units: milliseconds, seconds, minutes, hours, days, months, years.

### 11. `rewriteIntervalLiterals`

- Source: `LptsSparkDialect.scala:482-483`.
- Input: `INTERVAL '1 microsecond'`.
- Output: `INTERVAL 1 MICROSECOND`.
- Also: `INTERVAL '30 days'` -> `INTERVAL 30 DAYS`.
- Why: DuckDB accepts quoted interval literals. Spark expects interval quantity
  and unit tokens.

### 12. `rewriteCountStar`

- Source: `LptsSparkDialect.scala:486-487`.
- Input: `count_star()`.
- Output: `COUNT(*)`.
- Why: DuckDB exposes COUNT-star as a function-shaped spelling. Spark expects
  aggregate syntax. OpenIVM aggregate refresh SQL emits `count_star()` in CTEs.

### 13. `rewriteErrorFn`

- Source: `LptsSparkDialect.scala:495-525`.
- Input: `error('scalar subquery returned more than one row')`.
- Output: `raise_error('scalar subquery returned more than one row')`.
- Why: DuckDB has `error(...)`; Spark's equivalent is `raise_error(...)`.
- Safety: string literals are placeholder-protected, so text containing
  `error(` is preserved.

### 14. `rewriteDoubleQuotedIdentifiers`

- Source: `LptsSparkDialect.scala:564-603`.
- Input: `SELECT "name", "user", "value" FROM t`.
- Output: ``SELECT `name`, `user`, `value` FROM t``.
- Why: DuckDB quotes identifiers with double quotes. Spark's safe identifier
  quote is the backtick. This matters for reserved words and generated aliases.
- Safety: single-quoted string literals are protected first; only
  identifier-shaped double-quoted tokens are rewritten.

## Qualified sources: the two-sided rewrite

Qualified Spark source names require cooperation between compiler-side and
refresh-side code.

### The problem

Spark users, especially dbt/Hive users, write sources as `<db>.<table>`:

```sql
SELECT region, SUM(amount) AS total
FROM qualified_src_db.sales_qs2
GROUP BY region
```

The DuckDB compiler subprocess registers only short tables, for example:

```sql
CREATE TABLE sales_qs2 (...)
```

If the qualified SQL reaches DuckDB unchanged, compile fails because DuckDB does
not have Spark's `qualified_src_db` schema. But after compilation, OpenIVM may
emit live-source references as `memory.main.sales_qs2`; Spark must expand those
back to `qualified_src_db.sales_qs2`, not resolve `sales_qs2` in the current
schema.

`QualifiedSourceSpec` documents this failure and the fix
(`QualifiedSourceSpec.scala:12-27`).

### Half 1: `stripDbQualifiers` before DuckDB

Source: `OpenIvmCompiler.stripDbQualifiers`, `OpenIvmCompiler.scala:221-234`.
Call site: `OpenIvmCompiler.scala:194`.

Input:

```sql
SELECT region FROM tpcdi.sales WHERE amount > 0
```

Map:

```scala
Map("sales" -> "tpcdi.sales")
```

Output sent to DuckDB:

```sql
SELECT region FROM sales WHERE amount > 0
```

Why: DuckDB only sees short temporary tables. The implementation uses tracked
qualified names, sorts longest first, and rewrites `<db>.<table>` to `<table>`
so `PRAGMA compile_refresh` can bind. Unit examples cover no-op, multi-source,
case-insensitive, and 3-part-name cases (`OpenIvmCompilerSpec.scala:373-421`).

### Half 2: `rewriteMemoryMainPrefix` before Spark execution

Source: `SparkRefreshRewriter.rewriteMemoryMainPrefix`,
`SparkRefreshRewriter.scala:474-495`.

Input from OpenIVM:

```sql
SELECT * FROM memory.main.sales_qs2
```

Active map:

```scala
Map("sales_qs2" -> "qualified_src_db.sales_qs2")
```

Output for Spark:

```sql
SELECT * FROM `qualified_src_db`.`sales_qs2`
```

Internal fallback:

```sql
memory.main.openivm_delta_sales_qs2
```

becomes:

```sql
`openivm_delta_sales_qs2`
```

Why: OpenIVM's Spark target often includes DuckDB's `memory.main.` catalog
prefix. Internal OpenIVM temp names should become short backticked names; live
source names with a registered mapping must become fully-qualified Spark names.
Otherwise Spark resolves `<short>` against the current schema and can raise
`DELTA_TABLE_NOT_FOUND`.

### Why `ThreadLocal`

Source: `activeQualifiedNames`, `SparkRefreshRewriter.scala:49-65`.

`SparkRefreshRewriter` is a singleton object. The active short-to-qualified map
is per rewrite call, so it cannot be a normal global mutable field. The rewriter
stores it in a `ThreadLocal[Map[String, String]]`.

At the start of `rewrite`, the prior value is saved and the call's map is set
(`SparkRefreshRewriter.scala:122-127`). In a `finally` block, the prior value is
restored (`SparkRefreshRewriter.scala:176-178`). This prevents concurrent
refreshes on different threads from trampling each other's qualification
context. Forked test JVMs are process-isolated; the `ThreadLocal` protects
parallelism inside each JVM.

### Initial-load SQL

`OpenIvmCompiler.parseInitialLoadSql` also replaces `memory.main.<short>` with
qualified Spark names before calling `LptsSparkDialect.translate`
(`OpenIvmCompiler.scala:313-349`). This covers OpenIVM's initial-load query from
`openivm_compiled_queries_<view>.sql`.

## Concrete demo: aggregate MV output

`SparkRefreshRewriterSpec` stores empirical OpenIVM output for:

```sql
CREATE MATERIALIZED VIEW mv_r AS
SELECT region, SUM(amount) AS total
FROM sales
GROUP BY region
```

The captured seven-statement program is cited at
`SparkRefreshRewriterSpec.scala:21-40`. A shortened excerpt follows.

Before `translate()`:

```sql
WITH scan_0 (...) AS (
  SELECT region, amount, openivm_multiplicity
  FROM memory.main.openivm_delta_sales
  WHERE openivm_timestamp >= '2026-05-16 10:00:55'::TIMESTAMP
),
aggregate_6 (...) AS (
  SELECT t10_region, t10_scalar_2, sum(t10_amount), count_star()
  FROM projection_5
  GROUP BY t10_region, t10_scalar_2
)
INSERT INTO openivm_delta_mv_r
  (region, total, openivm_count_star, openivm_multiplicity)
SELECT * FROM projection_11;

UPDATE openivm_delta_tables
SET last_update = COALESCE(
      (SELECT MAX(openivm_timestamp) + INTERVAL '1 microsecond'
       FROM openivm_delta_sales),
      now()
    );
```

After `LptsSparkDialect.translate(...)`:

```sql
WITH scan_0 (...) AS (
  SELECT region, amount, openivm_multiplicity
  FROM memory.main.openivm_delta_sales
  WHERE openivm_timestamp >= CAST('2026-05-16 10:00:55' AS TIMESTAMP)
),
aggregate_6 (...) AS (
  SELECT t10_region, t10_scalar_2, sum(t10_amount), COUNT(*)
  FROM projection_5
  GROUP BY t10_region, t10_scalar_2
)
INSERT INTO openivm_delta_mv_r
  (region, total, openivm_count_star, openivm_multiplicity)
SELECT * FROM projection_11;

UPDATE openivm_delta_tables
SET last_update = COALESCE(
      (SELECT MAX(openivm_timestamp) + INTERVAL 1 MICROSECOND
       FROM openivm_delta_sales),
      now()
    );
```

What changed:

- `'2026-05-16 10:00:55'::TIMESTAMP` ->
  `CAST('2026-05-16 10:00:55' AS TIMESTAMP)`.
- `count_star()` -> `COUNT(*)`.
- `INTERVAL '1 microsecond'` -> `INTERVAL 1 MICROSECOND`.

What did not change in this isolated demo:

- `memory.main.openivm_delta_sales`.

That prefix is handled by `SparkRefreshRewriter.rewriteMemoryMainPrefix`, not by
`LptsSparkDialect` itself.

## Compact multi-pass example

Input:

```sql
SELECT "name",
       struct_extract(struct_extract(payload, 'customer'), 'id') AS cid,
       to_timestamp(CAST('2024-01-01 00:00:00' AS DOUBLE)) AS ts,
       generate_series(1, openivm_multiplicity::BIGINT) AS pos,
       count_star() AS c,
       CAST(total AS HUGEINT) AS h,
       to_milliseconds(CAST(1 AS DOUBLE)) AS one_ms,
       error('bad row') AS err
FROM memory.main.openivm_delta_sales
WHERE event_ts >= '2024-01-01'::TIMESTAMP
  AND event_ts < now()::timestamp + INTERVAL '1 day'
```

Output from `translate()`:

```sql
SELECT `name`,
       payload.customer.id AS cid,
       to_timestamp('2024-01-01 00:00:00') AS ts,
       sequence(1, CAST(openivm_multiplicity AS BIGINT)) AS pos,
       COUNT(*) AS c,
       CAST(total AS BIGINT) AS h,
       ((CAST(1 AS DOUBLE)) * INTERVAL 1 MILLISECOND) AS one_ms,
       raise_error('bad row') AS err
FROM memory.main.openivm_delta_sales
WHERE event_ts >= CAST('2024-01-01' AS TIMESTAMP)
  AND event_ts < current_timestamp() + INTERVAL 1 DAY
```

`memory.main.openivm_delta_sales` remains for the refresh rewriter. If it is an
OpenIVM temp view, it becomes `` `openivm_delta_sales` ``. If it is a tracked
live source, it can become `` `db`.`sales` ``.

## Debugging ownership

Use this split when a refresh statement fails:

1. `::TYPE`, `count_star()`, `generate_series`, quoted intervals,
   `struct_extract`, `HUGEINT`, `error(...)`, or double-quoted identifiers:
   `LptsSparkDialect`.
2. `memory.main.<short>` for a live source: the qualified-source map and
   `rewriteMemoryMainPrefix`.
3. `openivm_data_<view>` or `openivm_delta_<view>` still visible at execution:
   `SparkRefreshRewriter` statement-shape rewriting.
4. Spark-only functions rejected during compile: `OpenIvmCompiler` shim
   pre-pass plus `rewriteSparkFunctionInlinings` post-pass.
5. CREATE succeeds but REFRESH recompiles: MV metadata properties
   `_ivm_compiled_sql` and `_ivm_compiled_initial_load_sql`.

## Source map

| Concern | Source |
| --- | --- |
| `translate()` order | `LptsSparkDialect.scala:104-131` |
| `now()::timestamp` | `LptsSparkDialect.scala:349-350` |
| `to_timestamp(CAST('<literal>' AS DOUBLE))` | `LptsSparkDialect.scala:138-142` |
| Spark shim back-translation | `LptsSparkDialect.scala:165-174`, `SparkFunctionShimSql.scala:87-109` |
| timestamp timezone suffixes | `LptsSparkDialect.scala:278-285` |
| `struct_extract` | `LptsSparkDialect.scala:305-344` |
| postfix casts | `LptsSparkDialect.scala:363-421` |
| parenthesized postfix casts | `LptsSparkDialect.scala:433-476` |
| bare `VARCHAR`/`CHAR`/`TEXT` | `LptsSparkDialect.scala:540-543` |
| bare `HUGEINT`/`UHUGEINT` | `LptsSparkDialect.scala:551-554` |
| `generate_series` | `LptsSparkDialect.scala:353-354` |
| `to_<unit>` helpers | `LptsSparkDialect.scala:205-263` |
| interval literals | `LptsSparkDialect.scala:482-483` |
| `count_star()` | `LptsSparkDialect.scala:486-487` |
| `error(...)` | `LptsSparkDialect.scala:495-525` |
| double-quoted identifiers | `LptsSparkDialect.scala:564-603` |
| `stripDbQualifiers` | `OpenIvmCompiler.scala:221-234` |
| compiler strip call site | `OpenIvmCompiler.scala:194` |
| initial-load `memory.main` rewrite | `OpenIvmCompiler.scala:344-348` |
| `ThreadLocal` qualified names | `SparkRefreshRewriter.scala:49-65` |
| set/restore context | `SparkRefreshRewriter.scala:122-127`, `SparkRefreshRewriter.scala:176-178` |
| `rewriteMemoryMainPrefix` | `SparkRefreshRewriter.scala:474-495` |
| refresh call with `postProcess` | `MaterializedViewCommands.scala:983-1003` |

## Takeaways

`LptsSparkDialect` owns token-level dialect cleanup. It is pure string logic and
has no Spark or DuckDB runtime dependency.

`SparkRefreshRewriter` owns statement structure: keeping, dropping, converting,
and retargeting OpenIVM maintenance statements.

Qualified sources span both layers: compiler-side stripping lets DuckDB compile
against short table names, while refresh-side `memory.main` expansion restores
fully-qualified Spark table names under a per-thread context.
