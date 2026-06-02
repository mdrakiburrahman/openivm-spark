# 6. LPTS dialect support

This note explains how LPTS targets multiple SQL dialects.
It focuses on the upstream LPTS checkout used by OpenIVM and on the
Spark-facing glue in `openivm-spark`.

The important takeaway is deliberately simple:
LPTS has a small built-in dialect enum, not a pluggable dialect object.
Dialect behavior is implemented by helper functions and `if (dialect == ...)`
branches while serializing the AST and CTE list.
OpenIVM reads `target_dialect` from the per-call CompileFacts JSON and forwards
that enum to LPTS whenever it turns a logical plan into SQL.
`openivm-spark` asks for `spark`, then runs a final string post-processor for
remaining DuckDB/OpenIVM tokens that LPTS does not own.

## Source map

| Area | Source |
|---|---|
| LPTS dialect enum | `.temp/lpts/src/include/sql_dialect.hpp:13-21` |
| Identifier quoting helper | `.temp/lpts/src/lpts_helpers.cpp:9-27` |
| Qualified table names | `.temp/lpts/src/lpts_helpers.cpp:68-92` |
| LPTS setting registration | `.temp/lpts/src/lpts_extension.cpp:286-292` |
| LPTS setting read path | `.temp/lpts/src/lpts_extension.cpp:25-34`, `.temp/lpts/src/lpts_extension.cpp:117-120` |
| Dialect parser | `.temp/lpts/src/lpts_pipeline.cpp:2506-2519` |
| Function-name remapping | `.temp/lpts/src/lpts_pipeline.cpp:908-941` |
| Window-frame Spark checks | `.temp/lpts/src/lpts_pipeline.cpp:780-812` |
| CTE serialization | `.temp/lpts/src/cte_nodes.cpp:82-93`, `.temp/lpts/src/cte_nodes.cpp:372-389` |
| Set operation serialization | `.temp/lpts/src/cte_nodes.cpp:245-284` |
| OpenIVM CompileFacts surface | `.temp/openivm/src/include/compile_facts.hpp` |
| OpenIVM compile table-function path | `.temp/openivm/src/upsert/refresh.cpp` |
| OpenIVM CREATE-time LPTS call | `.temp/openivm/src/core/parser.cpp:301-305` |
| OpenIVM refresh LPTS call | `.temp/openivm/src/upsert/refresh_sql.cpp:867-873` |
| OpenIVM insert-rule LPTS calls | `.temp/openivm/src/rules/refresh_insert_rule.cpp:125-146` |
| Spark bridge sets target | `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/OpenIvmCompiler.scala:149-176` |
| Spark post-processor | `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/LptsSparkDialect.scala:94-131` |

## 1. Dialect enum and abstraction

LPTS defines the dialects in `sql_dialect.hpp`.
The enum is named `SqlDialect`.
It currently has three values:

```cpp
enum class SqlDialect {
    DUCKDB,
    POSTGRES,
    SPARK
};
```

The header documents the intended behavior:
DuckDB is the default.
Postgres uses PostgreSQL-compatible table references and double-quote
identifier syntax.
Spark uses catalog/schema/table qualification, backtick identifiers, and
Spark-compatible window frames.
See `.temp/lpts/src/include/sql_dialect.hpp:13-17`.

There is no `DialectTraits` struct in the current implementation.
The abstraction boundary is the enum plus a small set of helpers:

- `ParseSqlDialect(value)` parses text into the enum.
- `DialectQuoteIdent(name, dialect)` quotes identifiers.
- `DialectVecToQuotedIdentifierList(...)` applies that quoting to lists.
- `DialectQualifiedTableName(...)` renders catalog/schema/table names.
- expression serialization branches on `dialect` for functions and windows.

`ParseSqlDialect` accepts `duckdb`, `postgres`, `postgresql`, and `spark`,
case-insensitively for the all-uppercase spelling shown in the code.
Unknown values throw `InvalidInputException`.
See `.temp/lpts/src/lpts_pipeline.cpp:2508-2519`.

The LPTS extension registers a session option named `lpts_dialect`.
The default is `duckdb`.
A direct LPTS caller can write:

```sql
SET lpts_dialect = 'spark';
PRAGMA lpts('SELECT a FROM t');
```

The registration is in `.temp/lpts/src/lpts_extension.cpp:286-292`.
The read path is `ReadDialect`, which checks the current setting and falls
back to `DUCKDB`; see `.temp/lpts/src/lpts_extension.cpp:25-34`.
`PRAGMA lpts`, `lpts_query`, `lpts_exec`, and `lpts_check` all read the dialect
and pass it into `LogicalPlanToAst` and `AstToCteList`.
The `PRAGMA lpts` path is visible at `.temp/lpts/src/lpts_extension.cpp:117-120`.

## 2. Per-dialect rule set

The following table is the practical rule set.
It distinguishes rules implemented inside LPTS from rules handled later by
`openivm-spark`.

| Rule family | DuckDB dialect | Spark dialect | Postgres dialect | Source / note |
|---|---|---|---|---|
| Identifier quoting | Uses DuckDB `KeywordHelper::WriteOptionallyQuoted`; simple names are often unquoted, reserved/special names use `"name"`. | Always emits backticks, doubling embedded backticks: `` `name` ``. | Same as DuckDB helper; double quotes only when needed. | `.temp/lpts/src/lpts_helpers.cpp:9-27` |
| Column-list quoting | `DialectVecToQuotedIdentifierList` uses the dialect-specific quoting helper. | Same helper, therefore every scan/final alias is backticked. | Same helper as DuckDB. | `.temp/lpts/src/lpts_helpers.cpp:68-76` |
| Qualified table names | `"catalog"."schema"."table"` when catalog is known. | `` `catalog`.`schema`.`table` `` when catalog is known. | The same helper can render three-part names, though Postgres normally wants `schema.table`; LPTS only has limited Postgres compatibility. | `.temp/lpts/src/lpts_helpers.cpp:88-92`, `.temp/lpts/src/cte_nodes.cpp:107-115` |
| Function names | Mostly pass through DuckDB bound function names. | Maps a small set of DuckDB names to Spark equivalents. | Maps `strptime` and `strftime` to Postgres-style names. | `.temp/lpts/src/lpts_pipeline.cpp:908-941` |
| Internal compression functions | `__internal_compress_*` and `__internal_decompress_*` are stripped by rendering the first child. | Same. | Same. | `.temp/lpts/src/lpts_pipeline.cpp:898-907` |
| Operators | Binary arithmetic/string operators render as infix when possible. | Same. | Same. | `.temp/lpts/src/lpts_pipeline.cpp:955-967` |
| Keyword-like scalar functions | `position`, `substring`, `overlay`, and `trim` are emitted as quoted function identifiers to avoid DuckDB parser keyword syntax. | Same current implementation, even though Spark may not need double-quoted function identifiers. | Same. | `.temp/lpts/src/lpts_pipeline.cpp:975-985` |
| Aggregate name aliases | `sum_no_overflow` becomes `sum`; window `count_star` becomes `count`. | Same. | Same. | `.temp/lpts/src/lpts_pipeline.cpp:566-575`, `.temp/lpts/src/lpts_pipeline.cpp:1694-1700` |
| Type names in casts | Uses DuckDB `LogicalType::ToString()`. | Same raw LPTS output; Spark normalization is mainly post-processor work. | Same raw LPTS output. | `.temp/lpts/src/lpts_pipeline.cpp:874-879` |
| Set operations | Emits positional `UNION`, `UNION ALL`, `EXCEPT`, `EXCEPT ALL`, `INTERSECT`, `INTERSECT ALL`. | Same raw syntax; Spark accepts many but not all semantic shapes. | Same. | `.temp/lpts/src/cte_nodes.cpp:245-284` |
| `UNION ALL BY NAME` | Not emitted. DuckDB may parse it as input, but LPTS serializes normalized positional set ops. | Not emitted. | Not emitted. | No `BY NAME` support in LPTS serializer; see set-op source above. |
| Pivot / unpivot | No explicit LPTS support found. Unsupported logical operators fall through to `NotImplementedException`. | Same. | Same. | No `PIVOT`/`UNPIVOT` matches in `.temp/lpts/src`; generic unsupported path is `.temp/lpts/src/lpts_pipeline.cpp:2380-2383`. |
| Window frames | Emits `ROWS`, `RANGE`, or `GROUPS` based on DuckDB window boundaries. | Rejects `GROUPS` and `EXCLUDE` because Spark SQL does not support them. | Same as DuckDB except no Postgres-specific normalization. | `.temp/lpts/src/lpts_pipeline.cpp:608-625`, `.temp/lpts/src/lpts_pipeline.cpp:780-812` |
| LATERAL / dependent joins | DuckDB decorrelates LATERAL into `LOGICAL_DELIM_JOIN` / `LOGICAL_DEPENDENT_JOIN`; LPTS emits ordinary CTE joins plus `SELECT DISTINCT` delim scans. | Same output shape; no `LATERAL` keyword is emitted. | Same. | `.temp/lpts/src/lpts_pipeline.cpp:2258-2355`, `.temp/lpts/src/lpts_pipeline.cpp:2930-2978` |
| CTE materialization hints | Input hints may affect DuckDB's plan, but output CTEs are always `name AS (...)`. | Same. | Same. | `.temp/lpts/src/cte_nodes.cpp:82-93`, `.temp/lpts/test/sql/cte.test:122-155` |
| Recursive CTEs | Emits `WITH RECURSIVE` when needed. | Same raw syntax; Spark support depends on runtime/version. | Same. | `.temp/lpts/src/cte_nodes.cpp:352-389` |
| String literal escaping | LPTS doubles single quotes for embedded SQL strings. | Same; the Spark post-processor protects string literals before regex rewrites. | Same. | `.temp/lpts/src/lpts_helpers.cpp:94-103`, `spark-ext/.../LptsSparkDialect.scala:356-421` |

The table reveals an important design choice.
LPTS is not trying to be a full SQL pretty-printer for every engine.
It serializes DuckDB's optimized logical plan as SQL CTEs and only adjusts the
spots where the output would otherwise be obviously wrong for the selected
engine.

## 3. Function-name tables by dialect

The function-name table is implemented in `ExpressionToAliasedString`.
The relevant block starts at `.temp/lpts/src/lpts_pipeline.cpp:896` and the
actual dialect mapping is at `.temp/lpts/src/lpts_pipeline.cpp:908-941`.

### DuckDB function-name table

DuckDB is the default.
Most bound function names are emitted unchanged.
The generic non-dialect-specific rewrites still apply:

| Input bound function or expression | DuckDB output | Notes |
|---|---|---|
| `__internal_compress_* (x)` | `x` | Strips optimizer compression wrappers. |
| `__internal_decompress_* (x)` | `x` | Same. |
| `sum_no_overflow(x)` aggregate | `sum(x)` | Internal aggregate variant is user-facing `sum`. |
| `count_star()` as a window aggregate | `count(*)` shape through `count` + empty args | See window function-name handling. |
| `+`, `-`, `*`, `/`, `%`, `||` binary calls | infix operator | `a + b`, `a || b`, etc. |
| `position`, `substring`, `overlay`, `trim` | `"position"(...)`, etc. | Quoted to avoid DuckDB keyword syntax collisions. |
| `struct_pack` / `row` returning named struct | `struct_pack("field" := expr, ...)` | Field names come from the return type. |
| Other scalar functions | unchanged | `length`, `lower`, `upper`, `coalesce`, etc. |

Source references:

- compression stripping: `.temp/lpts/src/lpts_pipeline.cpp:898-907`
- aggregate aliasing: `.temp/lpts/src/lpts_pipeline.cpp:1694-1700`
- infix operators: `.temp/lpts/src/lpts_pipeline.cpp:955-967`
- keyword function quoting: `.temp/lpts/src/lpts_pipeline.cpp:975-985`
- struct field emission: `.temp/lpts/src/lpts_pipeline.cpp:968-994`

### Spark function-name table

Spark inherits the generic table above, then applies the following names:

| DuckDB / LPTS bound name | Spark raw LPTS output | Notes |
|---|---|---|
| `strftime` | `date_format` | Spark's closest date formatting builtin. |
| `strptime` | `to_timestamp` | Works for some timestamp patterns but not every shim expansion. |
| `list_transform` | `transform` | Spark higher-order function. |
| `array_transform` | `transform` | Alias to Spark higher-order function. |
| `list_aggregate` | `aggregate` | Spark higher-order aggregate. |
| `array_aggregate` | `aggregate` | Same. |
| `list_filter` | `filter` | Spark higher-order filter. |
| `array_filter` | `filter` | Same. |
| `list_value` | `array` | Spark array constructor. |
| `list_contains` | `array_contains` | Spark builtin. |
| `array_contains` | `array_contains` | Already Spark-compatible. |
| `list_extract` | `element_at` | Spark `element_at` is 1-indexed, matching DuckDB list semantics. |
| `array_extract` | `element_at` | Same. |

Source: `.temp/lpts/src/lpts_pipeline.cpp:916-940`.

This table is intentionally small.
The source comment says Spark functions with identical signatures pass through
unchanged and unsupported functions should fail on the Spark side rather than
being silently mistranslated.
See `.temp/lpts/src/lpts_pipeline.cpp:917-922`.

### Postgres function-name table

Postgres inherits the generic table and maps only two names today:

| DuckDB / LPTS bound name | Postgres output | Notes |
|---|---|---|
| `strptime` | `to_timestamp` | Postgres-style timestamp parsing name. |
| `strftime` | `to_char` | Postgres-style formatting name. |

Source: `.temp/lpts/src/lpts_pipeline.cpp:910-915`.

No broader Postgres function catalog is modeled.
For example, array/list higher-order functions are not translated to Postgres
syntax, and type names still come from DuckDB logical types.

## 4. Spark dialect in detail

Spark is the dialect most relevant to this repository.
`openivm-spark` runs OpenIVM inside a DuckDB CLI subprocess and asks OpenIVM to
emit Spark-targeted refresh SQL through the CompileFacts JSON:

```scala
private[compiler] val SparkCompileFactsJson: String =
  "{\"target_dialect\":\"spark\",\"compile_only\":true,\"force_view_delta_cascade\":true}"
```

Source: `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/OpenIvmCompiler.scala:426-447`.

That facts key eventually becomes `SqlDialect::SPARK` inside OpenIVM.
OpenIVM defines the CompileFacts surface at `.temp/openivm/src/include/compile_facts.hpp`.
When OpenIVM calls LPTS for CREATE-time view SQL or refresh-time delta SQL, it
passes that dialect into both phases:

```cpp
auto ast = LogicalPlanToAst(context, plan, dialect);
auto cte_list = AstToCteList(*ast, dialect);
```

See `.temp/openivm/src/core/parser.cpp:301-305` and
`.temp/openivm/src/upsert/refresh_sql.cpp:867-873`.

### Spark rules LPTS gets right directly

| Input / logical shape | Spark raw LPTS behavior | Why it is useful |
|---|---|---|
| Plain identifiers | Emits backticks: `` `a` ``, `` `memory`.`main`.`t` ``. | Spark accepts backticks for reserved words and mixed case. |
| `SELECT a, COUNT(*) FROM t GROUP BY a` | Emits CTEs with Spark-safe quoted aliases and standard `GROUP BY`. | Common aggregate MV path works without post-processing. |
| `strftime(ts, fmt)` | Emits `date_format(ts, fmt)`. | Spark name is selected by the dialect branch. |
| `list_transform(arr, lambda x: ...)` | Emits `transform(arr, lambda x: ...)`. | Spark higher-order function name is selected. |
| `list_extract(arr, i)` | Emits `element_at(arr, i)`. | Indexing semantics line up for 1-indexed list access. |
| Window frame with `ROWS` or `RANGE` | Emits `ROWS ...` / `RANGE ...`. | Spark accepts these frame units. |
| Window frame with `GROUPS` | Throws instead of emitting invalid Spark SQL. | Prevents silent invalid SQL. |
| Window `EXCLUDE` clause | Throws for Spark. | Spark SQL 3.5 does not support window `EXCLUDE`. |
| Correlated/LATERAL shape after DuckDB decorrelation | Emits ordinary joins and delim CTEs, not `LATERAL`. | Avoids Spark-specific lateral syntax differences. |

Spark-specific window checks are at `.temp/lpts/src/lpts_pipeline.cpp:792-812`.
Delim join serialization is at `.temp/lpts/src/lpts_pipeline.cpp:2930-2978`.

### Spark rules `openivm-spark` must still fix

The Spark dialect does not own all strings in OpenIVM's generated refresh SQL.
Some SQL is hand-assembled by OpenIVM refresh compilers or comes from DuckDB's
binder in a form that LPTS does not normalize.
`LptsSparkDialect.translate` applies the final Spark pass.
Its pipeline is in `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/LptsSparkDialect.scala:94-131`.

| Raw OpenIVM/LPTS Spark output | After `LptsSparkDialect` | Reason |
|---|---|---|
| `struct_extract(s, 'k')` or `STRUCT_EXTRACT(s, 'k')` | `s.k` | Spark SQL uses dot field access; no `struct_extract` builtin. |
| `generate_series(1, n)` | `sequence(1, n)` | Spark's generator/array function name differs. |
| `now()::timestamp` | `current_timestamp()` | Spark does not parse DuckDB postfix cast syntax. |
| `'2024-01-01'::TIMESTAMP` | `CAST('2024-01-01' AS TIMESTAMP)` | Generic postfix casts are rewritten to standard `CAST`. |
| `COALESCE(a, b)::DOUBLE` | `CAST(COALESCE(a, b) AS DOUBLE)` | Parenthesized postfix casts need balanced scanning. |
| `INTERVAL '1 hour'` | `INTERVAL 1 HOUR` | Spark interval literal grammar differs. |
| `count_star()` | `COUNT(*)` | DuckDB count-star spelling can leak through compiled SQL. |
| `CAST(x AS TIMESTAMP WITH TIME ZONE)` | `CAST(x AS TIMESTAMP)` | Spark 3.5 does not accept the SQL-standard suffix. |
| `CAST(NULL AS HUGEINT)` | `CAST(NULL AS BIGINT)` | Spark lacks DuckDB `HUGEINT`. |
| `error('msg')` | `raise_error('msg')` | Spark's error function is named `raise_error`. |
| `regexp_matches(s, p)` | `regexp_like(s, p)` | Macro shim expansion must be translated back. |
| `strptime(raw, fmt)` after shim expansion | `to_timestamp(raw, fmt)` | Restores Spark function spelling. |
| `strftime(ts, fmt)` after shim expansion | `date_format(ts, fmt)` | Restores Spark function spelling. |

Relevant sources:

- struct field rewrite: `LptsSparkDialect.scala:287-344`
- temporal/type/cast rewrites: `LptsSparkDialect.scala:176-285`, `356-483`
- count/error rewrites: `LptsSparkDialect.scala:485-520`
- shim back-translation: `LptsSparkDialect.scala:144-174`
- tests: `spark-ext/ivm-compiler/src/test/scala/org/openivm/spark/compiler/LptsSparkDialectSpec.scala:278-326`, `546-685`

### What Spark raw LPTS looks like

A simple aggregate body such as:

```sql
SELECT a, COUNT(*) FROM t GROUP BY a
```

is serialized as a CTE pipeline:

```sql
WITH scan_0(`a`) AS (SELECT `a` FROM `memory`.`main`.`t`),
aggregate_1(...) AS (SELECT t0_a, count(*) FROM scan_0 GROUP BY t0_a)
SELECT ...;
```

The exact CTE column names depend on DuckDB's optimized logical plan, but the
Spark dialect properties are stable:
identifiers are backticked, table names are three-part when a catalog is known,
and function names are passed through or mapped by the Spark function table.

A struct field body such as:

```sql
SELECT struct_extract(s, 'k') FROM t
```

can still produce raw SQL containing `struct_extract`.
The Spark post-processor changes it to `s.k`.
This is intentionally outside core LPTS because the rewrite is a Spark parser
compatibility patch for OpenIVM output, not a full logical expression rewrite.

## 5. Dialect extensions and customization

A caller can select one of the built-in dialects.
A caller cannot provide a runtime custom dialect object.
There is no `DialectTraits` registry and no callback-based serializer API.
The only public customization knobs are string settings:

```sql
SET lpts_dialect = 'duckdb';
SET lpts_dialect = 'postgres';
SET lpts_dialect = 'spark';
```

for direct LPTS.

For OpenIVM refresh SQL generation, the dialect is a per-call CompileFacts key:

```json
{"target_dialect":"spark","compile_only":true,"force_view_delta_cascade":true}
```

The direct LPTS setting is registered in `.temp/lpts/src/lpts_extension.cpp:286-292`.
The OpenIVM CompileFacts surface is defined in `.temp/openivm/src/include/compile_facts.hpp`.
Both paths parse through LPTS's dialect parser.

Therefore a new dialect requires code changes:
add a new enum value, parse it, and teach every dialect-sensitive serializer
branch how to behave.
The step-by-step guide appears near the end of this document.

## 6. Dialect compatibility matrix

This matrix is about the generated SQL being acceptable to each target.
It assumes the MV body itself can be parsed and bound by DuckDB first, because
LPTS works from DuckDB's logical plan.
`openivm-spark` has the extra constraint that the original MV body must also be
valid Spark SQL at user-facing CREATE time.

| MV body | DuckDB output | Spark output raw | Spark output after `LptsSparkDialect` | Postgres output |
|---|---|---|---|---|
| `SELECT a, COUNT(*) FROM t GROUP BY a` | OK. Standard CTEs, `count(*)`, DuckDB identifiers. | OK. Backticked identifiers and standard aggregate. | OK. No extra rewrite needed. | Mostly OK; simple aggregate and quoted identifiers are compatible. |
| `SELECT struct_extract(s,'k') FROM t` | OK if DuckDB binds the struct field. | Not OK for Spark if `struct_extract` remains; Spark parser has no builtin of that name. | OK: `struct_extract(s,'k')` becomes `s.k`. | Error/unsupported: no Postgres mapping for DuckDB struct extraction. |
| `WITH t AS (SELECT a FROM base) SELECT a FROM t` | OK; optimizer may inline or materialize, output is LPTS CTEs. | OK; output CTEs do not preserve materialization hints. | OK. | OK for simple CTE shape. |
| `WITH t AS MATERIALIZED (SELECT a FROM base) SELECT a FROM t` | OK; hint affects DuckDB plan, but output is plain `AS`. | Raw OK if the resulting CTE plan is supported. | OK; no hint remains. | OK syntax-wise for plain output, but hint is not preserved. |
| `SELECT strftime(ts, '%Y-%m') FROM t` | OK as `strftime`. | OK raw as `date_format(ts, '%Y-%m')`. | OK; may also rewrite shim-expanded `strftime` to `date_format`. | OK-ish as `to_char(ts, '%Y-%m')`, though format-token semantics may differ. |
| `SELECT strptime(s, fmt) FROM t` | OK as `strptime`. | Raw emits `to_timestamp(s, fmt)` for LPTS-owned expressions. | OK; shim expansions are also restored to `to_timestamp`. | OK-ish as `to_timestamp(s, fmt)`, format-token semantics may differ. |
| `SELECT list_transform(xs, lambda x: x + 1) FROM t` | OK in DuckDB. | Raw emits `transform(xs, lambda x: x + 1)`. Spark higher-order lambda syntax may still require parser-compatible shape. | Usually OK if Spark accepts the lambda spelling; otherwise workload-specific. | Error/unsupported: no Postgres mapping for DuckDB list lambdas. |
| `SELECT list_extract(xs, 1) FROM t` | OK. | Raw emits `element_at(xs, 1)`. | OK. | Error/unsupported: no Postgres array/list mapping. |
| `SELECT row_number() OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t` | OK. | OK raw. | OK. | OK. |
| `SELECT sum(x) OVER (ORDER BY ts GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t` | OK if DuckDB supports the frame. | LPTS Spark throws before emitting. | Error by design; user must rewrite to ROWS/RANGE. | Raw may be OK on modern Postgres, but LPTS has no Postgres-specific check. |
| `SELECT sum(x) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM t` | OK if DuckDB supports it. | LPTS Spark throws because Spark has no window `EXCLUDE`. | Error by design. | Postgres may support `EXCLUDE`, but LPTS emits it without deeper compatibility checks. |
| `SELECT a FROM t1 UNION ALL SELECT a FROM t2` | OK. | OK raw. | OK. | OK. |
| `SELECT * FROM t1 UNION ALL BY NAME SELECT * FROM t2` | DuckDB input may parse, but LPTS output is positional `UNION ALL`; name matching is not preserved as syntax. | Raw output is positional, so valid only if column order is compatible. | Same. | Not a Postgres feature. |
| `SELECT * FROM t PIVOT (...)` | Not implemented in LPTS unless DuckDB optimizes it into supported operators. | Same. | Same. | Same. |
| `SELECT w_id, slot FROM wh CROSS JOIN LATERAL (...)` | OK in tested dependent-join cases; LPTS emits decorrelated joins/delim CTEs. | Raw is ordinary joins, not `LATERAL`; often OK. | OK if downstream Spark accepts the generated joins/functions. | Often OK only after semantics are expressible without DuckDB-specific table functions. |
| `SELECT generate_series(1, n) FROM t` | OK in OpenIVM-assembled SQL. | Raw OpenIVM can contain `generate_series`. | OK after rewrite to `sequence(1, n)`. | Error/unsupported unless Postgres `generate_series` context matches. |
| `SELECT now()::timestamp FROM t` | OK DuckDB syntax. | Raw OpenIVM can contain postfix cast. | OK after rewrite to `current_timestamp()`. | Postgres supports postfix casts but `now()` type semantics differ. |
| `SELECT INTERVAL '1 hour' FROM t` | OK DuckDB syntax. | Raw OpenIVM can contain quoted interval literal. | OK after rewrite to `INTERVAL 1 HOUR`. | OK in Postgres with quoted interval syntax. |

The main lesson from the matrix:
LPTS Spark is enough for many logical-plan-owned expressions, but not for every
OpenIVM refresh string.
`openivm-spark` therefore treats LPTS Spark as a strong first pass, not as the
last parser-compatibility pass.

## 7. Dialect targeting in OpenIVM

OpenIVM now receives the target dialect through the per-call CompileFacts JSON
passed to `openivm_compile_with_facts`.
Valid values are `duckdb`, `postgres`, and `spark`.
The default schema version is 1, and unsupported schema versions are rejected.
The CompileFacts surface is defined in `.temp/openivm/src/include/compile_facts.hpp`.

The Scala bridge passes Spark facts when compiling the MV:

```sql
SELECT * FROM openivm_compile_with_facts(
  '<view>',
  '{"target_dialect":"spark","compile_only":true,"force_view_delta_cascade":true}'
);
```

Source: `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/OpenIvmCompiler.scala:149-195`.

From there the flow is:

1. Spark extension receives a materialized-view compile request.
2. `OpenIvmCompiler.buildScript` embeds the Spark CompileFacts JSON in the table-function call.
3. The DuckDB CLI loads OpenIVM and creates the source stubs and MV.
4. `openivm_compile_with_facts` resolves the view, parses the CompileFacts payload, and builds refresh SQL.
5. Each LPTS call uses the per-call target dialect.
6. OpenIVM passes `SqlDialect::SPARK` into `LogicalPlanToAst`.
7. OpenIVM passes the same enum into `AstToCteList`.
8. `CteList::ToQuery(...)` serializes CTE SQL using Spark quoting and mappings.
9. The Scala side parses the JSON-line compile result.
10. `LptsSparkDialect.translate` cleans up remaining Spark-incompatible tokens.

Important call sites:

| OpenIVM phase | Dialect hand-off |
|---|---|
| CompileFacts payload | `.temp/openivm/src/include/compile_facts.hpp` |
| Refresh delta plan serialization | `.temp/openivm/src/upsert/refresh_sql.cpp:867-873` |
| Insert-rule delta serialization | `.temp/openivm/src/rules/refresh_insert_rule.cpp:125-146` |
| Compile table-function path | `.temp/openivm/src/upsert/refresh.cpp` |

Older docs described this as a PRAGMA-style target setting.
In the code path used here, the CompileFacts JSON is passed as a single argument
to `openivm_compile_with_facts`; no global session state is touched, so
concurrent compiles cannot interfere via leaked flags.

## 8. Adding a new dialect

Because there is no runtime `DialectTraits` object, adding a dialect is a code
change across LPTS, OpenIVM, and any downstream integration that wants to use it.
A safe checklist follows.

### Step 1: Add the enum value

Edit `.temp/lpts/src/include/sql_dialect.hpp`.
Add a new value to `enum class SqlDialect`.
Document the intended quoting and feature restrictions in the enum comment.

Example:

```cpp
enum class SqlDialect {
    DUCKDB,
    POSTGRES,
    SPARK,
    BIGQUERY
};
```

### Step 2: Parse the setting

Edit `ParseSqlDialect` in `.temp/lpts/src/lpts_pipeline.cpp:2508-2519`.
Accept lower-case and upper-case spellings.
Update the error message so invalid settings list the new value.

### Step 3: Update extension-option docs

Edit the `lpts_dialect` registration string in
`.temp/lpts/src/lpts_extension.cpp:286-292`.
Update README / implementation docs in the LPTS repo if they list valid values.

### Step 4: Implement identifier quoting

Edit `DialectQuoteIdent` in `.temp/lpts/src/lpts_helpers.cpp:9-27`.
Decide whether the dialect uses:

- no quotes for simple identifiers,
- double quotes,
- backticks,
- brackets,
- or another engine-specific rule.

Also verify `DialectQualifiedTableName` in `.temp/lpts/src/lpts_helpers.cpp:88-92`.
Some engines do not accept three-part names or require project/dataset/table
ordering that differs from DuckDB catalog/schema/table.

### Step 5: Implement function-name mappings

Edit the function-name remapping block in
`.temp/lpts/src/lpts_pipeline.cpp:908-941`.
Add a branch for the new dialect.
Start with only functions that are semantically safe.
Do not silently map functions whose null handling, indexing, timezone behavior,
or format tokens differ unless tests prove equivalence.

Recommended test categories:

- date parse and format functions,
- array/list functions,
- struct field access,
- string regex functions,
- aggregate edge cases,
- arithmetic and null behavior.

### Step 6: Implement type-name mappings

LPTS currently emits `cast_expr.return_type.ToString()` for casts.
That is DuckDB type spelling.
If the new engine needs aliases, add a dialect-aware type rendering helper and
use it in cast emission.
For Spark, this is currently handled by `openivm-spark` post-processing for
`VARCHAR`, `CHAR`, `TEXT`, `HUGEINT`, `UHUGEINT`, and timestamp-with-time-zone
forms.
A new core LPTS dialect should ideally centralize this in LPTS rather than
requiring a downstream regex pass.

### Step 7: Audit set operations

Check `.temp/lpts/src/cte_nodes.cpp:245-284`.
Decide whether the new engine supports:

- `UNION`,
- `UNION ALL`,
- `EXCEPT`,
- `EXCEPT ALL`,
- `INTERSECT`,
- `INTERSECT ALL`,
- name-based set operations.

If an operation is unsupported, either throw for that dialect or rewrite to an
equivalent CTE pattern.
Do not emit invalid SQL silently.

### Step 8: Audit joins and lateral/dependent subqueries

Check regular join serialization in `.temp/lpts/src/cte_nodes.cpp:187-242`.
Check delim/dependent join handling in `.temp/lpts/src/lpts_pipeline.cpp:2258-2355`
and `.temp/lpts/src/lpts_pipeline.cpp:2930-2978`.
Some engines spell semi/anti joins differently, and some do not support them.
Spark supports `SEMI JOIN` / `ANTI JOIN` in the shapes used here, but the Scala
pre-normalizer also rewrites user `LEFT SEMI JOIN` / `LEFT ANTI JOIN` before
DuckDB sees the query.
See `OpenIvmCompiler.scala:236-262`.

### Step 9: Audit window syntax

Check `.temp/lpts/src/lpts_pipeline.cpp:608-812`.
Add dialect restrictions for frame units, `EXCLUDE`, null handling, and window
function names.
Spark already rejects `GROUPS` and `EXCLUDE` here.
A new dialect should fail early rather than emit syntax that cannot parse.

### Step 10: Audit CTE materialization and recursion

CTEs are emitted by `.temp/lpts/src/cte_nodes.cpp:82-93` and the final `WITH`
clause by `.temp/lpts/src/cte_nodes.cpp:372-389`.
If the new dialect requires `WITH RECURSIVE`, disallows it, or supports
`MATERIALIZED` / `NOT MATERIALIZED` hints that must be preserved, this is the
place to change.
Today LPTS does not preserve materialization hints in output.

### Step 11: Add LPTS tests

Add or extend sqllogictests under `.temp/lpts/test/sql/`.
At minimum:

- a setting test for `SET lpts_dialect = '<new>'`,
- an identifier quoting test,
- a scalar function mapping test,
- a type/cast test,
- a set-operation test,
- a window-frame accept/reject test,
- a CTE test,
- a correlated/LATERAL-style test if the dialect supports the output.

Existing examples:

- direct dialect setting coverage: `.temp/lpts/test/sql/operators.test:253-266`
- function tests: `.temp/lpts/test/sql/functions.test`
- window tests: `.temp/lpts/test/sql/window.test`
- lateral/dependent join tests: `.temp/lpts/test/sql/lateral_join.test`
- CTE materialization tests: `.temp/lpts/test/sql/cte.test:122-155`
- set operation tests: `.temp/lpts/test/sql/setops_unnest.test:19-47`

### Step 12: Wire OpenIVM if refresh SQL should target it

OpenIVM uses the LPTS enum directly.
After LPTS can parse the dialect string, thread the new value through the
CompileFacts validation and parser.
Then test:

```sql
SELECT * FROM openivm_compile_with_facts(
  'mv_name',
  '{"target_dialect":"<new>","compile_only":true,"force_view_delta_cascade":true}'
);
```

Look at CREATE-time LPTS, refresh-time LPTS, and insert-rule LPTS call sites.
They should all receive the new enum through the same helper.

### Step 13: Add downstream post-processing only if unavoidable

For Spark, `openivm-spark` has `LptsSparkDialect` because OpenIVM also emits
hand-written DuckDB-style fragments outside LPTS.
A new downstream integration should first try to put dialect behavior inside
LPTS.
Only add a post-processor for tokens that truly originate outside LPTS or for
engine-specific compatibility patches that cannot be represented from the
logical plan.

If a post-processor is necessary, make it:

- pure,
- idempotent,
- string-literal aware,
- comment aware where practical,
- covered by unit tests,
- documented with examples of raw and rewritten SQL.

## 9. Practical guidance for OpenIVM-Spark contributors

When debugging invalid Spark SQL from a compiled refresh program, classify the
problem before changing code.

1. If the invalid token is inside an LPTS-owned expression, fix the LPTS Spark
   branch or add an LPTS test.
2. If the invalid token is from OpenIVM hand-written refresh SQL, fix
   `LptsSparkDialect` or the OpenIVM string generator.
3. If the user's MV body is Spark-only and DuckDB cannot bind it, add a compile
   bridge shim before the SQL reaches DuckDB.
4. If DuckDB binds a Spark function to a different overload, use the shim
   rename/prologue/back-translation pattern in `OpenIvmCompiler` and
   `SparkFunctionShimSql`.
5. If Spark and DuckDB semantics differ, do not add a name-only mapping unless
   parity tests prove the behavior.

The current Spark shim system does exactly this for selected functions.
`OpenIvmCompiler.normalizeSparkSqlForDuckdb` rewrites user-facing Spark SQL
before DuckDB binding, including function shim renames and `LEFT SEMI/ANTI JOIN`
normalization.
See `OpenIvmCompiler.scala:236-262`.
The shim macro prologue is documented at `OpenIvmCompiler.scala:491-525`.
The post-processor then reverses macro expansions back to Spark names.
See `LptsSparkDialect.scala:144-174` and `SparkFunctionShimSql.scala:91-113`.

## Summary

LPTS dialect support is enum-based and intentionally compact.
The built-in dialects are DuckDB, Spark, and Postgres.
Identifier quoting, qualified table names, selected function names, set ops,
window restrictions, and CTE serialization are the main dialect-sensitive areas.
Spark support is the most developed non-DuckDB path, but OpenIVM-Spark still
needs a post-processing layer for DuckDB syntax that comes from OpenIVM's
hand-written refresh SQL or from binder artifacts outside LPTS's dialect branch.
Adding a dialect means extending the enum, parser, helper functions, expression
serializer, CTE serializer, OpenIVM setting text, and tests.
