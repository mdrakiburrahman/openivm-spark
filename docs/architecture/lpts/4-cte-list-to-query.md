# 4. `CteList::ToQuery`: final emit from CTE list to SQL

## Scope
This chapter covers the last LPTS phase:

```text
Logical plan -> AST -> CTE list -> SQL string
```

The relevant sources are:

```text
.temp/lpts/src/cte_nodes.cpp
.temp/lpts/src/include/cte_nodes.hpp
.temp/lpts/src/lpts_pipeline.cpp
.temp/lpts/src/lpts_helpers.cpp
.temp/lpts/src/include/sql_dialect.hpp
```

The final phase is deliberately small.
It does not re-plan the query.
It does not optimize the CTE graph.
It does not rediscover dependencies.
It serializes the already-flattened `CteList`.
Most hard decisions are made earlier:
1. `LogicalPlanToAst` extracts DuckDB plan semantics.
2. `AstToCteList` flattens the tree into an ordered list.
3. `CteList::ToQuery` emits text.

## Role
`CteList::ToQuery` emits the final SQL string.
The normal output shape is:

```sql
WITH cte_1 AS (...),
cte_2 AS (...),
cte_3 AS (...)
SELECT ... FROM cte_3;
```

The recursive output shape is:

```sql
WITH RECURSIVE recursive_cte AS (...)
SELECT ... FROM recursive_cte;
```

The `CteList` declaration shows the whole state needed by the emitter:

```cpp
class CteList {
    vector<unique_ptr<CteNode>> nodes;
    unique_ptr<RootNode> final_node;
    bool has_recursive_cte;
    string ToQuery(bool use_newlines, const vector<string> &output_names = {});
};
```

Source: `.temp/lpts/src/include/cte_nodes.hpp:397-414`.
The implementation does this:
1. optionally override final column aliases;
2. print `WITH` or `WITH RECURSIVE` if there are CTE nodes;
3. iterate `nodes` in vector order;
4. print each `nodes[i]->ToCteQuery()`;
5. print `final_node->ToQuery()`;
6. append `;`.
Source: `.temp/lpts/src/cte_nodes.cpp:359-389`.
So the final emitter is a serializer.
It trusts that `AstToCteList` already produced safe CTE order.

## The CTE list data structure
Every node inherits from `CteBaseNode`.
The base class stores:

```cpp
const size_t idx;
SqlDialect dialect = SqlDialect::DUCKDB;
virtual string ToQuery() = 0;
```

Source: `.temp/lpts/src/include/cte_nodes.hpp:45-57`.
`idx` is the numeric seed for generated names such as `scan_0`.
`dialect` is filled in after flattening.
`ToQuery()` prints the body for one node.
Intermediate nodes inherit from `CteNode`.
`CteNode` adds:

```cpp
string cte_name;
vector<string> cte_column_list;
```

Source: `.temp/lpts/src/include/cte_nodes.hpp:118-138`.
The emitted SQL identity is `cte_name`.
There is no CTE hash in this data structure.
There is no structural fingerprint.
There is no reference count.
Concrete classes choose deterministic names from type plus `idx`:
| CTE node | Name pattern | Source |
| --- | --- | --- |
| `GetNode` | `scan_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:154-164` |
| `FilterNode` | `filter_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:175-180` |
| `ProjectNode` | `projection_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:192-199` |
| `AggregateNode` | `aggregate_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:212-219` |
| `JoinNode` | `join_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:232-239` |
| `UnionNode` | `union_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:250-257` |
| `SetOperationNode` | `setop_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:286-292` |
| `OrderNode` | `order_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:301-305` |
| `LimitNode` | `limit_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:318-325` |
| `TopNNode` | `topn_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:337-343` |
| `DistinctNode` | `distinct_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:352-356` |
| `RecursiveCteNode` | `recursive_cte_<idx>` | `.temp/lpts/src/include/cte_nodes.hpp:388-394` |
`CteNode::ToCteQuery()` wraps a body into:

```text
cte_name (col1, col2) AS (<body>)
```

It prints the CTE name.
It prints `cte_column_list` if non-empty.
It delegates the body to the concrete `ToQuery()`.
Source: `.temp/lpts/src/cte_nodes.cpp:82-93`.
The final statement is a `RootNode`, not a `CteNode`.
Common root nodes are `FinalReadNode` and `InsertNode`.
Source: `.temp/lpts/src/include/cte_nodes.hpp:59-104`.

## How CTEs are collected during AST building
Collection happens in `AstFlattener`, the Phase 2 helper.
It owns:

```cpp
size_t node_count = 0;
vector<unique_ptr<CteNode>> cte_nodes;
SqlDialect dialect;
bool has_recursive_cte = false;
```

Source: `.temp/lpts/src/lpts_pipeline.cpp:2531-2537`.
The phase comment says it walks the AST in post-order and produces a flat
`CteList`.
Source: `.temp/lpts/src/lpts_pipeline.cpp:2521-2529`.
The generic algorithm is:

```text
for each child:
    child_cte = FlattenNode(child)
    remember child_cte->cte_name
    remember child_cte->cte_column_list
    push child_cte into cte_nodes
my_index = node_count++
return this node's CteNode
```

Source: `.temp/lpts/src/lpts_pipeline.cpp:3028-3044`.
That is the central invariant.
Children are appended before parents.
Parents store child names, not child vector indexes.
For a regular `SELECT`, `Flatten()` builds a `FinalReadNode` from the last CTE,
pushes that last CTE, stamps dialect, and returns `CteList`.
Source: `.temp/lpts/src/lpts_pipeline.cpp:3177-3201`.
For an `INSERT`, `Flatten()` builds an `InsertNode`, pushes the child CTE, stamps
dialect, and returns `CteList`.
Source: `.temp/lpts/src/lpts_pipeline.cpp:3164-3174`.

## What identifies a CTE?
There are three identities at different layers.

### Emitted identity: `cte_name`
The SQL name is `CteNode::cte_name`.
Parents refer to this string in fields such as `child_cte_name`,
`left_cte_name`, and `right_cte_name`.
Sources:
- `.temp/lpts/src/include/cte_nodes.hpp:134-137`
- `.temp/lpts/src/include/cte_nodes.hpp:167-180`
- `.temp/lpts/src/include/cte_nodes.hpp:183-199`
- `.temp/lpts/src/include/cte_nodes.hpp:222-239`

### Name seed: `idx`
`idx` is the unique numeric seed stored by `CteBaseNode`.
It forms names like `scan_0` and `filter_1`.
Source: `.temp/lpts/src/include/cte_nodes.hpp:52-56`.

### DuckDB CTE identity: `cte_table_index`
Materialized CTEs and recursive CTE refs are matched by DuckDB table/index IDs
while the AST is being flattened.
The AST nodes carry `cte_table_index`.
Source: `.temp/lpts/src/include/lpts_ast.hpp:329-367`.
The flattener map is:

```cpp
unordered_map<idx_t, pair<string, vector<string>>> cte_index_to_body_info;
```

Source: `.temp/lpts/src/lpts_pipeline.cpp:2538-2543`.
The map value is the generated LPTS CTE name and column list.
A `CteRef` uses the DuckDB index to find that generated name.
Source: `.temp/lpts/src/lpts_pipeline.cpp:2897-2928`.
There is no hash-based identity in `CteList`.
If common subplans are materialized, DuckDB's optimizer exposes them as
`LogicalMaterializedCTE` plus `LogicalCTERef` before the final emitter runs.
Source: `.temp/lpts/src/lpts_extension.cpp:39-45`.

## CTE dependency ordering
`CteList::ToQuery` does not sort.
It iterates `nodes` in stored vector order.
Source: `.temp/lpts/src/cte_nodes.cpp:373-384`.
The ordering guarantee is created earlier.
The header documents the invariant:
> The bottom-up traversal of the logical plan guarantees that each CTE only
> references CTEs defined before it.
Source: `.temp/lpts/src/include/cte_nodes.hpp:14-22`.
The algorithm is therefore post-order traversal, not a topological sort over an
explicit graph.
It also is not reference counting.
The vector is already in dependency order when it reaches `ToQuery`.

### Materialized CTE ordering
For `MaterializedCte`, the flattener:
1. flattens the body;
2. stores body name and columns by `cte_table_index`;
3. pushes the body CTE;
4. flattens the outer query.
Source: `.temp/lpts/src/lpts_pipeline.cpp:2897-2928`.

### Delim join ordering
For `DelimJoin`, Phase 2 order is:

```text
left CTEs -> DELIM_GET CTEs -> right CTEs -> JOIN
```

Source: `.temp/lpts/src/include/lpts_ast.hpp:417-420`.
The flattener implements that by flattening the outer child first, registering
it as the `DelimGet` source, then flattening the inner child, then building the
join CTE.
Source: `.temp/lpts/src/lpts_pipeline.cpp:2930-2961`.

### Recursive ordering
Recursive CTEs cannot be plain post-order CTE chains.
The recursive step self-references the recursive CTE.
LPTS serializes that step as inline SQL instead of forward-referencing flat CTEs.
Source: `.temp/lpts/src/lpts_pipeline.cpp:2570-2576`.

## CTE inlining vs naming
A single-use CTE could theoretically be inlined into its parent.
The current final emitter does not do that.
`CteNode::ToCteQuery()` always emits a named CTE definition.
`CteList::ToQuery()` always calls `nodes[i]->ToCteQuery()`.
Sources:
- `.temp/lpts/src/cte_nodes.cpp:82-93`
- `.temp/lpts/src/cte_nodes.cpp:373-384`
There is no single-use decision in `CteList::ToQuery`.
There is no multi-use decision in `CteList::ToQuery`.
There is no reference-count pass in the final emitter.
The closest decision is earlier.
`PlanQuery` runs DuckDB's optimizer.
Its comment says:
- `CTE_INLINING` can inline CTEs into the query body;
- `MATERIALIZED_CTE` can preserve a materialized CTE;
- `COMMON_SUBPLAN` can produce materialized CTEs and refs.
Source: `.temp/lpts/src/lpts_extension.cpp:36-57`.
So the actual decision table is:
| Shape before final emit | Owner | Final emitter behavior |
| --- | --- | --- |
| Optimizer inlined a user CTE | DuckDB optimizer | LPTS emits normal per-operator CTEs. |
| Optimizer kept/materialized a user CTE | DuckDB optimizer and AST builder | LPTS emits named body/ref CTEs. |
| Generated CTE is used once | No final optimization | Still named. |
| Generated CTE is used multiple times | Materialized/ref path | Named references are required. |
The current design favors stable, flat, inspectable SQL over smaller SQL.

## Recursive CTEs
Recursive CTEs require `WITH RECURSIVE`.
`CteList` tracks this with `has_recursive_cte`.
Source: `.temp/lpts/src/include/cte_nodes.hpp:397-408`.
The prefix is selected here:

```cpp
sql_str << (has_recursive_cte ? "WITH RECURSIVE " : "WITH ");
```

Source: `.temp/lpts/src/cte_nodes.cpp:372-375`.
`RecursiveCteNode` stores:
- anchor CTE name;
- anchor columns;
- recursive step SQL;
- `union_all`.
Source: `.temp/lpts/src/include/cte_nodes.hpp:376-394`.
Its body is:

```text
SELECT <anchor cols> FROM <anchor cte>
UNION [ALL]
<recursive_step_sql>
```

Source: `.temp/lpts/src/cte_nodes.cpp:352-354`.
Flattening a recursive CTE is a six-step path:
1. flatten the anchor subtree;
2. derive stripped user-visible names for the recursive CTE header;
3. allocate `recursive_cte_<idx>`;
4. register that name for self-referencing `CteRef` nodes;
5. render the recursive step with `AstToInlineSQL`;
6. push `RecursiveCteNode` and return a scan that maps exposed names back to
   LPTS-prefixed parent names.
Source: `.temp/lpts/src/lpts_pipeline.cpp:2981-3025`.
The inline renderer is necessary because flat CTE nodes cannot safely represent
the recursive cycle.
Source: `.temp/lpts/src/lpts_pipeline.cpp:2570-2576`.

## Dialect-specific emission
The dialect enum is:

```cpp
enum class SqlDialect { DUCKDB, POSTGRES, SPARK };
```

Source: `.temp/lpts/src/include/sql_dialect.hpp:13-17`.
The session setting is `lpts_dialect`, defaulting to `duckdb`.
It accepts `duckdb`, `postgres`, and `spark`.
Sources:
- `.temp/lpts/src/lpts_extension.cpp:24-34`
- `.temp/lpts/src/lpts_extension.cpp:286-292`
- `.temp/lpts/src/lpts_pipeline.cpp:2508-2518`
The dialect is passed into `LogicalPlanToAst` and `AstToCteList`.
After flattening, `StampDialect` writes it onto all CTE nodes and the root.
Sources:
- `.temp/lpts/src/lpts_extension.cpp:117-120`
- `.temp/lpts/src/lpts_pipeline.cpp:3204-3214`

### Rule table
| Concern | LPTS rule | Spark-specific result | Source |
| --- | --- | --- | --- |
| SELECT lists | Join vector entries with `", "`. | Same. | `.temp/lpts/src/lpts_helpers.cpp:29-37` |
| Final aliases | `child_col AS <quoted final>`. | Final name backticked. | `.temp/lpts/src/cte_nodes.cpp:39-56` |
| CTE headers | `name (cols) AS (...)`. | Generated columns are not additionally quoted. | `.temp/lpts/src/cte_nodes.cpp:82-93` |
| Table scan columns | Quote physical column identifiers. | Backtick each column. | `.temp/lpts/src/cte_nodes.cpp:96-105` |
| Qualified tables | Quote each segment. | `` `catalog`.`schema`.`table` ``. | `.temp/lpts/src/lpts_helpers.cpp:88-91` |
| Postgres tables | Drop catalog/schema in Phase 2. | Spark keeps them. | `.temp/lpts/src/lpts_pipeline.cpp:3048-3056` |
| Identifier quote | DuckDB/Postgres optional double quote. | Spark always backticks; embedded backticks double. | `.temp/lpts/src/lpts_helpers.cpp:9-27` |
| Function calls | `name(arg1, arg2)`. | Some names remapped. | `.temp/lpts/src/lpts_pipeline.cpp:896-1028` |
| Infix operators | `(left OP right)`. | Same. | `.temp/lpts/src/lpts_pipeline.cpp:955-966` |
| `strftime` | Remap in Spark dialect. | `date_format`. | `.temp/lpts/src/lpts_pipeline.cpp:923-924` |
| `strptime` | Remap in Spark dialect. | `to_timestamp`. | `.temp/lpts/src/lpts_pipeline.cpp:925-926` |
| `list_transform` / `array_transform` | Remap. | `transform`. | `.temp/lpts/src/lpts_pipeline.cpp:927-928` |
| `list_aggregate` / `array_aggregate` | Remap. | `aggregate`. | `.temp/lpts/src/lpts_pipeline.cpp:929-930` |
| `list_filter` / `array_filter` | Remap. | `filter`. | `.temp/lpts/src/lpts_pipeline.cpp:931-932` |
| `list_value` | Remap. | `array`. | `.temp/lpts/src/lpts_pipeline.cpp:933-934` |
| `list_contains` / `array_contains` | Remap. | `array_contains`. | `.temp/lpts/src/lpts_pipeline.cpp:935-936` |
| `list_extract` / `array_extract` | Remap. | `element_at`. | `.temp/lpts/src/lpts_pipeline.cpp:937-940` |
| Aggregate names | Mostly preserve DuckDB names. | `sum_no_overflow` -> `sum`; `count_star` left for post-pass. | `.temp/lpts/src/lpts_pipeline.cpp:1692-1700` |
| Cast expressions | `CAST(x AS T)` / `TRY_CAST`. | Syntax is Spark-compatible; type names may not be. | `.temp/lpts/src/lpts_pipeline.cpp:874-879` |
| Synthetic empty casts | `NULL::<type>`. | DuckDB/Postgres leak; Spark post-pass must fix. | `.temp/lpts/src/lpts_pipeline.cpp:2066-2083` |
| Type names | `LogicalType::ToString()`. | Not fully Spark-normalized in C++. | `.temp/lpts/src/lpts_pipeline.cpp:874-879` |
| Values | DuckDB `Value::ToSQLString()`. | Usually portable; still DuckDB-shaped. | `.temp/lpts/src/lpts_pipeline.cpp:2110-2124` |
| String escaping | Double single quotes. | Same SQL literal escaping. | `.temp/lpts/src/lpts_helpers.cpp:94-103` |
| Window `GROUPS` | Available generally. | Spark dialect throws. | `.temp/lpts/src/lpts_pipeline.cpp:793-797` |
| Window `EXCLUDE` | Available generally. | Spark dialect throws. | `.temp/lpts/src/lpts_pipeline.cpp:808-812` |

### SELECT lists
Node emitters use `VecToSeparatedList` for comma-separated lists.
Examples include final reads, projections, aggregates, joins, set operations,
orders, limits, and distinct nodes.
Sources:
- `.temp/lpts/src/lpts_helpers.cpp:29-37`
- `.temp/lpts/src/cte_nodes.cpp:39-56`
- `.temp/lpts/src/cte_nodes.cpp:157-184`
- `.temp/lpts/src/cte_nodes.cpp:187-242`

### Function calls
Function calls are emitted by `ExpressionToAliasedString`.
Spark-specific renames are an `if` chain, not an external lookup file.
Source: `.temp/lpts/src/lpts_pipeline.cpp:916-940`.
Verified mapping:

```text
strftime        -> date_format
strptime        -> to_timestamp
list_transform  -> transform
array_transform -> transform
list_aggregate  -> aggregate
array_aggregate -> aggregate
list_filter     -> filter
array_filter    -> filter
list_value      -> array
list_contains   -> array_contains
array_contains  -> array_contains
list_extract    -> element_at
array_extract   -> element_at
```

There is no verified LPTS rule here for aggregate `LIST(...)` -> Spark `ARRAY`.
The `list_value` scalar constructor maps to `array`.
Aggregate names are mostly preserved.
Unsupported functions pass through rather than being silently mistranslated.
Source: `.temp/lpts/src/lpts_pipeline.cpp:917-922`.

### Identifiers
`DialectQuoteIdent` is central.
Spark always uses backticks.
DuckDB/Postgres use DuckDB's optional quoting helper.
Sources:
- `.temp/lpts/src/lpts_helpers.cpp:9-27`
- `.temp/lpts/src/include/sql_dialect.hpp:23-26`

### Type names and casts
Normal bound casts emit:

```sql
CAST(<expr> AS <return_type>)
```

Source: `.temp/lpts/src/lpts_pipeline.cpp:874-879`.
The type text is DuckDB's `LogicalType::ToString()`.
That means names such as `INTEGER`, `BIGINT`, `VARCHAR`, and `HUGEINT` can leak
through depending on the plan.
Spark accepts some, rejects some, and sometimes rejects them only in bare cast
contexts.
Synthetic empty-result scans can emit postfix casts:

```sql
NULL::<type>
```

Source: `.temp/lpts/src/lpts_pipeline.cpp:2066-2083`.
openivm-spark's post-processor rewrites remaining postfix casts and normalizes
bare `VARCHAR`/`CHAR`/`TEXT` and `HUGEINT`/`UHUGEINT`.
Source: `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/LptsSparkDialect.scala:176-181`.

### String and char escaping
LPTS mostly delegates literals to DuckDB printers:
- constants use `Expression::ToString()`;
- `VALUES` rows use `Value::ToSQLString()`;
- quantile arguments use `Value::ToSQLString()`.
Sources:
- `.temp/lpts/src/lpts_pipeline.cpp:849-851`
- `.temp/lpts/src/lpts_pipeline.cpp:2110-2124`
- `.temp/lpts/src/lpts_pipeline.cpp:413-453`
The explicit `EscapeSingleQuotes` helper doubles `'`.
It is used for string-agg separators and for wrapping PRAGMA results.
Sources:
- `.temp/lpts/src/lpts_helpers.cpp:94-103`
- `.temp/lpts/src/lpts_pipeline.cpp:1717-1724`
- `.temp/lpts/src/lpts_extension.cpp:121-124`

## Pretty-printing
SQL pretty-printing is minimal.
With `use_newlines = true`, the emitter adds one newline after each CTE.
It does not indent CTE bodies.
It does not wrap long lines.
It does not enforce max-line-width.
Source: `.temp/lpts/src/cte_nodes.cpp:372-384`.
With `use_newlines = false`, the final CTE is followed by a space before the
root statement.
In both modes, each node body is whatever that node's `ToQuery()` returned.
Source: `.temp/lpts/src/cte_nodes.cpp:359-389`.
AST pretty-printing is separate debug functionality.
`AstNode::ToString(int indent)` and `PrintAst` do not affect emitted SQL.
Source: `.temp/lpts/src/include/lpts_ast.hpp:41-63`.

## Worked example: three CTEs
Input query:

```sql
SELECT region, SUM(amount) AS total
FROM sales
WHERE amount > 0
GROUP BY region;
```

Simplified input AST:

```text
Aggregate
  group_by_columns: [t0_region]
  aggregate_expressions: [sum(t0_amount)]
  cte_column_names: [t2_region, t3_total]
  child:
    Filter
      conditions: [(t0_amount) > (CAST(0 AS INTEGER))]
      child:
        Get
          table: memory.main.sales
          column_names: [region, amount]
          cte_column_names: [t0_region, t0_amount]
```

Dependency graph:

```text
scan_0 -> filter_1 -> aggregate_2 -> final SELECT
```

Topological order equals the vector order produced by post-order flattening:

```text
1. scan_0
2. filter_1
3. aggregate_2
```

Representative CTE bodies:

```sql
scan_0 AS (
  SELECT `region`, `amount` FROM `memory`.`main`.`sales`
)
```

```sql
filter_1 AS (
  SELECT t0_region, t0_amount FROM scan_0
  WHERE ((t0_amount) > (CAST(0 AS INTEGER)))
)
```

```sql
aggregate_2 AS (
  SELECT t0_region, sum(t0_amount) FROM filter_1 GROUP BY t0_region
)
```

Final read:

```sql
SELECT t2_region AS `region`, t3_total AS `total` FROM aggregate_2;
```

Emitted SQL shape:

```sql
WITH scan_0 (t0_region, t0_amount) AS (SELECT `region`, `amount` FROM `memory`.`main`.`sales`),
filter_1 (t0_region, t0_amount) AS (SELECT t0_region, t0_amount FROM scan_0 WHERE ((t0_amount) > (CAST(0 AS INTEGER)))),
aggregate_2 (t2_region, t3_total) AS (SELECT t0_region, sum(t0_amount) FROM filter_1 GROUP BY t0_region)
SELECT t2_region AS `region`, t3_total AS `total` FROM aggregate_2;
```

This example uses the same implementation pieces as the real emitter:
- `GetNode::ToQuery()` for scans;
- `FilterNode::ToQuery()` for predicates;
- `AggregateNode::ToQuery()` for grouping;
- `FinalReadNode::ToQuery()` for output aliases.
Sources:
- `.temp/lpts/src/cte_nodes.cpp:96-134`
- `.temp/lpts/src/cte_nodes.cpp:137-155`
- `.temp/lpts/src/cte_nodes.cpp:170-184`
- `.temp/lpts/src/cte_nodes.cpp:39-56`

## Where openivm-spark intercepts
openivm-spark invokes OpenIVM through a DuckDB CLI script.
That script passes Spark as a per-call CompileFacts key:

```json
{"target_dialect":"spark","compile_only":true,"force_view_delta_cascade":true}
```

Source: `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/OpenIvmCompiler.scala:144-195`.
The LPTS output is therefore close to Spark SQL.
It is not necessarily executable by Spark 3.5 / Delta Lake 3.2.
openivm-spark runs `LptsSparkDialect.translate` as a string post-pass.
Source: `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/LptsSparkDialect.scala:94-131`.
Refresh statements are post-processed after statement-level rewriting.
Source: `spark-ext/ivm-common/src/main/scala/org/openivm/spark/common/SparkRefreshRewriter.scala:167-175`.
Initial-load SQL is also translated after `memory.main.<short>` replacement.
Source: `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/OpenIvmCompiler.scala:313-349`.

### LPTS-side rewrites openivm-spark relies on
| LPTS-side behavior | Why it matters | Source |
| --- | --- | --- |
| Spark dialect backticks identifiers. | Avoids Spark reserved-word and case problems. | `.temp/lpts/src/lpts_helpers.cpp:9-27` |
| Spark keeps catalog/schema-qualified scans. | Lets later `memory.main` rewriting identify sources. | `.temp/lpts/src/lpts_pipeline.cpp:3048-3056` |
| Normal bound casts use `CAST(x AS T)`. | Reduces downstream cast cleanup. | `.temp/lpts/src/lpts_pipeline.cpp:874-879` |
| Spark function names for `strftime`/`strptime`. | Emits `date_format` and `to_timestamp`. | `.temp/lpts/src/lpts_pipeline.cpp:923-926` |
| Spark higher-order list names. | Emits `transform`, `aggregate`, `filter`, `array`, `element_at`. | `.temp/lpts/src/lpts_pipeline.cpp:927-940` |
| Spark rejects unsupported window `GROUPS`. | Prevents known-bad Spark SQL. | `.temp/lpts/src/lpts_pipeline.cpp:793-797` |
| Spark rejects window `EXCLUDE`. | Prevents unsupported Spark syntax. | `.temp/lpts/src/lpts_pipeline.cpp:808-812` |
| CTE dependency order is already safe. | Spark resolves textual CTE order. | `.temp/lpts/src/include/cte_nodes.hpp:14-22` |
| Recursive CTEs use `WITH RECURSIVE`. | Preserves recursive statement shape. | `.temp/lpts/src/cte_nodes.cpp:372-375` |

### What openivm-spark still fixes
The `translate()` pipeline runs these passes:
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
Source: `spark-ext/ivm-compiler/src/main/scala/org/openivm/spark/compiler/LptsSparkDialect.scala:94-131`.
Ownership examples:
| Remaining shape | openivm-spark rewrite | Source |
| --- | --- | --- |
| `now()::timestamp` | `current_timestamp()` | `LptsSparkDialect.scala:14-18` |
| `to_timestamp(CAST('<literal>' AS DOUBLE))` | remove spurious double cast | `LptsSparkDialect.scala:72-90`, `:133-142` |
| inlined Spark function macros | recover Spark spellings | `OpenIvmCompiler.scala:180-189`, `SparkFunctionShimSql.scala:87-109` |
| `TIMESTAMP WITH TIME ZONE` | normalize to `TIMESTAMP` | `LptsSparkDialect.scala:94-131` |
| `struct_extract(s, 'k')` | Spark dot access | `LptsSparkDialect.scala:94-131` |
| `x::BIGINT` | `CAST(x AS BIGINT)` | `LptsSparkDialect.scala:31-51` |
| bare `VARCHAR`/`CHAR`/`TEXT` | `STRING` | `LptsSparkDialect.scala:176-181` |
| `HUGEINT` / `UHUGEINT` | `BIGINT` | `LptsSparkDialect.scala:176-181` |
| `generate_series` | `sequence` | `LptsSparkDialect.scala:26-29` |
| `to_<unit>(...)` | interval multiplication | `LptsSparkDialect.scala:183-214` |
| `INTERVAL '1 day'` | `INTERVAL 1 DAY` | `LptsSparkDialect.scala:53-56` |
| `count_star()` | `COUNT(*)` | `LptsSparkDialect.scala:58-61` |
| `error(...)` | `raise_error(...)` | `LptsSparkDialect.scala:63-70` |
| double-quoted identifiers | backticks | `LptsSparkDialect.scala:19-24` |
| `memory.main.<short>` | qualified Spark table or short temp name | `SparkRefreshRewriter.scala:474-495` |
| `SELECT * EXCEPT (...)` | explicit column list | `SparkRefreshRewriter.scala:167-175` |

## Debugging checklist
If CTEs are out of order, inspect `AstFlattener`.
The final emitter does not sort.
If a CTE name is missing, inspect the child-specific flattening path.
The special paths are materialized CTEs, delim joins, and recursive CTEs.
If final aliases are wrong, inspect how `Flatten()` derived
`final_column_list` from the last CTE columns.
Source: `.temp/lpts/src/lpts_pipeline.cpp:3180-3198`.
If a Spark function name is wrong, check LPTS Spark mapping first.
Then check openivm-spark's `LptsSparkDialect` post-pass.
If a type name or postfix cast is wrong, remember that LPTS C++ is not a full
Spark type normalizer.
Those fixes usually belong to openivm-spark's post-processor.
If recursive SQL is wrong, inspect `AstToInlineSQL`.
Recursive steps intentionally bypass normal flat CTE generation.
Source: `.temp/lpts/src/lpts_pipeline.cpp:2570-2576`.

## Takeaways
`CteList::ToQuery` is a serializer, not a planner.
A CTE is identified in emitted SQL by generated `cte_name`.
The numeric `idx` only seeds that name.
Materialized and recursive references use DuckDB table indices only during
flattening.
There is no CTE hash in the final data structure.
Dependency order comes from post-order flattening.
The final emitter preserves vector order.
There is no topological sort or reference-count algorithm in the final phase.
The final phase does not inline single-use CTEs.
Every `CteNode` in `nodes` is named and emitted.
Recursive CTEs switch the prefix to `WITH RECURSIVE` and render the recursive
step inline to avoid forward references.
The LPTS Spark dialect gets output close to Spark SQL.
openivm-spark's `LptsSparkDialect` is still the final compatibility gate.
