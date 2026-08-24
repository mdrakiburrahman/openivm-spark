# 5. LPTS operator coverage map

This chapter maps DuckDB logical-plan and expression coverage at the pinned
OpenIVM/LPTS versions used by this repository:

- `spark-ext/dev/pins.env`: `OPENIVM_COMMIT=2845bc4...`.
- `spark-ext/dev/pins.env`: `LPTS_COMMIT=b3baf0b...`.
- DuckDB submodule pinned by both forks: `8a585197...`.

LPTS means the upstream `lpts` DuckDB extension, not the Scala post-processor in
`openivm-spark`. The full pipeline is:

1. DuckDB parses and binds the MV body.
1. LPTS converts `LogicalOperator` to an AST (`LogicalPlanToAst`).
1. LPTS flattens the AST to a CTE list (`AstToCteList`).
1. LPTS renders dialect-specific SQL (`duckdb`, `postgres`, or `spark`).
1. `openivm-spark` applies `LptsSparkDialect.translate` to close remaining Spark gaps.

Source anchors used below:

| Concern                             | Source                                                             |
| ----------------------------------- | ------------------------------------------------------------------ |
| DuckDB logical operator enum        | `duckdb/src/include/duckdb/common/enums/logical_operator_type.hpp` |
| DuckDB expression enum              | `duckdb/src/include/duckdb/common/enums/expression_type.hpp`       |
| LPTS AST nodes                      | `lpts/src/include/lpts_ast.hpp`                                    |
| LPTS `AstBuilder::BuildNode` switch | `lpts/src/lpts_pipeline.cpp:1249-2384`                             |
| LPTS expression serializer          | `lpts/src/lpts_pipeline.cpp:839-1118`                              |
| LPTS CTE SQL renderer               | `lpts/src/cte_nodes.cpp:80-285`                                    |
| OpenIVM LPTS fallback               | `openivm/src/core/parser.cpp:294-322`, `579-624`, `667-799`        |

## Reading the support column

| Value   | Meaning                                                                   |
| ------- | ------------------------------------------------------------------------- |
| yes     | Explicit `AstBuilder` and `AstFlattener`/renderer support exists.         |
| limited | A case exists, but only for a subset of DuckDB shapes.                    |
| varies  | Support depends on planner shape, dialect, or caller fallback policy.     |
| no      | No `AstBuilder` case; the default `NotImplementedException` path applies. |
| n/a     | Not part of SELECT-query serialization in the MV-body path.               |

## Complete `LogicalOperatorType` coverage table

The table is based on the DuckDB enum at the pinned DuckDB submodule. The user
facing phrase `LOGICAL_SET_OPERATION` corresponds to the concrete DuckDB enum
members `LOGICAL_UNION`, `LOGICAL_EXCEPT`, and `LOGICAL_INTERSECT`.

| DuckDB `LogicalOperatorType`     | LPTS support | AstBuilder case                          | Emitted SQL shape                                             | Notes                                                                                                      |
| -------------------------------- | -----------: | ---------------------------------------- | ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `LOGICAL_INVALID`                |           no | default throw                            | none                                                          | Sentinel only.                                                                                             |
| `LOGICAL_PROJECTION`             |          yes | `AstProjectNode`                         | `SELECT expr AS col FROM child`                               | Skips DuckDB compression wrappers; flattens chained projections by carrying CTE aliases.                   |
| `LOGICAL_FILTER`                 |          yes | `AstFilterNode`                          | `WHERE (c1) AND (c2)`                                         | Multiple filter expressions are AND'd.                                                                     |
| `LOGICAL_AGGREGATE_AND_GROUP_BY` |          yes | `AstAggregateNode`                       | `SELECT groups, aggs FROM child GROUP BY ...`                 | Handles scalar and grouped aggregates, grouping sets, filters, and selected aggregate order-bys.           |
| `LOGICAL_WINDOW`                 |          yes | `AstProjectNode` with window expressions | `fn(...) OVER (PARTITION BY ... ORDER BY ... frame)`          | Spark dialect rejects `GROUPS` frames and `EXCLUDE`.                                                       |
| `LOGICAL_UNNEST`                 |       varies | `AstProjectNode`                         | `UNNEST(expr)` in select list                                 | Basic expression unnest is serialized; Spark execution may need dialect lowering outside LPTS.             |
| `LOGICAL_LIMIT`                  |          yes | `AstLimitNode`                           | `LIMIT n OFFSET k`                                            | Expression limits are allowed; column-dependent limits become scalar subqueries.                           |
| `LOGICAL_ORDER_BY`               |          yes | `AstOrderNode`                           | `ORDER BY expr ASC/DESC`                                      | Null ordering is preserved in window order items; top-level order tracks direction.                        |
| `LOGICAL_TOP_N`                  |          yes | `AstTopNNode`                            | `ORDER BY ... LIMIT n OFFSET k`                               | Covers DuckDB's fused top-N optimizer node.                                                                |
| `LOGICAL_COPY_TO_FILE`           |           no | default throw                            | none                                                          | Utility statement, not MV-body SELECT.                                                                     |
| `LOGICAL_DISTINCT`               |          yes | `AstDistinctNode`                        | `SELECT DISTINCT ...`                                         | Pass-through bindings from child.                                                                          |
| `LOGICAL_SAMPLE`                 |           no | default throw                            | none                                                          | No AST node; rewrite SQL to avoid sample.                                                                  |
| `LOGICAL_PIVOT`                  |           no | default throw                            | none                                                          | DuckDB pivot node is unsupported by LPTS.                                                                  |
| `LOGICAL_COPY_DATABASE`          |           no | default throw                            | none                                                          | Utility statement.                                                                                         |
| `LOGICAL_GET`                    |          yes | `AstGetNode`                             | `<catalog>.<schema>.<table>` or table function                | Handles normal scans, table functions, DuckLake time travel, DuckLake change scans, table-filter pushdown. |
| `LOGICAL_CHUNK_GET`              |      limited | `AstGetNode` over values                 | `(VALUES (...), (...))`                                       | Used for materialized constant chunks, especially large `IN` lists.                                        |
| `LOGICAL_DELIM_GET`              |      limited | `AstDelimGetNode`                        | `SELECT DISTINCT keys FROM outer_cte`                         | Requires parent `DELIM_JOIN` pre-registration.                                                             |
| `LOGICAL_EXPRESSION_GET`         |          yes | `AstGetNode` over values                 | `(VALUES (...), (...))`                                       | Backs SQL `VALUES` clauses.                                                                                |
| `LOGICAL_DUMMY_SCAN`             |          yes | `AstGetNode` over `(SELECT 1)`           | `SELECT 1`                                                    | One-row input for scalar constants.                                                                        |
| `LOGICAL_EMPTY_RESULT`           |          yes | `AstGetNode` with false filter           | `SELECT NULL::type ... FROM (SELECT 1) WHERE false`           | Preserves schema while returning zero rows.                                                                |
| `LOGICAL_CTE_REF`                |          yes | `AstCteRefNode`                          | `SELECT body_cols AS ref_cols FROM cte`                       | Handles pruned materialized CTE body columns.                                                              |
| `LOGICAL_JOIN`                   |           no | default throw                            | none                                                          | Generic enum member is not the concrete join class handled here.                                           |
| `LOGICAL_DELIM_JOIN`             |      limited | `AstDelimJoinNode`                       | `JOIN` plus generated `DelimGet` CTEs                         | Used for decorrelated subqueries; handles several join-type normalizations.                                |
| `LOGICAL_COMPARISON_JOIN`        |          yes | `AstJoinNode`                            | `INNER/LEFT/RIGHT/FULL/SEMI/ANTI JOIN ... ON lhs op rhs`      | Equi and comparison joins. `MARK` becomes `LEFT` plus mark expression.                                     |
| `LOGICAL_ANY_JOIN`               |          yes | `AstJoinNode`                            | `JOIN ... ON arbitrary_condition`                             | Non-equi or opaque join predicate.                                                                         |
| `LOGICAL_CROSS_PRODUCT`          |          yes | `AstJoinNode`                            | `INNER JOIN ... ON TRUE`                                      | Semantically cross join; rendered through join node.                                                       |
| `LOGICAL_POSITIONAL_JOIN`        |           no | default throw                            | none                                                          | Positional join has no AST case.                                                                           |
| `LOGICAL_ASOF_JOIN`              |           no | default throw                            | none                                                          | ASOF join has no AST case.                                                                                 |
| `LOGICAL_DEPENDENT_JOIN`         |      limited | `AstDelimJoinNode`                       | normalized decorrelation join                                 | Shares the `DELIM_JOIN` path.                                                                              |
| `LOGICAL_UNION`                  |          yes | `AstUnionNode`                           | `UNION [ALL]`                                                 | N-ary unions are chained left-deep in the flattener.                                                       |
| `LOGICAL_EXCEPT`                 |          yes | `AstSetOperationNode`                    | `EXCEPT [ALL]`                                                | Concrete enum behind the requested `LOGICAL_SET_OPERATION (EXCEPT)`.                                       |
| `LOGICAL_INTERSECT`              |          yes | `AstSetOperationNode`                    | `INTERSECT [ALL]`                                             | Concrete enum behind the requested `LOGICAL_SET_OPERATION (INTERSECT)`.                                    |
| `LOGICAL_RECURSIVE_CTE`          |          yes | `AstRecursiveCteNode`                    | `WITH RECURSIVE name(cols) AS (anchor UNION [ALL] recursive)` | Recursive step is rendered inline to avoid forward-reference CTE ordering issues.                          |
| `LOGICAL_MATERIALIZED_CTE`       |          yes | `AstMaterializedCteNode`                 | flat CTE body plus `CteRef` readers                           | Handles normal `WITH` materialization.                                                                     |
| `LOGICAL_INSERT`                 |      limited | `AstInsertNode`                          | `INSERT [OR ...] INTO target SELECT * FROM cte`               | Root DML only; not normally needed for MV body serialization.                                              |
| `LOGICAL_DELETE`                 |           no | default throw                            | none                                                          | DML maintenance is owned by OpenIVM/openivm-spark, not LPTS MV-body SELECT.                                |
| `LOGICAL_UPDATE`                 |           no | default throw                            | none                                                          | DML maintenance is owned by OpenIVM/openivm-spark.                                                         |
| `LOGICAL_MERGE_INTO`             |           no | default throw                            | none                                                          | No AST case.                                                                                               |
| `LOGICAL_ALTER`                  |           no | default throw                            | none                                                          | DDL utility statement.                                                                                     |
| `LOGICAL_CREATE_TABLE`           |           no | default throw                            | none                                                          | DDL utility statement.                                                                                     |
| `LOGICAL_CREATE_INDEX`           |           no | default throw                            | none                                                          | DDL utility statement.                                                                                     |
| `LOGICAL_CREATE_SEQUENCE`        |           no | default throw                            | none                                                          | DDL utility statement.                                                                                     |
| `LOGICAL_CREATE_VIEW`            |           no | default throw                            | none                                                          | DDL utility statement.                                                                                     |
| `LOGICAL_CREATE_SCHEMA`          |           no | default throw                            | none                                                          | DDL utility statement.                                                                                     |
| `LOGICAL_CREATE_MACRO`           |           no | default throw                            | none                                                          | DDL utility statement.                                                                                     |
| `LOGICAL_DROP`                   |           no | default throw                            | none                                                          | DDL utility statement.                                                                                     |
| `LOGICAL_PRAGMA`                 |           no | default throw                            | none                                                          | Utility statement.                                                                                         |
| `LOGICAL_TRANSACTION`            |           no | default throw                            | none                                                          | Utility statement.                                                                                         |
| `LOGICAL_CREATE_TYPE`            |           no | default throw                            | none                                                          | DDL utility statement.                                                                                     |
| `LOGICAL_ATTACH`                 |           no | default throw                            | none                                                          | Catalog utility statement.                                                                                 |
| `LOGICAL_DETACH`                 |           no | default throw                            | none                                                          | Catalog utility statement.                                                                                 |
| `LOGICAL_EXPLAIN`                |           no | default throw                            | none                                                          | Diagnostic statement.                                                                                      |
| `LOGICAL_PREPARE`                |           no | default throw                            | none                                                          | Prepared-statement wrapper.                                                                                |
| `LOGICAL_EXECUTE`                |           no | default throw                            | none                                                          | Prepared-statement wrapper.                                                                                |
| `LOGICAL_EXPORT`                 |           no | default throw                            | none                                                          | Utility statement.                                                                                         |
| `LOGICAL_VACUUM`                 |           no | default throw                            | none                                                          | Utility statement.                                                                                         |
| `LOGICAL_SET`                    |           no | default throw                            | none                                                          | Session setting statement.                                                                                 |
| `LOGICAL_LOAD`                   |           no | default throw                            | none                                                          | Extension loading statement.                                                                               |
| `LOGICAL_RESET`                  |           no | default throw                            | none                                                          | Session setting statement.                                                                                 |
| `LOGICAL_UPDATE_EXTENSIONS`      |           no | default throw                            | none                                                          | Utility statement.                                                                                         |
| `LOGICAL_CREATE_SECRET`          |           no | default throw                            | none                                                          | Secret DDL; never part of MV body.                                                                         |
| `LOGICAL_EXTENSION_OPERATOR`     |           no | default throw                            | none                                                          | Extension-defined physical/logical escape hatch; no generic serializer.                                    |

## Operator coverage matrix

| Family                 |                                                                  yes |       limited/varies |                                                 no/n/a |
| ---------------------- | -------------------------------------------------------------------: | -------------------: | -----------------------------------------------------: |
| Relational SELECT core | projection, filter, aggregate, window, limit, order, top-n, distinct |               unnest |                                          sample, pivot |
| Data sources           |               get, expression-get, dummy-scan, empty-result, cte-ref | chunk-get, delim-get |                                     extension operator |
| Joins                  |                                               comparison, any, cross |     delim, dependent |                    generic join enum, positional, asof |
| Set operations         |            union, except, intersect, recursive CTE, materialized CTE |                 none |                                                   none |
| DML                    |                                                                 none |               insert |                                  update, delete, merge |
| DDL/utility            |                                                                 none |                 none | create/drop/alter/copy/pragma/transaction/explain/etc. |

## Deep dives for `varies` and `limited` rows

### `LOGICAL_UNNEST`

`LOGICAL_UNNEST` has an explicit `AstBuilder` case. LPTS copies child columns
through when the child is not a dummy scan, serializes each unnest expression
with `ExpressionToAliasedString`, and emits an `AstProjectNode`.

What works:

- Basic `UNNEST(list_expr)` in a projection.
- Child-column pass-through for lateral-looking unnest shapes that DuckDB binds
  as a `LogicalUnnest` over a real child.
- `BoundUnnestExpression` serialization as `UNNEST(child)`.

What is limited:

- LPTS does not create a dedicated lateral-view node.
- Spark often wants `LATERAL VIEW EXPLODE(...)` or `explode(...)`/`inline(...)`
  shapes rather than DuckDB's exact `UNNEST(...)` syntax.
- Multiple-column unnest semantics can diverge between DuckDB and Spark.

Workarounds:

- Prefer Spark/DuckDB intersection SQL where the final emitted SQL stays as a
  select-list expression accepted by Spark.
- Add a Spark dialect lowering in LPTS if the logical shape is clear.
- Add a post-processor in `openivm-spark` only if the rewrite is token-local;
  otherwise do it in LPTS while the logical shape is still available.

### `LOGICAL_CHUNK_GET`

`LOGICAL_CHUNK_GET` is implemented as an `AstGetNode` whose table name is a
`(VALUES ...)` subquery. DuckDB uses it for materialized `ColumnDataCollection`
constants, such as large constant `IN` lists.

What works:

- Constant rows are read from the collection and rendered as SQL literals.
- Synthetic columns `c0`, `c1`, ... are assigned and mapped to LPTS CTE columns.
- The CTE renderer detects table names with parentheses and appends table
  function-style column aliases when needed.

What is limited:

- It assumes the collection can be losslessly represented by each value's
  `ToSQLString()`.
- It is best suited to constant values, not arbitrary runtime data.
- Very large constant sets can produce very large SQL strings.

Workarounds:

- Keep large lookup sets as real tables when possible.
- If the generated SQL grows too large, lower the query to a join against a
  small table rather than a huge `IN (...)` predicate.

### `LOGICAL_DELIM_GET` and `LOGICAL_DELIM_JOIN`

DuckDB uses delim joins during decorrelation of correlated subqueries. LPTS has
explicit AST nodes for both sides:

- `AstDelimJoinNode` owns the decorrelation scope.
- `AstDelimGetNode` becomes `SELECT DISTINCT correlation_keys FROM outer_cte`.
- The recursive traversal processes the outer child first, pre-registers all
  delim-get columns in the inner child, then processes the inner child.

What works:

- Decorrelated `EXISTS`, `IN`, and scalar-subquery-like plans when DuckDB emits
  comparison-style delim joins.
- `MARK` joins converted to `LEFT` joins plus a computed boolean mark column.
- `SINGLE` joins normalized to `LEFT` joins.
- `RIGHT_SEMI` and `RIGHT_ANTI` normalized when `delim_flipped` changes the
  physical child order.
- Nested delim joins are partially handled by stopping ownership at nested
  delim scopes and registering only the appropriate outer child.

What is limited:

- `DELIM_GET` cannot stand alone. If no parent registered source columns, LPTS
  throws `DELIM_GET ... was not pre-registered`.
- Some scalar correlated subquery shapes still stress Spark after serialization;
  `openivm-spark` has a known Spark 3.5 `EXCEPT ALL` constraint bug workaround
  in tests.
- The generated SQL can be semantically complex because the duplicate-eliminated
  key CTE must line up with DuckDB's decorrelation assumptions.

Workarounds:

- Rewrite correlated subqueries as explicit joins when possible.
- For scalar subqueries, ensure the SQL guarantees one row per outer key.
- Add focused LPTS support for the exact failing decorrelation shape; do not
  patch the emitted SQL blindly after CTE flattening if ownership information is
  needed.

### `LOGICAL_DEPENDENT_JOIN`

`LOGICAL_DEPENDENT_JOIN` shares the same implementation as `LOGICAL_DELIM_JOIN`.
It is therefore limited by the same correlation pre-registration and join-type
normalization assumptions.

What works:

- Dependent joins that DuckDB exposes as `LogicalComparisonJoin` with duplicate
  eliminated columns.
- Decorrelated shapes where the preserved outer side is identifiable.

What is limited:

- Arbitrary dependent execution semantics are not represented as a first-class
  SQL operator.
- Unsupported join subtypes still fail during rendering.

Workarounds:

- Prefer decorrelatable SQL.
- Force the original query path in OpenIVM for constructs that LPTS should not
  normalize before incremental classification.

### `LOGICAL_INSERT`

LPTS has an `AstInsertNode`, and the flattener treats an insert root specially:
it flattens the child query and emits `INSERT ... SELECT * FROM child_cte`.

What works:

- Root insert with conflict actions DuckDB exposes as `THROW`, `REPLACE`,
  `UPDATE`, or `NOTHING`.
- Insert as the final statement, not as a nested MV-body operator.

What is limited:

- The MV body compiled by OpenIVM is a SELECT. Insert is usually not needed
  there.
- Spark and DuckDB conflict-action syntax differ; OpenIVM's maintenance DML is
  handled elsewhere.
- `UPDATE`, `DELETE`, and `MERGE` do not have equivalent root nodes.

Workarounds:

- Let OpenIVM/openivm-spark generate maintenance DML.
- Use LPTS for query serialization, not for full DML program translation.

### `LOGICAL_PIVOT`

The pinned DuckDB enum contains `LOGICAL_PIVOT`, but LPTS has no case for it.
It falls into the default `AstBuilder` throw.

What works:

- Nothing at LPTS logical-operator level.
- OpenIVM can choose an original-SQL/full-refresh path for unsupported
  constructs.

What does not work:

- DuckDB `PIVOT` does not round-trip through LPTS to a portable CTE AST.
- Spark has `PIVOT`, but the syntax and binder behavior are not identical to
  DuckDB's logical node.

Workarounds:

- Rewrite the query as conditional aggregates, e.g. `SUM(CASE WHEN k='x' THEN v ELSE 0 END) AS x`.
- Add an LPTS `AstPivotNode` if pivot should become a supported construct.
- Add a pre-processor that lowers pivot to aggregate/projection before LPTS.

### `LOGICAL_UNPIVOT`

The requested topic includes `LOGICAL_UNPIVOT`, but the pinned DuckDB enum used
by this repository does not list a `LOGICAL_UNPIVOT` member. If a future DuckDB
version adds one, current LPTS would not handle it until an `AstBuilder` case is
added.

Workarounds:

- Express unpivot as `UNION ALL` branches.
- Add a pre-processor that lowers unpivot to `UNION ALL` before LPTS.
- Re-check the DuckDB enum whenever the pinned submodule is bumped.

### `LOGICAL_SAMPLE`

`LOGICAL_SAMPLE` has no LPTS case.

What does not work:

- `TABLESAMPLE`, `USING SAMPLE`, or optimizer-produced sample nodes.
- Deterministic round-trip of sampling semantics across DuckDB, Spark, and
  Postgres.

Workarounds:

- Remove sampling from MV definitions.
- Materialize sampled data into a base table outside the MV if it is intentional
  and stable.

### `LOGICAL_POSITIONAL_JOIN` and `LOGICAL_ASOF_JOIN`

Both appear in the DuckDB enum and both are unsupported by LPTS.

What does not work:

- Positional row-number-based joins.
- ASOF nearest-preceding temporal joins.

Workarounds:

- Rewrite positional joins using explicit row numbers and equality joins.
- Rewrite ASOF joins using range predicates plus a window/top-1 pattern, then
  validate that OpenIVM classifies the result as an acceptable refresh type.

### `LOGICAL_UPDATE`, `LOGICAL_DELETE`, and `LOGICAL_MERGE_INTO`

These DML nodes are not needed for MV-body SELECT serialization. LPTS only has a
limited insert root; update/delete/merge fall through to the default throw.

Workarounds:

- Keep user MV bodies as SELECT queries.
- Let `openivm-spark`'s refresh rewriter own Spark/Delta `MERGE`, `UPDATE`, and
  `INSERT OVERWRITE` statement generation.

## Expression coverage table

DuckDB has two relevant enums:

- `ExpressionClass`: the runtime class of the expression object.
- `ExpressionType`: the operation subtype, such as `COMPARE_EQUAL` or
  `WINDOW_ROW_NUMBER`.

LPTS dispatches primarily on `ExpressionClass` in
`ExpressionToAliasedString`. Some classes then dispatch on `ExpressionType`.

| DuckDB `ExpressionClass` |          LPTS support | Serializer behavior                                | Notes                                                                                              |
| ------------------------ | --------------------: | -------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `INVALID`                |                    no | default throw                                      | Sentinel only.                                                                                     |
| `AGGREGATE`              |                   n/a | none                                               | Parsed expression; LPTS sees bound aggregate expressions inside aggregate nodes.                   |
| `CASE`                   |                   n/a | none                                               | Parsed expression; bound form is supported.                                                        |
| `CAST`                   |                   n/a | none                                               | Parsed expression; bound form is supported.                                                        |
| `COLUMN_REF`             |                   n/a | none                                               | Parsed expression; bound form is supported.                                                        |
| `COMPARISON`             |                   n/a | none                                               | Parsed expression; bound form is supported.                                                        |
| `CONJUNCTION`            |                   n/a | none                                               | Parsed expression; bound form is supported.                                                        |
| `CONSTANT`               |                   n/a | none                                               | Parsed expression; bound form is supported.                                                        |
| `DEFAULT`                |                   n/a | none                                               | Parsed expression; no MV-body support.                                                             |
| `FUNCTION`               |                   n/a | none                                               | Parsed expression; bound form is supported.                                                        |
| `OPERATOR`               |                   n/a | none                                               | Parsed expression; bound form has selected support.                                                |
| `STAR`                   |                   n/a | none                                               | Expanded before LPTS.                                                                              |
| `SUBQUERY`               |                   n/a | none                                               | Parsed expression; bound subquery class is unsupported directly.                                   |
| `WINDOW`                 |                   n/a | none                                               | Parsed expression; bound window is supported.                                                      |
| `PARAMETER`              |                   n/a | none                                               | Parsed expression; bound parameter is unsupported.                                                 |
| `COLLATE`                |                   n/a | none                                               | No explicit serializer case.                                                                       |
| `LAMBDA`                 |                   n/a | none                                               | Parsed expression; lambda support is via bound function bind data and bound refs.                  |
| `POSITIONAL_REFERENCE`   |                   n/a | none                                               | Resolved before LPTS in normal bound SELECT plans.                                                 |
| `BETWEEN`                |                   n/a | none                                               | Parsed expression; bound between is supported.                                                     |
| `LAMBDA_REF`             |                   n/a | none                                               | Parsed expression; bound lambda ref is supported.                                                  |
| `TYPE`                   |                   n/a | none                                               | Parser helper.                                                                                     |
| `BOUND_AGGREGATE`        | yes, context-specific | Serialized in `LOGICAL_AGGREGATE_AND_GROUP_BY`     | Not handled by generic expression switch except through aggregate node code.                       |
| `BOUND_CASE`             |                   yes | `CASE WHEN ... THEN ... ELSE ... END`              | Handles multiple branches.                                                                         |
| `BOUND_CAST`             |                   yes | `CAST(child AS type)` or `TRY_CAST(child AS type)` | Uses DuckDB type string; Spark post-processing fixes some type names.                              |
| `BOUND_COLUMN_REF`       |                   yes | CTE alias lookup from `column_map`                 | Throws if binding is absent.                                                                       |
| `BOUND_COMPARISON`       |                   yes | `(left) op (right)`                                | Uses DuckDB `ExpressionTypeToOperator`.                                                            |
| `BOUND_CONJUNCTION`      |                   yes | `(child0) AND/OR (child1) ...`                     | N-ary conjunction supported.                                                                       |
| `BOUND_CONSTANT`         |                   yes | `expression->ToString()`                           | Relies on DuckDB SQL literal rendering.                                                            |
| `BOUND_DEFAULT`          |                    no | default throw                                      | Not expected in MV body.                                                                           |
| `BOUND_FUNCTION`         |                   yes | `func(args...)`, infix for operators               | Includes dialect-name lookup for selected functions.                                               |
| `BOUND_OPERATOR`         |               limited | selected operator spellings                        | Supports null tests, `NOT`, `IN`, `NOT IN`, `COALESCE`, `NULLIF`, `TRY`; other subtypes throw.     |
| `BOUND_PARAMETER`        |                    no | default throw                                      | Prepared-statement parameters are not serialized.                                                  |
| `BOUND_REF`              |                   yes | `expression->ToString()`                           | Used for lambda parameters.                                                                        |
| `BOUND_SUBQUERY`         |             no direct | default throw                                      | Correlated subqueries must be decorrelated to joins/delim joins before expression serialization.   |
| `BOUND_WINDOW`           |                   yes | `fn(...) OVER (...)`                               | Window function/name/frame serializer.                                                             |
| `BOUND_BETWEEN`          |                   yes | lower/upper comparisons joined by `AND`            | Expands to comparison predicates.                                                                  |
| `BOUND_UNNEST`           |                   yes | `UNNEST(child)`                                    | Dialect-specific execution caveats apply.                                                          |
| `BOUND_LAMBDA`           |      indirect/limited | lambda body from `ListLambdaBindData`              | LPTS serializes lambdas attached to bound list functions, not arbitrary standalone lambda classes. |
| `BOUND_LAMBDA_REF`       |                   yes | `expression->ToString()`                           | Used for lambda parameters.                                                                        |
| `BOUND_EXPRESSION`       |                    no | default throw                                      | Generic wrapper not handled.                                                                       |
| `BOUND_EXPANDED`         |                    no | default throw                                      | Expanded expressions should be removed before this point.                                          |

## Highlighted expression cases

### `BoundColumnRefExpression`

Column refs are not emitted as original table columns. LPTS resolves the
`ColumnBinding(table_index, column_index)` through `column_map` and emits a CTE
scoped name such as `t0_amount`.

Failure mode:

- If the binding is not in `column_map`, `FindColumnBinding` throws a
  `NotImplementedException` explaining the missing binding context.

Why it matters:

- Every operator case must register its output bindings before parent
  expressions are serialized.
- Projection and aggregate code includes fallback registration for optimizer
  shapes where parent expressions still refer to lower-scope bindings.

### `BoundConstantExpression`

Constants use DuckDB's `ToString()` / `ToSQLString()` style rendering.

Works well for:

- Numbers.
- Strings.
- NULLs.
- Date/timestamp literals that DuckDB can render as SQL.

Caveat:

- Spark may not accept every DuckDB literal/type spelling. The Scala
  `LptsSparkDialect` post-processor handles known emitted cases such as postfix
  casts, quoted intervals, and type suffixes.

### `BoundCastExpression`

LPTS emits:

```sql
CAST(child AS DuckDBType)
TRY_CAST(child AS DuckDBType)
```

Known Spark follow-up rewrites:

- `VARCHAR`, `CHAR`, `TEXT` -> `STRING` for bare casts.
- `HUGEINT`, `UHUGEINT` -> `BIGINT`.
- `TIMESTAMP WITH/WITHOUT TIME ZONE` -> `TIMESTAMP`.
- DuckDB postfix casts are normalized when they appear in OpenIVM output.

### `BoundFunctionExpression`

LPTS serializes most functions as normal function calls, with three important
special cases:

1. Internal compression/decompression wrappers are stripped by returning the
   first child expression.
1. Selected functions are renamed for Postgres and Spark dialects.
1. Operator-like functions with two children can render in infix form.

Spark dialect function remaps in LPTS:

| DuckDB/LPTS input                   | Spark output     |
| ----------------------------------- | ---------------- |
| `strftime`                          | `date_format`    |
| `strptime`                          | `to_timestamp`   |
| `list_transform`, `array_transform` | `transform`      |
| `list_aggregate`, `array_aggregate` | `aggregate`      |
| `list_filter`, `array_filter`       | `filter`         |
| `list_value`                        | `array`          |
| `list_contains`, `array_contains`   | `array_contains` |
| `list_extract`, `array_extract`     | `element_at`     |

Unsupported functions are usually emitted unchanged. That is intentional: a
Spark-side error is safer than a silent semantic mistranslation.

### `BoundOperatorExpression`

The explicit `BOUND_OPERATOR` switch supports:

| `ExpressionType`       | Emitted SQL        |
| ---------------------- | ------------------ |
| `OPERATOR_IS_NULL`     | `(x) IS NULL`      |
| `OPERATOR_IS_NOT_NULL` | `(x) IS NOT NULL`  |
| `OPERATOR_NOT`         | `NOT (x)`          |
| `COMPARE_IN`           | `(x) IN (...)`     |
| `COMPARE_NOT_IN`       | `(x) NOT IN (...)` |
| `OPERATOR_COALESCE`    | `COALESCE(...)`    |
| `OPERATOR_NULLIF`      | `NULLIF(a, b)`     |
| `OPERATOR_TRY`         | `TRY(x)`           |

Other `BOUND_OPERATOR` subtypes throw:

```text
Not implemented BOUND_OPERATOR subtype for ExpressionToAliasedString: <type>
```

Arithmetic such as `+`, `-`, `*`, `/`, `%`, and concatenation `||` commonly
arrives as `BOUND_FUNCTION` with operator names and is handled in the function
case's infix path.

### `BoundCaseExpression`

LPTS emits a standard searched CASE:

```sql
CASE WHEN cond THEN value ELSE fallback END
```

This is one of the safest expression forms across DuckDB, Spark, and Postgres.
It is also the recommended lowering target for unsupported pivot-like logic.

### `BoundSubqueryExpression`

There is no direct `BOUND_SUBQUERY` case in `ExpressionToAliasedString`.
Subqueries must be removed by DuckDB planning/decorrelation before LPTS reaches
expression serialization.

What works:

- Subqueries decorrelated into joins, mark joins, delim joins, CTE refs, or set
  operations.

What fails:

- A bound subquery expression that survives into projection/filter expression
  serialization hits the default expression throw.

Workarounds:

- Rewrite correlated subqueries as joins.
- Use CTEs to make subquery outputs explicit.
- Add support at the logical-operator/decorrelation level, not as a raw string
  subquery pasted into an arbitrary expression.

### `BoundLambdaExpression`

Standalone `BOUND_LAMBDA` does not have a direct switch case. LPTS supports the
lambda functions it knows through `BoundFunctionExpression` metadata:

- It detects `func_expr.function.bind_lambda`.
- It serializes non-lambda arguments first.
- It reads `ListLambdaBindData` to render `lambda p0, p1: body`.
- It maps DuckDB list/array function names to Spark names such as `transform`,
  `filter`, and `aggregate`.

Caveats:

- Lambda parameter names are reconstructed from bound references.
- Nested lambda behavior is intentionally conservative.
- Spark supports higher-order array functions, but not every DuckDB lambda/list
  function has identical semantics.

### `BoundUnnestExpression`

`BOUND_UNNEST` emits `UNNEST(child)`. This is correct DuckDB SQL and may be
usable in LPTS-DuckDB output.

Spark caveat:

- Spark often represents the same idea with `explode`, `posexplode`, `inline`,
  or `LATERAL VIEW`. If the emitted shape fails Spark parsing, add a logical
  Spark dialect lowering rather than a fragile regex rewrite.

### `BoundWindowExpression`

LPTS has a dedicated `WindowExpressionToAliasedString` helper. It supports:

- Aggregate windows.
- `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`.
- `first_value`, `last_value`, `nth_value`, `lead`, `lag`, `fill`.
- Partitioning, ordering, argument ordering, filters, `IGNORE NULLS`, and frame
  start/end rendering.

Spark-specific limitations:

- `GROUPS` frame units throw in Spark dialect.
- `EXCLUDE` window clauses throw in Spark dialect.

### `BoundParameterExpression`

`BOUND_PARAMETER` is not supported. MV definitions compiled through OpenIVM are
expected to be concrete SQL, not prepared statements with runtime parameters.

## Important `ExpressionType` coverage

| `ExpressionType` family/member                                                 |                              LPTS support | Notes                                                                                             |
| ------------------------------------------------------------------------------ | ----------------------------------------: | ------------------------------------------------------------------------------------------------- |
| `COMPARE_EQUAL`, `COMPARE_NOTEQUAL`, `<`, `>`, `<=`, `>=`                      |                                       yes | In `BOUND_COMPARISON` and join conditions through `ExpressionTypeToOperator`.                     |
| `COMPARE_DISTINCT_FROM`, `COMPARE_NOT_DISTINCT_FROM`                           | yes if DuckDB operator string is accepted | Rendered through `ExpressionTypeToOperator`; downstream dialect must parse it.                    |
| `COMPARE_BETWEEN`, `COMPARE_NOT_BETWEEN`                                       |                                       yes | Bound between expands to comparisons.                                                             |
| `COMPARE_IN`, `COMPARE_NOT_IN`                                                 |                                   limited | Supported in `BOUND_OPERATOR`; subquery `IN` must decorrelate first.                              |
| `CONJUNCTION_AND`, `CONJUNCTION_OR`                                            |                                       yes | N-ary conjunction serializer.                                                                     |
| `VALUE_CONSTANT`, `VALUE_NULL`                                                 |                         yes after binding | Bound constant path.                                                                              |
| `VALUE_PARAMETER`                                                              |                                        no | Bound parameter unsupported.                                                                      |
| `BOUND_AGGREGATE`                                                              |                 yes in aggregate operator | Not a general scalar expression.                                                                  |
| `GROUPING_FUNCTION`                                                            |                 yes in aggregate operator | Rendered as `GROUPING(...)` for aggregate grouping functions.                                     |
| `WINDOW_*`                                                                     |                               yes/limited | Supported window functions listed above; Spark frame/exclude caveats.                             |
| `BOUND_FUNCTION`                                                               |                               yes/limited | Generic function call plus selected dialect remaps.                                               |
| `CASE_EXPR`                                                                    |                         yes after binding | Bound case path.                                                                                  |
| `OPERATOR_NULLIF`                                                              |                                       yes | `NULLIF`.                                                                                         |
| `OPERATOR_COALESCE`                                                            |                                       yes | `COALESCE`.                                                                                       |
| `ARRAY_EXTRACT`, `ARRAY_SLICE`, `STRUCT_EXTRACT`, `ARRAY_CONSTRUCTOR`, `ARROW` |                                    varies | Usually arrive as functions/operators; Spark may need post-processing, especially struct extract. |
| `OPERATOR_TRY`                                                                 |                                   limited | Emits `TRY(...)`; Spark support depends on function availability.                                 |
| `SUBQUERY`                                                                     |                                 no direct | Must decorrelate.                                                                                 |
| `CAST` / `OPERATOR_CAST`                                                       |                         yes after binding | Bound cast path.                                                                                  |
| `BOUND_REF`, `BOUND_LAMBDA_REF`                                                |                                       yes | Uses DuckDB string form for lambda refs.                                                          |
| `BOUND_UNNEST`                                                                 |                                    varies | Emits `UNNEST(...)`; Spark caveats.                                                               |

## Unsupported operator failure mode

The key throw site is the default case in `AstBuilder::BuildNode`:

```cpp
default:
  throw NotImplementedException(
      "AstBuilder: operator '%s' is not yet implemented",
      LogicalOperatorToString(op->type));
```

There are related expression-level throw sites:

```cpp
throw NotImplementedException(
    "Not implemented BOUND_OPERATOR subtype for ExpressionToAliasedString: %s",
    ExpressionTypeToString(op_expr.GetExpressionType()));

throw NotImplementedException(
    "Not implemented expression for ExpressionToAliasedString: %s",
    ExpressionTypeToString(expression->type));
```

The caller path in OpenIVM is intentionally defensive:

1. During `CREATE MATERIALIZED VIEW`, OpenIVM tries to run LPTS on a rewritten
   select plan.
1. If LPTS throws `Exception`, `parser.cpp` catches it, sets
   `lpts_fallback=true`, and stores the original user query as `view_query`.
1. If LPTS throws a non-DuckDB exception, the generic catch does the same.
1. Later classification marks unsupported incremental constructs or
   non-incremental-compatible plans as `RefreshType::FULL_REFRESH`.
1. Some constructs use recompute-style refresh types instead of full refresh,
   but the safe end of the recovery ladder is `FULL_REFRESH` rather than a
   malformed incremental SQL program.

This distinction matters: an LPTS serialization failure is not itself the final
refresh type. It is a signal that OpenIVM must not rely on LPTS-rewritten SQL for
that view. Classification then decides whether a recompute path is available or
whether the view must use full refresh.

## Dialect-specific function-name table

| Standard semantic       | DuckDB                           | Spark                                               | Postgres                | Notes                                                                                 |
| ----------------------- | -------------------------------- | --------------------------------------------------- | ----------------------- | ------------------------------------------------------------------------------------- |
| length(string)          | `length`                         | `length`                                            | `length`                | Universal enough to pass through.                                                     |
| concat                  | `concat` / \`                    |                                                     | \`                      | `concat` / \`                                                                         |
| regex match             | `regexp_matches`                 | `regexp_like` / `rlike` / `regexp`                  | `~` or `regexp_match`   | `openivm-spark` post-pass rewrites shimmed `regexp_matches` to Spark-compatible form. |
| parse timestamp         | `strptime(s, fmt)`               | `to_timestamp(s, fmt)`                              | `to_timestamp(s, fmt)`  | LPTS maps Spark/Postgres names. Spark post-pass handles shim inlinings.               |
| format timestamp        | `strftime(ts, fmt)`              | `date_format(ts, fmt)`                              | `to_char(ts, fmt)`      | LPTS maps both Spark and Postgres.                                                    |
| array/list literal      | `list_value(a,b)` / `[a,b]`      | `array(a,b)`                                        | `ARRAY[a,b]`            | LPTS Spark maps `list_value` to `array`.                                              |
| list transform          | `list_transform(xs, lambda...)`  | `transform(xs, lambda...)`                          | no universal equivalent | Higher-order semantics differ.                                                        |
| list filter             | `list_filter(xs, lambda...)`     | `filter(xs, lambda...)`                             | no universal equivalent | Spark supports higher-order arrays.                                                   |
| list reduce/aggregate   | `list_aggregate(xs, lambda...)`  | `aggregate(xs, lambda...)`                          | no universal equivalent | Check argument order and initial value semantics.                                     |
| list contains           | `list_contains(xs, x)`           | `array_contains(xs, x)`                             | `x = ANY(array)`        | LPTS Spark maps list/array contains.                                                  |
| list extract            | `list_extract(xs, i)`            | `element_at(xs, i)`                                 | `xs[i]`                 | Spark and DuckDB are both 1-indexed for this mapping.                                 |
| struct construct        | `struct_pack(k := v)`            | `named_struct('k', v)` or `struct(v AS k)`          | `ROW(...)` / composite  | LPTS preserves DuckDB-ish `struct_pack`; Spark may need additional handling.          |
| struct extract          | `struct_extract(s,'k')`          | `s.k`                                               | `(s).k`                 | `openivm-spark` post-processes `struct_extract` to dot access.                        |
| count star              | `count_star()`                   | `COUNT(*)`                                          | `COUNT(*)`              | `openivm-spark` rewrites `count_star()` in emitted refresh SQL.                       |
| sum no overflow         | `sum_no_overflow`                | `sum`                                               | `sum`                   | LPTS normalizes internal aggregate/window name to `sum`.                              |
| current timestamp       | `now()::timestamp`               | `current_timestamp()`                               | `now()::timestamp`      | Spark post-pass rewrites the common emitted shape.                                    |
| generate integer series | `generate_series(a,b)`           | `sequence(a,b)`                                     | `generate_series(a,b)`  | Spark post-pass rewrites function name in OpenIVM output.                             |
| interval unit helper    | `to_days(n)` etc.                | `n * INTERVAL 1 DAY`                                | interval arithmetic     | Spark post-pass lowers DuckDB temporal helper functions.                              |
| raise error             | `error(msg)`                     | `raise_error(msg)`                                  | no exact universal      | Spark post-pass rewrites `error(...)`.                                                |
| string aggregate        | `string_agg(x, sep)`             | `string_agg` / `array_join(collect_list(...), sep)` | `string_agg`            | LPTS reconstructs separator from DuckDB bind data.                                    |
| quantile                | `quantile_cont`, `quantile_disc` | `percentile_cont`/`percentile_approx` variants      | ordered-set aggregates  | LPTS reconstructs bind-data arguments but dialect semantics must be checked.          |
| date cast text          | `CAST(x AS VARCHAR)`             | `CAST(x AS STRING)`                                 | `CAST(x AS VARCHAR)`    | Spark post-pass rewrites bare text types.                                             |
| huge integer cast       | `CAST(x AS HUGEINT)`             | `CAST(x AS BIGINT)`                                 | no native hugeint       | Spark post-pass maps to `BIGINT`.                                                     |

## What to do if your operator is not supported

### Option 1: rewrite the user SQL to avoid the operator

Use this when the unsupported feature is not central to the MV definition.

Examples:

- Replace `PIVOT` with conditional aggregates.
- Replace `UNPIVOT` with `UNION ALL` branches.
- Replace `ASOF JOIN` with an explicit range join plus `row_number()` filter.
- Replace positional joins with explicit row numbers.
- Replace sampling with a pre-materialized sampled base table.

This is the fastest path and usually the safest for OpenIVM refresh correctness.

### Option 2: add an `AstBuilder` case

Use this when the operator has stable relational semantics that should be
portable through the AST.

Checklist:

1. Add or reuse an `AstNode` in `src/include/lpts_ast.hpp`.
1. Add `ToString`, `NodeType`, and `OutputColumnNames` behavior.
1. Add the `AstBuilder::BuildNode` switch case.
1. Register every output `ColumnBinding` in `column_map`.
1. Add an `AstFlattener::FlattenNode` case.
1. Add or reuse a `CteNode` renderer in `cte_nodes.hpp/cpp`.
1. Test DuckDB dialect first.
1. Test Spark dialect with `target_dialect="spark"` in the CompileFacts JSON.
1. Add an `openivm-spark` parity test if Spark execution is expected to work.

Rules of thumb:

- Do not emit a SQL string before registering bindings.
- Do not use `SELECT *` if DuckDB projection pruning changed the expected CTE
  header.
- Do not silently map an operator to a weaker semantic shape.

### Option 3: add a pre-processor that lowers the operator

Use this when the operator is syntactic sugar for already-supported operators.

Good candidates:

- Pivot to aggregate + projection.
- Unpivot to union all.
- Sample removal or replacement for test-only SQL.
- Certain struct/list function aliases.

Bad candidates:

- Correlated subquery decorrelation that needs binding ownership.
- Join types whose null-preserving side matters.
- Window frame semantics that differ by dialect.

If the lowering needs plan metadata, do it in LPTS or OpenIVM before CTE
flattening. If it is purely token-level and already appears in emitted SQL, it
can live in `openivm-spark`'s `LptsSparkDialect` post-processor.

## Practical support recipes

| Symptom                                                        | Likely unsupported area                  | Best first fix                                                            |
| -------------------------------------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------- |
| `AstBuilder: operator 'LOGICAL_PIVOT' is not yet implemented`  | Pivot operator                           | Rewrite as conditional aggregates.                                        |
| `AstBuilder: operator 'LOGICAL_SAMPLE' is not yet implemented` | Sampling                                 | Remove sample from MV or materialize sample.                              |
| `Not implemented expression ... BOUND_SUBQUERY`                | Subquery survived decorrelation          | Rewrite as join/CTE or add decorrelation support.                         |
| Spark parser rejects `UNNEST(...)`                             | Unnest dialect gap                       | Lower to Spark `explode`/lateral view in LPTS Spark dialect.              |
| Spark parser rejects `GROUPS` window frame                     | Window dialect gap                       | Rewrite to `ROWS`/`RANGE`; Spark dialect intentionally throws.            |
| Spark parser rejects `struct_extract`                          | Function dialect gap                     | Ensure `LptsSparkDialect.translate` ran; add post-pass case if new shape. |
| Missing CTE column binding                                     | Operator did not register output binding | Fix the relevant `AstBuilder` case.                                       |
| Wrong duplicate behavior in mark join                          | Decorrelated subquery join               | Check MARK-to-LEFT conversion and RHS deduplication.                      |

## Summary

LPTS has strong coverage for the relational SELECT core: scans, filters,
projections, aggregates, windows, distinct, order/limit/top-n, joins, set ops,
CTEs, values, and common decorrelation nodes.

The deliberate gaps are utility statements, DDL, most DML, sampling, pivot,
positional/asof joins, generic extension operators, raw bound subqueries, and
prepared parameters.

For `openivm-spark`, the most important boundary is this:

- LPTS should own logical semantics while the plan still has operators and
  bindings.
- `LptsSparkDialect` should own token-level Spark cleanup after OpenIVM emits
  SQL.
- OpenIVM should fall back to original SQL, recompute, or full refresh rather
  than silently generating a weakened incremental plan.
