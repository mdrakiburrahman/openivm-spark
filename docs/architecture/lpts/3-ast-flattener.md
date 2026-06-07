# 3. AstFlattener: lowering an AST into flat SQL
> Scope: LPTS phase 2, logically the AST flattener. In this checkout the code is
> embedded in `.temp/lpts/src/lpts_pipeline.cpp` as `class AstFlattener`
> (`lpts_pipeline.cpp:2522-3222`), not split into a standalone
> `.temp/lpts/src/ast_flattener.cpp` file.
## 3.1 Mental model
LPTS converts queries through this pipeline:
```text
DuckDB LogicalOperator tree -> LPTS AstNode tree -> CteList -> SQL string
```
Phase 1 keeps DuckDB's tree shape. Phase 2, `AstFlattener`, removes that tree
shape by assigning every intermediate result a CTE name. Parent-child pointers
become string references such as `filter_1 FROM scan_0`.
Example input AST:
```text
Project
  Filter
    Get(users)
```
Flattened CTE program:
```text
scan_0 -> filter_1 -> projection_2 -> FinalReadNode
```
SQL shape:
```sql
WITH scan_0(...) AS (...),
filter_1(...) AS (SELECT ... FROM scan_0 WHERE ...),
projection_2(...) AS (SELECT ... FROM filter_1)
SELECT ... FROM projection_2;
```
That is what "flattening" means in this source: not necessarily fewer relational
operators, but fewer nested anonymous subqueries and explicit named dependencies.
## 3.2 Why flattening is needed
DuckDB's optimized logical plan is correct, but it is not a readable SQL surface.
A small query can become a stack of projections, filters, order/limit wrappers,
CTE wrappers, or delim-join nodes.
A naive AST renderer might emit this:
```sql
SELECT * FROM (
  SELECT name FROM (
    SELECT * FROM (
      SELECT id, name, age FROM users
    ) WHERE age > 25
  )
) ORDER BY name LIMIT 10;
```
The flattener emits a debuggable CTE program instead:
```sql
WITH scan_0(t0_id, t0_name, t0_age) AS (
  SELECT id, name, age FROM memory.main.users
),
filter_1(t0_id, t0_name, t0_age) AS (
  SELECT t0_id, t0_name, t0_age FROM scan_0 WHERE (t0_age > 25)
),
projection_2(t1_name) AS (
  SELECT t0_name FROM filter_1
),
topn_3(t1_name) AS (
  SELECT t1_name FROM projection_2 ORDER BY t1_name ASC LIMIT 10
)
SELECT t1_name AS name FROM topn_3;
```
The output is still more verbose than hand-written SQL. It is, however, much
shorter and safer than arbitrary nested subqueries, and every intermediate step
is testable with the same SQL engine.
## 3.3 What the source actually implements
The user-facing design often talks about passes such as projection collapse,
filter merge, CTE inlining, and redundant `SELECT *` elimination. Those are not
standalone functions in this checkout. Searches for `void Flatten*()` or
`Pass*()` lead to one structural traversal:
- `FlattenNode(const AstNode&)` at `lpts_pipeline.cpp:2892-3152`;
- `Flatten(const AstNode&)` at `lpts_pipeline.cpp:3159-3201`;
- `AstToCteList(...)` at `lpts_pipeline.cpp:3220-3222`.
So the "passes" below are cases inside one traversal. Important non-passes:
- adjacent `Project` nodes are not collapsed;
- adjacent `Filter` nodes are not merged by phase 2;
- `ORDER BY + LIMIT` is one CTE only if DuckDB emitted `LOGICAL_TOP_N`;
- constants are not generally hoisted from select lists;
- chained `UNION ALL` is lowered as a left-deep chain;
- there is no `LPTS_INLINE_CTE` environment-variable branch in source;
- `SELECT *` is avoided locally when correctness requires explicit columns.
## 3.4 Driver, ordering, and fixpoint behavior
`AstToCteList` constructs one flattener and calls `Flatten` once:
```cpp
unique_ptr<CteList> AstToCteList(const AstNode &root, SqlDialect dialect) {
    AstFlattener flattener(dialect);
    return flattener.Flatten(root);
}
```
Citation: `lpts_pipeline.cpp:3220-3222`.
There is no fixpoint loop. Ordinary nodes are post-order:
```cpp
for (const auto &child : ast_node.children) {
    unique_ptr<CteNode> child_cte = FlattenNode(*child);
    children_names.push_back(child_cte->cte_name);
    children_column_lists.push_back(child_cte->cte_column_list);
    cte_nodes.push_back(std::move(child_cte));
}
```
Citation: `lpts_pipeline.cpp:3028-3040`.
Special nodes override that order when semantics require it: materialized CTEs
flatten body before outer query, delim joins flatten outer before inner, and
recursive CTEs flatten the anchor before generating inline recursive-step SQL.
## 3.5 State and invariants carried by the flattener
The core fields are:
```cpp
size_t node_count = 0;
vector<unique_ptr<CteNode>> cte_nodes;
SqlDialect dialect;
bool has_recursive_cte = false;
unordered_map<idx_t, pair<string, vector<string>>> cte_index_to_body_info;
unordered_map<idx_t, string> delim_get_source_cte;
```
`node_count` generates stable names (`scan_0`, `join_7`). `cte_nodes` stores
output in dependency order. `cte_index_to_body_info` resolves materialized and
recursive CTE references. `delim_get_source_cte` resolves correlation keys in
delim joins. `dialect` is stamped onto all output nodes after the list is built.
## 3.6 Implemented pass list
Implemented cases in `FlattenNode` and `Flatten`:
1. materialized CTE body-first flattening;
2. CTE reference rewriting as a scan over the materialized body;
3. delim-join source registration;
4. delim-get hoisting as `SELECT DISTINCT`;
5. recursive CTE anchor/step handling;
6. generic post-order child flattening;
7. `Get` -> `GetNode`;
8. `Filter` -> `FilterNode`;
9. `Project` -> `ProjectNode`;
10. `Aggregate` -> `AggregateNode`;
11. `Join` -> `JoinNode`;
12. `Union` -> `UnionNode` or left-deep chain;
13. `SetOperation` -> `CteSetOperationNode`;
14. `Order` -> `OrderNode`;
15. `Limit` -> `LimitNode`;
16. `TopN` -> `TopNNode`;
17. `Distinct` -> `DistinctNode`;
18. root `Insert` -> `InsertNode`;
19. root `SELECT` -> `FinalReadNode`;
20. dialect stamping.
The following sections give compact before/after examples for each.
## 3.7 Materialized CTE body-first flattening
Source: `lpts_pipeline.cpp:2897-2908`.
Before AST:
```text
MaterializedCte(table_index=7)
  Project(body) -> Get(big_orders)
  Project(outer) -> CteRef(table_index=7)
```
After CTEs:
```text
scan_0, projection_1(body tail), scan_2(ref scan), projection_3, FinalReadNode
```
Emitted SQL:
```sql
WITH scan_0(t0_id, t0_amount) AS (SELECT id, amount FROM memory.main.big_orders),
projection_1(t1_id) AS (SELECT t0_id FROM scan_0),
scan_2(t2_id) AS (SELECT t1_id FROM projection_1),
projection_3(t3_id) AS (SELECT t2_id FROM scan_2)
SELECT t3_id AS id FROM projection_3;
```
The body must be flattened first so `CteRef` can resolve `table_index=7` to the
body tail CTE name and column list. Unknown indexes throw.
## 3.8 CTE reference rewriting
Source: `lpts_pipeline.cpp:2911-2928`.
Before AST:
```text
CteRef(table_index=7, cte_columns=[t2_id, t2_amount])
```
After CTE:
```text
GetNode(scan_2) reading projection_1 and exposing [t2_id, t2_amount]
```
Emitted SQL:
```sql
scan_2(t2_id, t2_amount) AS (
  SELECT t1_id, t1_amount FROM projection_1
)
```
The CTE header provides the names expected by the reference's scope; the select
list reads the names exposed by the materialized body.
## 3.9 Delim-join source registration
Source: `lpts_pipeline.cpp:2930-2961`; phase-1 child normalization:
`lpts_pipeline.cpp:2412-2425`.
Before AST:
```text
DelimJoin(delim_table_indices=[42])
  Project([t1_customer_id]) -> Get(customers)
  Aggregate(count(*)) -> Filter(...) -> DelimGet(table_index=42)
```
After CTE order:
```text
scan_0(customers), projection_1(outer), scan_2(delim keys), filter_3, aggregate_4, join_5
```
Representative SQL:
```sql
scan_2(t2_customer_id) AS (
  SELECT DISTINCT t1_customer_id FROM projection_1
),
join_5(...) AS (
  SELECT ... FROM projection_1 LEFT JOIN aggregate_4 ON ...
)
```
The outer child is flattened first and registered as the source for every inner
`DelimGet`. That preserves correlated-subquery semantics after decorrelation.
## 3.10 Delim-get hoisting
Source: `lpts_pipeline.cpp:2964-2979`.
Before logical query:
```sql
SELECT c.id FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);
```
After flattening, the local correlation key set is hoisted:
```sql
scan_2(t2_id) AS (
  SELECT DISTINCT t1_id FROM projection_1
)
```
This is one of the main CTE-hoisting interactions. A local correlated input
becomes a top-level CTE with a stable name and duplicate-eliminated rows.
## 3.11 Recursive CTE handling
Source: `lpts_pipeline.cpp:2981-3025` and inline renderer
`lpts_pipeline.cpp:2570-2889`.
Before AST:
```text
RecursiveCte(table_index=9, union_all=true)
  Project(anchor: 1 AS n) -> Get((SELECT 1))
  Project(step: n + 1 AS n) -> Filter(n < 5) -> CteRef(table_index=9)
```
After CTE order:
```text
scan_0, projection_1(anchor), recursive_cte_2, scan_3(exposed columns), FinalReadNode
```
Emitted SQL shape:
```sql
WITH RECURSIVE scan_0(t0_1) AS (...),
projection_1(t1_n) AS (...),
recursive_cte_2(n) AS (
  SELECT t1_n FROM projection_1
  UNION ALL
  SELECT n + 1 AS t2_n FROM (SELECT n AS t2_n FROM recursive_cte_2)
),
scan_3(t3_n) AS (SELECT n FROM recursive_cte_2)
SELECT t3_n AS n FROM scan_3;
```
The recursive step is inline SQL because a normal later CTE would need a forward
reference to `recursive_cte_2`.
## 3.12 Generic post-order flattening
Source: `lpts_pipeline.cpp:3028-3152`.
Before AST:
```text
Project([t0_name]) -> Filter([t0_age > 25]) -> Get(users)
```
After CTEs:
```text
scan_0 -> filter_1 -> projection_2 -> FinalReadNode
```
Emitted SQL:
```sql
WITH scan_0(t0_id, t0_name, t0_age) AS (SELECT id, name, age FROM memory.main.users),
filter_1(t0_id, t0_name, t0_age) AS (SELECT t0_id, t0_name, t0_age FROM scan_0 WHERE (t0_age > 25)),
projection_2(t1_name) AS (SELECT t0_name FROM filter_1)
SELECT t1_name AS name FROM projection_2;
```
This common path recurses into children, stores their names and column lists,
then constructs the parent node.
## 3.13 Scan (`Get`) pass
Source: `lpts_pipeline.cpp:3046-3056`; serializer: `cte_nodes.cpp:96-135`.
Before AST:
```text
Get(table=memory.main.users, columns=[id,name,age], cte_columns=[t0_id,t0_name,t0_age])
```
After SQL:
```sql
scan_0(t0_id, t0_name, t0_age) AS (
  SELECT id, name, age FROM memory.main.users
)
```
With pushed filters, `GetNode::ToQuery()` appends `WHERE ...`. For Postgres
output, catalog and schema are dropped before constructing the node.
## 3.14 Filter pass
Source: `lpts_pipeline.cpp:3059-3065`; serializer: `cte_nodes.cpp:137-155`.
Before AST:
```text
Filter([t0_age > 25, t0_active]) -> Get(users)
```
After SQL:
```sql
filter_1(t0_id, t0_name, t0_age, t0_active) AS (
  SELECT t0_id, t0_name, t0_age, t0_active FROM scan_0
  WHERE (t0_age > 25) AND (t0_active)
)
```
One `LogicalFilter` may carry multiple expressions, and those are joined with
`AND`. Adjacent filter nodes are not merged. Explicit select lists preserve
DuckDB `COLUMN_LIFETIME` pruning.
## 3.15 Projection pass
Source: `lpts_pipeline.cpp:3067-3071`; serializer: `cte_nodes.cpp:157-168`.
Before AST:
```text
Project([t0_name, t0_age + 1], cte_columns=[t1_name,t1_age_plus_one]) -> Filter(...)
```
After SQL:
```sql
projection_2(t1_name, t1_age_plus_one) AS (
  SELECT t0_name, t0_age + 1 FROM filter_1
)
```
Adjacent projections are not collapsed. Keeping them separate avoids unsafe
alias substitution when a name is rebound or referenced by an outer expression.
## 3.16 Aggregate pass
Source: `lpts_pipeline.cpp:3073-3077`; serializer: `cte_nodes.cpp:170-185`.
Before AST:
```text
Aggregate(group_by=[t0_region], aggregates=[sum(t0_amount),count_star()]) -> Get(sales)
```
After SQL:
```sql
aggregate_1(t1_region, t2_sum, t2_count) AS (
  SELECT t0_region, sum(t0_amount), count_star() FROM scan_0 GROUP BY t0_region
)
```
The flattener transfers expressions already rendered by phase 1; it does not
reinterpret aggregate semantics.
## 3.17 Join pass
Source: `lpts_pipeline.cpp:3079-3083`; serializer: `cte_nodes.cpp:187-243`.
Before AST:
```text
Join(INNER, [t0_id = t1_user_id])
  Get(users)
  Get(orders)
```
After SQL:
```sql
join_2(t0_id, t0_name, t1_total) AS (
  SELECT t0_id, t0_name, t1_total FROM scan_0 INNER JOIN scan_1 ON t0_id = t1_user_id
)
```
Edge cases: `RIGHT_SEMI`/`RIGHT_ANTI` put the preserved side first; `SINGLE`
serializes as `LEFT`; MARK joins compute a mark expression and deduplicate the
RHS with `SELECT DISTINCT *` to avoid row multiplication.
## 3.18 Union pass
Source: `lpts_pipeline.cpp:3085-3114`; serializer: `cte_nodes.cpp:245-257`.
Before AST:
```text
Union(all=true)
  Get(a)
  Get(b)
  Get(c)
```
After CTEs:
```text
scan_0, scan_1, scan_2, union_3(scan_0 UNION ALL scan_1), union_4(union_3 UNION ALL scan_2)
```
After SQL:
```sql
union_3(t2_id) AS (SELECT * FROM scan_0 UNION ALL SELECT * FROM scan_1),
union_4(t2_id) AS (SELECT * FROM union_3 UNION ALL SELECT * FROM scan_2)
```
The implementation does not emit one multi-arity `UNION ALL`. It chains n-ary
union left-deep and stores child CTE names rather than relying on vector indexes.
## 3.19 EXCEPT and INTERSECT pass
Source: `lpts_pipeline.cpp:3117-3123`; serializer: `cte_nodes.cpp:273-284`.
Before AST:
```text
SetOperation(op_name=EXCEPT, is_all=true) -> Get(a), Get(b)
```
After SQL:
```sql
setop_2(t0_id) AS (SELECT * FROM scan_0 EXCEPT ALL SELECT * FROM scan_1)
```
The flattener requires exactly two children. Wrong arity is an internal error.
## 3.20 Order pass
Source: `lpts_pipeline.cpp:3126-3130`; serializer: `cte_nodes.cpp:286-295`.
Before AST:
```text
Order([t1_name ASC]) -> Project([t0_name])
```
After SQL:
```sql
order_3(t1_name) AS (
  SELECT t1_name FROM projection_2 ORDER BY t1_name ASC
)
```
Order passes columns through and uses an explicit select list for column-count
correctness under projection pruning.
## 3.21 Limit pass
Source: `lpts_pipeline.cpp:3132-3137`; serializer: `cte_nodes.cpp:297-317`.
Before AST:
```text
Limit(limit=10, offset=5) -> Order([t1_name ASC])
```
After SQL:
```sql
limit_4(t1_name) AS (SELECT t1_name FROM order_3 LIMIT 10 OFFSET 5)
```
If the limit or offset is a child scalar, serialization uses a scalar subquery
such as `LIMIT (SELECT first(t2_limit) FROM order_3)`.
## 3.22 TopN pass
Source: `lpts_pipeline.cpp:3139-3142`; phase-1 creation: `2009-2031`.
Before AST:
```text
TopN(order=[t1_score DESC], limit=10, offset=0) -> Project([t0_score])
```
After SQL:
```sql
topn_3(t1_score) AS (
  SELECT t1_score FROM projection_2 ORDER BY t1_score DESC LIMIT 10
)
```
This is the implemented `ORDER BY + LIMIT` combination. It happens only when
DuckDB has already produced `LOGICAL_TOP_N`; phase 2 does not fuse separate
`Order` and `Limit` nodes.
## 3.23 Distinct pass
Source: `lpts_pipeline.cpp:3145-3148`; serializer: `cte_nodes.cpp:334-338`.
Before AST:
```text
Distinct([t1_region]) -> Project([t0_region])
```
After SQL:
```sql
distinct_2(t1_region) AS (SELECT DISTINCT t1_region FROM projection_1)
```
If the distinct node lacks an explicit output list, the child list is used.
## 3.24 Root nodes and final read
Source: `lpts_pipeline.cpp:3164-3201`; serializer: `cte_nodes.cpp:37-79`.
For inserts, the final root is:
```sql
INSERT INTO target SELECT * FROM child_cte;
```
For selects, `FinalReadNode` maps LPTS names back to user-visible aliases:
```text
projection_2(t1_name, t1_age) -> SELECT t1_name AS name, t1_age AS age FROM projection_2
```
This is why pass-through nodes must preserve accurate `cte_column_list` values.
## 3.25 Dialect stamping
Source: `lpts_pipeline.cpp:3204-3214`.
After all nodes are built, `StampDialect` copies the selected dialect to every
CTE and root node. This keeps AST construction dialect-neutral while allowing
`ToQuery()` to quote identifiers and qualify tables appropriately.
## 3.26 Requested beautification passes that are absent
Projection collapse would turn `Project -> Project -> Get` into one projection.
It is absent, and safely implementing it requires alias-conflict checks.
Filter merge would turn `Filter(b) -> Filter(a) -> Get` into one `WHERE (a) AND
(b)`. It is absent; phase 2 emits two filter CTEs if the AST contains two.
Constant hoisting would move `1 AS one` or similar aliases out of select lists.
It is absent; constants stay in `ProjectNode` expressions.
Single-use CTE inlining controlled by `LPTS_INLINE_CTE` is absent in this source.
The only inline renderer is `AstToInlineSQL`, used for recursive CTE steps.
Global redundant-star elimination is absent. `UnionNode` still uses `SELECT *`,
while filters and orders use explicit columns because doing so is required for
column-pruning correctness.
## 3.27 No information loss invariant
Flattening must change representation, not semantics.
The implementation preserves information through these rules:
- every AST node exposes output column names;
- child CTE names and child column lists are carried upward explicitly;
- pass-through nodes preserve or derive the child column list;
- materialized CTE bodies are registered before references;
- delim outer CTEs are registered before inner `DelimGet` nodes;
- recursive steps are inline to avoid invalid forward CTE references;
- unsupported or impossible states throw instead of guessing.
Structural debug assertions (`D_ASSERT`) check child counts and similar facts.
They are not full plan-equivalence assertions.
The practical equivalence guard is `PRAGMA lpts_check`. It runs original SQL and
LPTS SQL and compares bag equality with `EXCEPT ALL` in both directions:
```sql
(A EXCEPT ALL B) is empty AND (B EXCEPT ALL A) is empty
```
Citation: `lpts_extension.cpp:199-231`.
There is no internal debug-only plan-equivalence assert in this checked-out
flattener.
## 3.28 CTE hoisting interaction
Materialized CTEs are hoisted into named LPTS CTEs:
```sql
WITH expensive AS (
  SELECT user_id, sum(total) AS total FROM orders GROUP BY user_id
)
SELECT user_id FROM expensive WHERE total > 100;
```
Flattened shape:
```sql
WITH scan_0(...) AS (... orders ...),
aggregate_1(t1_user_id, t2_total) AS (...),
scan_2(t3_user_id, t3_total) AS (SELECT t1_user_id, t2_total FROM aggregate_1),
filter_3(t3_user_id, t3_total) AS (SELECT t3_user_id, t3_total FROM scan_2 WHERE (t3_total > 100))
SELECT t3_user_id AS user_id FROM filter_3;
```
Delim-get hoisting turns correlated-subquery key sets into top-level CTEs:
```sql
scan_2(t2_id) AS (SELECT DISTINCT t1_id FROM projection_1)
```
Both cases trade local nested structure for explicit reusable names.
## 3.29 Worked end-to-end example
Input SQL:
```sql
SELECT name
FROM (SELECT name, age FROM users WHERE active) u
WHERE age > 25
ORDER BY name
LIMIT 3;
```
Naive AST with ten logical pieces:
```text
01 Limit(limit=3)
02   Order(name ASC)
03     Project(output name)
04       Filter(age > 25)
05         Project(subquery name, age)
06           Filter(active)
07             Get(users)
08             binding id
09             binding name
10             binding age
```
Current flattened CTE list:
```text
scan_0 -> filter_1 -> projection_2 -> filter_3 -> projection_4 -> order_5 -> limit_6 -> FinalReadNode
```
Current SQL shape:
```sql
WITH scan_0(t0_id, t0_name, t0_age, t0_active) AS (
  SELECT id, name, age, active FROM memory.main.users
),
filter_1(t0_id, t0_name, t0_age, t0_active) AS (
  SELECT t0_id, t0_name, t0_age, t0_active FROM scan_0 WHERE (t0_active)
),
projection_2(t1_name, t1_age) AS (
  SELECT t0_name, t0_age FROM filter_1
),
filter_3(t1_name, t1_age) AS (
  SELECT t1_name, t1_age FROM projection_2 WHERE (t1_age > 25)
),
projection_4(t2_name) AS (
  SELECT t1_name FROM filter_3
),
order_5(t2_name) AS (
  SELECT t2_name FROM projection_4 ORDER BY t2_name ASC
),
limit_6(t2_name) AS (
  SELECT t2_name FROM order_5 LIMIT 3
)
SELECT t2_name AS name FROM limit_6;
```
If DuckDB emits `LOGICAL_TOP_N`, the tail shrinks:
```text
scan_0 -> filter_1 -> projection_2 -> filter_3 -> projection_4 -> topn_5 -> FinalReadNode
```
Tail SQL:
```sql
topn_5(t2_name) AS (
  SELECT t2_name FROM projection_4 ORDER BY t2_name ASC LIMIT 3
)
SELECT t2_name AS name FROM topn_5;
```
A future readability optimizer could reduce this conceptually to three or four
nodes: scan, combined select/filter/project, top-n, final read. The checked-out
flattener does not perform that collapse.
## 3.30 Edge cases
Column pruning: filters and orders use explicit select lists because DuckDB's
`COLUMN_LIFETIME` optimizer can prune columns above those nodes. `SELECT *`
could make the CTE body disagree with the CTE header.
Alias conflicts: projection collapse can change resolution when the same alias
is rebound or when an outer expression references an intermediate alias. Keeping
projection CTEs separate avoids this class of errors.
Union position bugs: n-ary union lowering inserts intermediate CTEs, so a node's
index can diverge from its position in `cte_nodes`. Store CTE names, not vector
positions.
Delim child order: phase 1 normalizes `DelimJoin.children[0] = outer` and
`children[1] = inner`; phase 2 relies on that order.
Materialized CTE order: bodies must be flattened and registered before refs.
Unknown refs throw.
Recursive self-reference: recursive steps use inline SQL so they can refer to
the recursive CTE itself. Unsupported inline node types throw.
MARK join multiplicity: MARK joins deduplicate the RHS with `SELECT DISTINCT *`
so duplicate matches do not multiply preserved left rows.
Set-operation arity: `EXCEPT` and `INTERSECT` require exactly two children.
Wrong arity throws.
Nondeterminism: `lpts_check` uses strict bag equality. Tied `ORDER BY ... LIMIT`,
volatile values, or nondeterministic aggregate order can fail the check even
when the transformation is structurally reasonable.
## 3.31 Debugging checklist
1. Run `PRAGMA print_ast('...')` and identify the AST node shape.
2. Find the corresponding `FlattenNode` case.
3. Verify child CTE names and child column lists.
4. Check special ordering for CTE refs, delim joins, and recursive CTEs.
5. Inspect `PRAGMA lpts('...')` output.
6. Run `PRAGMA lpts_check('...')` for strict bag equivalence.
Verbose but correct SQL usually means a missing beautification pass. Incorrect
SQL usually means lost columns, wrong child ordering, wrong alias mapping, or an
unsafe serializer shortcut.
## 3.32 Summary
`AstFlattener` is a one-shot post-order lowering from AST tree to flat CTE list.
It names every intermediate result, preserves column lists, handles special CTE
and correlation scopes, creates the root statement, and stamps dialect settings.
It does not currently run a fixpoint suite of SQL beautification passes.
The guiding invariant is:
```text
Preserve semantics first.
Make dependencies explicit.
Throw on missing binding information.
Use lpts_check to prove bag equivalence.
```
