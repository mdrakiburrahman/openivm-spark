# 7. Roundtrip correctness: `lpts_check`
`lpts_check` is the main correctness harness for LPTS.
It is the thing every new operator implementation is expected to satisfy before the generated SQL is trusted.
The short version is:
```sql
PRAGMA lpts_check('<query>');
```
and the expected answer is:
```text
true
```
Source map:
- LPTS extension entry points: `.temp/lpts/src/lpts_extension.cpp:108-232`.
- `PRAGMA lpts_check` implementation: `.temp/lpts/src/lpts_extension.cpp:198-232`.
- Extension option `lpts_dialect`: `.temp/lpts/src/lpts_extension.cpp:286-311`.
- Dialect enum and parser: `.temp/lpts/src/include/sql_dialect.hpp:13-21` and `.temp/lpts/src/lpts_pipeline.cpp:2508-2518`.
- SQLLogicTest corpus: `.temp/lpts/test/sql/*.test`.
- SQLLogicTest rule: `.temp/lpts/test/README.md:13-26`.
- SQLStorm benchmark wrapper: `.temp/lpts/benchmark/sqlstorm/lpts_sqlstorm_benchmark.cpp:659-713`.
- Benchmark CLI: `.temp/lpts/benchmark/sqlstorm/lpts_sqlstorm_benchmark.cpp:1315-1358`.
One important correction up front:
there is no standalone `lpts_check` C++ executable in this checkout.
The checked implementation is a DuckDB PRAGMA named `lpts_check`.
The build produces the DuckDB shell, SQLLogicTest runner, loadable extension, and `lpts_sqlstorm_benchmark`, not an `lpts_check` binary.
The release build products are documented in `.temp/lpts/IMPLEMENTATION.md:19-26`.
The LPTS CMake file only defines the `lpts_sqlstorm_benchmark` native executable at `.temp/lpts/CMakeLists.txt:43-46`.
So when this chapter says "run `lpts_check`", read it as one of these:
```sql
PRAGMA lpts_check('SELECT 1');
```
or:
```bash
build/release/duckdb -unsigned -c "LOAD 'build/release/extension/lpts/lpts.duckdb_extension'; PRAGMA lpts_check('SELECT 1')"
```
or a SQLLogicTest file containing that PRAGMA.
---
## 7.1 The roundtrip principle
LPTS means **Logical Plan To SQL**.
Its job is to turn a DuckDB logical plan back into equivalent SQL.
The conceptual roundtrip can be written as:
```text
user SQL q
  -> DuckDB parse/bind/optimize
  -> logical plan P1
  -> LPTS emits SQL q'
  -> DuckDB parses/executes q'
  -> same query result as q
```
A stricter design ideal is plan roundtrip equality:
```text
parse(q)  -> P1
emit(P1)  -> q'
parse(q') -> P2
assert P1 == P2
```
That ideal is useful for thinking about semantic preservation.
If `q'` parses back to the same logical computation as `q`, LPTS has not lost the meaning of the plan.
But the current `PRAGMA lpts_check` source does **not** implement structural plan comparison.
It implements **extensional result-bag equality**.
The implementation does this:
1. Read the query string argument.
2. Plan and optimize the original query with DuckDB.
3. Convert that optimized plan to the LPTS AST.
4. Flatten the AST to a CTE list.
5. Render a SQL string with `CteList::ToQuery(true)`.
6. Normalize the original SQL to DuckDB's first parsed statement.
7. Compare `(original EXCEPT ALL generated)` and `(generated EXCEPT ALL original)`.
8. Return one boolean column named `match`.
The exact comparison SQL is constructed at `.temp/lpts/src/lpts_extension.cpp:225-231`.
In pseudocode:
```sql
SELECT
  (SELECT count(*) FROM ((orig) EXCEPT ALL (lpts_sql))) = 0
  AND
  (SELECT count(*) FROM ((lpts_sql) EXCEPT ALL (orig))) = 0
  AS match;
```
This matters because LPTS emits SQL for DuckDB's **optimized** plan.
`PlanQuery` explicitly parses, plans, and then runs DuckDB's optimizer before LPTS sees the tree.
That path is in `.temp/lpts/src/lpts_extension.cpp:57-80`.
The source comments name optimizer effects such as CTE inlining, materialized CTEs, statistics propagation, compressed materialization, column lifetime pruning, filter reordering, and join filter pushdown.
So the harness is not asking whether LPTS preserves the user's original spelling.
It is asking whether LPTS preserves the optimized logical computation.
That is the right level for an optimizer-oriented plan-to-SQL tool.
---
## 7.2 What "plan equality" means here
In this codebase, "plan equality" should not be read as byte-for-byte AST equality.
The implemented check is even more semantic than that: it compares query results under bag semantics.
This handles many optimizer-induced shape differences naturally.
For example:
- A filter predicate can be split, pushed into a scan, or reordered.
- A join tree can be reordered by DuckDB if the join semantics allow it.
- A projection can be rewritten with generated CTE column names.
- A CTE can be inlined or materialized.
- A `TopN` operator can replace an `ORDER BY` plus `LIMIT` shape.
- A scalar subquery can be decorrelated into delim joins or mark joins.
None of those require a custom structural comparator in `lpts_check`.
They are tolerated if the final result bag is identical.
The current comparator is therefore:
```text
same column count
same comparable column types
same row multiplicities
same values, including duplicates
```
It is **not**:
```text
same SQL text
same CTE names
same logical operator tree shape
same expression object identity
same original aliases
same original CTE structure
```
`EXCEPT ALL` is the key choice.
Plain `EXCEPT` would compare sets and erase duplicate multiplicities.
`EXCEPT ALL` preserves bag semantics.
That is important because SQL query results are bags by default.
A query that accidentally duplicates rows, drops duplicate rows, or changes multiplicities should fail.
The bidirectional form is also important.
`orig EXCEPT ALL generated` only proves generated output has at least the original rows.
`generated EXCEPT ALL orig` proves it has no extras.
Both must be empty.
### 7.2.1 Projection-column reordering
There is no special "order-insensitive projection comparator" in `lpts_check`.
SQL result schemas are positional.
`EXCEPT ALL` compares rows by column position.
So if LPTS emits the same values in a different final column order, the check can fail or fail to bind.
LPTS handles projections by preserving the output order in generated SQL.
The final read node maps CTE column names back to user-visible output names.
`FinalReadNode::ToQuery` emits `SELECT child_col AS final_col ...` in vector order at `.temp/lpts/src/cte_nodes.cpp:37-57`.
The AST flattener builds the final column list from the root output CTE columns at `.temp/lpts/src/lpts_pipeline.cpp:3177-3201`.
That is why projection ordering bugs usually appear as `lpts_check = false`, parser errors, or column-count errors.
The comparator does not repair the order.
The generator must preserve it.
### 7.2.2 Join-side reordering
There is no hand-written equivalence rule saying "inner join left/right sides may swap".
Instead, `lpts_check` executes both queries.
For commutative inner joins, a different physical join order is acceptable if it produces the same result bag.
LPTS's own join serializer also has to respect output-column order.
`JoinNode::ToQuery` emits an explicit `SELECT` list instead of `SELECT *`, specifically to avoid duplicate join-key surprises.
That code is at `.temp/lpts/src/cte_nodes.cpp:187-242`.
Special cases include:
- `RIGHT_SEMI` and `RIGHT_ANTI` are emitted with the preserved side on the SQL-left side.
- `SINGLE` joins are emitted as `LEFT` joins when the scalar-subquery cardinality assumption holds.
- `MARK` joins are converted to `LEFT` joins plus a computed mark expression.
- Mark-join right sides are deduplicated with `SELECT DISTINCT` to avoid multiplying left rows.
Those rules are generation rules, not comparison rules.
If they are wrong, `EXCEPT ALL` catches the semantic difference.
### 7.2.3 Filter conjunct reordering
There is no structural conjunct sorter in `lpts_check`.
DuckDB may reorder predicates during optimization.
LPTS serializes the filter conditions it sees.
`FilterNode::ToQuery` joins multiple conditions with `AND` and wraps each condition in parentheses at `.temp/lpts/src/cte_nodes.cpp:137-155`.
Because SQL `AND` is commutative for deterministic predicates under normal three-valued logic, reordered conjuncts produce the same result bag.
So the result comparison accepts them.
If a predicate is volatile, order can matter.
Volatile expressions are a limitation of this harness.
### 7.2.4 CTE order and names
CTE names like `scan_0`, `filter_1`, and `projection_2` are implementation details.
They are not part of equality.
The flattener walks AST nodes in post-order, so dependencies appear before consumers.
That behavior is described in `.temp/lpts/src/lpts_pipeline.cpp:2521-2530`.
The generated names may differ after a refactor.
`lpts_check` does not care unless the SQL no longer executes correctly.
### 7.2.5 Aliases
LPTS is not a source-to-source formatter.
It often replaces user aliases with internal stable names such as `t0_name` or `t1_aggregate_0`.
The final read node restores visible column names when the query result is returned.
But alias spelling inside intermediate CTEs is not preserved.
The README states this limitation directly: LPTS does not preserve formatting, alias spelling, or original CTE structure.
See `.temp/lpts/README.md:78-85`.
---
## 7.3 The `lpts_check` entry point
`PRAGMA lpts_check` is registered in the extension loader.
Registration is at `.temp/lpts/src/lpts_extension.cpp:306-311`.
The implementation is `LptsCheckPragmaFunction`.
It starts at `.temp/lpts/src/lpts_extension.cpp:210`.
The signature is one string argument:
```sql
PRAGMA lpts_check('<query>');
```
The output is a single boolean column:
```text
match
-----
true
```
The SQLLogicTest files usually declare it as:
```text
query I
PRAGMA lpts_check('SELECT * FROM t');
----
true
```
Examples are in `.temp/lpts/test/sql/pragmas.test:45-55`.
### 7.3.1 No `--sql` or `--dialect` CLI
There is no checked source file named `src/lpts_check.cpp`.
There is no `make lpts_check` target in the LPTS repository's own CMake file.
There is no CLI parser that accepts `--sql` or `--dialect` for an `lpts_check` executable.
Dialect is controlled as a DuckDB session setting:
```sql
SET lpts_dialect = 'duckdb';
SET lpts_dialect = 'postgres';
SET lpts_dialect = 'spark';
```
The option is registered at `.temp/lpts/src/lpts_extension.cpp:286-292`.
The accepted values are parsed at `.temp/lpts/src/lpts_pipeline.cpp:2508-2518`.
The enum documents the dialect intent at `.temp/lpts/src/include/sql_dialect.hpp:13-17`.
So the CLI equivalent of a dialect run is:
```bash
cd /home/mdrrahman/openivm-spark/.temp/lpts
build/release/duckdb -unsigned -c "\
  LOAD 'build/release/extension/lpts/lpts.duckdb_extension'; \
  SET lpts_dialect='spark'; \
  PRAGMA lpts_check('SELECT 1')"
```
This still executes inside DuckDB.
It checks that LPTS's Spark-dialect SQL remains semantically equivalent in the harness environment.
It is not a full Spark parser or Spark runtime test.
For openivm-spark, Spark parity specs remain the final proof that Spark accepts and executes the emitted SQL.
### 7.3.2 Related entry points
LPTS exposes several adjacent tools:
| Entry point | Use |
| --- | --- |
| `PRAGMA lpts('<query>')` | Return generated CTE SQL. |
| `lpts_query('<query>')` | Table-function form for tests and scripts. |
| `PRAGMA lpts_exec('<query>')` | Generate SQL and execute it. |
| `PRAGMA lpts_check('<query>')` | Compare original and generated SQL with bag equality. |
| `PRAGMA print_ast('<query>')` | Print the AST tree to stdout. |
| `print_ast_query('<query>')` | Table-function form of AST printing. |
These are documented in `.temp/lpts/README.md:44-53` and implemented in `.temp/lpts/src/lpts_extension.cpp:108-232`.
### 7.3.3 SQLStorm benchmark binary
The native executable that does exist is `lpts_sqlstorm_benchmark`.
It runs many SQLStorm TPC-H queries through `PRAGMA lpts_check`.
The runner builds `PRAGMA lpts_check('<escaped sql>')` at `.temp/lpts/benchmark/sqlstorm/lpts_sqlstorm_benchmark.cpp:659`.
It reads the first boolean result at `.temp/lpts/benchmark/sqlstorm/lpts_sqlstorm_benchmark.cpp:693-695`.
If the value is `true`, it records `SUCCESS` and `strict_match`.
If the value is `false`, it classifies the query as nondeterministic or incorrect.
That logic is at `.temp/lpts/benchmark/sqlstorm/lpts_sqlstorm_benchmark.cpp:696-713`.
The benchmark CLI is:
```text
Usage: lpts_sqlstorm_benchmark [options]
Options:
  --queries <dir>    SQLStorm TPC-H queries directory
  --out <csv>        Output CSV path
  --timeout <sec>    Per-query timeout in seconds
  --tpch_sf <float>  TPC-H scale factor
  --compare_perf     Compare DuckDB execution against generated LPTS SQL execution
  -h, --help         Show this help message
```
Source: `.temp/lpts/benchmark/sqlstorm/lpts_sqlstorm_benchmark.cpp:1315-1358`.
The CSV columns include:
```text
query_index,query,duckdb_time_ms,lpts_check_time_ms,state,strict_match,diagnostic_state,diagnostic_reason,phase,error
```
With `--compare_perf`, it adds:
```text
lpts_exec_time_ms,perf_ratio,perf_bucket
```
Source: `.temp/lpts/benchmark/sqlstorm/lpts_sqlstorm_benchmark.cpp:1171-1199`.
---
## 7.4 Test data
The primary regression corpus is under:
```text
/home/mdrrahman/openivm-spark/.temp/lpts/test/sql/
```
In this checkout, there are 24 SQLLogicTest files in that directory.
There are 354 `PRAGMA lpts_check` occurrences across those files.
Representative files include:
| File | Representative coverage |
| --- | --- |
| `select.test` | scans, filters, projections, NULLs, aliases |
| `group_by.test` | grouped aggregates and aggregate edge cases |
| `having.test` | aggregate plus HAVING filters |
| `join.test` | inner, outer, semi, anti, mark, single joins |
| `cross_product.test` | cross joins and Cartesian products |
| `union.test` | `UNION` and `UNION ALL` |
| `setops_unnest.test` | `EXCEPT`, `INTERSECT`, `UNNEST` shapes |
| `order_limit.test` | `ORDER BY`, `LIMIT`, `OFFSET`, `TOP_N` |
| `window.test` | window operators and ranking functions |
| `distinct.test` | `SELECT DISTINCT` and distinct rewrites |
| `functions.test` | scalar functions, arithmetic, table functions, structs |
| `cast.test` | `CAST`, `TRY_CAST`, nested casts |
| `lambda.test` | list lambdas and nested lambda expressions |
| `cte.test` | inlined CTEs, materialized CTEs, recursive CTEs |
| `lateral_join.test` | lateral and delim-join shapes |
| `optimizer.test` | optimizer-produced nodes and pruning edge cases |
| `rendering_edges.test` | fragile SQL rendering regressions |
| `tpch.test` | all 22 TPC-H query shapes at small scale |
| `ducklake.test` | DuckLake table function and snapshot scan shapes |
| `pragmas.test` | `lpts_exec`, `lpts_check`, and input normalization |
The tests follow DuckDB SQLLogicTest syntax.
The test README requires every feature test to include a roundtrip check that returns `true`.
That rule is in `.temp/lpts/test/README.md:13-26`.
A typical minimal test is:
```text
query I
PRAGMA lpts_check('SELECT name FROM users WHERE age > 25');
----
true
```
A few examples worth reading first:
- `.temp/lpts/test/sql/pragmas.test:45-55` for the smallest checks.
- `.temp/lpts/test/sql/cast.test:23-40` for cast preservation.
- `.temp/lpts/test/sql/lambda.test:29-39` for lambda expression preservation.
- `.temp/lpts/test/sql/cte.test:175-243` for recursive CTEs.
- `.temp/lpts/test/sql/rendering_edges.test:18-83` for bug-shaped regressions.
There is also a SQLStorm benchmark corpus path expected by the benchmark:
```text
benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries
```
The benchmark README says the runner executes the SQLStorm TPC-H query set through `PRAGMA lpts_check` and reports success, DuckDB errors, LPTS errors, unsupported cases, timeouts, and incorrect results.
Source: `.temp/lpts/benchmark/README.md:15-36`.
In this checkout that SQLStorm query directory was not populated, but the benchmark code and README are present.
---
## 7.5 Failure modes
A failing `lpts_check` means one of several things.
It does not always mean "the generated SQL is semantically wrong".
The harness is strict and therefore catches both real LPTS bugs and cases outside its proof model.
### 7.5.1 Generated SQL does not parse or bind
The generated SQL may be syntactically invalid.
Or it may reference a CTE column name that was not produced.
Or a CTE header may have a different column count than the body.
Many CTE-node implementations deliberately use explicit column lists to avoid this.
For example, filters and order nodes avoid `SELECT *` when DuckDB's `COLUMN_LIFETIME` optimizer prunes columns.
See `.temp/lpts/src/cte_nodes.cpp:137-155` and `.temp/lpts/src/cte_nodes.cpp:286-295`.
### 7.5.2 Unsupported logical operator or expression
The AST builder has an explicit default failure:
```cpp
throw NotImplementedException("AstBuilder: operator '%s' is not yet implemented", ...)
```
Source: `.temp/lpts/src/lpts_pipeline.cpp:2381-2384`.
The flattener also fails if it receives an AST node with no CTE rendering path.
Source: `.temp/lpts/src/lpts_pipeline.cpp:3151-3152`.
These are good failures.
They mean LPTS refused to silently emit incorrect SQL.
### 7.5.3 Aliasing and naming bugs
Aliases are fragile because DuckDB plans expressions with internal `ColumnBinding` IDs while SQL must reference names.
LPTS uses a column map from bindings to stable CTE names.
The core bookkeeping is introduced in `.temp/lpts/src/lpts_pipeline.cpp:73-90` and stored at `.temp/lpts/src/lpts_pipeline.cpp:185-202`.
Regression tests around reserved identifiers and visible aliases live in `.temp/lpts/test/sql/rendering_edges.test:73-83`.
A common symptom is a generated CTE that refers to an old alias after projection pruning or aggregate remapping.
### 7.5.4 Implicit and explicit casts
DuckDB often inserts casts during binding.
LPTS must serialize those casts in a way that reparses correctly.
The cast corpus includes string-to-integer casts, integer-to-string casts, `TRY_CAST`, nested casts, casts in filters, and NULL casts.
Examples are in `.temp/lpts/test/sql/cast.test:13-168`.
A cast bug can produce either a bind failure or a subtle value mismatch.
### 7.5.5 Lambda-expression preservation
DuckDB list functions can contain lambda expressions.
LPTS must preserve lambda arguments and nested lambda scopes.
The lambda corpus covers `list_transform`, `list_filter`, and nested `list_reduce` / `list_zip` shapes.
Examples are in `.temp/lpts/test/sql/lambda.test:18-147`.
Failure usually means expression serialization lost a lambda variable or captured the wrong scope.
### 7.5.6 Recursive CTE corner cases
Recursive CTE support is special because the recursive step refers to the CTE currently being built.
The AST traversal handles recursive CTEs separately.
It traverses the anchor first, registers the recursive output columns, then traverses the recursive step.
Source: `.temp/lpts/src/lpts_pipeline.cpp:2432-2463`.
The test corpus covers counters, Fibonacci, recursive joins, recursive `TopN`, semi joins, correlated scalar subqueries, `IN` subqueries, and inline values.
Examples are in `.temp/lpts/test/sql/cte.test:175-243`.
Failures here often look like unresolved self-reference columns or wrong recursive CTE column aliases.
### 7.5.7 Nondeterministic result order or values
`lpts_check` compares result bags, not ordered streams.
But some SQL expressions produce nondeterministic values or nondeterministic choices before the bag comparison happens.
The source comments warn that false can happen for:
- unordered `string_agg` or `list` aggregates;
- `row_number()` over tied ordering keys;
- `ORDER BY ... LIMIT` with tied boundary rows.
Source: `.temp/lpts/src/lpts_extension.cpp:204-207` and `.temp/lpts/src/lpts_extension.cpp:306-309`.
The SQLStorm benchmark expands the nondeterminism detector to include:
- unordered `string_agg`;
- unordered `listagg`;
- unordered `list` or `array_agg`;
- `random()`;
- rank functions over potentially tied orderings;
- `lag`, `lead`, `first_value`, `last_value`, `nth_value` over potentially tied orderings;
- `ORDER BY` with `LIMIT`, `OFFSET`, or `FETCH`;
- floating aggregates such as `avg`, `stddev`, and `variance`.
Source: `.temp/lpts/benchmark/sqlstorm/lpts_sqlstorm_benchmark.cpp:337-394`.
These can be real semantic issues or harness limitations.
For tests, add deterministic `ORDER BY` keys inside order-sensitive aggregates and windows.
### 7.5.8 Floating-point evaluation order
Even if two plans are mathematically equivalent, floating-point aggregates can differ because evaluation order changes rounding.
The benchmark treats strict floating aggregate equality as potentially order-dependent.
Source: `.temp/lpts/benchmark/sqlstorm/lpts_sqlstorm_benchmark.cpp:388-392`.
Unit tests should use stable data, explicit casts, or exact types when they are trying to test SQL rendering rather than floating arithmetic.
### 7.5.9 Dialect mismatch
`lpts_check` runs in DuckDB.
Setting `lpts_dialect='spark'` changes how LPTS renders SQL.
It does not move execution into Spark.
So a Spark-dialect `lpts_check` can catch many rendering regressions, but it cannot prove every emitted statement is accepted by Spark 3.5.
For Spark acceptance, run openivm-spark parity tests.
---
## 7.6 Development workflow
### 7.6.1 When adding a new operator to LPTS
Suppose you change `AstBuilder` to handle a new logical operator.
The workflow should be:
1. Add or update the AST node in `src/include/lpts_ast.hpp`.
2. Extract the DuckDB logical operator fields in `LogicalPlanToAst`.
3. Register every output `ColumnBinding` that parents may reference.
4. Add or reuse a CTE node in `src/include/cte_nodes.hpp`.
5. Render the CTE body in `src/cte_nodes.cpp`.
6. Add a SQLLogicTest with `PRAGMA lpts_check('<query>')` returning `true`.
7. Run the focused test file.
8. Run the broader regression corpus before pushing.
The implementation guide gives the same operator-development outline at `.temp/lpts/IMPLEMENTATION.md:165-177`.
Focused run:
```bash
cd /home/mdrrahman/openivm-spark/.temp/lpts
GEN=ninja make
build/release/test/unittest "test/sql/<file>.test"
```
Full SQL test run:
```bash
cd /home/mdrrahman/openivm-spark/.temp/lpts
make unittest
```
Interactive debugging:
```bash
cd /home/mdrrahman/openivm-spark/.temp/lpts
make shell
```
Then inside DuckDB:
```sql
PRAGMA lpts('SELECT ...');
PRAGMA lpts_exec('SELECT ...');
PRAGMA lpts_check('SELECT ...');
```
Use `PRAGMA lpts` to inspect the generated SQL.
Use `PRAGMA lpts_exec` when concrete output rows are easier to reason about.
Use `PRAGMA lpts_check` as the final semantic gate.
### 7.6.2 When changing expression rendering
Expression rendering bugs often show up far from the changed code.
A small cast change can break aggregates.
A lambda fix can break nested list functions.
A quote-ident change can break reserved words or table functions.
Good targeted files are:
```bash
build/release/test/unittest "test/sql/cast.test"
build/release/test/unittest "test/sql/functions.test"
build/release/test/unittest "test/sql/lambda.test"
build/release/test/unittest "test/sql/rendering_edges.test"
```
If all pass, run broader coverage:
```bash
build/release/test/unittest "test/sql/tpch.test"
build/release/test/unittest "test/sql/optimizer.test"
```
### 7.6.3 When validating Spark-dialect output for openivm-spark
The openivm-spark bridge asks OpenIVM to compile refresh SQL using the Spark target dialect.
A useful LPTS-side smoke test is:
```sql
SET lpts_dialect='spark';
PRAGMA lpts('<query>');
PRAGMA lpts_check('<query>');
```
That verifies that the Spark-dialect renderer can roundtrip the query inside the LPTS harness.
For a new openivm-spark parity spec, a practical workflow is:
1. Take the MV body.
2. Reduce it to the smallest DuckDB schema and data that exercises the operator.
3. Run `PRAGMA lpts_check` in DuckDB with the default dialect.
4. Run again with `SET lpts_dialect='spark'`.
5. Use OpenIVM `openivm_compile_with_facts` to see the refresh program.
6. Run the actual openivm-spark Scala parity spec against Spark/Delta.
The Spark-dialect LPTS check is an early warning.
It is not the final Spark contract.
The final contract is Spark parser acceptance plus MV/result bag equality in `ivm-it`.
### 7.6.4 When SQLStorm should run
SQLStorm is a broad stress test, not a tight edit/compile loop.
The LPTS project guidance says the benchmark runs all 17,036 SQLStorm TPC-H queries through `lpts_check` and should be run only when a feature is fully done or before pushing.
Source: `.temp/lpts/CLAUDE.md:23-27`.
Command:
```bash
cd /home/mdrrahman/openivm-spark/.temp/lpts
build/release/extension/lpts/lpts_sqlstorm_benchmark --tpch_sf 0.001 --timeout 10
```
Useful options are documented in `.temp/lpts/benchmark/README.md:25-36`.
---
## 7.7 Integration with OpenIVM
OpenIVM vendors LPTS as source.
In `.temp/openivm/CMakeLists.txt`, `LPTS_DIR` is set to `third_party/lpts` and OpenIVM includes LPTS headers and source files.
Source: `.temp/openivm/CMakeLists.txt:10-11` and `.temp/openivm/CMakeLists.txt:60-63`.
That means OpenIVM's compiler can call the LPTS pipeline directly.
But OpenIVM's own test suite does not appear to invoke `PRAGMA lpts_check` directly.
A grep excluding the vendored `third_party/lpts` tree found zero OpenIVM-owned references to `lpts_check`.
The matches under `.temp/openivm/third_party/lpts/` are the vendored LPTS docs/tests/benchmark, not OpenIVM tests.
OpenIVM correctness tests instead validate materialized-view refresh results.
The OpenIVM project rule is to compare MV output with the base query using `EXCEPT ALL` in both directions.
That is conceptually the same bag-equality oracle as `lpts_check`, applied at the IVM refresh layer rather than at the LPTS plan-to-SQL layer.
So the integration story is:
```text
LPTS tests:       original query  == LPTS-generated SQL
OpenIVM tests:    materialized MV == original view query
openivm-spark:    Spark MV table  == Spark query result
```
All three are bag-equality checks.
They sit at different layers.
---
## 7.8 Running locally
The documented build path is:
```bash
cd /home/mdrrahman/openivm-spark/.temp/lpts
GEN=ninja make
```
Build outputs should include:
```text
build/release/duckdb
build/release/test/unittest
build/release/extension/lpts/lpts.duckdb_extension
build/release/extension/lpts/lpts_sqlstorm_benchmark
```
Source: `.temp/lpts/IMPLEMENTATION.md:19-26`.
There is no expected `build/release/extension/lpts/lpts_check` binary.
### 7.8.1 Run a single SQLLogicTest file
```bash
cd /home/mdrrahman/openivm-spark/.temp/lpts
build/release/test/unittest "test/sql/pragmas.test"
```
The `pragmas.test` file includes basic `lpts_exec` and `lpts_check` checks.
The first `lpts_check` examples are at `.temp/lpts/test/sql/pragmas.test:45-55`.
### 7.8.2 Run manually in the DuckDB shell
```bash
cd /home/mdrrahman/openivm-spark/.temp/lpts
build/release/duckdb -unsigned
```
Then:
```sql
LOAD 'build/release/extension/lpts/lpts.duckdb_extension';
PRAGMA lpts_check('SELECT 1');
```
Expected shape:
```text
┌─────────┐
│  match  │
│ boolean │
├─────────┤
│ true    │
└─────────┘
```
For a table query:
```sql
CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 22), (3, 'Carol', 28);
PRAGMA lpts_check('SELECT name FROM users WHERE age > 25');
```
Expected result:
```text
true
```
The README uses that exact query shape and expected boolean at `.temp/lpts/README.md:34-42`.
### 7.8.3 Run with Spark dialect
```sql
SET lpts_dialect='spark';
PRAGMA lpts('SELECT name FROM users WHERE age > 25');
PRAGMA lpts_check('SELECT name FROM users WHERE age > 25');
```
Remember:
- this changes LPTS rendering;
- it still runs the check inside DuckDB;
- it is a smoke test for Spark-dialect SQL generation;
- Spark integration still needs Spark tests.
### 7.8.4 Run SQLStorm
```bash
cd /home/mdrrahman/openivm-spark/.temp/lpts
build/release/extension/lpts/lpts_sqlstorm_benchmark --tpch_sf 0.001 --timeout 10
```
This reports aggregate counts for success, DuckDB errors, LPTS errors, unsupported queries, timeouts, nondeterminism, and incorrect results.
It can also write a CSV with `--out <csv>`.
---
## 7.9 Limitations
`lpts_check` is high signal, but it is not a proof of everything.
It does not prove that the emitted SQL has the same logical plan shape.
It does not preserve or compare original SQL formatting.
It does not preserve internal alias spelling.
It does not preserve the user's original CTE structure.
It does not compare operator names one by one.
It does not check execution cost.
It does not ensure the generated CTE program is faster.
It does not validate all target dialects in their native engines.
It does not prove Spark accepts Spark-dialect output.
It does not prove PostgreSQL accepts PostgreSQL-dialect output.
It does not protect against nondeterministic functions such as `random()`.
It does not fully handle nondeterministic ordering choices before `LIMIT`.
It does not make unordered `string_agg`, `list`, or `array_agg` deterministic.
It can be sensitive to floating-point aggregate evaluation order.
It requires source tables and functions to exist in the DuckDB session.
It checks only the first parsed statement of the input query.
That first-statement normalization is implemented at `.temp/lpts/src/lpts_extension.cpp:91-98` and used at `.temp/lpts/src/lpts_extension.cpp:219-223`.
It is designed for `SELECT`-style plan-to-SQL correctness, not as a general SQL migration validator.
It is therefore best used as one layer in a stack:
1. `PRAGMA lpts` for generated SQL inspection.
2. `PRAGMA lpts_exec` for concrete result inspection.
3. `PRAGMA lpts_check` for bag equality.
4. SQLLogicTests for regression coverage.
5. SQLStorm for broad stress coverage.
6. OpenIVM refresh tests for materialized-view maintenance.
7. openivm-spark parity tests for Spark/Delta execution.
---
## 7.10 Mental checklist for failures
When `lpts_check` fails, ask these in order:
1. Did the original query execute in DuckDB by itself?
2. Did `PRAGMA lpts('<query>')` produce parseable SQL?
3. Did `PRAGMA lpts_exec('<query>')` fail or return wrong rows?
4. Is the query nondeterministic?
5. Does the query use unordered order-sensitive aggregates?
6. Does it use `ORDER BY ... LIMIT` with tied keys?
7. Does it use floating aggregates where order changes rounding?
8. Did a new operator path forget to register output column bindings?
9. Did a projection/filter/order node expose a different number of columns than its CTE header?
10. Did a join rewrite preserve row multiplicities?
11. Did a recursive CTE self-reference resolve to the recursive CTE's output names?
12. Did dialect rendering introduce syntax that DuckDB cannot parse?
13. Is this an unsupported operator that should throw `NotImplementedException`?
14. Is the test missing deterministic data or deterministic order keys?
The best debugging loop is small:
```sql
PRAGMA lpts('<query>');
PRAGMA lpts_exec('<query>');
PRAGMA lpts_check('<query>');
```
If the generated SQL is wrong, fix the pipeline.
Do not weaken the test.
Do not replace `lpts_check` with a row-count check.
Do not silently demote a real operator to an unsupported path.
`lpts_check` exists so semantic bugs become visible early.
