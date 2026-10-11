# openivm-spark

## Crash-safe state-sync snapshots

CREATE/REFRESH still wait synchronously for remote state durability. Concurrent
post-commit backup requests share a full pass only when queued before that pass's
request barrier; later arrivals need another pass. A failed batch fails its
waiters, and a later explicit request can retry.

State-sync now writes complete immutable generations under
`.openivm-state-sync/snapshots/` and publishes a small, closed completion record
via same-filesystem rename under `.openivm-state-sync/commits/`. The filesystem
must provide atomic file rename (for example HDFS or hierarchical-namespace ABFS).
Only committed generations are restored. Interrupted uploads cannot truncate
the previous generation's CURRENT/manifest files. Flat legacy snapshots remain
readable but are not overwritten; existing legacy corruption is not repaired.
Restore failures fail closed instead of being treated as empty state.
Old and incomplete generations are intentionally retained; this change does not
introduce automatic remote retention or shared-file garbage collection. Every
generation copies the complete state tree, including immutable SST files. Copies\nuse at most 16 daemon workers JVM-wide, with bounded batches; all non-CURRENT\nfiles finish before CURRENT files, and every task finishes before validation\nand commit. Copy errors fail the pass without publishing the generation.\nA fair namespace pass lock prevents an asynchronous follow-up from overtaking\na synchronous request that is already waiting.

## Native namespace session isolation

CREATE/REFRESH session clones retain inherited current-catalog/database state
without revalidating unchanged namespaces through public catalog setters. This
preserves Fabric internal database names while still propagating non-default
V2 catalogs and multipart namespaces when their cloned state differs.

## Targeted streaming status polling

`SHOW STREAMING TABLES [IN namespace] [LIKE 'pattern']` returns the existing nine status fields.
LIKE uses Spark SHOW patterns (case-insensitive, `*` wildcard and `|` alternatives) on the unqualified table name, not the backtick-qualified `table_name` result. For a result identity `` `catalog`.`schema`.`table_name` ``, use `IN catalog.schema LIKE 'table_name'`; retain the fully qualified identity when validating the returned row.
Candidates are filtered before Delta target resolution, manifest reads and ownership validation; unmatched targets incur no per-target status reads.
Poll a specific table with `SHOW STREAMING TABLES IN catalog.schema LIKE 'table_name'`; keep checking identity fields, failures, and `is_active` until the query has actually stopped.
Omitting LIKE retains namespace enumeration and validation. Filtering does not cache ownership metadata or bypass completion waiting.

Spark SQL extension delivering **OpenIVM incremental view maintenance** without Delta CDF.

> Status: under active development

## Runtime support

| Target      | Spark | Delta | Scala   | Java | Maven artifact                |
| ----------- | ----- | ----- | ------- | ---- | ----------------------------- |
| `spark-3.5` | 3.5.1 | 3.2.0 | 2.12.17 | 17   | `ivmextension-spark-3.5_2.12` |
| `spark-4.1` | 4.1.0 | 4.2.0 | 2.13.17 | 21   | `ivmextension-spark-4.1_2.13` |

Spark 3.5 remains the default for every developer command that omits `--target`.

## Documentation

- [Materialized views](docs/MATERIALIZED_VIEWS.md)
- [Standalone streaming tables](docs/STREAMING_TABLES.md)
- [Completed execution telemetry](docs/EXECUTION_TELEMETRY.md)
- [Request-scoped SQL insights](docs/SQL_INSIGHTS.md)
- [Request-scoped SQL log export](docs/QUERY_LOG_EXPORT.md)
- [Activation](docs/ACTIVATION.md)
- [Development](docs/DEVELOPMENT.md)

## License

See [LICENSE](../LICENSE).

### STREAM over catalog views

`STREAM(view_name)` resolves the stored view through Catalyst under its recorded
catalog/namespace, then reads each physical Delta leaf as a stream. Nested and
qualified views preserve output casts, column aliases, projections, branch-local
filters, and `UNION ALL` multiplicity; repeated leaves remain separate source
occurrences in their original order. Relation-local `WITH` reader options apply
to every leaf within that STREAM boundary. Other relations remain batch reads.

Non-Delta/constant leaves, subquery expressions, and reader options that change
the stored input schema (for example enabling CDF on an ordinary Delta view) fail
explicitly. Spark's normal streaming operator/output-mode checks still apply:
wrapping a view does not make arbitrary batch SQL streamable.

Expanded plans, Delta table identities, schemas, and reader options participate
in the normal definition fingerprint. Recreating a base table or changing view
semantics/leaf ordering requires the normal rejection or `onQueryChange=rebuild`
policy. Restarts reuse the managed target checkpoint; no view-specific checkpoint
or offset rewrite is introduced.
