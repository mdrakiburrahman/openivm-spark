# Spark extension

## Crash-safe state-sync snapshots

Synchronous backups retain per-namespace request barriers: only requests captured
before a pass receive its outcome. Later requests require a follow-up pass;
covered failures propagate and a later explicit request can retry. Fair pass
admission prevents asynchronous work overtaking waiting synchronous requests.

New generations pack the complete state tree into one immutable `snapshot.zip`
under `.openivm-state-sync/snapshots/<generation>/`. Packing is local; a stable
generation requires identical path/size/mtime signatures before packing, after
packing, and after remote upload. Rotation or mutation retries up to 20 passes.
Each stable generation uses one data-object upload regardless of file count.
No request succeeds before upload/close, validation and atomic commit publication.

The closed `OPENIVM_STATE_SNAPSHOT_V2` completion record contains generation,
archive length and SHA256; same-filesystem rename publishes it under
`.openivm-state-sync/commits/`. Restore verifies archive length/checksum and bounded
safe ZIP entries/CRC in an isolated local staging directory before atomically
publishing local state. Corrupt committed snapshots fail closed, without falling
back to older or empty state. Incomplete generations are ignored.

V1 committed tree snapshots and legacy flat snapshots remain readable. Older
readers cannot read V2: downgrades must not silently reuse V2 state. Generations
are retained; this change adds no garbage collection. Archives are bounded to
100,000 entries and 64 GiB compressed/expanded bytes; links and unsafe paths are
rejected.

## Generated pattern-match dialect

The Spark SQL generation boundary translates DuckDB pattern operators `~~`,
`!~~`, `~~*` and `!~~*` to LIKE, NOT LIKE, ILIKE and NOT ILIKE. String literals,
quoted identifiers and comments are protected; source business SQL is unchanged.

## Native streaming status lookup diagnostics

Targeted SHOW analyzes the exact target without suppressing native catalog/storage
exceptions. A known-owned target whose lookup fails remains a failure row:
`last_failure` includes the bounded, redacted exception/cause chain. Ownership is
not assumed from registry state; no path fallback, retry or successful-completion
substitution is performed.

## Managed initial CTAS namespaces

When `spark.openivm.managedTablesRoot` is set, initial CREATE data writes use the
logical qualified target identifier plus LOCATION, not a `delta.<path>` CTAS
identifier. This keeps Fabric auto-discovery in the target's real namespace.
Non-managed deployments retain concurrent path CTAS. Ownership, table identity,
precommit cleanup, committed-path reuse and subsequent registration are unchanged.

## Native namespace session isolation

CREATE/REFRESH session clones retain inherited current-catalog/database state
without revalidating unchanged namespaces through public catalog setters. This
preserves Fabric internal database names while still propagating non-default
V2 catalogs and multipart namespaces when their cloned state differs.

## Targeted streaming status polling

`SHOW STREAMING TABLES [IN namespace] [LIKE 'pattern']` returns the existing nine status fields.
LIKE uses Spark SHOW patterns (case-insensitive, `*` wildcard and `|` alternatives) on the unqualified table name, not the backtick-qualified `table_name` result. For a result identity `` `catalog`.`schema`.`table_name` ``, use `IN catalog.schema LIKE 'table_name'`; retain the fully qualified identity when validating the returned row.
Simple literal LIKE names (`[A-Za-z0-9_]+`) resolve the one qualified target\ndirectly without `SHOW TABLES`/namespace enumeration. Wildcard/regex patterns\nand unfiltered SHOW retain catalog enumeration. Selected targets still undergo\nDelta resolution, manifest reads and ownership validation; registered missing\ntargets still report metadata errors.
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
