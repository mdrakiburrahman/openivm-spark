# openivm-spark

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
