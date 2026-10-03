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
