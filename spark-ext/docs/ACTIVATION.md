# Activation

## spark-shell / spark-submit

```bash
spark-shell \
    --jars spark-ext/ivm-extension/target/spark-3.5-antlr-4.9.3/scala-2.12/ivmExtension-0.1.0-SNAPSHOT-assembly.jar \
    --conf spark.sql.extensions=org.openivm.spark.OpenIvmSparkExtensions \
    --conf spark.openivm.enabled=true \
    --conf spark.driver.extraJavaOptions="$(cat .sbtopts | grep -oE '^-J.*' | sed 's/^-J//' | xargs)"
```

The feature gate (`spark.openivm.enabled`) defaults to false, so the jar is
opt-in even when on the classpath.

## JVM module-opens block (JDK 17)

The extension and its tests require Spark's standard JDK-17 `--add-opens` /
`--add-exports` flags:

```text
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
```

These live in `spark-ext/.sbtopts` for sbt-launched JVMs and must be replicated
in `spark.driver.extraJavaOptions` / `spark.executor.extraJavaOptions` when
running spark-shell / spark-submit.
