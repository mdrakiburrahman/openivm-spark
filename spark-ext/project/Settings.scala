import sbt._
import sbt.Keys._
import sbt.Tests.{Group, SubProcess}
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy
import sbtassembly.PathList

object Settings {

  /// Per-class parallel JVM fork for heavy SparkSession tests.
  ///
  /// Each ScalaTest spec runs in its own forked JVM and the JVMs run in
  /// parallel — capped at the `Tags.ForkedTestGroup` limit set in build.sbt.
  /// This delivers two wins simultaneously:
  ///
  ///   1. **Isolation** — Spark's process-wide caches (InMemoryFileIndex,
  ///      Delta snapshot cache, session catalog), Derby in-process locks and
  ///      `~/.ivy2` file locks are all per-JVM, so warehouse deletes in one
  ///      spec's `afterAll` cannot race against another spec still holding a
  ///      cached file path.
  ///
  ///   2. **Throughput** — N independent JVMs saturate the available cores
  ///      (16-way default; tune via `Global / concurrentRestrictions`).
  ///
  /// Cost: ~3-5 s of JVM warmup per class.  Acceptable for the ivm-it module
  /// (58 parity specs) where total wall-time is dominated by Spark session
  /// boot inside each test anyway.
  val parallelForkSettings: Seq[Def.Setting[_]] = Seq(
    Test / fork               := true,
    Test / testForkedParallel := true,
    Test / parallelExecution  := true,
    Test / logBuffered        := false,
    Test / baseDirectory      := (ThisBuild / baseDirectory).value,
    Test / testGrouping := {
      val javaOpts = (Test / javaOptions).value
      val envs     = (Test / envVars).value
      val baseDir  = (Test / baseDirectory).value
      (Test / definedTests).value.map { test =>
        Group(
          name = test.name,
          tests = Seq(test),
          runPolicy = SubProcess(
            ForkOptions()
              .withRunJVMOptions(javaOpts.toVector)
              .withEnvVars(envs)
              .withWorkingDirectory(Some(baseDir))
          )
        )
      }
    }
  )

  /// JVM module-opens / module-exports block — must match the Spark 3.5 + JDK 17
  /// startup options. Mirrored from RESEARCH.md §10. Forwarded to:
  ///   * sbt-launched test JVMs (via Test/javaOptions)
  ///   * sbt-launched assembly fat-jar runs
  ///   * spark-shell / spark-submit invocations (caller must set
  ///     `spark.driver.extraJavaOptions` / `spark.executor.extraJavaOptions`)
  val jvmModuleOpts: Seq[String] = Seq(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
  )

  val commonSettings: Seq[Def.Setting[_]] = Seq(
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xfatal-warnings",
      "-Ywarn-unused:imports",
      "-target:jvm-1.8"
    ),
    Test / fork              := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= jvmModuleOpts ++ Seq(
      "-Xmx4G",
      "-Dhadoop.home.dir=/",
      "-Dfile.encoding=UTF-8",
      // ── Spark / Delta test perf knobs (forwarded to SparkConf via -Dspark.*) ──
      // Each spec runs in its own forked JVM with a fresh SparkSession; these
      // global defaults remove per-commit IO that we do not exercise in tests.
      "-Dspark.sql.shuffle.partitions=1",
      "-Dspark.sql.adaptive.enabled=false",
      "-Dspark.sql.autoBroadcastJoinThreshold=-1",
      "-Dspark.ui.enabled=false",
      "-Dspark.driver.host=localhost",
      "-Dspark.driver.bindAddress=127.0.0.1",
      // Delta: don't materialise checkpoints, don't write history metrics, skip
      // stats collection on small test data.
      "-Dspark.databricks.delta.checkpointInterval=99999999",
      "-Dspark.databricks.delta.history.metricsEnabled=false",
      "-Dspark.databricks.delta.snapshotPartitions=1",
      "-Dspark.databricks.delta.stalenessLimit=0",
      "-Dspark.databricks.delta.commitInfo.userMetadata=",
      "-Dspark.databricks.delta.replaceWhere.constraintCheck.enabled=false",
      "-Dspark.databricks.delta.dataSkipping.numIndexedCols=0",
      "-Dspark.databricks.delta.stats.collect=false",
      "-Dspark.databricks.delta.optimizeWrite.enabled=false",
      "-Dspark.databricks.delta.autoCompact.enabled=false",
      // Plan-string truncation noise — `SparkStringUtils` warns on every truncation.
      // Tests do not care about plan toString output; raise the limit so the warn
      // never fires (belt + suspenders with the log4j2 silencer in
      // `ivm-common/src/test/resources/log4j2.properties`).
      "-Dspark.sql.debug.maxToStringFields=2147483647"
    )
  )

  val assemblySettings: Seq[Def.Setting[_]] = Seq(
    assembly / assemblyJarName := s"${name.value}-${version.value}-assembly.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF")     => MergeStrategy.discard
      case PathList("META-INF", "services", _ @_*) => MergeStrategy.concat
      case PathList("META-INF", _ @_*)             => MergeStrategy.discard
      case "reference.conf"                        => MergeStrategy.concat
      case _                                       => MergeStrategy.first
    },
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("org.duckdb.**" -> "org.openivm.shaded.duckdb.@1").inAll,
      ShadeRule.rename("org.antlr.v4.runtime.**" -> "org.openivm.shaded.antlr.@1").inAll
    ),
    assembly / test := {}
  )
}
