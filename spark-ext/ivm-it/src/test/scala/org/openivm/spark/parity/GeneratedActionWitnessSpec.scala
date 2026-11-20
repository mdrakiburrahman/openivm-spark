package org.openivm.spark.parity

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.DataFrame
import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshProfileCatalog, RefreshSqlLogAsyncFlusher}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class GeneratedActionWitnessSpec extends IvmParitySpecBase("generated-action-witness") with InterceptMode {
  private val evidence = Paths.get("/workspaces/openivm-spark/.logs/generated-action-witness")
  Files.createDirectories(evidence.resolve("events"))

  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.openivm.profile.refresh"       -> "true",
    FeatureGate.QueryLogEnabledKey        -> "true",
    FeatureGate.ExplainCaptureKey         -> "true",
    FeatureGate.StateSyncAsyncEnabledKey  -> "false",
    FeatureGate.FuseScratchEnabledKey     -> "false",
    "spark.eventLog.enabled"              -> "true",
    "spark.eventLog.compress"             -> "false",
    "spark.eventLog.dir"                  -> evidence.resolve("events").toUri.toString
  )

  private def write(name: String, data: String): Unit =
    Files.write(evidence.resolve(name), data.getBytes(StandardCharsets.UTF_8))

  private def export(name: String, df: DataFrame): Unit =
    write(name, df.toJSON.collect().mkString("", "\n", "\n"))

  private def sameBag(a: DataFrame, b: DataFrame): Unit = {
    a.exceptAll(b).count() shouldBe 0L
    b.exceptAll(a).count() shouldBe 0L
  }

  it("captures ordinary target insert evidence across distinct changes and replay") {
    RefreshProfileCatalog.ensureTables(spark)
    sql("CREATE TABLE witness_src (id INT, v INT) USING DELTA")
    sql("INSERT INTO witness_src VALUES (1, 10), (2, 20)")
    sql("CREATE MATERIALIZED VIEW witness_mv AS SELECT id, v FROM witness_src WHERE v >= 10")
    val meta = MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier("witness_mv")).get
    write("target.txt", s"$meta\n")
    export("initial-mv.jsonl", spark.table("witness_mv").select("id", "v"))

    def run(label: String, mutation: Option[String]): Unit = {
      val previousMv = spark.table("witness_mv").select("id", "v").cache()
      val previousSource = sql("SELECT id, v FROM witness_src WHERE v >= 10").cache()
      previousMv.count()
      previousSource.count()
      mutation.foreach(sql)
      RefreshProfileCatalog.removeAll(spark)
      sql("REFRESH MATERIALIZED VIEW witness_mv").collect()
      val currentMv = spark.table("witness_mv").select("id", "v").cache()
      val currentSource = sql("SELECT id, v FROM witness_src WHERE v >= 10").cache()
      currentMv.count()
      currentSource.count()
      sameBag(currentMv, currentSource)
      sameBag(currentMv.exceptAll(previousMv), currentSource.exceptAll(previousSource))
      sameBag(previousMv.exceptAll(currentMv), previousSource.exceptAll(currentSource))
      export(s"$label-mv.jsonl", currentMv)
      export(s"$label-source.jsonl", currentSource)
      export(s"$label-added.jsonl", currentMv.exceptAll(previousMv))
      export(s"$label-removed.jsonl", previousMv.exceptAll(currentMv))
      export(s"$label-profile.jsonl", sql("SHOW OPENIVM REFRESH PROFILE"))
      RefreshSqlLogAsyncFlusher.awaitQuiescence(30000) shouldBe true
      export(s"$label-query-log.jsonl", sql("SHOW OPENIVM QUERY LOG"))
      export(s"$label-history.jsonl", sql(s"DESCRIBE HISTORY delta.`${meta.location.replace("`", "``")}`"))
      previousMv.unpersist()
      previousSource.unpersist()
      currentMv.unpersist()
      currentSource.unpersist()
    }

    run("insert-one", Some("INSERT INTO witness_src VALUES (3, 30), (4, 2)"))
    run("insert-two", Some("INSERT INTO witness_src VALUES (5, 50), (6, 8)"))
    run("replay", None)
  }
}
