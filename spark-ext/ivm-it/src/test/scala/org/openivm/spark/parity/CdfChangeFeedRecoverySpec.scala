package org.openivm.spark.parity

import java.io.File

import org.apache.spark.sql.delta.DeltaLog
import org.openivm.spark.common.{
  CdfWatermarkCatalog,
  ChangeFeedMode,
  ChangeWatermark,
  DeltaCommitClassifier,
  FeatureGate,
  MvCatalog,
  MvMetadata,
  RefreshProfileCatalog
}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

/** CDF-mode refreshes whose change feed cannot be applied on top of the view: a source with no
  * consumed version, and change rows Delta no longer holds. Each must recompute the view from the
  * current source snapshots, re-baseline the source, and then resume incremental maintenance.
  */
class CdfChangeFeedRecoverySpec extends IvmParitySpecBase("cdf-change-feed-recovery") with CdfMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.ProfileRefreshKey -> "true")

  private def meta(name: String): MvMetadata =
    MvCatalog
      .lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(name))
      .getOrElse(fail(s"MV $name not found in catalog"))

  private def body(src: String): String =
    s"SELECT k, SUM(v) AS total, COUNT(*) AS n FROM $src GROUP BY k"

  private def latestVersion(source: String): Long =
    DeltaCommitClassifier.latestVersion(spark, source)

  /** Refreshes `mv` and returns the refresh profile it recorded. */
  private def refreshProfile(mv: String): String = {
    RefreshProfileCatalog.removeAll(spark)
    refreshMv(mv)
    RefreshProfileCatalog.scanAll(spark).map(row => s"${row.stepName}:${row.detail}").mkString("\n")
  }

  /** Asserts a refresh recomputed `mv` for `reason`, matches its query, and re-baselined `src`. */
  private def assertRebaselined(src: String, mv: String, reason: String): Unit = {
    val profile = refreshProfile(mv)
    profile should include("outcome=full_refresh_executed")
    profile should include(reason)
    assertMvCorrect(mv, body(src))
    val source = meta(mv).sourceTables.head
    CdfWatermarkCatalog.get(spark, mv, source) shouldBe Some(latestVersion(source))
  }

  /** Asserts the refresh after inserting `rows` into `src` is incremental and exact. */
  private def assertIncrementalResumes(src: String, mv: String, rows: String): Unit = {
    sql(s"INSERT INTO $src VALUES $rows")
    val profile = refreshProfile(mv)
    profile should include("outcome=incremental_executed")
    assertMvCorrect(mv, body(src))
    val source = meta(mv).sourceTables.head
    CdfWatermarkCatalog.get(spark, mv, source) shouldBe Some(latestVersion(source))
  }

  private def cdfReadFails(source: String, startingVersion: Long): Boolean =
    scala.util
      .Try(
        spark.read
          .format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", startingVersion)
          .table(source)
          .collect()
      )
      .isFailure

  private def tableDir(name: String): File =
    new File(
      spark.sessionState.catalog.getTableMetadata(spark.sessionState.sqlParser.parseTableIdentifier(name)).location
    )

  describe("a source with no consumed CDF version") {

    it("recomputes and re-baselines a view created under intercept mode instead of replaying the change feed") {
      val src = "cfr_wm_src"
      val mv  = "cfr_wm_mv"
      val persistentCatalog = extraSparkConf ++ Map(
        "spark.sql.catalogImplementation" -> "hive",
        "spark.hadoop.javax.jdo.option.ConnectionURL" ->
          s"jdbc:derby:;databaseName=$warehouseDir/metastore;create=true"
      )
      try {
        restartSpark(persistentCatalog + (FeatureGate.ChangeFeedModeKey -> ChangeFeedMode.Intercept.value))
        sql(s"CREATE TABLE $src (k INT, v BIGINT) USING DELTA")
        sql(s"INSERT INTO $src VALUES (1, 10), (2, 20)")
        sql(s"INSERT INTO $src VALUES (1, 5)")
        sql(s"CREATE MATERIALIZED VIEW $mv AS ${body(src)}")

        restartSpark(persistentCatalog)
        val source = meta(mv).sourceTables.head
        meta(mv).changeWatermarks.get(source).collect { case version: ChangeWatermark.DeltaVersion =>
          version
        } shouldBe None
        CdfWatermarkCatalog.get(spark, mv, source) shouldBe None

        sql(s"INSERT INTO $src VALUES (2, 7), (3, 30)")
        assertRebaselined(src, mv, "cdf_watermark_missing")
        assertIncrementalResumes(src, mv, "(3, 3), (4, 40)")
      } finally restartSpark(extraSparkConf)
    }

    it("records the CREATE watermark of a view created in CDF mode, so its first refresh stays incremental") {
      val src = "cfr_new_src"
      val mv  = "cfr_new_mv"
      sql(s"CREATE TABLE $src (k INT, v BIGINT) USING DELTA")
      sql(s"INSERT INTO $src VALUES (1, 10), (2, 20)")
      sql(s"CREATE MATERIALIZED VIEW $mv AS ${body(src)}")

      val source = meta(mv).sourceTables.head
      meta(mv).changeWatermarks.get(source) shouldBe Some(ChangeWatermark.DeltaVersion(latestVersion(source)))
      assertIncrementalResumes(src, mv, "(1, 1), (3, 30)")
    }
  }

  describe("change rows Delta no longer holds") {

    Seq(false, true).foreach { deletionVectors =>
      it(s"recomputes and re-baselines after VACUUM removed change data (deletion vectors: $deletionVectors)") {
        val src = s"cfr_vacuum_${if (deletionVectors) "dv" else "files"}_src"
        val mv  = s"cfr_vacuum_${if (deletionVectors) "dv" else "files"}_mv"
        sql(
          s"CREATE TABLE $src (k INT, v BIGINT) USING DELTA " +
            s"TBLPROPERTIES ('delta.enableDeletionVectors' = '$deletionVectors')"
        )
        sql(s"INSERT INTO $src VALUES (1, 10), (2, 20), (3, 30)")
        sql(s"CREATE MATERIALIZED VIEW $mv AS ${body(src)}")
        val consumed = latestVersion(meta(mv).sourceTables.head)

        sql(s"UPDATE $src SET v = v + 1 WHERE k = 1")
        sql(s"DELETE FROM $src WHERE k = 2")
        sql(s"INSERT INTO $src VALUES (4, 40)")
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        try sql(s"VACUUM $src RETAIN 0 HOURS")
        finally spark.conf.unset("spark.databricks.delta.retentionDurationCheck.enabled")

        val unreadable = cdfReadFails(src, consumed + 1L)
        if (!deletionVectors) unreadable shouldBe true
        if (unreadable) assertRebaselined(src, mv, "cdf_change_data_unavailable")
        else {
          refreshProfile(mv) should include("outcome=incremental_executed")
          assertMvCorrect(mv, body(src))
        }
        assertIncrementalResumes(src, mv, "(5, 50)")
      }
    }

    it("recomputes and re-baselines after log cleanup removed the commits a refresh needs") {
      val src = "cfr_log_src"
      val mv  = "cfr_log_mv"
      sql(s"CREATE TABLE $src (k INT, v BIGINT) USING DELTA TBLPROPERTIES ('delta.checkpointInterval' = '2')")
      sql(s"INSERT INTO $src VALUES (1, 10), (2, 20)")
      sql(s"CREATE MATERIALIZED VIEW $mv AS ${body(src)}")
      sql(s"INSERT INTO $src VALUES (1, 1)")
      sql(s"INSERT INTO $src VALUES (3, 30)")
      sql(s"INSERT INTO $src VALUES (2, 2)")

      val latest = latestVersion(meta(mv).sourceTables.head)
      latest % 2L shouldBe 0L
      val logDir    = new File(tableDir(src), "_delta_log")
      val Versioned = """^(\d{20})\..*""".r
      val expired = logDir.listFiles().toSeq.filter { file =>
        file.getName match {
          case Versioned(version) => version.toLong < latest
          case _                  => false
        }
      }
      expired should not be empty
      expired.foreach(file => file.delete() shouldBe true)
      DeltaLog.clearCache()

      assertRebaselined(src, mv, "cdf_change_data_unavailable")
      assertIncrementalResumes(src, mv, "(5, 50)")
    }
  }
}
