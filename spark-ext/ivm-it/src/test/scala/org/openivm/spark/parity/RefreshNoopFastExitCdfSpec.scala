package org.openivm.spark.parity

import org.openivm.spark.common.{CdfWatermarkCatalog, DeltaCommitClassifier, FeatureGate, MvCatalog}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import java.io.File
import scala.jdk.CollectionConverters._

class RefreshNoopFastExitCdfSpec extends IvmParitySpecBase("refresh-noop-fast-exit") with CdfMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.NoopFastExitEnabledKey       -> "true",
      FeatureGate.ProfileRefreshKey            -> "true",
      FeatureGate.QueryLogEnabledKey           -> "true",
      FeatureGate.GroupedWindowStateEnabledKey -> "true",
      FeatureGate.StatePathKey                 -> s"$warehouseDir/state",
      FeatureGate.StateSyncUriKey              -> new File(warehouseDir, "remote-state").toURI.toString
    )

  describe("REFRESH NOOP fast exit") {
    it("backs up only telemetry for a profiled no-pending refresh and still publishes the next update") {
      sql("CREATE TABLE rn_checkpoint_input(id INT, name STRING) USING DELTA")
      sql("INSERT INTO rn_checkpoint_input VALUES(1,'Alice'),(2,'Bob')")
      sql("CREATE MATERIALIZED VIEW rn_checkpoint_mv AS SELECT * FROM rn_checkpoint_input")
      val expected = "SELECT * FROM rn_checkpoint_input"
      val before   = mvDataVersion("rn_checkpoint_mv")
      (1 to 2).foreach { _ =>
        refreshMv("rn_checkpoint_mv")
        val entries = uploadedEntries()
        entries should not be empty
        withClue("no-pending refresh must not upload unrelated MV state: ") {
          entries.forall(path =>
            path.startsWith("refresh_profile/") || path.startsWith("refresh_sql_log/")
          ) shouldBe true
        }
        mvDataVersion("rn_checkpoint_mv") shouldBe before
        assertMvCorrect("rn_checkpoint_mv", expected)
      }
      sql("INSERT INTO rn_checkpoint_input VALUES(3,'Carol')")
      refreshMv("rn_checkpoint_mv")
      uploadedEntries().exists(path =>
        !path.startsWith("refresh_profile/") && !path.startsWith("refresh_sql_log/")
      ) shouldBe true
      assertMvCorrect("rn_checkpoint_mv", expected)
    }

    it("preserves grouped-window state through mixed updates and telemetry-only checkpoints") {
      sql("CREATE TABLE rn_group_input(k BIGINT,v BIGINT) USING DELTA")
      sql("INSERT INTO rn_group_input VALUES(1,7),(1,7),(2,8),(3,NULL)")
      val query = "WITH grouped AS (SELECT k,COUNT(*) AS n,COUNT(DISTINCT v) AS d,SUM(v) AS s " +
        "FROM rn_group_input GROUP BY k) SELECT k,n,d,s,DENSE_RANK() OVER(ORDER BY n DESC) AS rank FROM grouped"
      sql(s"CREATE MATERIALIZED VIEW rn_grouped AS $query")
      MvCatalog
        .lookup(spark, TableIdentifier("rn_grouped"))
        .get
        .properties
        .contains("_ivm_grouped_window_input_v1") shouldBe true
      assertMvCorrect("rn_grouped", query)
      sql("INSERT INTO rn_group_input VALUES(1,9),(4,10)")
      sql("DELETE FROM rn_group_input WHERE k=3")
      refreshMv("rn_grouped")
      assertMvCorrect("rn_grouped", query)
      refreshMv("rn_grouped")
      uploadedEntries().forall(path =>
        path.startsWith("refresh_profile/") || path.startsWith("refresh_sql_log/")
      ) shouldBe true
      sql("INSERT INTO rn_group_input VALUES(2,11)")
      refreshMv("rn_grouped")
      assertMvCorrect("rn_grouped", query)
    }

    it("leaves the MV unchanged, advances the CDF watermark, and still processes the next INSERT") {
      sql("CREATE TABLE IF NOT EXISTS rn_users(id INT, name STRING) USING DELTA")
      sql("INSERT INTO rn_users VALUES (1, 'Alice'), (2, 'Bob')")
      sql("CREATE MATERIALIZED VIEW rn_mv AS SELECT id, name FROM rn_users")

      val meta = MvCatalog
        .lookup(spark, TableIdentifier("rn_mv"))
        .getOrElse(fail("rn_mv metadata missing"))
      val source = meta.sourceTables.head

      val beforeRows      = mvRows()
      val beforeMvVersion = DeltaCommitClassifier.latestVersion(spark, meta.location)

      sql("ALTER TABLE rn_users SET TBLPROPERTIES ('openivm.noop.marker' = 'v1')")
      val noopSourceVersion = DeltaCommitClassifier.latestVersion(spark, source)
      refreshMv("rn_mv")

      mvRows() shouldBe beforeRows
      DeltaCommitClassifier.latestVersion(spark, meta.location) shouldBe beforeMvVersion
      CdfWatermarkCatalog.get(spark, "rn_mv", source) shouldBe Some(noopSourceVersion)

      sql("INSERT INTO rn_users VALUES (3, 'Carol')")
      val insertSourceVersion = DeltaCommitClassifier.latestVersion(spark, source)
      refreshMv("rn_mv")

      assertMvCorrect("rn_mv", "SELECT id, name FROM rn_users")
      CdfWatermarkCatalog.get(spark, "rn_mv", source) shouldBe Some(insertSourceVersion)
    }
  }

  private def uploadedEntries(): Set[String] = {
    val snapshots = new File(warehouseDir, "remote-state/.openivm-state-sync/snapshots")
    val latest    = snapshots.listFiles().filter(_.isDirectory).maxBy(_.getName)
    val zip       = new java.util.zip.ZipFile(new File(latest, "snapshot.zip"))
    try zip.entries().asScala.map(_.getName).toSet
    finally zip.close()
  }

  private def mvRows(): Seq[Row] =
    spark.table("rn_mv").select("id", "name").orderBy("id").collect().toSeq
}
