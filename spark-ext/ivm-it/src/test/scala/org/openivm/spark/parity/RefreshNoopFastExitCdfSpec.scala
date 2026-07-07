package org.openivm.spark.parity

import org.openivm.spark.common.{CdfWatermarkCatalog, DeltaCommitClassifier, FeatureGate, MvCatalog}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier

class RefreshNoopFastExitCdfSpec extends IvmParitySpecBase("refresh-noop-fast-exit") with CdfMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.NoopFastExitEnabledKey -> "true")

  describe("REFRESH NOOP fast exit") {
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

  private def mvRows(): Seq[Row] =
    spark.table("rn_mv").select("id", "name").orderBy("id").collect().toSeq
}
