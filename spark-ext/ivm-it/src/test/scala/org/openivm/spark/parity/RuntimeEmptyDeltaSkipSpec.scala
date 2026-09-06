package org.openivm.spark.parity

import org.openivm.spark.common.{DeltaCommitClassifier, FeatureGate, MvCatalog, StagingCatalog, StagingDelta}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier

import java.sql.Timestamp
import java.util.UUID

class RuntimeEmptyDeltaSkipSpec extends IvmParitySpecBase("runtime-empty-delta-skip") with InterceptMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.RuntimeEmptyDeltaSkipEnabledKey -> "true")

  describe("runtime empty-delta refresh skip") {
    it("consumes an actually empty staging batch without writing a new MV version") {
      sql("CREATE TABLE IF NOT EXISTS rteds_users(id INT, name STRING) USING DELTA")
      sql("INSERT INTO rteds_users VALUES (1, 'Alice'), (2, 'Bob')")
      sql("CREATE MATERIALIZED VIEW rteds_mv AS SELECT id, name FROM rteds_users")

      val meta = MvCatalog
        .lookup(spark, TableIdentifier("rteds_mv"))
        .getOrElse(fail("rteds_mv metadata missing"))
      val source          = meta.sourceTables.head
      val beforeRows      = mvRows()
      val beforeMvVersion = DeltaCommitClassifier.latestVersion(spark, meta.location)
      val emptyPath       = s"$warehouseDir/rteds-empty-stage-${UUID.randomUUID().toString}"

      spark.table("rteds_users").limit(0).write.format("delta").mode("overwrite").save(emptyPath)
      StagingCatalog.record(
        spark,
        StagingDelta(
          baseTable = source,
          opType = StagingDelta.OpTypes.Insert,
          stagingPath = emptyPath,
          txnTs = new Timestamp(System.currentTimeMillis()),
          consumedBy = Seq.empty
        )
      )
      StagingCatalog.collectFor(spark, "rteds_mv", Seq(source)).nonEmpty shouldBe true

      refreshMv("rteds_mv")

      mvRows() shouldBe beforeRows
      assertMvCorrect("rteds_mv", "SELECT id, name FROM rteds_users")
      DeltaCommitClassifier.latestVersion(spark, meta.location) shouldBe beforeMvVersion
      StagingCatalog.collectFor(spark, "rteds_mv", Seq(source)).isEmpty shouldBe true

      sql("INSERT INTO rteds_users VALUES (3, 'Carol')")
      refreshMv("rteds_mv")
      assertMvCorrect("rteds_mv", "SELECT id, name FROM rteds_users")
    }
  }

  private def mvRows(): Seq[Row] =
    spark.table("rteds_mv").select("id", "name").orderBy("id").collect().toSeq
}
