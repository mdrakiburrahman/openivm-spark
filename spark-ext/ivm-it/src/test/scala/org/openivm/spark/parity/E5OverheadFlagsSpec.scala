package org.openivm.spark.parity

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class E5OverheadFlagsSpec extends IvmParitySpecBase("e5-overhead-flags") with InterceptMode {
  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.RefreshProgramCacheEnabledKey    -> "true",
      FeatureGate.RefreshStatementFusionEnabledKey -> "true",
      FeatureGate.RefreshSiblingParallelEnabledKey -> "true"
    )

  describe("E5 overhead-reduction flags") {
    it("preserve output, refresh type, and cache schema scoping with every E5 flag enabled") {
      sql("CREATE TABLE IF NOT EXISTS e5_users(id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO e5_users VALUES (1, 'Alice', 30), (2, 'Bob', 20)")
      sql("CREATE MATERIALIZED VIEW e5_mv AS SELECT id, name FROM e5_users WHERE age >= 25")

      val id       = TableIdentifier("e5_mv")
      val meta0    = MvCatalog.lookup(spark, id).get
      val fp0      = meta0.sourceSchemaFingerprint
      val type0    = meta0.refreshType
      val typeName = meta0.refreshTypeName
      type0 should not be RefreshTypeCode.FullRefresh

      sql("INSERT INTO e5_users VALUES (3, 'Carol', 35)")
      refreshMv("e5_mv")
      assertMvCorrect("e5_mv", "SELECT id, name FROM e5_users WHERE age >= 25")

      val meta1 = MvCatalog.lookup(spark, id).get
      meta1.refreshType shouldBe type0
      meta1.refreshTypeName shouldBe typeName

      sql("INSERT INTO e5_users VALUES (4, 'Dave', 40)")
      refreshMv("e5_mv")
      assertMvCorrect("e5_mv", "SELECT id, name FROM e5_users WHERE age >= 25")

      val meta2 = MvCatalog.lookup(spark, id).get
      meta2.sourceSchemaFingerprint shouldBe fp0
      meta2.refreshType shouldBe type0
      meta2.refreshTypeName shouldBe typeName
    }
  }
}
