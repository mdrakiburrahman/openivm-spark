package org.openivm.spark.parity

import org.openivm.spark.common.FeatureGate
import org.openivm.spark.parity.base.IvmParitySpecBase

/** Verifies the OpenIVM Delta deletion-vector MV data-table knob and keeps a
  * retracting SIMPLE_PROJECTION refresh on the deletion-vector-backed path.
  */
abstract class DeletionVectorsScenarios extends IvmParitySpecBase("deletion-vectors") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def deltaProperties(tableName: String): Map[String, String] = {
    val escaped = tableName.replace("`", "``")
    val props = spark
      .sql(s"DESCRIBE DETAIL `$escaped`")
      .select("properties")
      .head()
      .getAs[Map[String, String]]("properties")
    Option(props).getOrElse(Map.empty)
  }

  protected def deletionVectorsProperty(tableName: String): Option[String] =
    deltaProperties(tableName).get("delta.enableDeletionVectors")

  describe("Delta deletion-vector TBLPROPERTIES for MV data tables") {

    it("sets delta.enableDeletionVectors=true when the OpenIVM flag is ON") {
      restartSpark()
      sql("CREATE TABLE dv_users_on(id INT, name STRING) USING DELTA")
      sql("INSERT INTO dv_users_on VALUES (1, 'Alice')")
      sql("CREATE MATERIALIZED VIEW dv_mv_on AS SELECT id, name FROM dv_users_on")

      deletionVectorsProperty("dv_mv_on") should contain("true")
      assertMvCorrect("dv_mv_on", "SELECT id, name FROM dv_users_on")
    }

    it("omits delta.enableDeletionVectors when the OpenIVM flag is OFF") {
      restartSpark(Map(FeatureGate.DeltaEnableDeletionVectorsKey -> "false"))
      sql("CREATE TABLE dv_users_off(id INT, name STRING) USING DELTA")
      sql("INSERT INTO dv_users_off VALUES (1, 'Alice')")
      sql("CREATE MATERIALIZED VIEW dv_mv_off AS SELECT id, name FROM dv_users_off")

      deletionVectorsProperty("dv_mv_off").map(_.toLowerCase(java.util.Locale.ROOT)).contains("true") shouldBe false
      assertMvCorrect("dv_mv_off", "SELECT id, name FROM dv_users_off")
    }

    it("maintains a deletion-vector-backed SIMPLE_PROJECTION MV through retracting DML") {
      restartSpark()
      sql("CREATE TABLE dv_users_retract(id INT, name STRING) USING DELTA")
      sql("INSERT INTO dv_users_retract VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
      sql("CREATE MATERIALIZED VIEW dv_mv_retract AS SELECT id, name FROM dv_users_retract")

      deletionVectorsProperty("dv_mv_retract") should contain("true")

      sql("INSERT INTO dv_users_retract VALUES (4, 'Dave')")
      sql("DELETE FROM dv_users_retract WHERE id = 2")
      refreshMv("dv_mv_retract")

      assertMvCorrect("dv_mv_retract", "SELECT id, name FROM dv_users_retract")
    }
  }
}
