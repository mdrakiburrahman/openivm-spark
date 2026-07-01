package org.openivm.spark.parity

import org.openivm.spark.common.FeatureGate
import org.openivm.spark.parity.base.IvmParitySpecBase

abstract class RelyFkScenarios extends IvmParitySpecBase("rely-fk") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.DeclareRelyFkEnabledKey -> "true")

  describe("RELY FK constraints-cache declaration") {
    it("keeps child-delta/full-parent refresh correct when both sides receive matching inserts") {
      sql("CREATE TABLE relyfk_parent(id INT, name STRING) USING DELTA")
      sql(
        "CREATE TABLE relyfk_child(id INT, parent_id INT, amount INT) USING DELTA " +
          "TBLPROPERTIES ('spark.openivm.fk.parent_id' = 'relyfk_parent(id)')"
      )
      sql("INSERT INTO relyfk_parent VALUES (1, 'one')")
      sql("INSERT INTO relyfk_child VALUES (10, 1, 100)")

      sql(
        "CREATE MATERIALIZED VIEW relyfk_mv AS " +
          "SELECT c.id, p.name, c.amount FROM relyfk_child c JOIN relyfk_parent p ON c.parent_id = p.id"
      )
      assertMvCorrect(
        "relyfk_mv",
        "SELECT c.id, p.name, c.amount FROM relyfk_child c JOIN relyfk_parent p ON c.parent_id = p.id"
      )

      sql("INSERT INTO relyfk_parent VALUES (2, 'two')")
      sql("INSERT INTO relyfk_child VALUES (20, 2, 200)")
      refreshMv("relyfk_mv")

      assertMvCorrect(
        "relyfk_mv",
        "SELECT c.id, p.name, c.amount FROM relyfk_child c JOIN relyfk_parent p ON c.parent_id = p.id"
      )
    }
  }
}
