package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.StagingCatalog

/** Slice of `InsertRuleSpec` covering empty-delta edge cases: WHEREs that
  * match nothing on INSERT/DELETE/UPDATE, double-REFRESH no-op behaviour,
  * and INSERT … SELECT from an empty source.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
abstract class InsertRuleEdgeScenarios extends IvmParitySpecBase("insert-rule-edge") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def qualName(tbl: String): String = {
    import org.apache.spark.sql.execution.datasources.LogicalRelation
    val a = sql(s"SELECT * FROM $tbl").queryExecution.analyzed
    a.collectFirst {
      case r: LogicalRelation if r.catalogTable.isDefined =>
        val id = r.catalogTable.get.identifier
        id.database.fold(id.table)(db => s"$db.${id.table}")
    }.getOrElse(tbl)
  }

  protected def setupStaff(): String = {
    sql("DROP MATERIALIZED VIEW IF EXISTS mv_staff_ir")
    sql("DROP TABLE IF EXISTS staff_ir")
    sql(
      "CREATE TABLE staff_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
    )
    val qn        = qualName("staff_ir")
    val staleRows = StagingCatalog.collectFor(spark, "__cleanup_probe__", Seq(qn))
    if (staleRows.nonEmpty) {
      StagingCatalog.markConsumed(spark, "__cleanup_mv__", staleRows.map(_.stagingPath))
      StagingCatalog.pruneFullyConsumed(spark, Map(qn -> Seq("__cleanup_mv__")))
    }

    sql("INSERT INTO staff_ir VALUES (1,'Alice','eng',100),(2,'Bob','sales',200)")
    sql("CREATE MATERIALIZED VIEW mv_staff_ir AS SELECT id, name, dept, salary FROM staff_ir")
    val seedStaging = StagingCatalog.collectFor(spark, "mv_staff_ir", Seq(qn))
    if (seedStaging.nonEmpty) {
      StagingCatalog.markConsumed(spark, "mv_staff_ir", seedStaging.map(_.stagingPath))
    }
    qn
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (12) Empty-delta cases (insert_rule.test "Empty 1-7")
  // ──────────────────────────────────────────────────────────────────────────
  describe("(12) Empty-delta cases — operations that produce zero delta rows") {

    itIntercept("INSERT … SELECT WHERE that matches nothing leaves the MV unchanged") {
      setupStaff()
      sql("DROP TABLE IF EXISTS candidates_ir")
      sql(
        "CREATE TABLE candidates_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
      )
      sql(
        "INSERT INTO candidates_ir VALUES (80,'Yes','eng',100),(81,'No','fired',50)"
      )
      val before = spark.table("mv_staff_ir").collect().toSet
      sql("INSERT INTO staff_ir SELECT * FROM candidates_ir WHERE dept = 'nonexistent'")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").collect().toSet shouldBe before
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }

    itIntercept("DELETE with a WHERE that matches nothing leaves the MV unchanged") {
      setupStaff()
      val before = spark.table("mv_staff_ir").collect().toSet
      sql("DELETE FROM staff_ir WHERE id = -1")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").collect().toSet shouldBe before
    }

    itIntercept("UPDATE with a WHERE that matches nothing leaves the MV unchanged") {
      setupStaff()
      val before = spark.table("mv_staff_ir").collect().toSet
      sql("UPDATE staff_ir SET name = 'ghost' WHERE id = -1")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").collect().toSet shouldBe before
    }

    itIntercept("REFRESH with no pending changes is a clean no-op (no error)") {
      setupStaff()
      val before = spark.table("mv_staff_ir").collect().toSet
      refreshMv("mv_staff_ir")
      refreshMv("mv_staff_ir")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").collect().toSet shouldBe before
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (16) Empty-source INSERT … SELECT and double-no-op REFRESH
  //      (insert_rule.test Empty 5-7)
  // ──────────────────────────────────────────────────────────────────────────
  describe("(16) INSERT … SELECT from an empty source") {

    itIntercept("INSERT … SELECT from an empty source leaves the MV unchanged after refresh") {
      setupStaff()
      sql("DROP TABLE IF EXISTS empty_src_ir")
      sql(
        "CREATE TABLE empty_src_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
      )
      val before = spark.table("mv_staff_ir").collect().toSet
      sql("INSERT INTO staff_ir SELECT * FROM empty_src_ir")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").collect().toSet shouldBe before
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }
}
