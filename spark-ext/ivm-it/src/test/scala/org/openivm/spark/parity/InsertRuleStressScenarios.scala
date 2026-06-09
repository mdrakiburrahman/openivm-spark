package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.StagingCatalog

/** Slice of `InsertRuleSpec` covering stress / mixed-DML scenarios:
  * mixed INSERT+DELETE+UPDATE before a single REFRESH, INSERT-then-DELETE
  * net-zero deltas, rapid-fire INSERT+DELETE alternation, and batched
  * mixed DML on a filtered MV that exercises predicate-flip behaviour.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
abstract class InsertRuleStressScenarios extends IvmParitySpecBase("insert-rule-stress") {
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
  // (7) Mixed INSERT + DELETE + UPDATE without REFRESH between
  //      (insert_rule.test Tests 13 & "Mixed INSERT + DELETE + UPDATE")
  // ──────────────────────────────────────────────────────────────────────────
  describe("(7) Mixed INSERT + DELETE + UPDATE before a single REFRESH") {

    itIntercept("INSERT + DELETE + UPDATE in one window, then REFRESH, keeps bag-equality") {
      setupStaff()
      sql("INSERT INTO staff_ir VALUES (20, 'Mixed', 'eng', 999)")
      sql("DELETE FROM staff_ir WHERE id = 2")
      sql("UPDATE staff_ir SET salary = 111 WHERE id = 1")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (9) Stress: INSERT then immediately DELETE same row
  //      (insert_rule.test Stress 3)
  // ──────────────────────────────────────────────────────────────────────────
  describe("(9) Stress: INSERT then DELETE same row before REFRESH (net delta = 0)") {

    itIntercept("net effect is zero; MV unchanged after refresh") {
      setupStaff()
      val before = spark.table("mv_staff_ir").collect().toSet
      sql("INSERT INTO staff_ir VALUES (50, 'Ghost', 'eng', 0)")
      sql("DELETE FROM staff_ir WHERE id = 50")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").collect().toSet shouldBe before
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (11) Stress: rapid-fire INSERT + DELETE alternating
  //      (insert_rule.test Stress 8)
  // ──────────────────────────────────────────────────────────────────────────
  describe("(11) Stress: rapid-fire INSERT + DELETE alternating, single REFRESH") {

    itIntercept("batched alternating INSERT/DELETE collapses correctly under one REFRESH") {
      setupStaff()
      val before = spark.table("mv_staff_ir").collect().toSet
      sql("INSERT INTO staff_ir VALUES (70, 'A', 'eng', 1)")
      sql("INSERT INTO staff_ir VALUES (71, 'B', 'eng', 2)")
      sql("DELETE FROM staff_ir WHERE id = 70")
      sql("INSERT INTO staff_ir VALUES (72, 'C', 'eng', 3)")
      sql("DELETE FROM staff_ir WHERE id = 71")
      sql("DELETE FROM staff_ir WHERE id = 72")
      refreshMv("mv_staff_ir")
      // All three new rows ended up deleted — MV equals its starting state.
      spark.table("mv_staff_ir").collect().toSet shouldBe before
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (15) Batched mixed DML on an MV with a filtered SELECT body
  //      (analogous to the broader insert_rule "Mixed INSERT + DELETE + UPDATE"
  //      section, but applied through a non-trivial WHERE in the MV body to
  //      stress predicate-flip behaviour in the staging→refresh pipeline).
  // ──────────────────────────────────────────────────────────────────────────
  describe("(15) Batched mixed DML on a filtered MV") {

    itIntercept("INSERT/DELETE/UPDATE all preserve the predicate after a single REFRESH") {
      sql("DROP TABLE IF EXISTS staff_filt_ir")
      sql(
        "CREATE TABLE staff_filt_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
      )
      sql(
        "INSERT INTO staff_filt_ir VALUES " +
          "(1,'A','eng',100),(2,'B','eng',50),(3,'C','sales',200)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_staff_filt_ir AS " +
          "SELECT id, name, dept, salary FROM staff_filt_ir WHERE salary > 75"
      )

      // Batched DML: some rows flip in, some flip out.
      sql(
        "INSERT INTO staff_filt_ir VALUES (4,'D','eng',80),(5,'E','sales',30),(6,'F','eng',300)"
      )
      sql("DELETE FROM staff_filt_ir WHERE id = 3")
      // UPDATEs: id=1 stays in (100→120), id=2 stays out (50→55), id=4 flips
      // out (80→70).
      sql("UPDATE staff_filt_ir SET salary = 120 WHERE id = 1")
      sql("UPDATE staff_filt_ir SET salary = 55 WHERE id = 2")
      sql("UPDATE staff_filt_ir SET salary = 70 WHERE id = 4")
      refreshMv("mv_staff_filt_ir")
      assertMvCorrect(
        "mv_staff_filt_ir",
        "SELECT id, name, dept, salary FROM staff_filt_ir WHERE salary > 75"
      )
    }
  }
}
