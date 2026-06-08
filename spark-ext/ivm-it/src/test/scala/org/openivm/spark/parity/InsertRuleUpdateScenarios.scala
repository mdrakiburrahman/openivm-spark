package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.StagingCatalog

import java.util.UUID

/** Slice of `InsertRuleSpec` covering UPDATE: constant SET value (with
  * staging UPDATE_BEFORE / UPDATE_AFTER inspection), double-UPDATE
  * collapse, and multi-column SET propagation.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
abstract class InsertRuleUpdateScenarios extends IvmParitySpecBase("insert-rule-update") {
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

  protected def stagingRows(tbl: String) =
    StagingCatalog.collectFor(spark, s"__probe_${UUID.randomUUID().toString.take(6)}__", Seq(qualName(tbl)))

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
  // (6) UPDATE with constant SET value
  // ──────────────────────────────────────────────────────────────────────────
  describe("(6) UPDATE — constant SET value") {

    itIntercept("UPDATE writes UPDATE_BEFORE + UPDATE_AFTER staging rows and propagates correctly") {
      setupStaff()
      sql("UPDATE staff_ir SET name = 'Alice2' WHERE id = 1")

      val staged = stagingRows("staff_ir")
      staged.exists(_.opType == "UPDATE_BEFORE") shouldBe true
      staged.exists(_.opType == "UPDATE_AFTER") shouldBe true

      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (10) Stress: UPDATE same row twice before REFRESH
  //      (insert_rule.test Stress 4)
  // ──────────────────────────────────────────────────────────────────────────
  describe("(10) Stress: UPDATE same row twice before REFRESH") {

    itIntercept("the second UPDATE's value wins; MV reflects the final state") {
      setupStaff()
      sql("UPDATE staff_ir SET name = 'FirstUpdate' WHERE id = 1")
      sql("UPDATE staff_ir SET name = 'SecondUpdate' WHERE id = 1")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
      spark
        .sql("SELECT name FROM mv_staff_ir WHERE id = 1")
        .collect()
        .head
        .getAs[String](0) shouldBe "SecondUpdate"
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (14) UPDATE multiple columns at once (insert_rule.test Complex 6)
  // ──────────────────────────────────────────────────────────────────────────
  describe("(14) UPDATE multiple columns at once") {

    itIntercept("multi-column SET updates all three columns and propagates to the MV") {
      setupStaff()
      sql("INSERT INTO staff_ir VALUES (130, 'Multi', 'eng', 500)")
      refreshMv("mv_staff_ir")
      sql(
        "UPDATE staff_ir SET name = 'Updated', dept = 'sales', salary = 999 WHERE id = 130"
      )
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")

      val row = sql("SELECT id, name, dept, salary FROM mv_staff_ir WHERE id = 130").collect().head
      row.getAs[String]("name") shouldBe "Updated"
      row.getAs[String]("dept") shouldBe "sales"
      row.getAs[Int]("salary") shouldBe 999
    }
  }
}
