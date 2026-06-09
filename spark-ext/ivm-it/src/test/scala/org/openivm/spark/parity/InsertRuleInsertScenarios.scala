package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.StagingCatalog

import java.util.UUID

/** Slice of `InsertRuleSpec` covering INSERT paths: single-row VALUES,
  * multi-row VALUES batches, scalar-expression VALUES, INSERT … SELECT
  * from a non-tracked source, and INSERTs that exercise NULLs / quote
  * escapes / empty strings / negative numbers.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
abstract class InsertRuleInsertScenarios extends IvmParitySpecBase("insert-rule-insert") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Qualified-name of `tbl` as the DML interceptor sees it. */
  protected def qualName(tbl: String): String = {
    import org.apache.spark.sql.execution.datasources.LogicalRelation
    val a = sql(s"SELECT * FROM $tbl").queryExecution.analyzed
    a.collectFirst {
      case r: LogicalRelation if r.catalogTable.isDefined =>
        val id = r.catalogTable.get.identifier
        id.database.fold(id.table)(db => s"$db.${id.table}")
    }.getOrElse(tbl)
  }

  /** Returns every staging row for `tbl` that has not yet been consumed by
    * a unique probe view name (so the function returns ALL pending rows).
    */
  protected def stagingRows(tbl: String) =
    StagingCatalog.collectFor(spark, s"__probe_${UUID.randomUUID().toString.take(6)}__", Seq(qualName(tbl)))

  /** Build the shared staff + mv_staff scenario. Each call wipes the table
    * + MV completely so tests start from a clean slate.
    *
    * Returns the qualified base-table name.
    */
  protected def setupStaff(): String = {
    sql("DROP MATERIALIZED VIEW IF EXISTS mv_staff_ir")
    sql("DROP TABLE IF EXISTS staff_ir")
    sql(
      "CREATE TABLE staff_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
    )
    val qn = qualName("staff_ir")
    // Clear any leftover staging rows for this base table from a previous test.
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
  // (1) INSERT VALUES — single row
  // ──────────────────────────────────────────────────────────────────────────
  describe("(1) INSERT VALUES — single row") {

    itIntercept("a single-row INSERT writes one INSERT staging entry; refresh keeps bag-equality") {
      val qn = setupStaff()

      sql("INSERT INTO staff_ir VALUES (3, 'Carol', 'eng', 150)")

      // Staging entry recorded.
      val staged = stagingRows("staff_ir").filter(_.opType == "INSERT")
      staged should not be empty
      staged.head.baseTable shouldBe qn

      refreshMv("mv_staff_ir")
      assertMvCorrect(
        "mv_staff_ir",
        "SELECT id, name, dept, salary FROM staff_ir"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (2) INSERT VALUES — multi-row batch
  // ──────────────────────────────────────────────────────────────────────────
  describe("(2) INSERT VALUES — multi-row batch") {

    itIntercept("a 2-row INSERT VALUES propagates both rows in one REFRESH") {
      setupStaff()
      sql(
        "INSERT INTO staff_ir VALUES (4, 'Dave', 'sales', 250), (5, 'Eve', 'eng', 300)"
      )
      refreshMv("mv_staff_ir")
      assertMvCorrect(
        "mv_staff_ir",
        "SELECT id, name, dept, salary FROM staff_ir"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (3) INSERT VALUES with computed scalar expressions
  // ──────────────────────────────────────────────────────────────────────────
  describe("(3) INSERT VALUES with computed scalar expressions") {

    itIntercept("INSERT with concat/upper/arithmetic expressions stages correctly and refreshes") {
      setupStaff()
      sql(
        "INSERT INTO staff_ir VALUES (8, concat('Heidi', ''), upper('eng'), 300 + 25)"
      )
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (4) INSERT … SELECT from another table
  // ──────────────────────────────────────────────────────────────────────────
  describe("(4) INSERT … SELECT (subquery path)") {

    itIntercept("INSERT...SELECT from a non-tracked source propagates rows after refresh") {
      setupStaff()
      sql("DROP TABLE IF EXISTS temp_hires_ir")
      sql(
        "CREATE TABLE temp_hires_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
      )
      sql(
        "INSERT INTO temp_hires_ir VALUES (6,'Frank','eng',175),(7,'Grace','sales',225)"
      )

      sql("INSERT INTO staff_ir SELECT * FROM temp_hires_ir")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (8) INSERT with NULLs and special characters
  // ──────────────────────────────────────────────────────────────────────────
  describe("(8) INSERT with NULL values and special characters") {

    itIntercept("INSERTs containing NULLs propagate correctly through staging") {
      setupStaff()
      sql("INSERT INTO staff_ir VALUES (30, NULL, NULL, NULL)")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }

    itIntercept("INSERTs with single-quote-escaped strings survive the staging round trip") {
      setupStaff()
      sql("INSERT INTO staff_ir VALUES (31, \"O'Brien\", 'eng', 100)")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }

    itIntercept("INSERT empty string and zero values round-trip cleanly") {
      setupStaff()
      sql("INSERT INTO staff_ir VALUES (40, '', '', 0)")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }

    itIntercept("INSERT negative numbers round-trip cleanly") {
      setupStaff()
      sql("INSERT INTO staff_ir VALUES (41, 'Negative', 'eng', -999)")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }
}
