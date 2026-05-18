package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Slice of `InsertRuleSpec` covering stress / mixed-DML scenarios:
  * mixed INSERT+DELETE+UPDATE before a single REFRESH, INSERT-then-DELETE
  * net-zero deltas, rapid-fire INSERT+DELETE alternation, and batched
  * mixed DML on a filtered MV that exercises predicate-flip behaviour.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
class InsertRuleStressSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-ins-stress-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-InsertRuleStressSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
    }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    withClue(s"$mvName EXCEPT ALL <expected>: ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName: ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  private def qualName(tbl: String): String = {
    import org.apache.spark.sql.execution.datasources.LogicalRelation
    val a = spark.sql(s"SELECT * FROM $tbl").queryExecution.analyzed
    a.collectFirst {
      case r: LogicalRelation if r.catalogTable.isDefined =>
        val id = r.catalogTable.get.identifier
        id.database.fold(id.table)(db => s"$db.${id.table}")
    }.getOrElse(tbl)
  }

  private def setupStaff(): String = {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_staff_ir")
    spark.sql("DROP TABLE IF EXISTS staff_ir")
    spark.sql(
      "CREATE TABLE staff_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
    )
    val qn        = qualName("staff_ir")
    val staleRows = StagingCatalog.collectFor(spark, "__cleanup_probe__", Seq(qn))
    if (staleRows.nonEmpty) {
      StagingCatalog.markConsumed(spark, "__cleanup_mv__", staleRows.map(_.stagingPath))
      StagingCatalog.pruneFullyConsumed(spark, Map(qn -> Seq("__cleanup_mv__")))
    }

    spark.sql("INSERT INTO staff_ir VALUES (1,'Alice','eng',100),(2,'Bob','sales',200)")
    spark.sql("CREATE MATERIALIZED VIEW mv_staff_ir AS SELECT id, name, dept, salary FROM staff_ir")
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

    it("INSERT + DELETE + UPDATE in one window, then REFRESH, keeps bag-equality") {
      setupStaff()
      spark.sql("INSERT INTO staff_ir VALUES (20, 'Mixed', 'eng', 999)")
      spark.sql("DELETE FROM staff_ir WHERE id = 2")
      spark.sql("UPDATE staff_ir SET salary = 111 WHERE id = 1")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (9) Stress: INSERT then immediately DELETE same row
  //      (insert_rule.test Stress 3)
  // ──────────────────────────────────────────────────────────────────────────
  describe("(9) Stress: INSERT then DELETE same row before REFRESH (net delta = 0)") {

    it("net effect is zero; MV unchanged after refresh") {
      setupStaff()
      val before = spark.table("mv_staff_ir").collect().toSet
      spark.sql("INSERT INTO staff_ir VALUES (50, 'Ghost', 'eng', 0)")
      spark.sql("DELETE FROM staff_ir WHERE id = 50")
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

    it("batched alternating INSERT/DELETE collapses correctly under one REFRESH") {
      setupStaff()
      val before = spark.table("mv_staff_ir").collect().toSet
      spark.sql("INSERT INTO staff_ir VALUES (70, 'A', 'eng', 1)")
      spark.sql("INSERT INTO staff_ir VALUES (71, 'B', 'eng', 2)")
      spark.sql("DELETE FROM staff_ir WHERE id = 70")
      spark.sql("INSERT INTO staff_ir VALUES (72, 'C', 'eng', 3)")
      spark.sql("DELETE FROM staff_ir WHERE id = 71")
      spark.sql("DELETE FROM staff_ir WHERE id = 72")
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

    it("INSERT/DELETE/UPDATE all preserve the predicate after a single REFRESH") {
      spark.sql("DROP TABLE IF EXISTS staff_filt_ir")
      spark.sql(
        "CREATE TABLE staff_filt_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO staff_filt_ir VALUES " +
          "(1,'A','eng',100),(2,'B','eng',50),(3,'C','sales',200)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_staff_filt_ir AS " +
          "SELECT id, name, dept, salary FROM staff_filt_ir WHERE salary > 75"
      )

      // Batched DML: some rows flip in, some flip out.
      spark.sql(
        "INSERT INTO staff_filt_ir VALUES (4,'D','eng',80),(5,'E','sales',30),(6,'F','eng',300)"
      )
      spark.sql("DELETE FROM staff_filt_ir WHERE id = 3")
      // UPDATEs: id=1 stays in (100→120), id=2 stays out (50→55), id=4 flips
      // out (80→70).
      spark.sql("UPDATE staff_filt_ir SET salary = 120 WHERE id = 1")
      spark.sql("UPDATE staff_filt_ir SET salary = 55 WHERE id = 2")
      spark.sql("UPDATE staff_filt_ir SET salary = 70 WHERE id = 4")
      refreshMv("mv_staff_filt_ir")
      assertMvCorrect(
        "mv_staff_filt_ir",
        "SELECT id, name, dept, salary FROM staff_filt_ir WHERE salary > 75"
      )
    }
  }
}
