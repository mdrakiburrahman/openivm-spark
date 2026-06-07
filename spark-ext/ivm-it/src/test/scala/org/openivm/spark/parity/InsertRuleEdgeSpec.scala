package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Slice of `InsertRuleSpec` covering empty-delta edge cases: WHEREs that
  * match nothing on INSERT/DELETE/UPDATE, double-REFRESH no-op behaviour,
  * and INSERT … SELECT from an empty source.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
class InsertRuleEdgeSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-ins-edge-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-InsertRuleEdgeSpec")
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
  // (12) Empty-delta cases (insert_rule.test "Empty 1-7")
  // ──────────────────────────────────────────────────────────────────────────
  describe("(12) Empty-delta cases — operations that produce zero delta rows") {

    it("INSERT … SELECT WHERE that matches nothing leaves the MV unchanged") {
      setupStaff()
      spark.sql("DROP TABLE IF EXISTS candidates_ir")
      spark.sql(
        "CREATE TABLE candidates_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO candidates_ir VALUES (80,'Yes','eng',100),(81,'No','fired',50)"
      )
      val before = spark.table("mv_staff_ir").collect().toSet
      spark.sql("INSERT INTO staff_ir SELECT * FROM candidates_ir WHERE dept = 'nonexistent'")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").collect().toSet shouldBe before
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }

    it("DELETE with a WHERE that matches nothing leaves the MV unchanged") {
      setupStaff()
      val before = spark.table("mv_staff_ir").collect().toSet
      spark.sql("DELETE FROM staff_ir WHERE id = -1")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").collect().toSet shouldBe before
    }

    it("UPDATE with a WHERE that matches nothing leaves the MV unchanged") {
      setupStaff()
      val before = spark.table("mv_staff_ir").collect().toSet
      spark.sql("UPDATE staff_ir SET name = 'ghost' WHERE id = -1")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").collect().toSet shouldBe before
    }

    it("REFRESH with no pending changes is a clean no-op (no error)") {
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

    it("INSERT … SELECT from an empty source leaves the MV unchanged after refresh") {
      setupStaff()
      spark.sql("DROP TABLE IF EXISTS empty_src_ir")
      spark.sql(
        "CREATE TABLE empty_src_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
      )
      val before = spark.table("mv_staff_ir").collect().toSet
      spark.sql("INSERT INTO staff_ir SELECT * FROM empty_src_ir")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").collect().toSet shouldBe before
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }
}
