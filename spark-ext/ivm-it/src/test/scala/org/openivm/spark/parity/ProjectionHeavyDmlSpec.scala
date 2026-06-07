package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy carve-out of `ProjectionSpec.scala` §(1) — the emp_names pure
  * projection walk through insert / delete / no-op / batch / mixed-DML rounds
  * (~3m42).  Lives in its own forked JVM so the rest of the parity suite is
  * not blocked by this monster test.
  *
  * Table / MV names are prefixed `proj_heavy_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
class ProjectionHeavyDmlSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-proj-heavy-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ProjectionHeavyDmlSpec")
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

  /** Bidirectional `EXCEPT ALL` equivalence check.
    *
    * Projects the MV to the same columns as `expectedSql` before comparing so
    * any internal `openivm_*` bookkeeping columns on the physical data table
    * are stripped from the comparison.
    */
  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    withClue(s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  // ============================================================================
  // (1) Pure projection: SELECT id, name FROM employees
  //     openivm projection.test:1-90  — emp_names walk-through
  // ============================================================================
  describe("(1) Pure projection: SELECT id, name FROM employees") {
    it("incremental refresh propagates insert, delete, no-op, batch, and mixed DML") {
      spark.sql("CREATE TABLE IF NOT EXISTS proj_heavy_employees(id INT, name STRING, dept STRING) USING DELTA")
      spark.sql("INSERT INTO proj_heavy_employees VALUES (1, 'Alice', 'eng'), (2, 'Bob', 'sales')")
      spark.sql("CREATE MATERIALIZED VIEW proj_heavy_emp_names AS SELECT id, name FROM proj_heavy_employees")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")

      // Insert a new employee
      spark.sql("INSERT INTO proj_heavy_employees VALUES (3, 'Charlie', 'eng')")
      refreshMv("proj_heavy_emp_names")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")

      // Delete an employee
      spark.sql("DELETE FROM proj_heavy_employees WHERE id = 1")
      refreshMv("proj_heavy_emp_names")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")

      // No-op refresh
      refreshMv("proj_heavy_emp_names")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")

      // Batch insert
      spark.sql("INSERT INTO proj_heavy_employees VALUES (4, 'Diana', 'eng'), (5, 'Eve', 'sales')")
      refreshMv("proj_heavy_emp_names")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")

      // Mixed INSERT + DELETE before a single refresh
      spark.sql("INSERT INTO proj_heavy_employees VALUES (6, 'Frank', 'ops')")
      spark.sql("DELETE FROM proj_heavy_employees WHERE id = 2")
      refreshMv("proj_heavy_emp_names")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")
    }
  }
}
