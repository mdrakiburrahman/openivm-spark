package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy carve-out of `FullOuterJoinSpec.scala` §(1) — the emp_projects FULL
  * OUTER JOIN projection walk through matched / unmatched-left /
  * unmatched-right rows under repeated DML (~4m44).  Lives in its own forked
  * JVM so the rest of the parity suite is not blocked by this monster test.
  *
  * Table / MV names are prefixed `foj_heavy_outer_` to guarantee no
  * Delta-path collision with the host spec or any other parallel forked JVM.
  */
class FullOuterJoinHeavyOuterSidesSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-foj-heavy-outer-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-FullOuterJoinHeavyOuterSidesSpec")
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

  /** Bidirectional EXCEPT ALL equivalence check.
    *
    * Projects the MV to the same column set as `expected` first to drop any
    * hidden bookkeeping columns (e.g. `openivm_count_star`, `openivm_match_count`).
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
  // (1) Basic FULL OUTER JOIN projection
  //     openivm/test/sql/full_outer_join.test L26–L269
  // ============================================================================

  describe("(1) FULL OUTER JOIN projection: employees ⟗ projects") {
    it("maintains matched / unmatched-left / unmatched-right rows through repeated DML") {
      spark.sql("CREATE TABLE IF NOT EXISTS foj_heavy_outer_employees(id INT, name STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS foj_heavy_outer_projects(id INT, emp_id INT, title STRING) USING DELTA")
      spark.sql("INSERT INTO foj_heavy_outer_employees VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      spark.sql("INSERT INTO foj_heavy_outer_projects VALUES (10, 1, 'Alpha'), (20, 1, 'Beta'), (30, 4, 'Gamma')")

      spark.sql(
        "CREATE MATERIALIZED VIEW foj_heavy_outer_emp_projects AS " +
          "SELECT e.name, p.title " +
          "FROM foj_heavy_outer_employees e FULL OUTER JOIN foj_heavy_outer_projects p ON e.id = p.emp_id"
      )

      val viewBody =
        "SELECT e.name, p.title " +
          "FROM foj_heavy_outer_employees e FULL OUTER JOIN foj_heavy_outer_projects p ON e.id = p.emp_id"

      // Initial: Alice matched (2 projects), Bob/Charlie unmatched-left, Gamma unmatched-right
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Insert into right side: Bob gets a project (unmatched-left → matched)
      spark.sql("INSERT INTO foj_heavy_outer_projects VALUES (40, 2, 'Delta')")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Insert into right side with no match: new unmatched-right row
      spark.sql("INSERT INTO foj_heavy_outer_projects VALUES (50, 99, 'Epsilon')")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Delete from right side: remove Alpha (matched pair removed; Alice still has Beta)
      spark.sql("DELETE FROM foj_heavy_outer_projects WHERE id = 10")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Delete from right side: remove Beta — Alice becomes unmatched-left (Alice, NULL)
      spark.sql("DELETE FROM foj_heavy_outer_projects WHERE id = 20")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Insert into left: new employee with no project (unmatched-left)
      spark.sql("INSERT INTO foj_heavy_outer_employees VALUES (5, 'Eve')")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Delete unmatched-right: remove Gamma (emp_id=4, no matching employee)
      spark.sql("DELETE FROM foj_heavy_outer_projects WHERE id = 30")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Stress: batch mixed DML on both sides before a single refresh
      spark.sql("INSERT INTO foj_heavy_outer_employees VALUES (6, 'Frank')")
      spark.sql("INSERT INTO foj_heavy_outer_projects VALUES (60, 6, 'Zeta'), (70, 5, 'Eta'), (80, 100, 'Theta')")
      spark.sql("DELETE FROM foj_heavy_outer_employees WHERE id = 2")
      spark.sql("DELETE FROM foj_heavy_outer_projects WHERE id = 50")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)
    }
  }
}
