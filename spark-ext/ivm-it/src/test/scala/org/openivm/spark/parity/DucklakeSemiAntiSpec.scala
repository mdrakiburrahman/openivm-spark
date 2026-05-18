package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P6e — ScalaTest port of `openivm/test/sql/ducklake_semi_anti.test`.
  *
  * In openivm the DuckLake SEMI/ANTI shape with `WHERE col IN (subq)`
  * classifies as `SEMI_ANTI_RECOMPUTE` (type 9) because the parser registers
  * an aux-meta row at CREATE time.  On openivm-spark, per
  * `SemiAntiSpec.scala` documentation, the compile-only bridge does not yet
  * persist that aux-meta and the view is demoted to `FULL_REFRESH` (rt3).
  * The MV is still correct because every refresh re-executes the live view
  * body over the current source snapshot — exactly the Delta-equivalent
  * invariant called out in PLAN.md §9.
  *
  * This spec mirrors the single section of `ducklake_semi_anti.test`:
  *   - 2-table SEMI shape with `WHERE c_id IN (subq)`
  *   - Multi-op batch: INSERT customer, INSERT orders, UPDATE order amount
  *     across the predicate threshold, UPDATE last_name (predicate-neutral),
  *     DELETE order, DELETE customer.
  *   - Bidirectional EXCEPT ALL verifies bag equality after a single refresh.
  */
class DucklakeSemiAntiSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-dlsa-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DucklakeSemiAntiSpec")
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

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

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

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  // ── 2-table SEMI shape — IN (subquery with WHERE predicate) ───────────────

  describe("DuckLake SEMI-style view: WHERE c_id IN (SELECT o_c_id FROM orders WHERE o_ol_cnt > 3)") {
    it("multi-op batch (INSERT/UPDATE/DELETE on both sides) refreshes to a bag-equal MV") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlsa_customer(c_id INT, c_w_id INT, c_last STRING) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlsa_order(o_id INT, o_c_id INT, o_ol_cnt INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO dlsa_customer VALUES (1, 1, 'alpha'), (2, 1, 'beta'), (3, 1, 'gamma')"
      )
      spark.sql("INSERT INTO dlsa_order VALUES (10, 1, 5), (11, 2, 2)")

      val viewSql =
        "SELECT c.c_w_id, c.c_id, c.c_last " +
          "FROM dlsa_customer c " +
          "WHERE c.c_id IN (SELECT o.o_c_id FROM dlsa_order o WHERE o.o_ol_cnt > 3)"
      spark.sql(s"CREATE MATERIALIZED VIEW mv_dlsa_in AS $viewSql")
      assertMvCorrect("mv_dlsa_in", viewSql)

      // Multi-op batch — covers every interesting transition for SEMI semantics:
      //   - INSERT new customer (id 4) that the new order makes match
      //   - INSERT new order (12) above threshold for customer 4 → match
      //   - INSERT new order (13) above threshold for customer 3 → match
      //   - UPDATE order 11 above threshold → customer 2 now matches
      //   - UPDATE customer 3 last name (predicate-neutral, projection-affecting)
      //   - DELETE order 10 → customer 1 stops matching
      //   - DELETE customer 1 → row drops anyway
      spark.sql("INSERT INTO dlsa_customer VALUES (4, 1, 'delta')")
      spark.sql("INSERT INTO dlsa_order VALUES (12, 4, 6), (13, 3, 8)")
      spark.sql("UPDATE dlsa_order SET o_ol_cnt = 7 WHERE o_id = 11")
      spark.sql("UPDATE dlsa_customer SET c_last = 'gamma-x' WHERE c_id = 3")
      spark.sql("DELETE FROM dlsa_order WHERE o_id = 10")
      spark.sql("DELETE FROM dlsa_customer WHERE c_id = 1")

      refreshMv("mv_dlsa_in")
      assertMvCorrect("mv_dlsa_in", viewSql)
    }
  }
}
