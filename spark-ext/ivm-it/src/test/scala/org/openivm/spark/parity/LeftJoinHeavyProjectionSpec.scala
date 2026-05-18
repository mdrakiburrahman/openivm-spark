package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy-test isolation spin-off of [[LeftJoinSpec]] section (1).
  *
  * Extracted into its own spec class so that `Test/testGrouping` runs it in a
  * dedicated forked JVM (per `Settings.parallelForkSettings`), shrinking the
  * crowded host spec's wall-clock.
  *
  * Table / MV names inside this spec are prefixed `ljh_*` / `ljh_mv_*` so two
  * parallel JVMs (this one and the host `LeftJoinSpec`) cannot collide on
  * Delta paths.
  */
class LeftJoinHeavyProjectionSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-lj-heavy-proj-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-LeftJoinHeavyProjectionSpec")
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

  // ============================================================================
  // (1) LEFT JOIN customers/orders — INSERT right, DELETE right, INSERT left,
  //     and a batched mixed-DML refresh.
  // ============================================================================
  describe("(1) LEFT JOIN customer_orders: nullable right side, full narrative") {
    it("incrementally maintains the LEFT JOIN projection through insert/delete/batch DML") {
      spark.sql("CREATE TABLE IF NOT EXISTS ljh_customers(id INT, name STRING) USING DELTA")
      spark.sql(
        "CREATE TABLE IF NOT EXISTS ljh_orders(customer_id INT, product STRING, amount INT) USING DELTA"
      )
      spark.sql("INSERT INTO ljh_customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      spark.sql("INSERT INTO ljh_orders VALUES (1, 'Widget', 100), (1, 'Gadget', 200)")

      val viewBody =
        "SELECT c.name, o.product, o.amount " +
          "FROM ljh_customers c LEFT JOIN ljh_orders o ON c.id = o.customer_id"

      spark.sql(s"CREATE MATERIALIZED VIEW ljh_mv_customer_orders AS $viewBody")

      // Initial: Alice has 2 orders, Bob and Charlie have NULL
      assertMvCorrect("ljh_mv_customer_orders", viewBody)

      // Insert into right side: Bob gets an order (NULL row should be replaced)
      spark.sql("INSERT INTO ljh_orders VALUES (2, 'Bolt', 50)")
      refreshMv("ljh_mv_customer_orders")
      assertMvCorrect("ljh_mv_customer_orders", viewBody)

      // Delete from right side: remove Alice's only remaining 'Widget' order
      // (she gets a NULL-extended row back if all orders are removed)
      spark.sql("DELETE FROM ljh_orders WHERE customer_id = 1 AND product = 'Widget'")
      refreshMv("ljh_mv_customer_orders")
      assertMvCorrect("ljh_mv_customer_orders", viewBody)

      // Insert into left side (new customer with no orders → NULL extended)
      spark.sql("INSERT INTO ljh_customers VALUES (4, 'Dave')")
      refreshMv("ljh_mv_customer_orders")
      assertMvCorrect("ljh_mv_customer_orders", viewBody)

      // Mixed batched DML: insert + delete in same refresh
      spark.sql("INSERT INTO ljh_orders VALUES (3, 'Screw', 10), (4, 'Nail', 5)")
      spark.sql("DELETE FROM ljh_orders WHERE customer_id = 2")
      refreshMv("ljh_mv_customer_orders")
      assertMvCorrect("ljh_mv_customer_orders", viewBody)
    }
  }
}
