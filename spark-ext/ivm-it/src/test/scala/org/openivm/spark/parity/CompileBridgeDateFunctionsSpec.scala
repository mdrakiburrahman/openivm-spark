package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Parity coverage for the compile-bridge shims that keep Spark's 2-arg
  * `to_date`, `to_timestamp`, and `date_format` on the incremental path.
  *
  * All table / MV names are prefixed with `cbdf_` so parallel forked specs do
  * not collide on Delta warehouse paths.
  */
class CompileBridgeDateFunctionsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-cbdf-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-CompileBridgeDateFunctionsSpec")
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

  private def assertIncremental(mvName: String): Unit = {
    val id   = spark.sessionState.sqlParser.parseTableIdentifier(mvName)
    val meta = MvCatalog.lookup(spark, id).getOrElse(fail(s"Missing MV metadata for $mvName"))
    meta.refreshTypeName should not equal "FULL_REFRESH"
  }

  describe("compile-bridge shim: to_date(raw, fmt)") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      spark.sql("CREATE TABLE cbdf_to_date_src (id INT, trade_date_raw STRING) USING DELTA")
      spark.sql(
        "INSERT INTO cbdf_to_date_src VALUES (1, '20240101'), (2, '20240115'), (3, '20240201')"
      )

      val mvName   = "cbdf_mv_to_date"
      val viewBody = "SELECT id, to_date(trade_date_raw, 'yyyyMMdd') AS trade_date FROM cbdf_to_date_src"
      spark.sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)

      spark.sql("INSERT INTO cbdf_to_date_src VALUES (4, '20240215'), (5, '20240301')")
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: to_timestamp(raw, fmt)") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      spark.sql("CREATE TABLE cbdf_to_timestamp_src (id INT, action_ts_raw STRING) USING DELTA")
      spark.sql(
        "INSERT INTO cbdf_to_timestamp_src VALUES " +
          "(1, '2024-01-01 09:15:00'), (2, '2024-01-01 09:30:45'), (3, '2024-01-02 10:45:59')"
      )

      val mvName = "cbdf_mv_to_timestamp"
      val viewBody =
        "SELECT id, to_timestamp(action_ts_raw, 'yyyy-MM-dd HH:mm:ss') AS action_ts FROM cbdf_to_timestamp_src"
      spark.sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)

      spark.sql(
        "INSERT INTO cbdf_to_timestamp_src VALUES (4, '2024-01-03 00:00:00'), (5, '2024-01-03 12:34:56')"
      )
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: date_format(ts, fmt)") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      spark.sql("CREATE TABLE cbdf_date_format_src (id INT, action_ts TIMESTAMP) USING DELTA")
      spark.sql(
        "INSERT INTO cbdf_date_format_src VALUES " +
          "(1, TIMESTAMP'2024-01-01 09:15:00'), (2, TIMESTAMP'2024-01-01 09:30:45'), " +
          "(3, TIMESTAMP'2024-01-02 10:45:59')"
      )

      val mvName   = "cbdf_mv_date_format"
      val viewBody = "SELECT id, date_format(action_ts, 'yyyyMMdd') AS action_day FROM cbdf_date_format_src"
      spark.sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)

      spark.sql(
        "INSERT INTO cbdf_date_format_src VALUES (4, TIMESTAMP'2024-01-03 00:00:00'), (5, TIMESTAMP'2024-01-03 12:34:56')"
      )
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }
}
