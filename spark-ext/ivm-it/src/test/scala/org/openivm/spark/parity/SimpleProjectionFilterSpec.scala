package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Predicate/projection shapes slice of the SIMPLE_PROJECTION parity coverage.
  * Verifies pure projections, projection + filter, expression columns,
  * NULL-handling predicates, IN-list predicates, and inserts that fail the
  * filter. See [[SimpleProjectionDmlSpec]] for the DML-focused tests.
  */
class SimpleProjectionFilterSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-proj-filter-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-SimpleProjectionFilterSpec")
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

  // ── (1) Pure projection ───────────────────────────────────────────────────

  describe("(1) Pure projection: SELECT name, age FROM users") {
    it("incremental refresh propagates INSERTs to a SELECT name, age MV") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p1(user_id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO users_p1 VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp1 AS SELECT name, age FROM users_p1"
      )
      spark.sql("INSERT INTO users_p1 VALUES (3, 'Carol', 28)")
      refreshMv("mv_sp1")
      assertMvCorrect("mv_sp1", "SELECT name, age FROM users_p1")
    }
  }

  // ── (2) Projection + filter ───────────────────────────────────────────────

  describe("(2) Projection + filter: SELECT name FROM users WHERE age > 25") {
    it("incremental refresh only includes rows passing the age > 25 predicate") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p2(user_id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO users_p2 VALUES (1, 'Alice', 30), (2, 'Bob', 20)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp2 AS SELECT name FROM users_p2 WHERE age > 25"
      )
      spark.sql("INSERT INTO users_p2 VALUES (3, 'Carol', 28), (4, 'Dave', 22)")
      refreshMv("mv_sp2")
      assertMvCorrect("mv_sp2", "SELECT name FROM users_p2 WHERE age > 25")
    }
  }

  // ── (3) Expression columns + filter ──────────────────────────────────────

  describe("(3) Expression + filter: SELECT user_id, UPPER(name) AS uname WHERE age BETWEEN 18 AND 65") {
    it("incremental refresh applies UPPER() expression and BETWEEN predicate") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p3(user_id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO users_p3 VALUES (1, 'alice', 30), (2, 'bob', 70), (3, 'carol', 20)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp3 AS " +
          "SELECT user_id, UPPER(name) AS uname FROM users_p3 WHERE age BETWEEN 18 AND 65"
      )
      spark.sql("INSERT INTO users_p3 VALUES (4, 'dave', 40), (5, 'eve', 80)")
      refreshMv("mv_sp3")
      assertMvCorrect(
        "mv_sp3",
        "SELECT user_id, UPPER(name) AS uname FROM users_p3 WHERE age BETWEEN 18 AND 65"
      )
    }
  }

  // ── (4) NULL-handling predicate ───────────────────────────────────────────

  describe("(4) NULL-handling predicate: SELECT a FROM t WHERE NULL IS NOT DISTINCT FROM NULL") {
    it("always-true NULL predicate makes the MV equivalent to a full SELECT") {
      spark.sql("CREATE TABLE IF NOT EXISTS t_null(a INT, b INT) USING DELTA")
      spark.sql("INSERT INTO t_null VALUES (1, 2), (3, NULL)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp4 AS SELECT a FROM t_null WHERE NULL IS NOT DISTINCT FROM NULL"
      )
      spark.sql("INSERT INTO t_null VALUES (5, 6)")
      refreshMv("mv_sp4")
      assertMvCorrect(
        "mv_sp4",
        "SELECT a FROM t_null WHERE NULL IS NOT DISTINCT FROM NULL"
      )
    }
  }

  // ── (5) IN list predicate ─────────────────────────────────────────────────

  describe("(5) IN list: SELECT a FROM t WHERE b IN (1, 2, 3)") {
    it("incremental refresh only keeps rows whose b value is in the literal list") {
      spark.sql("CREATE TABLE IF NOT EXISTS t_inlist(a INT, b INT) USING DELTA")
      spark.sql("INSERT INTO t_inlist VALUES (10, 1), (20, 4), (30, 2)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp5 AS SELECT a FROM t_inlist WHERE b IN (1, 2, 3)"
      )
      spark.sql("INSERT INTO t_inlist VALUES (40, 3), (50, 5)")
      refreshMv("mv_sp5")
      assertMvCorrect("mv_sp5", "SELECT a FROM t_inlist WHERE b IN (1, 2, 3)")
    }
  }

  // ── (8) INSERT rows failing the filter ───────────────────────────────────

  describe("(8) INSERT rows failing the filter — MV unchanged") {
    it("INSERTs that do NOT satisfy the WHERE clause leave the MV unchanged") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p8(user_id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO users_p8 VALUES (1, 'Alice', 30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp8 AS SELECT name FROM users_p8 WHERE age > 25"
      )
      val before = spark.table("mv_sp8").collect().toSet
      spark.sql("INSERT INTO users_p8 VALUES (2, 'Bob', 20), (3, 'Carol', 15)")
      refreshMv("mv_sp8")
      val after = spark.table("mv_sp8").collect().toSet
      after shouldBe before
      assertMvCorrect("mv_sp8", "SELECT name FROM users_p8 WHERE age > 25")
    }
  }
}
