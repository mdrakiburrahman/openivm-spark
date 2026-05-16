package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** End-to-end parity tests for RefreshType 2 SIMPLE_PROJECTION.
  *
  * Covers pure projections, projection + filter, expression columns, NULL-handling
  * predicates, IN-list predicates, and DML scenarios (INSERT / DELETE / UPDATE /
  * mixed batches).  Correctness is verified with a bidirectional `EXCEPT ALL`
  * between the incrementally-refreshed MV (openivm_* internal columns stripped)
  * and the equivalent view body re-evaluated over the live source table.
  *
  * See RESEARCH.md §6.8 (assembler), §12 risk 8 (_ivm_rowid / duplicate-row caveat).
  */
class SimpleProjectionSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-proj-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-SimpleProjectionSpec")
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
    * that any openivm internal bookkeeping columns (prefixed `openivm_*`) that
    * may be present in the physical data table are excluded.
    */
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

  // ── (7) INSERT rows passing the filter ───────────────────────────────────

  describe("(7) INSERT rows passing the filter — MV gains new rows") {
    it("INSERTs that satisfy the WHERE clause are reflected in the MV after refresh") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p7(user_id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO users_p7 VALUES (1, 'Alice', 30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp7 AS SELECT name FROM users_p7 WHERE age > 25"
      )
      spark.sql("INSERT INTO users_p7 VALUES (2, 'Bob', 28), (3, 'Carol', 20)")
      refreshMv("mv_sp7")
      assertMvCorrect("mv_sp7", "SELECT name FROM users_p7 WHERE age > 25")
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

  // ── (9) DELETE rows in the MV ─────────────────────────────────────────────

  describe("(9) DELETE rows in the MV — MV reflects the removal") {
    it("DELETE from source removes the corresponding row from the MV after refresh") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p9(user_id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO users_p9 VALUES (1, 'Alice', 30), (2, 'Bob', 28), (3, 'Carol', 35)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp9 AS SELECT name, age FROM users_p9"
      )
      spark.sql("DELETE FROM users_p9 WHERE user_id = 2")
      refreshMv("mv_sp9")
      assertMvCorrect("mv_sp9", "SELECT name, age FROM users_p9")
    }
  }

  // ── (10) UPDATE rows ──────────────────────────────────────────────────────

  describe("(10) UPDATE rows: predicate-flipping and value change") {
    it("UPDATE that flips a row out of the predicate removes it from the MV") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p10a(user_id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO users_p10a VALUES (1, 'Alice', 30), (2, 'Bob', 28)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp10a AS SELECT name FROM users_p10a WHERE age > 25"
      )
      // Flip Bob out of the predicate: age 28 → 20 (below 25)
      spark.sql("UPDATE users_p10a SET age = 20 WHERE user_id = 2")
      refreshMv("mv_sp10a")
      assertMvCorrect("mv_sp10a", "SELECT name FROM users_p10a WHERE age > 25")
    }

    it("UPDATE that changes a projected column value is reflected in the MV") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p10b(user_id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO users_p10b VALUES (1, 'Alice', 30), (2, 'Bob', 28)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp10b AS SELECT name, age FROM users_p10b"
      )
      spark.sql("UPDATE users_p10b SET age = 31 WHERE user_id = 1")
      refreshMv("mv_sp10b")
      assertMvCorrect("mv_sp10b", "SELECT name, age FROM users_p10b")
    }
  }

  // ── (11) Batched mixed DML ────────────────────────────────────────────────

  describe("(11) Batched mixed DML: 5 INSERTs + 3 DELETEs + 2 UPDATEs → single REFRESH") {
    it("single REFRESH after mixed DML produces MV ≡ view body") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p11(user_id INT, name STRING, age INT) USING DELTA")
      spark.sql(
        "INSERT INTO users_p11 VALUES " +
          "(1,'Alice',30),(2,'Bob',28),(3,'Carol',35),(4,'Dave',22),(5,'Eve',40)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp11 AS SELECT name, age FROM users_p11 WHERE age > 25"
      )
      // INSERTs (3 pass filter, 2 fail)
      spark.sql(
        "INSERT INTO users_p11 VALUES " +
          "(6,'Frank',50),(7,'Grace',27),(8,'Heidi',18),(9,'Ivan',33),(10,'Judy',17)"
      )
      // DELETEs
      spark.sql("DELETE FROM users_p11 WHERE user_id IN (2, 4, 8)")
      // UPDATEs: flip Carol out, update Alice's age
      spark.sql("UPDATE users_p11 SET age = 24 WHERE user_id = 3") // Carol: 35→24 (leaves MV)
      spark.sql("UPDATE users_p11 SET age = 32 WHERE user_id = 1") // Alice: 30→32 (stays in MV)
      refreshMv("mv_sp11")
      assertMvCorrect("mv_sp11", "SELECT name, age FROM users_p11 WHERE age > 25")
    }
  }

  // ── (12) Empty initial table → INSERTs → REFRESH ─────────────────────────

  describe("(12) Empty initial table → INSERTs → REFRESH") {
    it("MV starts empty, INSERTs are reflected after refresh") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p12(user_id INT, name STRING, age INT) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sp12 AS SELECT name, age FROM users_p12"
      )
      spark.table("mv_sp12").count() shouldBe 0L
      spark.sql("INSERT INTO users_p12 VALUES (1,'Alice',30),(2,'Bob',25)")
      refreshMv("mv_sp12")
      assertMvCorrect("mv_sp12", "SELECT name, age FROM users_p12")
    }
  }

  // ── (13) Large table: INSERT + DELETE → row count check ──────────────────

  describe("(13) 100 initial rows → INSERT 50 → DELETE 30 → MV has 120 rows") {
    it("incremental refresh produces exactly 100 + 50 - 30 = 120 rows") {
      spark.sql("CREATE TABLE IF NOT EXISTS users_p13(user_id INT, name STRING, age INT) USING DELTA")

      // Insert 100 initial rows
      val initial100 = (1 to 100)
        .map(i => s"($i, 'User$i', ${20 + (i % 50)})")
        .mkString(", ")
      spark.sql(s"INSERT INTO users_p13 VALUES $initial100")

      spark.sql("CREATE MATERIALIZED VIEW mv_sp13 AS SELECT user_id, name, age FROM users_p13")

      // Insert 50 more
      val next50 = (101 to 150)
        .map(i => s"($i, 'User$i', ${20 + (i % 50)})")
        .mkString(", ")
      spark.sql(s"INSERT INTO users_p13 VALUES $next50")

      // Delete 30 rows from the original 100
      val deleteIds = (1 to 30).mkString(", ")
      spark.sql(s"DELETE FROM users_p13 WHERE user_id IN ($deleteIds)")

      refreshMv("mv_sp13")

      spark.table("mv_sp13").count() shouldBe 120L
      assertMvCorrect("mv_sp13", "SELECT user_id, name, age FROM users_p13")
    }
  }
}
