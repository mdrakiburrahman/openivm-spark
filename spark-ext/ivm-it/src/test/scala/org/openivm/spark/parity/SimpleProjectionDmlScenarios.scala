package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.MvCatalog

/** DML slice of the SIMPLE_PROJECTION parity coverage (INSERT/DELETE/UPDATE,
  * batched mixed DML, empty-table → INSERT, large-table count check). See
  * [[SimpleProjectionFilterSpec]] for the predicate / projection shape tests.
  */
abstract class SimpleProjectionDmlScenarios extends IvmParitySpecBase("simple-projection-dml") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── (7) INSERT rows passing the filter ───────────────────────────────────

  describe("(7) INSERT rows passing the filter — MV gains new rows") {
    it("INSERTs that satisfy the WHERE clause are reflected in the MV after refresh") {
      sql("CREATE TABLE IF NOT EXISTS users_p7(user_id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO users_p7 VALUES (1, 'Alice', 30)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sp7 AS SELECT name FROM users_p7 WHERE age > 25"
      )
      sql("INSERT INTO users_p7 VALUES (2, 'Bob', 28), (3, 'Carol', 20)")
      refreshMv("mv_sp7")
      assertMvCorrect("mv_sp7", "SELECT name FROM users_p7 WHERE age > 25")
    }
  }

  // ── (9) DELETE rows in the MV ─────────────────────────────────────────────

  describe("(9) DELETE rows in the MV — MV reflects the removal") {
    it("DELETE from source removes the corresponding row from the MV after refresh") {
      sql("CREATE TABLE IF NOT EXISTS users_p9(user_id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO users_p9 VALUES (1, 'Alice', 30), (2, 'Bob', 28), (3, 'Carol', 35)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sp9 AS SELECT name, age FROM users_p9"
      )
      sql("DELETE FROM users_p9 WHERE user_id = 2")
      refreshMv("mv_sp9")
      assertMvCorrect("mv_sp9", "SELECT name, age FROM users_p9")
    }
  }

  // ── (10) UPDATE rows ──────────────────────────────────────────────────────

  describe("(10) UPDATE rows: predicate-flipping and value change") {
    it("UPDATE that flips a row out of the predicate removes it from the MV") {
      sql("CREATE TABLE IF NOT EXISTS users_p10a(user_id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO users_p10a VALUES (1, 'Alice', 30), (2, 'Bob', 28)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sp10a AS SELECT name FROM users_p10a WHERE age > 25"
      )
      // Flip Bob out of the predicate: age 28 → 20 (below 25)
      sql("UPDATE users_p10a SET age = 20 WHERE user_id = 2")
      refreshMv("mv_sp10a")
      assertMvCorrect("mv_sp10a", "SELECT name FROM users_p10a WHERE age > 25")
    }

    it("UPDATE that changes a projected column value is reflected in the MV") {
      sql("CREATE TABLE IF NOT EXISTS users_p10b(user_id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO users_p10b VALUES (1, 'Alice', 30), (2, 'Bob', 28)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sp10b AS SELECT name, age FROM users_p10b"
      )
      sql("UPDATE users_p10b SET age = 31 WHERE user_id = 1")
      refreshMv("mv_sp10b")
      assertMvCorrect("mv_sp10b", "SELECT name, age FROM users_p10b")
    }
  }

  // ── (11) Batched mixed DML ────────────────────────────────────────────────

  describe("(11) Batched mixed DML: 5 INSERTs + 3 DELETEs + 2 UPDATEs → single REFRESH") {
    it("single REFRESH after mixed DML produces MV ≡ view body") {
      sql("CREATE TABLE IF NOT EXISTS users_p11(user_id INT, name STRING, age INT) USING DELTA")
      sql(
        "INSERT INTO users_p11 VALUES " +
          "(1,'Alice',30),(2,'Bob',28),(3,'Carol',35),(4,'Dave',22),(5,'Eve',40)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_sp11 AS SELECT name, age FROM users_p11 WHERE age > 25"
      )
      // INSERTs (3 pass filter, 2 fail)
      sql(
        "INSERT INTO users_p11 VALUES " +
          "(6,'Frank',50),(7,'Grace',27),(8,'Heidi',18),(9,'Ivan',33),(10,'Judy',17)"
      )
      // DELETEs
      sql("DELETE FROM users_p11 WHERE user_id IN (2, 4, 8)")
      // UPDATEs: flip Carol out, update Alice's age
      sql("UPDATE users_p11 SET age = 24 WHERE user_id = 3") // Carol: 35→24 (leaves MV)
      sql("UPDATE users_p11 SET age = 32 WHERE user_id = 1") // Alice: 30→32 (stays in MV)
      refreshMv("mv_sp11")
      assertMvCorrect("mv_sp11", "SELECT name, age FROM users_p11 WHERE age > 25")
    }
  }

  // ── (12) Empty initial table → INSERTs → REFRESH ─────────────────────────

  describe("(12) Empty initial table → INSERTs → REFRESH") {
    it("MV starts empty, INSERTs are reflected after refresh") {
      sql("CREATE TABLE IF NOT EXISTS users_p12(user_id INT, name STRING, age INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW mv_sp12 AS SELECT name, age FROM users_p12"
      )
      spark.table("mv_sp12").count() shouldBe 0L
      sql("INSERT INTO users_p12 VALUES (1,'Alice',30),(2,'Bob',25)")
      refreshMv("mv_sp12")
      assertMvCorrect("mv_sp12", "SELECT name, age FROM users_p12")
    }
  }

  // ── (13) Large table: INSERT + DELETE → row count check ──────────────────

  describe("(13) 100 initial rows → INSERT 50 → DELETE 30 → MV has 120 rows") {
    it("incremental refresh produces exactly 100 + 50 - 30 = 120 rows") {
      sql("CREATE TABLE IF NOT EXISTS users_p13(user_id INT, name STRING, age INT) USING DELTA")

      // Insert 100 initial rows
      val initial100 = (1 to 100)
        .map(i => s"($i, 'User$i', ${20 + (i % 50)})")
        .mkString(", ")
      sql(s"INSERT INTO users_p13 VALUES $initial100")

      sql("CREATE MATERIALIZED VIEW mv_sp13 AS SELECT user_id, name, age FROM users_p13")

      // Insert 50 more
      val next50 = (101 to 150)
        .map(i => s"($i, 'User$i', ${20 + (i % 50)})")
        .mkString(", ")
      sql(s"INSERT INTO users_p13 VALUES $next50")

      // Delete 30 rows from the original 100
      val deleteIds = (1 to 30).mkString(", ")
      sql(s"DELETE FROM users_p13 WHERE user_id IN ($deleteIds)")

      refreshMv("mv_sp13")

      spark.table("mv_sp13").count() shouldBe 120L
      assertMvCorrect("mv_sp13", "SELECT user_id, name, age FROM users_p13")
    }
  }

  // ── (14) Positive-only refresh skips delete MERGE ─────────────────────────

  describe("(14) Positive-only refresh skips delete MERGE") {
    it("refreshing a positive-only batch appends rows without a MERGE commit on the MV Delta history") {
      sql("CREATE TABLE IF NOT EXISTS users_p14(user_id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO users_p14 VALUES (1, 'Alice', 30)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sp14 AS SELECT name, age FROM users_p14 WHERE age > 25"
      )

      val meta = MvCatalog
        .lookup(spark, TableIdentifier("mv_sp14"))
        .getOrElse(fail("mv_sp14 metadata missing"))
      val escapedLocation = meta.location.replace("`", "``")
      val preRefreshVersion = spark
        .sql(s"DESCRIBE HISTORY delta.`$escapedLocation`")
        .selectExpr("max(version) AS version")
        .head()
        .getAs[Long]("version")

      sql("INSERT INTO users_p14 VALUES (2, 'Bob', 28), (3, 'Carol', 35)")
      refreshMv("mv_sp14")

      assertMvCorrect("mv_sp14", "SELECT name, age FROM users_p14 WHERE age > 25")

      val refreshHistory = spark
        .sql(s"DESCRIBE HISTORY delta.`$escapedLocation`")
        .where(s"version > $preRefreshVersion")
        .collect()

      refreshHistory should have size 1
      refreshHistory.head.getAs[String]("operation") shouldBe "WRITE"
      Option(refreshHistory.head.getAs[Map[String, String]]("operationParameters"))
        .flatMap(_.get("mode")) shouldBe Some("Append")
    }
  }
}
