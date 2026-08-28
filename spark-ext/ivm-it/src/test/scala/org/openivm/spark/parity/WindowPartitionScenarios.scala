package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** Coverage of RefreshType 5 (WINDOW_PARTITION).
  *
  * WINDOW_PARTITION applies whenever the openivm classifier identifies a view
  * whose top-level projection contains one or more window functions with a
  * non-empty `PARTITION BY` clause (`src/core/parser.cpp:692-694`, where
  * `found_window` together with non-empty partition columns routes to
  * WINDOW_PARTITION).  The classifier captures the partition columns into the
  * view's metadata; OpenIVM's `BuildWindowPartitionRefresh()` /
  * `CompileWindowRecompute()` (`src/upsert/refresh_window.cpp:353-385` →
  * `src/upsert/refresh_compiler_aux.cpp:265-291`) then emits a partition-scoped
  * DELETE+INSERT program of the form:
  *
  * {{{
  *   DELETE FROM openivm_data_<v>
  *     WHERE <part_col> IN (SELECT DISTINCT <part_col> FROM openivm_delta_<src>
  *                          WHERE openivm_timestamp > '<last_refresh>'::TIMESTAMP);
  *   INSERT INTO openivm_data_<v>
  *   SELECT * FROM (<full view body, reading from memory.main.<src>>) openivm_recompute
  *     WHERE <part_col> IN (SELECT DISTINCT <part_col> FROM openivm_delta_<src>
  *                          WHERE openivm_timestamp > '<last_refresh>'::TIMESTAMP);
  * }}}
  *
  * With openivm `4471f4e929fd3b21ac55ea0c47249d4716853c98` and
  * `force_view_delta_cascade=true` (which openivm-spark always sets in its
  * CompileFacts payload), WINDOW_PARTITION now ALSO
  * emits an `INSERT INTO openivm_delta_<view>` companion: openivm snapshots
  * the affected pre-refresh rows into `openivm_old_<view>`, the recomputed
  * post-refresh rows into `openivm_new_<view>`, and emits `-1/+1` rows into
  * `openivm_delta_<view>`. For a bounded affected-key set and this raw signed
  * snapshot shape, Spark materializes the affected-key set once, persists the
  * cascade, and applies its positive rows. Small literal key sets use one
  * partition-scoped `REPLACE WHERE`; larger sets use the materialized keys for
  * DELETE and the cascade for INSERT. The signed view delta also keeps
  * downstream MV-over-MV chains incremental.
  *
  * == Observed `refreshType` per test ==
  *
  *   (1)  ROW_NUMBER + PARTITION BY               → 5  WINDOW_PARTITION
  *   (2)  RANK + PARTITION BY ORDER BY DESC       → 5  WINDOW_PARTITION
  *   (3)  DENSE_RANK + PARTITION BY               → 5  WINDOW_PARTITION
  *   (4)  SUM aggregate OVER PARTITION BY only    → 5  WINDOW_PARTITION
  *   (5)  LAG + PARTITION BY ORDER BY             → 5  WINDOW_PARTITION
  *   (6)  LEAD + PARTITION BY ORDER BY            → 5  WINDOW_PARTITION
  *   (7)  INSERT into existing partition          → other partitions unchanged
  *   (8)  DELETE within partition                 → remaining rows re-windowed
  *   (9)  UPDATE within partition                 → re-windowed
  *   (10) INSERT into brand-new partition         → fresh partition appears
  *   (11) Composite PARTITION BY (k1, k2)         → 5  WINDOW_PARTITION
  *   (12) Batched conflicting DML across many partitions → single REFRESH
  *
  * == Rewriter mechanics ==
  *
  * The compiled DELETE statement matches `SparkRefreshRewriter.ScalarDeleteMv`
  * (the `startsWith("DELETE FROM openivm_data_<v>")` arm), and the compiled
  * INSERT statement matches `SparkRefreshRewriter.ScalarFullRecomputeInsert`
  * (because the wrapped view body contains `memory.main.<src>`).  Both
  * rewriters strip the inner `openivm_timestamp > '<ts>'::TIMESTAMP` predicate
  * (no-op for MIN/MAX and GROUP_RECOMPUTE shapes, removes the spurious filter
  * for WINDOW_PARTITION) and substitute `openivm_data_<v>` → backticked MV
  * name and `memory.main.<short>` → backticked `<short>`.
  *
  * == Frame caveat ==
  *
  * Per PLAN.md §8 and RESEARCH §5.2, lpts emits NotImplemented for window
  * frames using GROUPS or EXCLUDE.  All tests below use the default frame
  * (ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) or no explicit
  * frame at all — both are accepted by lpts.
  */
abstract class WindowPartitionScenarios extends IvmParitySpecBase("window-partition") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Looks up the recorded refresh type for `name` via the MV catalog. */
  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ── (1) ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) ───────────

  describe("(1) ROW_NUMBER PARTITION BY region ORDER BY amount → WINDOW_PARTITION") {
    it("classifies as WindowPartition and refreshes only affected partitions after INSERT") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp1(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp1 VALUES (1,'east',10), (2,'east',30), (3,'east',20), " +
          "(4,'west',5), (5,'west',15), (6,'west',25)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM sales_wp1"
      sql(s"CREATE MATERIALIZED VIEW mv_wp1 AS $viewSql")

      mvRefreshType("mv_wp1") shouldBe RefreshTypeCode.WindowPartition

      // INSERT into 'east' only — 'west' partition rows should keep their rn values.
      sql("INSERT INTO sales_wp1 VALUES (7,'east',25)")
      refreshMv("mv_wp1")
      assertMvCorrect("mv_wp1", viewSql)
    }
  }

  // ── (2) RANK() OVER (PARTITION BY region ORDER BY amount DESC) ────────────

  describe("(2) RANK PARTITION BY region ORDER BY amount DESC → WINDOW_PARTITION") {
    it("classifies as WindowPartition and refreshes correctly after INSERT") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp2(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp2 VALUES (1,'east',100), (2,'east',100), (3,'east',50), " +
          "(4,'west',300), (5,'west',150)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS rk FROM sales_wp2"
      sql(s"CREATE MATERIALIZED VIEW mv_wp2 AS $viewSql")

      mvRefreshType("mv_wp2") shouldBe RefreshTypeCode.WindowPartition

      // Insert a tie at the top of 'east' — RANK skips ranks after ties (1, 1, 3, …).
      sql("INSERT INTO sales_wp2 VALUES (6,'east',150)")
      refreshMv("mv_wp2")
      assertMvCorrect("mv_wp2", viewSql)
    }
  }

  // ── (3) DENSE_RANK() OVER (PARTITION BY region ORDER BY amount) ───────────

  describe("(3) DENSE_RANK PARTITION BY region ORDER BY amount → WINDOW_PARTITION") {
    it("classifies as WindowPartition and DENSE_RANK ties are handled correctly") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp3(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp3 VALUES (1,'east',10), (2,'east',10), (3,'east',20), " +
          "(4,'west',5), (5,'west',15)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "DENSE_RANK() OVER (PARTITION BY region ORDER BY amount) AS dr FROM sales_wp3"
      sql(s"CREATE MATERIALIZED VIEW mv_wp3 AS $viewSql")

      mvRefreshType("mv_wp3") shouldBe RefreshTypeCode.WindowPartition

      // Insert a duplicate min in 'east' — dense_rank does NOT skip ranks after ties.
      sql("INSERT INTO sales_wp3 VALUES (6,'east',10), (7,'east',30)")
      refreshMv("mv_wp3")
      assertMvCorrect("mv_wp3", viewSql)
    }
  }

  // ── (4) SUM() OVER (PARTITION BY region) — partition-only, no ORDER BY ────

  describe("(4) SUM() OVER (PARTITION BY region) — partition-only window aggregate") {
    it("classifies as WindowPartition; partition totals refresh after INSERT") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp4(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp4 VALUES (1,'east',100), (2,'east',200), " +
          "(3,'west',150), (4,'west',250)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "SUM(amount) OVER (PARTITION BY region) AS region_total FROM sales_wp4"
      sql(s"CREATE MATERIALIZED VIEW mv_wp4 AS $viewSql")

      mvRefreshType("mv_wp4") shouldBe RefreshTypeCode.WindowPartition

      // Insert into 'east' only — every 'east' row's region_total must update; 'west' totals stay.
      sql("INSERT INTO sales_wp4 VALUES (5,'east',300)")
      refreshMv("mv_wp4")
      assertMvCorrect("mv_wp4", viewSql)
    }
  }

  // ── (5) LAG(amount) OVER (PARTITION BY region ORDER BY id) ────────────────

  describe("(5) LAG PARTITION BY region ORDER BY id → WINDOW_PARTITION") {
    it("classifies as WindowPartition; LAG values refresh after INSERT") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp5(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp5 VALUES (1,'east',10), (2,'east',20), (3,'east',30), " +
          "(4,'west',100), (5,'west',200)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "LAG(amount) OVER (PARTITION BY region ORDER BY id) AS prev FROM sales_wp5"
      sql(s"CREATE MATERIALIZED VIEW mv_wp5 AS $viewSql")

      mvRefreshType("mv_wp5") shouldBe RefreshTypeCode.WindowPartition

      // Insert a row in 'east' with a new (larger) id — shifts the LAG cascade.
      sql("INSERT INTO sales_wp5 VALUES (6,'east',40)")
      refreshMv("mv_wp5")
      assertMvCorrect("mv_wp5", viewSql)
    }
  }

  // ── (6) LEAD(amount) OVER (PARTITION BY region ORDER BY id) ───────────────

  describe("(6) LEAD PARTITION BY region ORDER BY id → WINDOW_PARTITION") {
    it("classifies as WindowPartition; LEAD values refresh after INSERT") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp6(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp6 VALUES (1,'east',10), (2,'east',20), (3,'east',30), " +
          "(4,'west',100), (5,'west',200)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "LEAD(amount) OVER (PARTITION BY region ORDER BY id) AS next FROM sales_wp6"
      sql(s"CREATE MATERIALIZED VIEW mv_wp6 AS $viewSql")

      mvRefreshType("mv_wp6") shouldBe RefreshTypeCode.WindowPartition

      // Append a row at the end of 'west' partition — last row's LEAD goes from NULL to non-NULL.
      sql("INSERT INTO sales_wp6 VALUES (6,'west',300)")
      refreshMv("mv_wp6")
      assertMvCorrect("mv_wp6", viewSql)
    }
  }

  // ── (7) INSERT into existing partition — other partitions unchanged ───────

  describe("(7) INSERT into a single existing partition only affects that partition") {
    it("partitions not touched by the INSERT keep their original window values") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp7(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp7 VALUES (1,'east',10), (2,'east',20), " +
          "(3,'west',5), (4,'west',15), (5,'west',25)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM sales_wp7"
      sql(s"CREATE MATERIALIZED VIEW mv_wp7 AS $viewSql")

      mvRefreshType("mv_wp7") shouldBe RefreshTypeCode.WindowPartition

      // Capture 'west' rows BEFORE the INSERT so we can prove they're not disturbed.
      val westBefore = spark
        .table("mv_wp7")
        .where("region = 'west'")
        .select("id", "region", "amount", "rn")
        .collect()
        .toSet

      // Insert only into 'east'.
      sql("INSERT INTO sales_wp7 VALUES (6,'east',15)")
      refreshMv("mv_wp7")

      // 'west' rows must be byte-identical to their pre-refresh state.
      val westAfter = spark
        .table("mv_wp7")
        .where("region = 'west'")
        .select("id", "region", "amount", "rn")
        .collect()
        .toSet
      westAfter shouldBe westBefore

      // Full correctness against the base query.
      assertMvCorrect("mv_wp7", viewSql)
    }
  }

  // ── (8) DELETE from a partition — remaining rows in that partition re-windowed ─

  describe("(8) DELETE within a partition re-windows the survivors") {
    it("DELETE shifts the ROW_NUMBER for the remaining rows of the affected partition") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp8(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp8 VALUES (1,'east',10), (2,'east',20), (3,'east',30), " +
          "(4,'west',5), (5,'west',15)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM sales_wp8"
      sql(s"CREATE MATERIALIZED VIEW mv_wp8 AS $viewSql")

      mvRefreshType("mv_wp8") shouldBe RefreshTypeCode.WindowPartition

      // Delete the middle 'east' row — surviving 'east' rows must re-rank 1,2.
      sql("DELETE FROM sales_wp8 WHERE region='east' AND amount=20")
      refreshMv("mv_wp8")
      assertMvCorrect("mv_wp8", viewSql)
    }
  }

  // ── (9) UPDATE within a partition — re-window ─────────────────────────────

  describe("(9) UPDATE within a partition re-windows the affected partition") {
    it("UPDATE that changes the ORDER BY value shifts the ROW_NUMBER assignments") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp9(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp9 VALUES (1,'east',10), (2,'east',20), (3,'east',30), " +
          "(4,'west',5), (5,'west',15)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM sales_wp9"
      sql(s"CREATE MATERIALIZED VIEW mv_wp9 AS $viewSql")

      mvRefreshType("mv_wp9") shouldBe RefreshTypeCode.WindowPartition

      // Move the highest 'east' row to the bottom — the ROW_NUMBER assignment flips.
      sql("UPDATE sales_wp9 SET amount=5 WHERE id=3")
      refreshMv("mv_wp9")
      assertMvCorrect("mv_wp9", viewSql)
    }
  }

  // ── (10) INSERT into a brand-new partition — fresh partition appears ──────

  describe("(10) INSERT into a previously-empty partition creates that partition in the MV") {
    it("a new partition key appears with the correct window values") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp10(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp10 VALUES (1,'east',10), (2,'east',20), " +
          "(3,'west',5), (4,'west',15)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM sales_wp10"
      sql(s"CREATE MATERIALIZED VIEW mv_wp10 AS $viewSql")

      mvRefreshType("mv_wp10") shouldBe RefreshTypeCode.WindowPartition

      // Brand-new 'north' partition.
      sql("INSERT INTO sales_wp10 VALUES (5,'north',50), (6,'north',100)")
      refreshMv("mv_wp10")
      assertMvCorrect("mv_wp10", viewSql)
    }
  }

  // ── (11) Composite PARTITION BY (k1, k2) ──────────────────────────────────

  describe("(11) Composite PARTITION BY (region, product) → WINDOW_PARTITION") {
    it("classifies as WindowPartition; ROW_NUMBER scoped to (region, product) tuples") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp11(id INT, region STRING, product STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp11 VALUES " +
          "(1,'east','A',10), (2,'east','A',20), (3,'east','B',30), " +
          "(4,'west','A',5),  (5,'west','A',15), (6,'west','B',25)"
      )
      val viewSql =
        "SELECT id, region, product, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region, product ORDER BY amount) AS rn " +
          "FROM sales_wp11"
      sql(s"CREATE MATERIALIZED VIEW mv_wp11 AS $viewSql")

      mvRefreshType("mv_wp11") shouldBe RefreshTypeCode.WindowPartition

      // Insert into one of the existing (region, product) cells — only that cell re-windowed.
      sql("INSERT INTO sales_wp11 VALUES (7,'east','A',5)")
      refreshMv("mv_wp11")
      assertMvCorrect("mv_wp11", viewSql)
    }
  }

  // ── (12) Batched conflicting DML across multiple partitions → single REFRESH ─

  describe("(12) Batched mixed DML across multiple partitions consolidates into one REFRESH") {
    it("INSERT + DELETE + UPDATE spread over several partitions yield a correct single-refresh result") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_wp12(id INT, region STRING, amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_wp12 VALUES " +
          "(1,'east',10), (2,'east',20), (3,'east',30), " +
          "(4,'west',5),  (5,'west',15), (6,'west',25), " +
          "(7,'north',100), (8,'north',200)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM sales_wp12"
      sql(s"CREATE MATERIALIZED VIEW mv_wp12 AS $viewSql")

      mvRefreshType("mv_wp12") shouldBe RefreshTypeCode.WindowPartition

      // Batched DML touching 'east', 'west', and 'north' before a single refresh.
      sql("INSERT INTO sales_wp12 VALUES (9,'east',5), (10,'west',50)") // east, west
      sql("DELETE FROM sales_wp12 WHERE region='east' AND amount=20")   // east
      sql("UPDATE sales_wp12 SET amount=999 WHERE id=7")                // north
      // 'south' is a brand-new partition.
      sql("INSERT INTO sales_wp12 VALUES (11,'south',1), (12,'south',2)")
      refreshMv("mv_wp12")

      assertMvCorrect("mv_wp12", viewSql)
    }
  }
}
