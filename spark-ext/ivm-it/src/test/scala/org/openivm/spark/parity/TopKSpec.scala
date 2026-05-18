package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P5.topk — End-to-end parity tests for Top-K materialized views
  * (`ORDER BY … LIMIT k`).
  *
  * == Design ==
  *
  * Top-K is not a distinct RefreshType.  Per RESEARCH.md §6.8 and
  * `.temp/openivm/CLAUDE.md`, OpenIVM strips the trailing `ORDER BY … LIMIT k`
  * at CREATE time (`openivm/src/core/parser.cpp:239-291`), classifies the
  * inner stripped query as `AGGREGATE_GROUP` (0) or `SIMPLE_PROJECTION` (2),
  * and applies the ORDER BY/LIMIT at read time by wrapping the data table in
  * a thin user-facing VIEW.  `RefreshType::TOP_K` (7) is a dead enum value
  * that the classifier never assigns
  * (`spark-ext/ivm-common/.../RefreshTypeCode.scala:14-16`).
  *
  * `CreateMaterializedViewCommand` mirrors this: it splits any trailing
  * ORDER BY … LIMIT (`[OFFSET m]`) wrapper off the user-supplied SELECT body,
  * materializes the inner data table (unlimited) under
  * `<warehouse>/_ivm/views/<v>` for incremental maintenance, and creates a
  * Spark `CREATE OR REPLACE VIEW <v> AS SELECT <userCols> FROM <data> <suffix>`
  * on top.  Querying `<v>` returns the live top-k against the latest refresh
  * snapshot of the underlying data table.
  *
  * == EXCEPT ALL correctness check ==
  *
  * Strict bag equality (`EXCEPT ALL`) is fragile when ties exist in the sort
  * key: two rows with equal sort values can swap positions yet both be valid
  * top-k answers.  To keep the parity check deterministic these tests:
  *
  *   (a) use unique sort values whenever possible, and
  *   (b) when ties are intentional, include a deterministic secondary
  *       tie-breaker column in the ORDER BY (e.g. `ORDER BY amount DESC,
  *       id ASC`) so both the MV and the re-evaluated view body produce the
  *       same multiset.
  *
  * Each test calls `assertMvCorrect`, which projects the MV down to the
  * expected columns, strips any hidden `openivm_*` bookkeeping columns from
  * the underlying data table, and asserts bidirectional `EXCEPT ALL` returns
  * 0 rows.  Counts are also compared because EXCEPT ALL alone cannot detect
  * the unlikely "same multiset different cardinality" case.
  */
class TopKSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-topk-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-TopKSpec")
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

  /** Bidirectional `EXCEPT ALL` + count check.
    *
    * Projects the MV to the user-visible columns of `expectedSql` before
    * comparing so that any openivm internal bookkeeping columns (prefixed
    * `openivm_*`) present in the physical data table are excluded.  The MV
    * itself is a Spark VIEW that wraps the data table with `ORDER BY … LIMIT`,
    * but the projection guard is kept for defense-in-depth.
    */
  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    withClue(s"$mvName count == expected count: ") {
      mv.count() shouldBe expected.count()
    }
    withClue(s"$mvName EXCEPT ALL <expected>: ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName: ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  // ── (1) Pure projection top-K: SELECT * ORDER BY x DESC LIMIT k ─────────────

  describe("(1) Projection top-k: SELECT * FROM sales ORDER BY amount DESC LIMIT 3") {
    it("returns the 3 highest-amount rows after INSERTs are refreshed") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t1(id INT, region STRING, amount INT) USING DELTA"
      )
      // Unique amounts → tie-free ordering, EXCEPT ALL is deterministic.
      spark.sql(
        "INSERT INTO sales_t1 VALUES " +
          "(1,'east',10),(2,'east',40),(3,'west',20),(4,'west',50),(5,'north',30)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_topk_1 AS " +
          "SELECT * FROM sales_t1 ORDER BY amount DESC LIMIT 3"
      )
      assertMvCorrect(
        "mv_topk_1",
        "SELECT * FROM sales_t1 ORDER BY amount DESC LIMIT 3"
      )
      // Add a row that beats the current bottom of the top-3 (amount=30) and
      // one that does not.  After REFRESH, the MV should reflect the new top-3.
      spark.sql("INSERT INTO sales_t1 VALUES (6,'south',45),(7,'south',5)")
      refreshMv("mv_topk_1")
      assertMvCorrect(
        "mv_topk_1",
        "SELECT * FROM sales_t1 ORDER BY amount DESC LIMIT 3"
      )
    }
  }

  // ── (2) Aggregate top-K: GROUP BY + ORDER BY agg DESC LIMIT k ─────────────

  describe("(2) Aggregate top-k: SELECT region, SUM(amount) AS total … GROUP BY region ORDER BY total DESC LIMIT 5") {
    it("returns the top-5 regions by total amount after INSERTs are refreshed") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t2(id INT, region STRING, amount INT) USING DELTA"
      )
      // 7 regions; the top-5 are: a(100), b(70), c(60), d(50), e(40).
      // f(30) and g(20) are out.  All totals are unique so ORDER BY is unique.
      spark.sql(
        "INSERT INTO sales_t2 VALUES " +
          "(1,'a',60),(2,'a',40),(3,'b',70),(4,'c',60),(5,'d',50),(6,'e',40),(7,'f',30),(8,'g',20)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_topk_2 AS " +
          "SELECT region, SUM(amount) AS total FROM sales_t2 GROUP BY region " +
          "ORDER BY total DESC LIMIT 5"
      )
      assertMvCorrect(
        "mv_topk_2",
        "SELECT region, SUM(amount) AS total FROM sales_t2 GROUP BY region " +
          "ORDER BY total DESC LIMIT 5"
      )
      // Push 'g' over the threshold (was 20 → 95): it should enter the top-5
      // and bump 'e' out.
      spark.sql("INSERT INTO sales_t2 VALUES (9,'g',75)")
      refreshMv("mv_topk_2")
      assertMvCorrect(
        "mv_topk_2",
        "SELECT region, SUM(amount) AS total FROM sales_t2 GROUP BY region " +
          "ORDER BY total DESC LIMIT 5"
      )
    }
  }

  // ── (3) LIMIT … OFFSET ────────────────────────────────────────────────────

  describe("(3) LIMIT with OFFSET: SELECT * FROM sales ORDER BY amount LIMIT 3 OFFSET 2") {
    it("skips the lowest 2 amounts and returns the next 3 after refresh") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t3(id INT, region STRING, amount INT) USING DELTA"
      )
      // Unique amounts 5,10,15,20,25,30,35 → ORDER BY amount ASC LIMIT 3 OFFSET 2
      // skips 5,10 and returns {15,20,25}.
      spark.sql(
        "INSERT INTO sales_t3 VALUES " +
          "(1,'a',25),(2,'b',5),(3,'c',20),(4,'d',35),(5,'e',15),(6,'f',10),(7,'g',30)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_topk_3 AS " +
          "SELECT * FROM sales_t3 ORDER BY amount LIMIT 3 OFFSET 2"
      )
      assertMvCorrect(
        "mv_topk_3",
        "SELECT * FROM sales_t3 ORDER BY amount LIMIT 3 OFFSET 2"
      )
      // Insert a very-low amount; the OFFSET 2 window slides by one, so the
      // expected rows change.  Sanity-check post-refresh parity.
      spark.sql("INSERT INTO sales_t3 VALUES (8,'h',1)")
      refreshMv("mv_topk_3")
      assertMvCorrect(
        "mv_topk_3",
        "SELECT * FROM sales_t3 ORDER BY amount LIMIT 3 OFFSET 2"
      )
    }
  }

  // ── (4) Ties in sort key with deterministic tie-breaker ──────────────────

  describe("(4) Ties: ORDER BY amount DESC, id ASC LIMIT 3") {
    it("deterministically chooses among ties using id ASC as the tie-breaker") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t4(id INT, region STRING, amount INT) USING DELTA"
      )
      // Three rows tie on amount=50; the tie-breaker (id ASC) picks ids 1,2 and
      // amount=40 row with id=4 fills the third slot. id=3 (amount=50) loses
      // the tie-breaker for the third slot to id=4? No: 50>40 always wins.
      // The tied rows are (1,_,50),(2,_,50),(3,_,50) — top-3 by (amount DESC,
      // id ASC) is {1,2,3} regardless of insertion order.
      spark.sql(
        "INSERT INTO sales_t4 VALUES " +
          "(1,'a',50),(2,'b',50),(3,'c',50),(4,'d',40),(5,'e',30),(6,'f',20)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_topk_4 AS " +
          "SELECT id, region, amount FROM sales_t4 ORDER BY amount DESC, id ASC LIMIT 3"
      )
      assertMvCorrect(
        "mv_topk_4",
        "SELECT id, region, amount FROM sales_t4 ORDER BY amount DESC, id ASC LIMIT 3"
      )
      // Add a new tie (id=7, amount=50): tie-breaker still picks lowest ids,
      // so the MV stays {1,2,3} — id=7 is excluded by id ASC.
      spark.sql("INSERT INTO sales_t4 VALUES (7,'g',50)")
      refreshMv("mv_topk_4")
      assertMvCorrect(
        "mv_topk_4",
        "SELECT id, region, amount FROM sales_t4 ORDER BY amount DESC, id ASC LIMIT 3"
      )
    }
  }

  // ── (5) Composite ORDER BY ────────────────────────────────────────────────

  describe("(5) Multi-key ORDER BY: ORDER BY region DESC, amount ASC LIMIT 4") {
    it("orders by region descending then amount ascending and returns top 4") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t5(id INT, region STRING, amount INT) USING DELTA"
      )
      // Regions sort: e>d>c>b>a. region=e has two rows (10,20); region=d has
      // one (15). The top-4 by (region DESC, amount ASC) is:
      //   (e,10), (e,20), (d,15), (c,…) — pick the smallest amount for region c.
      spark.sql(
        "INSERT INTO sales_t5 VALUES " +
          "(1,'a',5),(2,'b',7),(3,'c',9),(4,'c',12),(5,'d',15),(6,'e',10),(7,'e',20)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_topk_5 AS " +
          "SELECT id, region, amount FROM sales_t5 " +
          "ORDER BY region DESC, amount ASC LIMIT 4"
      )
      assertMvCorrect(
        "mv_topk_5",
        "SELECT id, region, amount FROM sales_t5 " +
          "ORDER BY region DESC, amount ASC LIMIT 4"
      )
      // Insert a row in the top region (e) with a smaller amount than 10:
      // the new (e,3) row should slide into the top-4 and bump out (c, 9).
      spark.sql("INSERT INTO sales_t5 VALUES (8,'e',3)")
      refreshMv("mv_topk_5")
      assertMvCorrect(
        "mv_topk_5",
        "SELECT id, region, amount FROM sales_t5 " +
          "ORDER BY region DESC, amount ASC LIMIT 4"
      )
    }
  }

  // ── (6) Insert above threshold → MV churn ────────────────────────────────

  describe("(6) INSERTs above current top-k threshold cause MV churn") {
    it("rows above the threshold enter and current bottom rows fall out") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t6(id INT, region STRING, amount INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO sales_t6 VALUES " +
          "(1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_topk_6 AS " +
          "SELECT id, region, amount FROM sales_t6 ORDER BY amount DESC LIMIT 3"
      )
      assertMvCorrect(
        "mv_topk_6",
        "SELECT id, region, amount FROM sales_t6 ORDER BY amount DESC LIMIT 3"
      )
      // All three new rows beat the current threshold (30).  Expect the top-3
      // to roll over entirely to {100, 90, 80}.
      spark.sql("INSERT INTO sales_t6 VALUES (6,'f',80),(7,'g',90),(8,'h',100)")
      refreshMv("mv_topk_6")
      assertMvCorrect(
        "mv_topk_6",
        "SELECT id, region, amount FROM sales_t6 ORDER BY amount DESC LIMIT 3"
      )
    }
  }

  // ── (7) Delete a top-k row → next-best enters ────────────────────────────

  describe("(7) DELETE a row currently in top-k — next-best replaces it") {
    it("deleting the current #1 promotes the previous #4 into the MV") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t7(id INT, region STRING, amount INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO sales_t7 VALUES " +
          "(1,'a',100),(2,'b',90),(3,'c',80),(4,'d',70),(5,'e',60)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_topk_7 AS " +
          "SELECT id, region, amount FROM sales_t7 ORDER BY amount DESC LIMIT 3"
      )
      assertMvCorrect(
        "mv_topk_7",
        "SELECT id, region, amount FROM sales_t7 ORDER BY amount DESC LIMIT 3"
      )
      // Delete the current top (id=1, amount=100).  After refresh, the MV
      // should be {90, 80, 70}.
      spark.sql("DELETE FROM sales_t7 WHERE id = 1")
      refreshMv("mv_topk_7")
      assertMvCorrect(
        "mv_topk_7",
        "SELECT id, region, amount FROM sales_t7 ORDER BY amount DESC LIMIT 3"
      )
    }
  }

  // ── (8) Batched mixed DML around the threshold ───────────────────────────

  describe("(8) Batched DML: 3 INSERTs + 2 DELETEs + 1 UPDATE crossing the k threshold → single REFRESH") {
    it("single REFRESH after mixed DML produces MV ≡ ORDER BY … LIMIT k view body") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t8(id INT, region STRING, amount INT) USING DELTA"
      )
      // Start with seven rows, top-3 (by amount DESC, id ASC) = {(1,100),(2,90),(3,80)}.
      spark.sql(
        "INSERT INTO sales_t8 VALUES " +
          "(1,'a',100),(2,'b',90),(3,'c',80),(4,'d',70),(5,'e',60),(6,'f',50),(7,'g',40)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_topk_8 AS " +
          "SELECT id, region, amount FROM sales_t8 ORDER BY amount DESC, id ASC LIMIT 3"
      )
      assertMvCorrect(
        "mv_topk_8",
        "SELECT id, region, amount FROM sales_t8 ORDER BY amount DESC, id ASC LIMIT 3"
      )
      // INSERTs: one beats the current #1, one ties existing values, one is below threshold.
      spark.sql("INSERT INTO sales_t8 VALUES (8,'h',120),(9,'i',75),(10,'j',10)")
      // DELETEs: remove the current #1 (id=1, amount=100) and a below-threshold row.
      spark.sql("DELETE FROM sales_t8 WHERE id IN (1, 7)")
      // UPDATE: bump row id=4 from 70 → 95 so it crosses into the top-3.
      spark.sql("UPDATE sales_t8 SET amount = 95 WHERE id = 4")
      refreshMv("mv_topk_8")
      assertMvCorrect(
        "mv_topk_8",
        "SELECT id, region, amount FROM sales_t8 ORDER BY amount DESC, id ASC LIMIT 3"
      )
    }
  }

  // ── (9) Empty initial table → INSERT → REFRESH ───────────────────────────

  describe("(9) Empty initial table → INSERT → REFRESH") {
    it("MV starts empty and reflects inserts after a refresh") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t9(id INT, region STRING, amount INT) USING DELTA"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_topk_9 AS " +
          "SELECT id, region, amount FROM sales_t9 ORDER BY amount DESC LIMIT 2"
      )
      spark.table("mv_topk_9").count() shouldBe 0L
      spark.sql("INSERT INTO sales_t9 VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
      refreshMv("mv_topk_9")
      assertMvCorrect(
        "mv_topk_9",
        "SELECT id, region, amount FROM sales_t9 ORDER BY amount DESC LIMIT 2"
      )
    }
  }
}
