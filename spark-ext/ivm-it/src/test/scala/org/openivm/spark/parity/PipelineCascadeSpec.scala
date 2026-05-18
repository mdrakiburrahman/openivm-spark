package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Split from the original parity spec.  Scope:
  * Depth-2 fan-out, upstream cascade, empty-base and group-delete scenarios from `pipeline.test`.
  *
  * Includes sections: (G), (I), (L), (M).
  */
class PipelineCascadeSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-pipe-cs-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-PipelineCascadeSpec")
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

  /** Bidirectional `EXCEPT ALL` equivalence between the MV and the recomputed
    * view body, projecting the MV onto the expected column list to drop any
    * `openivm_*` bookkeeping columns. */
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

  /** Issue refreshes in dependency order: every name in `mvs` is refreshed
    * after the ones before it. */
  private def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => spark.sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  // ──────────────────────────────────────────────────────────────────────────
  // (G) Fan-out: one MV feeds two independent two-level children
  //     pipeline.test L288–L365
  //     sales → region_totals → top_region
  //     sales → region_totals → overall
  // ──────────────────────────────────────────────────────────────────────────

  describe("(G) Fan-out: region_totals feeds top_region and overall (both depth-2)") {

    it("first batch: refresh root then both children — both downstreams converge") {
      spark.sql("CREATE TABLE IF NOT EXISTS plc_sales(region STRING, amount INT) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW plc_region_totals AS " +
          "SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW plc_top_region AS " +
          "SELECT region, total FROM plc_region_totals"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW plc_overall AS " +
          "SELECT SUM(total) AS grand FROM plc_region_totals"
      )
      spark.sql("INSERT INTO plc_sales VALUES ('US',100),('EU',200),('US',300)")

      // Refresh root, then one child, then the other.
      refreshChain("plc_region_totals", "plc_top_region", "plc_overall")

      assertMvCorrect("plc_region_totals", "SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region")
      assertMvCorrect(
        "plc_top_region",
        "SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region"
      )
      assertMvCorrect(
        "plc_overall",
        "SELECT SUM(total) AS grand FROM (SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region) t"
      )
    }

    it("second batch: a new region propagates to both children") {
      spark.sql("INSERT INTO plc_sales VALUES ('JP',1000)")
      refreshChain("plc_region_totals", "plc_top_region", "plc_overall")

      assertMvCorrect("plc_region_totals", "SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region")
      assertMvCorrect(
        "plc_top_region",
        "SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region"
      )
      assertMvCorrect(
        "plc_overall",
        "SELECT SUM(total) AS grand FROM (SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (I) Upstream cascade equivalent — manual upstream-first refresh
  //     pipeline.test L430–L477: up_base → up_l1 → up_l2.
  //     openivm uses `SET openivm_cascade_refresh = 'upstream'`; here we
  //     refresh in dependency order explicitly.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(I) Manual upstream-first order: up_base → up_l1 → up_l2") {

    it("first batch: upstream refreshed before leaf yields correct chained state") {
      spark.sql("CREATE TABLE IF NOT EXISTS plc_up_base(x INT) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW plc_up_l1 AS SELECT SUM(x) AS total FROM plc_up_base"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW plc_up_l2 AS SELECT total * 2 AS doubled FROM plc_up_l1"
      )
      spark.sql("INSERT INTO plc_up_base VALUES (10),(20)")
      refreshChain("plc_up_l1", "plc_up_l2")
      assertMvCorrect("plc_up_l1", "SELECT SUM(x) AS total FROM plc_up_base")
      assertMvCorrect(
        "plc_up_l2",
        "SELECT total * 2 AS doubled FROM (SELECT SUM(x) AS total FROM plc_up_base) t"
      )
    }

    it("second batch: additional row propagates to both MVs") {
      spark.sql("INSERT INTO plc_up_base VALUES (5)")
      refreshChain("plc_up_l1", "plc_up_l2")
      assertMvCorrect("plc_up_l1", "SELECT SUM(x) AS total FROM plc_up_base")
      assertMvCorrect(
        "plc_up_l2",
        "SELECT total * 2 AS doubled FROM (SELECT SUM(x) AS total FROM plc_up_base) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (L) Full chain through 2 levels where base becomes empty then repopulates
  //     pipeline.test L541–L690 (`pipe_empty` block).
  // ──────────────────────────────────────────────────────────────────────────

  describe("(L) Two-level chain that goes empty and back: pipe_empty → pipe_e1 → pipe_e2") {

    it("initial populated state: both MVs match the recomputed view body") {
      spark.sql("CREATE TABLE IF NOT EXISTS plc_pipe_empty(user_id STRING, value INT) USING DELTA")
      spark.sql("INSERT INTO plc_pipe_empty VALUES ('a',100),('b',200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW plc_pipe_e1 AS " +
          "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW plc_pipe_e2 AS " +
          "SELECT SUM(total) AS grand FROM plc_pipe_e1"
      )
      refreshChain("plc_pipe_e1", "plc_pipe_e2")
      assertMvCorrect(
        "plc_pipe_e1",
        "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id"
      )
      assertMvCorrect(
        "plc_pipe_e2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id) t"
      )
    }

    it("DELETE everything: intermediate MV empties and the leaf collapses to NULL grand") {
      spark.sql("DELETE FROM plc_pipe_empty")
      refreshChain("plc_pipe_e1", "plc_pipe_e2")
      assertMvCorrect(
        "plc_pipe_e1",
        "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id"
      )
      assertMvCorrect(
        "plc_pipe_e2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id) t"
      )
    }

    it("repopulate: new group propagates back through both MVs") {
      spark.sql("INSERT INTO plc_pipe_empty VALUES ('x',50)")
      refreshChain("plc_pipe_e1", "plc_pipe_e2")
      assertMvCorrect(
        "plc_pipe_e1",
        "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id"
      )
      assertMvCorrect(
        "plc_pipe_e2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (M) Two-level chain with deletes that wipe a group at the source
  //     pipeline.test L693–L842 (`pipe_up` block — depth-2 portion only).
  //     Refreshed in upstream order; verifies grand sums for INSERT, repeated
  //     INSERT, and DELETE-by-group sequences.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(M) Two-level chain: pipe_up → pipe_up1 → pipe_up2 (upstream-first manual order)") {

    it("first batch: INSERT then refresh in order produces correct chained state") {
      spark.sql("CREATE TABLE IF NOT EXISTS plc_pipe_up(k STRING, v INT) USING DELTA")
      spark.sql("INSERT INTO plc_pipe_up VALUES ('a',10)")
      spark.sql(
        "CREATE MATERIALIZED VIEW plc_pipe_up1 AS " +
          "SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW plc_pipe_up2 AS " +
          "SELECT SUM(total) AS grand FROM plc_pipe_up1"
      )
      refreshChain("plc_pipe_up1", "plc_pipe_up2")
      assertMvCorrect("plc_pipe_up1", "SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k")
      assertMvCorrect(
        "plc_pipe_up2",
        "SELECT SUM(total) AS grand FROM (SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k) t"
      )
    }

    it("second batch: repeated INSERTs across multiple groups propagate") {
      spark.sql("INSERT INTO plc_pipe_up VALUES ('a',5),('b',20)")
      refreshChain("plc_pipe_up1", "plc_pipe_up2")
      assertMvCorrect("plc_pipe_up1", "SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k")
      assertMvCorrect(
        "plc_pipe_up2",
        "SELECT SUM(total) AS grand FROM (SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k) t"
      )
    }

    it("DELETE entire group 'a': group disappears at every level") {
      spark.sql("DELETE FROM plc_pipe_up WHERE k = 'a'")
      refreshChain("plc_pipe_up1", "plc_pipe_up2")
      assertMvCorrect("plc_pipe_up1", "SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k")
      assertMvCorrect(
        "plc_pipe_up2",
        "SELECT SUM(total) AS grand FROM (SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k) t"
      )
    }
  }
}
