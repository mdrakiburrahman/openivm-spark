package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Split from the original parity spec.  Scope:
  * Depth-2 chains exercised by fan-out, empty-intermediate, and UPDATE-driven DML mutations from `chained.test`.
  *
  * Includes sections: (E), (H), (I).
  */
class ChainedDmlSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-chain-dm-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ChainedDmlSpec")
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
  // (E) Fan-out: a single base table feeds two independent depth-2 chains
  //     (chained.test L326–L450).
  //     events → events_by_user → user_count
  //     events → events_by_type → type_count
  //
  //     Extracted to [[ChainedDmlHeavyConvergenceSpec]] (~3m40 wall, plus the
  //     coupled second-batch DELETE round which shares the same base table /
  //     MV chain so it had to travel with the heavy `it`) so it runs in its
  //     own forked JVM and does not bottleneck the rest of this spec.
  // ──────────────────────────────────────────────────────────────────────────

  // ──────────────────────────────────────────────────────────────────────────
  // (H) Intermediate MV becomes completely empty mid-test
  //     (chained.test L743–L899): chm_empty → chm_l1 → chm_l2.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(H) Two-level chain with an empty intermediate state") {

    it("initial populated state: both MVs match the recomputed view body") {
      spark.sql("CREATE TABLE IF NOT EXISTS chm_empty(grp STRING, val INT) USING DELTA")
      spark.sql("INSERT INTO chm_empty VALUES ('a',10),('b',20)")
      spark.sql(
        "CREATE MATERIALIZED VIEW chm_empty_l1 AS " +
          "SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW chm_empty_l2 AS " +
          "SELECT SUM(total) AS grand FROM chm_empty_l1"
      )
      refreshChain("chm_empty_l1", "chm_empty_l2")
      assertMvCorrect("chm_empty_l1", "SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp")
      assertMvCorrect(
        "chm_empty_l2",
        "SELECT SUM(total) AS grand FROM (SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp) t"
      )
    }

    it("DELETE ALL: intermediate MV empties and the leaf collapses to a NULL grand-total row") {
      spark.sql("DELETE FROM chm_empty")
      refreshChain("chm_empty_l1", "chm_empty_l2")
      assertMvCorrect("chm_empty_l1", "SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp")
      assertMvCorrect(
        "chm_empty_l2",
        "SELECT SUM(total) AS grand FROM (SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp) t"
      )
    }

    it("repopulation: new group bubbles back up through the chain") {
      spark.sql("INSERT INTO chm_empty VALUES ('x',100)")
      refreshChain("chm_empty_l1", "chm_empty_l2")
      assertMvCorrect("chm_empty_l1", "SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp")
      assertMvCorrect(
        "chm_empty_l2",
        "SELECT SUM(total) AS grand FROM (SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (I) Two-level chain exercised by UPDATEs that move rows between groups
  //     (chained.test L902–L1057): chm_update → ch4_l1 → ch4_l2.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(I) Two-level chain with UPDATEs that move data between groups") {

    it("initial INSERTs propagate through both MVs") {
      spark.sql("CREATE TABLE IF NOT EXISTS chm_update(grp STRING, val INT) USING DELTA")
      spark.sql("INSERT INTO chm_update VALUES ('a',10),('a',20),('b',30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW chm_update_l1 AS " +
          "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM chm_update GROUP BY grp"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW chm_update_l2 AS " +
          "SELECT SUM(total) AS grand FROM chm_update_l1"
      )
      refreshChain("chm_update_l1", "chm_update_l2")
      assertMvCorrect(
        "chm_update_l1",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM chm_update GROUP BY grp"
      )
      assertMvCorrect(
        "chm_update_l2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT grp, SUM(val) AS total FROM chm_update GROUP BY grp) t"
      )
    }

    it("UPDATE moves a single row from 'a' to 'b' — propagates through both MVs") {
      spark.sql("UPDATE chm_update SET grp = 'b' WHERE val = 20")
      refreshChain("chm_update_l1", "chm_update_l2")
      assertMvCorrect(
        "chm_update_l1",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM chm_update GROUP BY grp"
      )
      assertMvCorrect(
        "chm_update_l2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT grp, SUM(val) AS total FROM chm_update GROUP BY grp) t"
      )
    }

    it("UPDATE moves every 'b' row to 'c' — 'b' disappears, 'c' appears") {
      spark.sql("UPDATE chm_update SET grp = 'c' WHERE grp = 'b'")
      refreshChain("chm_update_l1", "chm_update_l2")
      assertMvCorrect(
        "chm_update_l1",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM chm_update GROUP BY grp"
      )
      assertMvCorrect(
        "chm_update_l2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT grp, SUM(val) AS total FROM chm_update GROUP BY grp) t"
      )
    }
  }
}
