package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy carve-out of `FullOuterJoinSpec.scala` §(3) — the cat_sales FULL
  * OUTER JOIN aggregate via Zhang & Larson MERGE walk through matched /
  * unmatched transitions and cross-group transfer (~4m23).  Lives in its own
  * forked JVM so the rest of the parity suite is not blocked by this monster
  * test.
  *
  * Table / MV names are prefixed `foj_heavy_merge_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
class FullOuterJoinHeavyMergeTransitionsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-foj-heavy-merge-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-FullOuterJoinHeavyMergeTransitionsSpec")
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

  /** Bidirectional EXCEPT ALL equivalence check.
    *
    * Projects the MV to the same column set as `expected` first to drop any
    * hidden bookkeeping columns (e.g. `openivm_count_star`, `openivm_match_count`).
    */
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

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  // ============================================================================
  // (3) FULL OUTER JOIN aggregate (Zhang & Larson MERGE)
  //     openivm/test/sql/full_outer_join.test L393–L585
  //
  //     openivm sets `openivm_full_outer_merge = true` (default) for this path.
  //     openivm-spark does not expose a per-session toggle, but the default
  //     behaviour is equivalent — correctness comes from bidirectional EXCEPT ALL.
  // ============================================================================

  describe("(3) FULL OUTER JOIN aggregate (Zhang & Larson MERGE): items ⟗ sales") {
    it("MERGE-style refresh handles matched/unmatched transitions and cross-group transfer") {
      spark.sql("CREATE TABLE IF NOT EXISTS foj_heavy_merge_items(id INT, category STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS foj_heavy_merge_sales(id INT, item_id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO foj_heavy_merge_items VALUES (1, 'A'), (2, 'B'), (3, 'C')")
      spark.sql("INSERT INTO foj_heavy_merge_sales VALUES (1, 1, 100), (2, 1, 200), (3, 5, 300)")

      spark.sql(
        "CREATE MATERIALIZED VIEW foj_heavy_merge_cat_sales AS " +
          "SELECT i.category, SUM(s.amount) AS total, COUNT(s.amount) AS cnt " +
          "FROM foj_heavy_merge_items i FULL OUTER JOIN foj_heavy_merge_sales s ON i.id = s.item_id " +
          "GROUP BY i.category"
      )

      val viewBody =
        "SELECT i.category, SUM(s.amount) AS total, COUNT(s.amount) AS cnt " +
          "FROM foj_heavy_merge_items i FULL OUTER JOIN foj_heavy_merge_sales s ON i.id = s.item_id " +
          "GROUP BY i.category"

      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: insert sale matching B (unmatched-left → matched)
      spark.sql("INSERT INTO foj_heavy_merge_sales VALUES (4, 2, 150)")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: insert unmatched-right sale (NULL group grows)
      spark.sql("INSERT INTO foj_heavy_merge_sales VALUES (5, 99, 400)")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: delete all sales for A (matched → unmatched-left, aggregates → NULL)
      spark.sql("DELETE FROM foj_heavy_merge_sales WHERE item_id = 1")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: delete unmatched-right sale (NULL group shrinks)
      spark.sql("DELETE FROM foj_heavy_merge_sales WHERE id = 3")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: cross-group transfer (add item matching a previously unmatched-right sale)
      spark.sql("INSERT INTO foj_heavy_merge_items VALUES (99, 'X')")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: batch mixed DML on both sides
      spark.sql("INSERT INTO foj_heavy_merge_items VALUES (5, 'D')")
      spark.sql("INSERT INTO foj_heavy_merge_sales VALUES (6, 3, 250), (7, 88, 500)")
      spark.sql("DELETE FROM foj_heavy_merge_sales WHERE id = 4")
      spark.sql("DELETE FROM foj_heavy_merge_items WHERE id = 2")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)
    }
  }
}
