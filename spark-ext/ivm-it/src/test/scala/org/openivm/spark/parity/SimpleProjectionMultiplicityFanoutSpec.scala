package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Repro for the SIMPLE_PROJECTION INSERT multiplicity-loss bug
  * (`.research/OPENIVM-BUG.md`, "fact_watches" failure).
  *
  * The bug manifests when a downstream SIMPLE_PROJECTION MV's source-delta has
  * rows with `|openivm_multiplicity| > 1`. openivm emits such rows whenever a
  * scan-level `aggregate_N` compacts multiple input rows that share the JOIN /
  * projection key (e.g. `dim_customer × syndicated_prospect` LEFT JOIN
  * produces K duplicate rows per (`customer_id`, `effective_timestamp`)).
  *
  * Reference DuckDB-openivm path replicates each row `mul` times using
  * `generate_series(1, mul::BIGINT)`
  * (`.temp/openivm/src/upsert/refresh_compiler.cpp:813-820`). The Spark
  * rewriter before the fix lost everything beyond the first copy because
  * `INSERT INTO mv SELECT cols FROM delta WHERE openivm_multiplicity > 0`
  * inserts one row per physical delta row, regardless of multiplicity.
  *
  * This spec exercises the K-duplicate-per-business-key pattern directly:
  * `dim_*_fan` carries `K=2` rows that share the join-and-output key
  * `(k, kept)` but differ on a non-output column (`dropped`). After
  * scan-level compaction the view-delta carries `multiplicity = 2`, so the
  * MV must end up with `K=2` copies of each `(k, kept)` for both the
  * initial CREATE and every subsequent incremental refresh.
  *
  * Determinism harness: `local[1]` + `shuffle.partitions=1` +
  * `adaptive.enabled=false`, matching `ChainedMinMaxJoinSpec`.
  */
class SimpleProjectionMultiplicityFanoutSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-spmf-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-SimpleProjectionMultiplicityFanoutSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.adaptive.enabled", "false")
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
    withClue(s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (1) New-key insert with K=2 dim fan-out — mult=+2 in view-delta. Without
  //     the multiplicity-expansion fix, the MV ends up with K-1 missing copies
  //     per new (k, kept) group.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(1) SIMPLE_PROJECTION JOIN with K=2 dim fan-out") {

    it("preserves multiplicity on incremental INSERT (mult=+2 → 2 copies)") {
      spark.sql("CREATE TABLE IF NOT EXISTS spmf_src1(k INT, label STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS spmf_dim1(k INT, dropped INT, kept INT) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW spmf_mv1 AS " +
          "SELECT b.k AS k, d.kept AS kept FROM spmf_src1 b JOIN spmf_dim1 d ON b.k = d.k"
      )

      // ── Batch 1: key=1 with K=2 dim duplicates collapsing on (k, kept).
      spark.sql("INSERT INTO spmf_src1 VALUES (1, 'a')")
      spark.sql("INSERT INTO spmf_dim1 VALUES (1, 10, 100), (1, 20, 100)")
      spark.sql("REFRESH MATERIALIZED VIEW spmf_mv1").collect()

      assertMvCorrect(
        "spmf_mv1",
        "SELECT b.k AS k, d.kept AS kept FROM spmf_src1 b JOIN spmf_dim1 d ON b.k = d.k"
      )

      // ── Batch 2: brand-new key=2 with K=2 dim duplicates. Scan-level
      // aggregate compacts (2,200) × 2 into mult=+2; the IE multiplies that
      // through to the view-delta, producing one physical row with
      // openivm_multiplicity=+2. The MV must end up with 2 copies of (2,200).
      spark.sql("INSERT INTO spmf_src1 VALUES (2, 'b')")
      spark.sql("INSERT INTO spmf_dim1 VALUES (2, 10, 200), (2, 20, 200)")
      spark.sql("REFRESH MATERIALIZED VIEW spmf_mv1").collect()

      assertMvCorrect(
        "spmf_mv1",
        "SELECT b.k AS k, d.kept AS kept FROM spmf_src1 b JOIN spmf_dim1 d ON b.k = d.k"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (2) Update of an existing K=2-fanned-out key — the cascade emits a
  //     mult=-2 retract followed by a mult=+2 insert. The DELETE-MERGE side
  //     deletes all K matching MV rows (correct because they all share the
  //     same projection key); the INSERT side must add K=2 copies of the new
  //     projection. Without the fix, MV loses K-1 INSERT copies and finishes
  //     with only 1 copy of the new (k, kept) tuple instead of 2.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(2) SIMPLE_PROJECTION update of a K=2-fanned-out key") {

    it("preserves multiplicity on update (mult=-2 retract + mult=+2 insert)") {
      spark.sql("CREATE TABLE IF NOT EXISTS spmf_src2(k INT, label STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS spmf_dim2(k INT, dropped INT, kept INT) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW spmf_mv2 AS " +
          "SELECT b.k AS k, d.kept AS kept FROM spmf_src2 b JOIN spmf_dim2 d ON b.k = d.k"
      )

      // ── Batch 1: key=1, K=2 dim duplicates with kept=100.
      spark.sql("INSERT INTO spmf_src2 VALUES (1, 'a')")
      spark.sql("INSERT INTO spmf_dim2 VALUES (1, 10, 100), (1, 20, 100)")
      spark.sql("REFRESH MATERIALIZED VIEW spmf_mv2").collect()

      assertMvCorrect(
        "spmf_mv2",
        "SELECT b.k AS k, d.kept AS kept FROM spmf_src2 b JOIN spmf_dim2 d ON b.k = d.k"
      )

      // ── Batch 2: replace K=2 dim rows for k=1 with K=2 new rows kept=999.
      spark.sql("DELETE FROM spmf_dim2 WHERE k = 1")
      spark.sql("INSERT INTO spmf_dim2 VALUES (1, 10, 999), (1, 20, 999)")
      spark.sql("REFRESH MATERIALIZED VIEW spmf_mv2").collect()

      assertMvCorrect(
        "spmf_mv2",
        "SELECT b.k AS k, d.kept AS kept FROM spmf_src2 b JOIN spmf_dim2 d ON b.k = d.k"
      )
    }
  }
}
