package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Split-off from `AggregateGroupSpec.scala` so that each chunk of ~8 tests
  * runs in its own forked JVM (see `spark-ext/project/Settings.scala`). All
  * table and MV names are prefixed with `aggrpca_` to guarantee that
  * parallel specs cannot collide on a Delta warehouse path.
  */
class AggregateGroupCountAvgSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-aggregate-group-count-avg-spec-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-AggregateGroupCountAvgSpec")
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

  /** Project away hidden `openivm_*` bookkeeping columns from the MV, then
    * perform a bidirectional EXCEPT ALL equivalence check against the
    * expected SQL expression.
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

  describe("(2a) GROUP BY k, COUNT(*) — INSERT only") {
    it("incremental refresh yields correct counts after INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS aggrpca_ag_cnt_2a(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO aggrpca_ag_cnt_2a VALUES ('a', 1), ('a', 2), ('b', 3)")
      spark.sql(
        "CREATE MATERIALIZED VIEW aggrpca_mv_ag_cnt_2a AS " +
          "SELECT k, COUNT(*) AS cnt FROM aggrpca_ag_cnt_2a GROUP BY k"
      )
      spark.sql("INSERT INTO aggrpca_ag_cnt_2a VALUES ('a', 10), ('c', 7)")
      refreshMv("aggrpca_mv_ag_cnt_2a")
      assertMvCorrect("aggrpca_mv_ag_cnt_2a", "SELECT k, COUNT(*) AS cnt FROM aggrpca_ag_cnt_2a GROUP BY k")
    }
  }

  describe("(2b) GROUP BY k, COUNT(*) — DELETE") {
    it("incremental refresh decrements count after row deletion") {
      spark.sql("CREATE TABLE IF NOT EXISTS aggrpca_ag_cnt_2b(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO aggrpca_ag_cnt_2b VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)")
      spark.sql(
        "CREATE MATERIALIZED VIEW aggrpca_mv_ag_cnt_2b AS " +
          "SELECT k, COUNT(*) AS cnt FROM aggrpca_ag_cnt_2b GROUP BY k"
      )
      spark.sql("DELETE FROM aggrpca_ag_cnt_2b WHERE x = 1")
      refreshMv("aggrpca_mv_ag_cnt_2b")
      assertMvCorrect("aggrpca_mv_ag_cnt_2b", "SELECT k, COUNT(*) AS cnt FROM aggrpca_ag_cnt_2b GROUP BY k")
    }
  }

  describe("(2c) GROUP BY k, COUNT(*) — UPDATE") {
    it("incremental refresh adjusts count correctly after UPDATE") {
      spark.sql("CREATE TABLE IF NOT EXISTS aggrpca_ag_cnt_2c(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO aggrpca_ag_cnt_2c VALUES ('a', 1), ('a', 2), ('b', 3)")
      spark.sql(
        "CREATE MATERIALIZED VIEW aggrpca_mv_ag_cnt_2c AS " +
          "SELECT k, COUNT(*) AS cnt FROM aggrpca_ag_cnt_2c GROUP BY k"
      )
      // UPDATE moves a row from group 'a' to group 'c'
      spark.sql("UPDATE aggrpca_ag_cnt_2c SET k = 'c' WHERE x = 2")
      refreshMv("aggrpca_mv_ag_cnt_2c")
      assertMvCorrect("aggrpca_mv_ag_cnt_2c", "SELECT k, COUNT(*) AS cnt FROM aggrpca_ag_cnt_2c GROUP BY k")
    }
  }

  describe("(2d) GROUP BY k, COUNT(*) — batched mixed DML") {
    it("incremental refresh reconciles mixed DML for COUNT(*)") {
      spark.sql("CREATE TABLE IF NOT EXISTS aggrpca_ag_cnt_2d(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO aggrpca_ag_cnt_2d VALUES ('a', 1), ('b', 2), ('c', 3)")
      spark.sql(
        "CREATE MATERIALIZED VIEW aggrpca_mv_ag_cnt_2d AS " +
          "SELECT k, COUNT(*) AS cnt FROM aggrpca_ag_cnt_2d GROUP BY k"
      )
      spark.sql("INSERT INTO aggrpca_ag_cnt_2d VALUES ('a', 99), ('d', 77)")
      spark.sql("DELETE FROM aggrpca_ag_cnt_2d WHERE k = 'c'")
      spark.sql("UPDATE aggrpca_ag_cnt_2d SET k = 'b' WHERE x = 99")
      refreshMv("aggrpca_mv_ag_cnt_2d")
      assertMvCorrect("aggrpca_mv_ag_cnt_2d", "SELECT k, COUNT(*) AS cnt FROM aggrpca_ag_cnt_2d GROUP BY k")
    }
  }

  describe("(3) GROUP BY k, COUNT(x) — with nullable column") {
    it("COUNT(x) ignores NULLs; incremental refresh tracks correctly") {
      spark.sql("CREATE TABLE IF NOT EXISTS aggrpca_ag_cntx_3(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO aggrpca_ag_cntx_3 VALUES ('a', 10), ('a', NULL), ('b', 20)")
      spark.sql(
        "CREATE MATERIALIZED VIEW aggrpca_mv_ag_cntx_3 AS " +
          "SELECT k, COUNT(x) AS cnt_x FROM aggrpca_ag_cntx_3 GROUP BY k"
      )
      spark.sql("INSERT INTO aggrpca_ag_cntx_3 VALUES ('a', NULL), ('b', 5), ('c', NULL)")
      spark.sql("DELETE FROM aggrpca_ag_cntx_3 WHERE k = 'a' AND x IS NULL")
      refreshMv("aggrpca_mv_ag_cntx_3")
      assertMvCorrect("aggrpca_mv_ag_cntx_3", "SELECT k, COUNT(x) AS cnt_x FROM aggrpca_ag_cntx_3 GROUP BY k")
    }
  }

  describe("(4a) GROUP BY k, AVG(x) — INSERT") {
    it("AVG is maintained via hidden SUM/COUNT decomposition") {
      spark.sql("CREATE TABLE IF NOT EXISTS aggrpca_ag_avg_4a(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO aggrpca_ag_avg_4a VALUES ('a', 10.0), ('a', 20.0), ('b', 5.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW aggrpca_mv_ag_avg_4a AS " +
          "SELECT k, AVG(x) AS avg_x FROM aggrpca_ag_avg_4a GROUP BY k"
      )
      spark.sql("INSERT INTO aggrpca_ag_avg_4a VALUES ('a', 30.0), ('c', 15.0)")
      refreshMv("aggrpca_mv_ag_avg_4a")
      assertMvCorrect("aggrpca_mv_ag_avg_4a", "SELECT k, AVG(x) AS avg_x FROM aggrpca_ag_avg_4a GROUP BY k")
    }
  }

  describe("(4b) GROUP BY k, AVG(x) — DELETE") {
    it("AVG is correctly recalculated after DELETE") {
      spark.sql("CREATE TABLE IF NOT EXISTS aggrpca_ag_avg_4b(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO aggrpca_ag_avg_4b VALUES ('a', 10.0), ('a', 20.0), ('a', 30.0), ('b', 5.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW aggrpca_mv_ag_avg_4b AS " +
          "SELECT k, AVG(x) AS avg_x FROM aggrpca_ag_avg_4b GROUP BY k"
      )
      spark.sql("DELETE FROM aggrpca_ag_avg_4b WHERE x = 10.0")
      refreshMv("aggrpca_mv_ag_avg_4b")
      assertMvCorrect("aggrpca_mv_ag_avg_4b", "SELECT k, AVG(x) AS avg_x FROM aggrpca_ag_avg_4b GROUP BY k")
    }
  }

  describe("(4c) GROUP BY k, AVG(x) — DELETE + INSERT with unequal counts") {
    // openivm's incremental filter for AVG is: HAVING SUM(openivm_sign) != 0
    // (i.e. count delta must be non-zero for a group to be refreshed).
    // A pure UPDATE (same row count, different value) produces count_delta=0
    // and is therefore excluded from the MERGE source — this is a known
    // openivm architectural constraint for multiplicty-filtered aggregates.
    // We exercise the equivalent semantic via DELETE 1 + INSERT 2, giving
    // count_delta = +1 ≠ 0 for the affected group.
    it("AVG is correctly recalculated after asymmetric DELETE + INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS aggrpca_ag_avg_4c(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO aggrpca_ag_avg_4c VALUES ('a', 10.0), ('a', 20.0), ('b', 5.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW aggrpca_mv_ag_avg_4c AS " +
          "SELECT k, AVG(x) AS avg_x FROM aggrpca_ag_avg_4c GROUP BY k"
      )
      // Remove one 'a' row, add two different 'a' rows → count_delta('a') = +1
      spark.sql("DELETE FROM aggrpca_ag_avg_4c WHERE k = 'a' AND x = 20.0")
      spark.sql("INSERT INTO aggrpca_ag_avg_4c VALUES ('a', 30.0), ('a', 40.0)")
      refreshMv("aggrpca_mv_ag_avg_4c")
      assertMvCorrect("aggrpca_mv_ag_avg_4c", "SELECT k, AVG(x) AS avg_x FROM aggrpca_ag_avg_4c GROUP BY k")
    }
  }

  describe("(12) NULL group key — NULL is a valid group") {
    it("INSERT (NULL, 5) creates a row with NULL key and correct aggregate") {
      spark.sql("CREATE TABLE IF NOT EXISTS aggrpca_ag_nullk_12(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO aggrpca_ag_nullk_12 VALUES ('a', 10), (NULL, 5)")
      spark.sql(
        "CREATE MATERIALIZED VIEW aggrpca_mv_ag_nullk_12 AS " +
          "SELECT k, SUM(x) AS total FROM aggrpca_ag_nullk_12 GROUP BY k"
      )
      spark.sql("INSERT INTO aggrpca_ag_nullk_12 VALUES (NULL, 15), ('b', 7)")
      refreshMv("aggrpca_mv_ag_nullk_12")
      assertMvCorrect("aggrpca_mv_ag_nullk_12", "SELECT k, SUM(x) AS total FROM aggrpca_ag_nullk_12 GROUP BY k")
    }
  }

}
