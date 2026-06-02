package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Depth-2 cascade regression for a GROUP_RECOMPUTE upstream after openivm
  * `4471f4e929fd3b21ac55ea0c47249d4716853c98` started emitting
  * `openivm_delta_<view>` from recompute paths whenever
  * `force_view_delta_cascade=true` is set in the CompileFacts payload (which
  * openivm-spark always sets).
  */
class ChainedGroupRecomputeCascadeSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-cgrc-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ChainedGroupRecomputeCascadeSpec")
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

  private def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => spark.sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  private def mvRefreshTypeName(name: String): String = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshTypeName
  }

  describe("GROUP_RECOMPUTE upstream → SIMPLE_PROJECTION downstream") {
    it("keeps the downstream incremental across a no-op recompute plus two refresh batches") {
      spark.sql("CREATE TABLE IF NOT EXISTS cgrc_src(id INT, region STRING, amount INT) USING DELTA")
      spark.sql(
        "INSERT INTO cgrc_src VALUES " +
          "(1,'east',10), (2,'east',10), (3,'east',20), (4,'west',30)"
      )

      val upstreamSql =
        "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM cgrc_src GROUP BY region"
      val downstreamSql =
        s"SELECT region, total_distinct FROM ($upstreamSql) cgrc_expected"

      spark.sql(s"CREATE MATERIALIZED VIEW cgrc_totals AS $upstreamSql")
      spark.sql("CREATE MATERIALIZED VIEW cgrc_totals_proj AS SELECT region, total_distinct FROM cgrc_totals")

      mvRefreshType("cgrc_totals") shouldBe RefreshTypeCode.GroupRecompute
      refreshChain("cgrc_totals", "cgrc_totals_proj")
      mvRefreshTypeName("cgrc_totals_proj") should not equal "FULL_REFRESH"
      assertMvCorrect("cgrc_totals_proj", downstreamSql)

      // No-op recompute: old rows == new rows for the affected region.
      spark.sql("UPDATE cgrc_src SET amount = 20 WHERE id = 3")
      refreshChain("cgrc_totals", "cgrc_totals_proj")
      assertMvCorrect("cgrc_totals_proj", downstreamSql)

      // Batch 1: INSERT a new distinct value into the existing 'east' group.
      spark.sql("INSERT INTO cgrc_src VALUES (5,'east',40)")
      refreshChain("cgrc_totals", "cgrc_totals_proj")
      assertMvCorrect("cgrc_totals_proj", downstreamSql)

      // Batch 2: UPDATE + DELETE in the same group across the next refresh.
      spark.sql("UPDATE cgrc_src SET amount = 25 WHERE id = 3")
      spark.sql("DELETE FROM cgrc_src WHERE id = 5")
      refreshChain("cgrc_totals", "cgrc_totals_proj")
      assertMvCorrect("cgrc_totals_proj", downstreamSql)
    }
  }
}
