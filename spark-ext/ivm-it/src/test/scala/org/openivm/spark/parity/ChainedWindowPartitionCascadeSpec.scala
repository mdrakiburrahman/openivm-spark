package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Depth-2 cascade regression for a WINDOW_PARTITION upstream after openivm
  * `4471f4e929fd3b21ac55ea0c47249d4716853c98` started emitting
  * `openivm_delta_<view>` from recompute paths whenever
  * `force_view_delta_cascade=true` is set in the CompileFacts payload (which
  * openivm-spark always sets).
  */
class ChainedWindowPartitionCascadeSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-cwpc-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ChainedWindowPartitionCascadeSpec")
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

  describe("WINDOW_PARTITION upstream → SIMPLE_PROJECTION downstream") {
    it("keeps the downstream incremental across a no-op recompute plus two refresh batches") {
      spark.sql("CREATE TABLE IF NOT EXISTS cwpc_src(id INT, dept STRING, salary INT) USING DELTA")
      spark.sql(
        "INSERT INTO cwpc_src VALUES " +
          "(1,'eng',100), (2,'eng',200), (3,'eng',300), " +
          "(4,'sales',50), (5,'sales',75)"
      )

      val upstreamSql =
        "SELECT id, dept, salary, " +
          "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary, id) AS rn FROM cwpc_src"
      val downstreamSql =
        s"SELECT id, dept, salary, rn FROM ($upstreamSql) cwpc_expected"

      spark.sql(s"CREATE MATERIALIZED VIEW cwpc_ranked AS $upstreamSql")
      spark.sql("CREATE MATERIALIZED VIEW cwpc_ranked_proj AS SELECT id, dept, salary, rn FROM cwpc_ranked")

      mvRefreshType("cwpc_ranked") shouldBe RefreshTypeCode.WindowPartition
      refreshChain("cwpc_ranked", "cwpc_ranked_proj")
      mvRefreshTypeName("cwpc_ranked_proj") should not equal "FULL_REFRESH"
      assertMvCorrect("cwpc_ranked_proj", downstreamSql)

      // No-op recompute: Spark still stages an UPDATE, but old rows == new rows.
      spark.sql("UPDATE cwpc_src SET salary = 200 WHERE id = 2")
      refreshChain("cwpc_ranked", "cwpc_ranked_proj")
      assertMvCorrect("cwpc_ranked_proj", downstreamSql)

      // Batch 1: INSERT into the already-existing 'eng' partition.
      spark.sql("INSERT INTO cwpc_src VALUES (6,'eng',150)")
      refreshChain("cwpc_ranked", "cwpc_ranked_proj")
      assertMvCorrect("cwpc_ranked_proj", downstreamSql)

      // Batch 2: UPDATE + DELETE in the same partition across the next refresh.
      spark.sql("UPDATE cwpc_src SET salary = 250 WHERE id = 3")
      spark.sql("DELETE FROM cwpc_src WHERE id = 6")
      refreshChain("cwpc_ranked", "cwpc_ranked_proj")
      assertMvCorrect("cwpc_ranked_proj", downstreamSql)
    }
  }
}
