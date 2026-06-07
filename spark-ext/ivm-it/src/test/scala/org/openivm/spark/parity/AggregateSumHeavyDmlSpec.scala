package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy carve-out of `AggregateSumSpec.scala`'s aggsum_sales 9-cycle DML walk
  * (~5m).  Lives in its own forked JVM so the rest of the parity suite is not
  * blocked by this monster test.
  *
  * Table / MV names are prefixed `aggsumheavy_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
class AggregateSumHeavyDmlSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-aggsum-heavy-dml-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-AggregateSumHeavyDmlSpec")
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
    * expected SQL expression. ARRAY columns must be passed in `arrayCols`
    * so they are JSON-serialised before the comparison.
    */
  private def assertMvCorrect(
      mvName: String,
      expectedSql: String,
      arrayCols: Set[String] = Set.empty
  ): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq

    def project(df: DataFrame): DataFrame = {
      val exprs = userCols.map { c =>
        if (arrayCols.contains(c)) s"to_json(`$c`) AS `$c`"
        else s"`$c`"
      }
      df.selectExpr(exprs: _*)
    }

    val expectedProj = project(expected)
    val mv           = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    val mvProj       = project(mv)

    withClue(s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      mvProj.exceptAll(expectedProj).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
      expectedProj.exceptAll(mvProj).count() shouldBe 0L
    }
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  describe("basic aggregate — aggsumheavy_sales(region, SUM(amount), COUNT(amount))") {
    it("INSERT/DELETE/batched/no-op operations all keep the MV in sync") {
      spark.sql("CREATE TABLE aggsumheavy_sales (id INT, region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO aggsumheavy_sales VALUES (1, 'east', 100), (2, 'west', 200), (3, 'east', 150)")
      val viewBody =
        "SELECT region, SUM(amount) AS total, COUNT(amount) AS cnt FROM aggsumheavy_sales GROUP BY region"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsumheavy_sales_summary AS $viewBody")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Insert into existing group
      spark.sql("INSERT INTO aggsumheavy_sales VALUES (4, 'east', 50)")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Insert into new group
      spark.sql("INSERT INTO aggsumheavy_sales VALUES (5, 'north', 300)")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Delete from existing group
      spark.sql("DELETE FROM aggsumheavy_sales WHERE id = 1")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Multiple inserts in same group
      spark.sql("INSERT INTO aggsumheavy_sales VALUES (6, 'west', 75), (7, 'west', 25)")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // No-op refresh
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Batch insert into multiple groups at once
      spark.sql("INSERT INTO aggsumheavy_sales VALUES (10, 'south', 100), (11, 'south', 200), (12, 'north', 50)")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Mixed insert + delete in same cycle
      spark.sql("INSERT INTO aggsumheavy_sales VALUES (13, 'west', 100)")
      spark.sql("DELETE FROM aggsumheavy_sales WHERE id = 6")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Delete ALL rows from a group
      spark.sql("DELETE FROM aggsumheavy_sales WHERE region = 'east'")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)
    }
  }
}
