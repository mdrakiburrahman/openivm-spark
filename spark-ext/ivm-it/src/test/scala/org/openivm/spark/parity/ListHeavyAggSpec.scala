package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy-test isolation spin-off of [[ListSpec]] section (1).
  *
  * Extracted into its own spec class so that `Test/testGrouping` runs it in a
  * dedicated forked JVM (per `Settings.parallelForkSettings`), shrinking the
  * crowded host spec's wall-clock.
  *
  * Table / MV names inside this spec are prefixed `lha_*` / `mv_lha_*` so two
  * parallel JVMs (this one and the host `ListSpec`) cannot collide on Delta
  * paths.
  */
class ListHeavyAggSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-list-heavy-agg-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ListHeavyAggSpec")
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

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  describe("(1) array_agg(val) over ARRAY<FLOAT> per group across INSERT/DELETE/mixed") {
    it("LIST aggregate maintained correctly across all DML; classifier = GROUP_RECOMPUTE-ish") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS lha_items(id INT, grp INT, val ARRAY<FLOAT>) USING DELTA"
      )
      spark.sql(
        "INSERT INTO lha_items VALUES " +
          "(1, 1, array(CAST(10.0 AS FLOAT), CAST(10.0 AS FLOAT))), " +
          "(2, 1, array(CAST(20.0 AS FLOAT), CAST(20.0 AS FLOAT))), " +
          "(3, 2, array(CAST(30.0 AS FLOAT), CAST(30.0 AS FLOAT)))"
      )

      val viewBody =
        "SELECT grp, array_sort(array_agg(val)) AS total, COUNT(*) AS n " +
          "FROM lha_items GROUP BY grp"
      spark.sql(s"CREATE MATERIALIZED VIEW mv_lha_list AS $viewBody")

      val rt = mvRefreshType("mv_lha_list")
      withClue(s"observed refreshType=$rt: ") {
        Seq(
          RefreshTypeCode.GroupRecompute,
          RefreshTypeCode.AggregateGroup,
          RefreshTypeCode.FullRefresh
        ) should contain(rt)
      }

      // Initial state — verified via bidirectional EXCEPT ALL with JSON-serialized arrays
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))

      // 1. Insert into existing group (grp=2)
      spark.sql("INSERT INTO lha_items VALUES (4, 2, array(CAST(40.0 AS FLOAT), CAST(40.0 AS FLOAT)))")
      refreshMv("mv_lha_list")
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))

      // 2. Insert new group (grp=3)
      spark.sql("INSERT INTO lha_items VALUES (5, 3, array(CAST(50.0 AS FLOAT), CAST(50.0 AS FLOAT)))")
      refreshMv("mv_lha_list")
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))

      // 3. Delete from existing group (id=1 in grp=1)
      spark.sql("DELETE FROM lha_items WHERE id = 1")
      refreshMv("mv_lha_list")
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))

      // 4. Mandatory stress: mixed INSERT + DELETE in same batch
      spark.sql("INSERT INTO lha_items VALUES (6, 1, array(CAST(100.0 AS FLOAT), CAST(100.0 AS FLOAT)))")
      spark.sql("DELETE FROM lha_items WHERE id = 2")
      refreshMv("mv_lha_list")
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))

      // 5. No-op refresh
      refreshMv("mv_lha_list")
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))
    }
  }
}
