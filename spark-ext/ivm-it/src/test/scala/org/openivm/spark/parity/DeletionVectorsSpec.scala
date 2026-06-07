package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{FeatureGate, MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Verifies the OpenIVM Delta deletion-vector MV data-table knob and keeps a
  * retracting SIMPLE_PROJECTION refresh on the deletion-vector-backed path.
  */
class DeletionVectorsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-dv-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    startSpark()
  }

  override def afterAll(): Unit =
    try {
      stopSpark()
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
    }

  private def startSpark(extraConf: Map[String, String] = Map.empty): Unit = {
    val builder = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DeletionVectorsSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
    extraConf.foreach { case (k, v) => builder.config(k, v) }
    spark = builder.getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  private def restartSpark(extraConf: Map[String, String] = Map.empty): Unit = {
    stopSpark()
    startSpark(extraConf)
  }

  private def stopSpark(): Unit =
    if (spark != null) {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      spark = null
    }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def deltaProperties(tableName: String): Map[String, String] = {
    val escaped = tableName.replace("`", "``")
    val props = spark
      .sql(s"DESCRIBE DETAIL `$escaped`")
      .select("properties")
      .head()
      .getAs[Map[String, String]]("properties")
    Option(props).getOrElse(Map.empty)
  }

  private def deletionVectorsProperty(tableName: String): Option[String] =
    deltaProperties(tableName).get("delta.enableDeletionVectors")

  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    withClue(s"$mvName EXCEPT ALL <expected>: ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName: ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  describe("Delta deletion-vector TBLPROPERTIES for MV data tables") {

    it("sets delta.enableDeletionVectors=true when the OpenIVM flag is ON") {
      restartSpark()
      spark.sql("CREATE TABLE dv_users_on(id INT, name STRING) USING DELTA")
      spark.sql("INSERT INTO dv_users_on VALUES (1, 'Alice')")
      spark.sql("CREATE MATERIALIZED VIEW dv_mv_on AS SELECT id, name FROM dv_users_on")

      deletionVectorsProperty("dv_mv_on") should contain("true")
      assertMvCorrect("dv_mv_on", "SELECT id, name FROM dv_users_on")
    }

    it("omits delta.enableDeletionVectors when the OpenIVM flag is OFF") {
      restartSpark(Map(FeatureGate.DeltaEnableDeletionVectorsKey -> "false"))
      spark.sql("CREATE TABLE dv_users_off(id INT, name STRING) USING DELTA")
      spark.sql("INSERT INTO dv_users_off VALUES (1, 'Alice')")
      spark.sql("CREATE MATERIALIZED VIEW dv_mv_off AS SELECT id, name FROM dv_users_off")

      deletionVectorsProperty("dv_mv_off").map(_.toLowerCase(java.util.Locale.ROOT)).contains("true") shouldBe false
      assertMvCorrect("dv_mv_off", "SELECT id, name FROM dv_users_off")
    }

    it("maintains a deletion-vector-backed SIMPLE_PROJECTION MV through retracting DML") {
      restartSpark()
      spark.sql("CREATE TABLE dv_users_retract(id INT, name STRING) USING DELTA")
      spark.sql("INSERT INTO dv_users_retract VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
      spark.sql("CREATE MATERIALIZED VIEW dv_mv_retract AS SELECT id, name FROM dv_users_retract")

      deletionVectorsProperty("dv_mv_retract") should contain("true")

      spark.sql("INSERT INTO dv_users_retract VALUES (4, 'Dave')")
      spark.sql("DELETE FROM dv_users_retract WHERE id = 2")
      refreshMv("dv_mv_retract")

      assertMvCorrect("dv_mv_retract", "SELECT id, name FROM dv_users_retract")
    }
  }
}
