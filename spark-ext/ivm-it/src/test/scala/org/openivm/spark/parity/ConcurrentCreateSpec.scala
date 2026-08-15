package org.openivm.spark.parity

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{CdfWatermarkCatalog, MvCatalog, StagingCatalog}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ConcurrentCreateSpec extends ConcurrentCreateScenarios with InterceptMode

abstract class ConcurrentCreateScenarios extends IvmParitySpecBase("concurrent-create") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def startSpark(extraConf: Map[String, String] = Map.empty): Unit = {
    val builder = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"openivm-spark-$specSlug-$modeLabel")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.openivm.changeFeed.mode", changeFeedMode.value)
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.openivm.driverAdmission.maxConcurrentHeavyStatements", "1")
      .config("spark.openivm.driverAdmission.minHeapHeadroom", "512m")
    extraConf.foreach { case (k, v) => builder.config(k, v) }
    spark = builder.getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
    CdfWatermarkCatalog.ensureTables(spark)
  }

  describe("concurrent CREATE MATERIALIZED VIEW against a fresh schema") {
    it("creates eight independent leaf MVs concurrently and each remains bag-equivalent") {
      val schema = "cc_schema"
      sql(s"CREATE DATABASE IF NOT EXISTS $schema")

      (1 to 8).foreach { idx =>
        sql(s"CREATE TABLE IF NOT EXISTS $schema.cc_src_$idx(id INT, label STRING) USING DELTA")
        val rows = (1 to 10).map(i => s"($i, 'src_${idx}_$i')").mkString(", ")
        sql(s"INSERT INTO $schema.cc_src_$idx VALUES $rows")
      }

      val pool = Executors.newFixedThreadPool(8)
      try {
        implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
        val futures = (1 to 8).map { idx =>
          Future {
            sql(
              s"CREATE MATERIALIZED VIEW $schema.cc_mv_$idx AS " +
                s"SELECT id, label FROM $schema.cc_src_$idx WHERE id >= 0"
            ).collect()
          }
        }
        Await.result(Future.sequence(futures), 180.seconds)
      } finally {
        pool.shutdown()
        pool.awaitTermination(30, TimeUnit.SECONDS)
      }

      (1 to 8).foreach { idx =>
        assertMvCorrect(
          s"$schema.cc_mv_$idx",
          s"SELECT id, label FROM $schema.cc_src_$idx WHERE id >= 0"
        )
      }
    }
  }
}
