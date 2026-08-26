package org.openivm.spark.parity

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.commands.CommandConcurrencyInjection
import org.openivm.spark.common.{CdfWatermarkCatalog, FeatureGate, MvCatalog, StagingCatalog}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}
import org.openivm.spark.testkit.ParkedCommandBarrier

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ConcurrentCreateSpec extends ConcurrentCreateScenarios with InterceptMode

abstract class ConcurrentCreateScenarios extends IvmParitySpecBase("concurrent-create") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  private def pathExists(location: String): Boolean = {
    val path = new Path(location)
    path.getFileSystem(spark.sessionState.newHadoopConf()).exists(path)
  }

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
      .config(FeatureGate.CreateCatalogPublicationMaxConcurrentKey, "32")
      .config(FeatureGate.DriverAdmissionEnabledKey, "false")
      .config("spark.openivm.driverAdmission.maxConcurrentHeavyStatements", "1")
      .config("spark.openivm.driverAdmission.minHeapHeadroom", "512m")
    extraConf.foreach { case (k, v) => builder.config(k, v) }
    spark = builder.getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
    CdfWatermarkCatalog.ensureTables(spark)
  }

  describe("concurrent CREATE MATERIALIZED VIEW against a fresh schema") {
    it("creates eight independent leaf MVs at full configured capacity without changing results") {
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

    it("lets a queued IF NOT EXISTS CREATE rerun CTAS after the leader rolls back its unpublished path") {
      val schema   = "cc_retry"
      val mvTable  = "cc_retry_mv"
      val mvName   = s"$schema.$mvTable"
      val viewSql  = s"SELECT id, label FROM $schema.cc_retry_src WHERE id >= 0"
      val location = s"${spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/")}/_ivm/views/$schema/$mvTable"

      sql(s"CREATE DATABASE IF NOT EXISTS $schema")
      sql(s"CREATE TABLE IF NOT EXISTS $schema.cc_retry_src(id INT, label STRING) USING DELTA")
      sql(s"INSERT INTO $schema.cc_retry_src VALUES (1, 'one'), (2, 'two')")

      val createBarrier     = ParkedCommandBarrier.forObservation(180.seconds)
      val injectedFailure   = new AtomicBoolean(false)
      val dataWriteAttempts = new AtomicInteger(0)

      CommandConcurrencyInjection.withBeforeCreateBody(createBarrier.park()) {
        CommandConcurrencyInjection.withBeforeCreateDataWrite({
          if (dataWriteAttempts.incrementAndGet() == 2) {
            pathExists(location) shouldBe false
            DeltaTable.isDeltaTable(spark, location) shouldBe false
          }
        }) {
          CommandConcurrencyInjection.withAfterCreateDataWrite({
            if (injectedFailure.compareAndSet(false, true))
              throw new IllegalStateException("fail before catalog registration")
          }) {
            val pool = Executors.newFixedThreadPool(2)
            try
              createBarrier.use {
                implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
                val firstCreate = Future {
                  intercept[IllegalStateException] {
                    sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewSql").collect()
                  }
                }
                withClue(s"first CREATE never reached the before-create hook (${createBarrier.describe}): ") {
                  createBarrier.awaitEntered() shouldBe true
                }
                val queuedCreate = Future {
                  sql(s"CREATE MATERIALIZED VIEW IF NOT EXISTS $mvName AS $viewSql").collect()
                }
                Thread.sleep(150L)
                queuedCreate.isCompleted shouldBe false
                createBarrier.isParked shouldBe true
                createBarrier.release()

                Await.result(firstCreate, 180.seconds).getMessage should include("before catalog registration")
                Await.result(queuedCreate, 180.seconds)
              }
            finally {
              pool.shutdown()
              pool.awaitTermination(30, TimeUnit.SECONDS)
            }
          }
        }
      }

      dataWriteAttempts.get() shouldBe 2
      pathExists(location) shouldBe true
      DeltaTable.isDeltaTable(spark, location) shouldBe true
      MvCatalog.lookup(spark, TableIdentifier(mvTable, Some(schema))).map(_.location) shouldBe Some(location)
      assertMvCorrect(mvName, viewSql)
    }
  }
}
