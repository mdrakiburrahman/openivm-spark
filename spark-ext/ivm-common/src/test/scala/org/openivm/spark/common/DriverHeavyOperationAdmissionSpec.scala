package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class DriverHeavyOperationAdmissionSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("openivm-spark-DriverHeavyOperationAdmissionSpec")
      .config("spark.ui.enabled", "false")
      .config(FeatureGate.DriverAdmissionMaxConcurrentKey, "1")
      .getOrCreate()
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }

  describe("DriverHeavyOperationAdmission") {
    it("is a true no-op by default even when maxConcurrent is configured to one") {
      val pool = Executors.newFixedThreadPool(2)
      try {
        implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
        val entered                       = new CountDownLatch(2)
        val tasks = (1 to 2).map { _ =>
          Future {
            DriverHeavyOperationAdmission.withPermit(spark, "test") {
              entered.countDown()
              entered.await(5, TimeUnit.SECONDS) shouldBe true
            }
          }
        }

        Await.result(Future.sequence(tasks), 10.seconds)
      } finally {
        pool.shutdown()
        pool.awaitTermination(30, TimeUnit.SECONDS)
      }
    }
  }
}
