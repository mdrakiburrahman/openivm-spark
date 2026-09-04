package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class CreateCatalogPublicationAdmissionSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("openivm-spark-CreateCatalogPublicationAdmissionSpec")
      .config("spark.ui.enabled", "false")
      .config(FeatureGate.CreateCatalogPublicationMaxConcurrentKey, "2")
      .getOrCreate()
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }

  describe("CreateCatalogPublicationAdmission") {
    it("bounds only the named catalog-publication phase") {
      val pool       = Executors.newFixedThreadPool(6)
      val active     = new AtomicInteger(0)
      val maxActive  = new AtomicInteger(0)
      val firstTwo   = new CountDownLatch(2)
      val releaseAll = new CountDownLatch(1)

      def recordMax(value: Int): Unit = {
        var previous = maxActive.get()
        while (value > previous && !maxActive.compareAndSet(previous, value))
          previous = maxActive.get()
      }

      try {
        implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
        val tasks = (1 to 6).map { _ =>
          Future {
            CreateCatalogPublicationAdmission.withPermit(spark) {
              val current = active.incrementAndGet()
              recordMax(current)
              firstTwo.countDown()
              try releaseAll.await(10L, TimeUnit.SECONDS) shouldBe true
              finally active.decrementAndGet()
            }
          }
        }

        firstTwo.await(10L, TimeUnit.SECONDS) shouldBe true
        Thread.sleep(100L)
        active.get() shouldBe 2
        maxActive.get() shouldBe 2
        releaseAll.countDown()
        Await.result(Future.sequence(tasks), 20.seconds)
        maxActive.get() shouldBe 2
      } finally {
        releaseAll.countDown()
        pool.shutdown()
        pool.awaitTermination(30L, TimeUnit.SECONDS)
      }
    }

    it("releases a permit when catalog publication fails") {
      intercept[IllegalStateException] {
        CreateCatalogPublicationAdmission.withPermit(spark) {
          throw new IllegalStateException("expected")
        }
      }
      CreateCatalogPublicationAdmission.withPermit(spark)(42) shouldBe 42
    }
  }
}
