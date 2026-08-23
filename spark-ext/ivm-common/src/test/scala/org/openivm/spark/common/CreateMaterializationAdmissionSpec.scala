package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class CreateMaterializationAdmissionSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("openivm-spark-CreateMaterializationAdmissionSpec")
      .config("spark.ui.enabled", "false")
      .config(FeatureGate.CreateMaterializationMaxConcurrentKey, "2")
      .getOrCreate()
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }

  describe("CreateMaterializationAdmission") {
    it("admits independent materializations concurrently while bounding the publication phase") {
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
            CreateMaterializationAdmission.withPermit(spark) {
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

    it("releases a permit when a materialization fails") {
      intercept[IllegalStateException] {
        CreateMaterializationAdmission.withPermit(spark) {
          throw new IllegalStateException("expected")
        }
      }
      CreateMaterializationAdmission.withPermit(spark)(42) shouldBe 42
    }
  }

  describe("CreateMaterializationTiming") {
    it("splits a CTAS at the Delta commit boundary and clamps stale commit timestamps") {
      CreateMaterializationTiming.fromDeltaCommit(
        startedAtEpochMs = 1000L,
        totalNanos = TimeUnit.SECONDS.toNanos(10L),
        committedAtEpochMs = 7000L
      ) shouldBe CreateMaterializationTiming(
        totalMs = 10000L,
        dataWriteMs = 6000L,
        hiveCatalogPublicationMs = 4000L
      )

      CreateMaterializationTiming.fromDeltaCommit(
        startedAtEpochMs = 1000L,
        totalNanos = TimeUnit.SECONDS.toNanos(10L),
        committedAtEpochMs = 500L
      ) shouldBe CreateMaterializationTiming(
        totalMs = 10000L,
        dataWriteMs = 0L,
        hiveCatalogPublicationMs = 10000L
      )
    }
  }
}
