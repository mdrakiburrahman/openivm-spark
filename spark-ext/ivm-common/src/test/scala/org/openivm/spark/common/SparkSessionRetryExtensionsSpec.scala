package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/** Unit tests for [[SparkSessionRetryExtensions]].
  *
  * Mirrors `azurearcdata.spark.SparkSessionRetryExtensionsTest` and adds an
  * openivm-spark-specific case for `DELTA_CONCURRENT_APPEND` (the recurring
  * flake from `ConcurrentAppendException`).
  */
class SparkSessionRetryExtensionsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  import SparkSessionRetryExtensions._

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-sse-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("openivm-spark-SparkSessionRetryExtensionsSpec")
      .master("local")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      deleteDir(new File(warehouseDir))
    } finally super.afterAll()
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  describe("spark.sqlWithRetry") {
    it("returns a DataFrame on the happy path") {
      val s = spark
      import s.implicits._
      Seq((1, "a"), (2, "b")).toDF("id", "val").createOrReplaceTempView("sse_t1")
      s.sqlWithRetry("SELECT * FROM sse_t1").count() shouldBe 2L
    }

    it("re-throws on a non-matching analysis error (no retry) — INVALID SQL @#$") {
      val s = spark
      intercept[Exception] {
        s.sqlWithRetry(
          "INVALID SQL @#$",
          retryPatterns = Array("DELTA".r),
          maxAttempts = 3
        ).collect()
      }
    }
  }

  describe("spark.retryOnPatterns") {
    it("retries a by-name operation on a matching exception") {
      val s        = spark
      val attempts = new AtomicInteger(0)
      val result = s.retryOnPatterns(Array("TestErr".r), maxAttempts = 3, attempt = 1) {
        val n = attempts.incrementAndGet()
        if (n < 2) throw new RuntimeException("TestErr") else "ok"
      }
      result shouldBe "ok"
      attempts.intValue() shouldBe 2
    }

    it("respects maxAttempts") {
      val s        = spark
      val attempts = new AtomicInteger(0)
      intercept[RuntimeException] {
        s.retryOnPatterns(Array("Err".r), maxAttempts = 2, attempt = 1) {
          attempts.incrementAndGet()
          throw new RuntimeException("Err")
        }
      }
      attempts.intValue() shouldBe 2
    }

    it("checks the entire exception chain for the matching pattern") {
      val s        = spark
      val attempts = new AtomicInteger(0)
      val result = s.retryOnPatterns(Array("Inner".r), maxAttempts = 3, attempt = 1) {
        val n = attempts.incrementAndGet()
        if (n == 1) throw new RuntimeException("Outer", new RuntimeException("Inner")) else "done"
      }
      result shouldBe "done"
      attempts.intValue() shouldBe 2
    }
  }

  describe("default Delta-conflict patterns on spark.sqlWithRetry") {
    it("retries on DELTA_CONCURRENT_APPEND (the openivm-spark recurring flake)") {
      // We can't easily trigger a real ConcurrentAppendException from a unit
      // test, so we verify the pattern set indirectly: build a custom
      // by-name caller that throws the same message and goes through the
      // same RetryPolicy.DeltaConflicts as `sqlWithRetry`'s default.
      val attempts = new AtomicInteger(0)
      val result = RetryPolicy.DeltaConflicts.copy(maxAttempts = 2, backoffMs = 1L).execute {
        val n = attempts.incrementAndGet()
        if (n == 1)
          throw new RuntimeException(
            "[DELTA_CONCURRENT_APPEND] ConcurrentAppendException: Files were added to the root of the table"
          )
        else "merged"
      }
      result shouldBe "merged"
      attempts.intValue() shouldBe 2
    }
  }
}
