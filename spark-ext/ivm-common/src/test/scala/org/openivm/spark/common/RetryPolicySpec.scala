package org.openivm.spark.common

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger

/** Unit tests for [[RetryPolicy]] and the predefined Delta-conflict pattern set.
  *
  * Verifies that:
  *   1. A success on the first attempt does not retry.
  *   2. A retryable exception triggers up to `maxAttempts - 1` retries with linear backoff.
  *   3. A non-retryable exception propagates immediately.
  *   4. Exhausting all attempts re-throws the last exception.
  *   5. Match logic considers the full cause chain.
  *   6. `RetryPolicy.DeltaConflicts` matches DELTA_CONCURRENT_APPEND, METADATA_CHANGED, etc.
  */
class RetryPolicySpec extends AnyFunSpec with Matchers {

  private val testPolicy: RetryPolicy =
    RetryPolicy(
      patterns = Array("DELTA_TEST_RETRY".r, "transient".r),
      maxAttempts = 3,
      backoffMs = 1L
    )

  describe("RetryPolicy.execute") {
    it("returns the result on first success without retrying") {
      val calls  = new AtomicInteger(0)
      val result = testPolicy.execute { calls.incrementAndGet(); 42 }
      result shouldBe 42
      calls.intValue() shouldBe 1
    }

    it("retries on a matching exception and eventually succeeds") {
      val calls = new AtomicInteger(0)
      val result = testPolicy.execute {
        val n = calls.incrementAndGet()
        if (n < 3) throw new RuntimeException("transient failure on attempt " + n)
        else "ok"
      }
      result shouldBe "ok"
      calls.intValue() shouldBe 3
    }

    it("re-throws after exhausting all attempts") {
      val calls = new AtomicInteger(0)
      val ex = intercept[RuntimeException] {
        testPolicy.execute {
          calls.incrementAndGet()
          throw new RuntimeException("transient — always fails")
        }
      }
      ex.getMessage should include("transient")
      calls.intValue() shouldBe 3
    }

    it("re-throws non-matching exceptions immediately") {
      val calls = new AtomicInteger(0)
      val ex = intercept[IllegalArgumentException] {
        testPolicy.execute {
          calls.incrementAndGet()
          throw new IllegalArgumentException("permanent — bad input")
        }
      }
      ex.getMessage should include("bad input")
      calls.intValue() shouldBe 1
    }

    it("matches a pattern anywhere in the cause chain, not just the outermost exception") {
      val calls = new AtomicInteger(0)
      val result = testPolicy.execute {
        val n = calls.incrementAndGet()
        if (n < 2) {
          val cause = new IllegalStateException("buried transient cause")
          throw new RuntimeException("wrapped", cause)
        } else "ok"
      }
      result shouldBe "ok"
      calls.intValue() shouldBe 2
    }

    it("matches against exception class name as well as message") {
      val classMatchPolicy = RetryPolicy(
        patterns = Array("ArithmeticException".r),
        maxAttempts = 2,
        backoffMs = 1L
      )
      val calls = new AtomicInteger(0)
      val result = classMatchPolicy.execute {
        val n = calls.incrementAndGet()
        if (n == 1) throw new ArithmeticException("division by zero")
        else 7
      }
      result shouldBe 7
      calls.intValue() shouldBe 2
    }
  }

  describe("RetryPolicy.DeltaConflicts predefined policy") {
    it("matches DELTA_CONCURRENT_APPEND error messages (the openivm-spark recurring flake)") {
      val calls = new AtomicInteger(0)
      val result = RetryPolicy.DeltaConflicts.copy(maxAttempts = 2, backoffMs = 1L).execute {
        val n = calls.incrementAndGet()
        if (n == 1)
          throw new RuntimeException(
            "[DELTA_CONCURRENT_APPEND] ConcurrentAppendException: Files were added to the root of the table by a concurrent update"
          )
        else "merged"
      }
      result shouldBe "merged"
      calls.intValue() shouldBe 2
    }

    it("matches DELTA_METADATA_CHANGED, DELTA_PROTOCOL_CHANGED, and DELTA_CONCURRENT_TRANSACTION") {
      val cases = Seq(
        "[DELTA_METADATA_CHANGED] MetadataChangedException: …",
        "[DELTA_PROTOCOL_CHANGED] ProtocolChangedException: …",
        "[DELTA_CONCURRENT_TRANSACTION] ConcurrentTransactionException: …"
      )
      cases.foreach { msg =>
        val calls = new AtomicInteger(0)
        val result = RetryPolicy.DeltaConflicts.copy(maxAttempts = 2, backoffMs = 1L).execute {
          val n = calls.incrementAndGet()
          if (n == 1) throw new RuntimeException(msg)
          else "ok"
        }
        withClue(s"message: $msg — ") {
          result shouldBe "ok"
          calls.intValue() shouldBe 2
        }
      }
    }

    it("matches SparkFileNotFoundException (cached-plan file-deletion races during teardown)") {
      val calls = new AtomicInteger(0)
      val result = RetryPolicy.DeltaConflicts.copy(maxAttempts = 2, backoffMs = 1L).execute {
        val n = calls.incrementAndGet()
        if (n == 1)
          throw new RuntimeException("org.apache.spark.SparkFileNotFoundException: foo.parquet not found")
        else "ok"
      }
      result shouldBe "ok"
      calls.intValue() shouldBe 2
    }
  }

  describe("DeltaRetrySupport trait + RetryExtensions implicit") {
    import org.openivm.spark.common.RetryExtensions._
    object Mix extends DeltaRetrySupport
    it("DeltaRetrySupport.withDeltaRetry delegates to RetryPolicy.DeltaConflicts") {
      val calls = new AtomicInteger(0)
      val ex = intercept[RuntimeException] {
        Mix.withDeltaRetry {
          calls.incrementAndGet()
          throw new RuntimeException("[DELTA_CONCURRENT_APPEND] always fails")
        }
      }
      ex.getMessage should include("DELTA_CONCURRENT_APPEND")
      // 5 attempts at 1s, 2s, 3s, 4s linear backoff = retried 4 times before giving up.
      calls.intValue() shouldBe RetryPolicy.DefaultMaxAttempts
    }

    it("RetryExtensions.RetryableOps.withDeltaRetry exposes the same policy") {
      val calls  = new AtomicInteger(0)
      val result = { calls.incrementAndGet(); 1 }.withDeltaRetry
      result shouldBe 1
      calls.intValue() shouldBe 1
    }

    it("RetryExtensions.RetryableOps.withDeltaRetry rethrows non-Delta errors immediately") {
      val calls = new AtomicInteger(0)
      val operation: () => Int = () => {
        calls.incrementAndGet()
        throw new RuntimeException("Not a Delta error — bad input")
      }
      val ex = intercept[RuntimeException] { operation().withDeltaRetry }
      ex.getMessage should include("Not a Delta error")
      calls.intValue() shouldBe 1
    }

    it("RetryExtensions.RetryableOps.withRetry honours a caller-supplied policy") {
      val calls  = new AtomicInteger(0)
      val policy = RetryPolicy(Array("Fail".r), maxAttempts = 2, backoffMs = 1L)
      val operation: () => Int = () => {
        calls.incrementAndGet()
        throw new RuntimeException("Fail always")
      }
      intercept[RuntimeException] { operation().withRetry(policy) }
      calls.intValue() shouldBe 2
    }
  }
}
