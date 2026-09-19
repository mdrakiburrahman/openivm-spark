package org.openivm.spark.streaming

import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.time.{Millis, Seconds, Span}

class StreamingTableFailureSpec extends AnyFunSpec with StreamingTableTestFixture with Eventually {

  describe("native streaming failure propagation") {
    it("surfaces an asynchronous native query failure through SHOW without a fallback write path") {
      val source = "strt_failure_source"
      val target = "strt_failure_target"
      createSource(source)
      spark.udf.register(
        "strt_fail_on_boom",
        (value: String) => if (value == "boom") throw new IllegalStateException("expected streaming failure") else value
      )
      val query = readStream(source).selectExpr("id", "strt_fail_on_boom(value) AS value", "part")
      val status = createStreaming(
        target,
        query,
        s"SELECT id, strt_fail_on_boom(value), part FROM STREAM $source"
      )
      val native = process(status)

      appendRows(source, "(1, 'boom', 'p')")
      an[Throwable] should be thrownBy {
        native.processAllAvailable()
      }
      eventually(timeout(Span(60, Seconds)), interval(Span(100, Millis))) {
        val shown = StreamingTableManager.show(spark, None).find(_.tableName == status.tableName).get
        shown.status shouldBe "failed"
        shown.lastFailure.get should include("expected streaming failure")
      }
    }
  }
}
