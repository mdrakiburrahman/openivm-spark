package org.openivm.spark.streaming

import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.time.{Millis, Seconds, Span}

class StreamingTableAvailableNowSpec extends AnyFunSpec with StreamingTableTestFixture with Eventually {

  describe("AvailableNow trigger") {
    it("runs a native Delta stream asynchronously and leaves its checkpoint-owned table query visible to SHOW") {
      val source = "strt_available_source"
      val target = "strt_available_target"
      createSource(source)
      appendRows(source, "(1, 'first', 'p'), (2, 'second', 'p')")

      val status = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source",
        options = Map("trigger" -> "availableNow")
      )

      status.status shouldBe "initializing"
      eventually(timeout(Span(90, Seconds)), interval(Span(100, Millis))) {
        assertBagEqual(
          target,
          "SELECT 1 AS id, 'first' AS value, 'p' AS part UNION ALL SELECT 2, 'second', 'p'"
        )
      }
      val shown = StreamingTableManager.show(spark, None).find(_.tableName == status.tableName).get
      shown.queryId shouldBe status.queryId
      shown.lastFailure shouldBe None
    }
  }
}
