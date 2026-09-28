package org.openivm.spark.streaming

import org.scalatest.funspec.AnyFunSpec

class StreamingTableValidationSpec extends AnyFunSpec with StreamingTableTestFixture {

  describe("pre-mutation native validation") {
    it("rejects invalid Delta sink options before creating a new target") {
      val source = "strt_validation_source"
      val target = "strt_validation_target"
      createSource(source)

      an[Throwable] should be thrownBy {
        createStreaming(
          target,
          readStream(source),
          s"SELECT id, value, part FROM STREAM $source",
          options = Map("mergeSchema" -> "not-a-boolean")
        )
      }
      spark.catalog.tableExists(target) shouldBe false
    }

    it("does not stop an existing writer when replacement validation fails") {
      val source = "strt_validation_existing_source"
      val target = "strt_validation_existing_target"
      createSource(source)
      val status = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source"
      )
      val active = process(status)

      an[Throwable] should be thrownBy {
        createStreaming(
          target,
          readStream(source),
          s"SELECT id, value, part FROM STREAM $source",
          options = Map("mergeSchema" -> "not-a-boolean")
        )
      }
      active.isActive shouldBe true
      appendRows(source, "(1, 'still-running', 'p')")
      active.processAllAvailable()
      assertBagEqual(target, "SELECT 1 AS id, 'still-running' AS value, 'p' AS part")
    }
  }
}
