package org.openivm.spark.common

class QueryLogExportLimitsSpec extends QueryLogExportTestBase("query-log-export-limits") {
  override protected def extraConf: Map[String, String] = Map(
    QueryLogExport.ConfigPrefix + "maxCaptures"     -> "4",
    QueryLogExport.ConfigPrefix + "maxInvocations"  -> "2",
    QueryLogExport.ConfigPrefix + "maxRecords"      -> "3",
    QueryLogExport.ConfigPrefix + "maxBytes"        -> "4096",
    QueryLogExport.ConfigPrefix + "maxTotalRecords" -> "4",
    QueryLogExport.ConfigPrefix + "maxTotalBytes"   -> "6144"
  )

  describe("bounded capture retention and snapshots") {
    it("rejects exhausted or duplicate slots instead of evicting an unexported request") {
      (1 to 4).foreach(i => begin(s"slot-$i"))
      intercept[IllegalArgumentException] { begin("overflow") }.getMessage should include("CAPTURE_LIMIT_EXCEEDED")
      intercept[IllegalArgumentException] { begin("slot-1") }.getMessage should include("CAPTURE_EXISTS")
      state("overflow").path("status").asText() shouldBe "missing"
      (1 to 4).foreach(i => state(s"slot-$i").path("status").asText() shouldBe "running")
      intercept[IllegalArgumentException] { QueryLogExport.release(spark, "slot-1") }
    }

    it("bounds native lifecycle metadata") {
      begin("many-invocations")
      (1 to 3).foreach(i => invocation("many-invocations", s"invocation_$i").finish("no_pending_deltas"))
      QueryLogExport.end(spark, "many-invocations", sqlSucceeded = true)
      val result = state("many-invocations")
      result.path("invocations").size() shouldBe 2
      result.path("failure").path("code").asText() shouldBe "CAPTURE_LIMIT_EXCEEDED"
      result.path("capture_complete").asBoolean() shouldBe false
    }

    it("fails explicitly on per-request rows or bytes without admitting a partial successful trace") {
      begin("rows")
      val rows = invocation("rows", "rows_native")
      (1 to 3).foreach(i => rows.accept(row(rows, i)) shouldBe true)
      rows.accept(row(rows, 4)) shouldBe false
      finish("rows", rows)
      state("rows").path("record_count").asInt() shouldBe 3
      state("rows").path("failure").path("code").asText() shouldBe "CAPTURE_LIMIT_EXCEEDED"
      records(state("rows")) shouldBe empty
      begin("bytes")
      val bytes = invocation("bytes", "bytes_native")
      bytes.accept(row(bytes, sql = "é" * 2048)) shouldBe false
      finish("bytes", bytes)
      state("bytes").path("failure").path("code").asText() shouldBe "CAPTURE_LIMIT_EXCEEDED"
    }

    it("bounds aggregate retained rows across requests and returns the reservation on release") {
      begin("total-a")
      val first = invocation("total-a", "total_a_native")
      (1 to 3).foreach(i => first.accept(row(first, i)) shouldBe true)
      finish("total-a", first)
      begin("total-b")
      val second = invocation("total-b", "total_b_native")
      second.accept(row(second)) shouldBe true
      second.accept(row(second, 1)) shouldBe false
      finish("total-b", second)
      state("total-b").path("failure").path("code").asText() shouldBe "CAPTURE_LIMIT_EXCEEDED"
      QueryLogExport.release(spark, "total-a")
      begin("total-c")
      val third = invocation("total-c", "total_c_native")
      third.accept(row(third)) shouldBe true
      finish("total-c", third)
    }

    it("bounds aggregate retained UTF-8 bytes independently of character counts") {
      (1 to 3).foreach { i =>
        val request = s"total-bytes-$i"
        begin(request)
        val capture = invocation(request, s"total_bytes_${i}_native")
        capture.accept(row(capture, sql = "é" * 1100)) shouldBe (i < 3)
        finish(request, capture)
      }
      state("total-bytes-3").path("failure").path("code").asText() shouldBe "CAPTURE_LIMIT_EXCEEDED"
    }

    it("returns explicit snapshot limits, never truncated SQL, and permits a larger bounded retry") {
      begin("output-bound")
      val capture = invocation("output-bound", "output_bound_native")
      append(capture, row(capture, sql = "x" * 800), row(capture, 1, sql = "y" * 800))
      finish("output-bound", capture)
      terminal("output-bound")
      Seq(
        state("output-bound", maxRows = 1),
        state("output-bound", maxBytes = 1600),
        state("output-bound", maxBytes = 2400)
      ).foreach { limited =>
        limited.path("status").asText() shouldBe "failed"
        limited.path("failure").path("code").asText() shouldBe "SNAPSHOT_LIMIT_EXCEEDED"
        limited.path("truncated").asBoolean() shouldBe false
        records(limited) shouldBe empty
      }
      intercept[IllegalArgumentException] { state("output-bound", maxBytes = 1) }
      records(state("output-bound", maxBytes = 8192)).map(_.path("sql_text").asText()) shouldBe
        Vector("x" * 800, "y" * 800)
      state("output-bound").path("status").asText() shouldBe "complete"
    }

    it("detects any row logged after native completion rather than silently changing a completed trace") {
      begin("after-finish")
      val capture = invocation("after-finish", "after_finish_native")
      finish("after-finish", capture, "no_pending_deltas")
      terminal("after-finish")
      capture.accept(row(capture)) shouldBe false
      state("after-finish").path("failure").path("code").asText() shouldBe "LATE_RECORD"
    }
  }
}
