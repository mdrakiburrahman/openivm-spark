package org.openivm.spark.insights

import org.openivm.spark.insights.OpenIvmRequestMetricsCollector.{AccumulatorValue, TaskMetricValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.Properties
import java.util.concurrent.{Callable, Executors, TimeUnit}

class OpenIvmRequestMetricsCollectorSpec extends AnyFunSpec with Matchers {

  describe("application-scoped request metrics") {
    it("correlates job properties and deduplicates stage and task retries") {
      val collector = new OpenIvmRequestMetricsCollector
      collector.begin("metrics-request")

      collector.recordJobStart(1, 100L, Seq(10, 11), properties("other-request"))
      collector.recordJobStart(2, 110L, Seq(20, 21), properties("metrics-request"))

      collector.recordTaskEnd(
        20,
        0,
        0,
        100L,
        0,
        120L,
        successful = true,
        TaskMetricValues(inputRecords = Some(100L), inputBytes = Some(1000L))
      )
      collector.recordStageCompleted(
        20,
        0,
        successful = false,
        Seq(AccumulatorValue(1L, "number of files read", 100L))
      )

      collector.recordTaskEnd(
        20,
        1,
        0,
        101L,
        0,
        130L,
        successful = false,
        TaskMetricValues(inputRecords = Some(999L), inputBytes = Some(9999L))
      )
      collector.recordTaskEnd(
        20,
        1,
        0,
        102L,
        1,
        140L,
        successful = true,
        TaskMetricValues(
          inputRecords = Some(10L),
          inputBytes = Some(100L),
          shuffleReadRecords = Some(4L),
          shuffleReadBytes = Some(40L),
          memorySpillBytes = Some(7L)
        )
      )
      collector.recordTaskEnd(
        20,
        1,
        1,
        103L,
        0,
        145L,
        successful = true,
        TaskMetricValues(
          inputRecords = Some(20L),
          inputBytes = Some(200L),
          outputRecords = Some(8L),
          outputBytes = Some(80L),
          shuffleWriteRecords = Some(5L),
          shuffleWriteBytes = Some(50L),
          diskSpillBytes = Some(9L)
        )
      )
      collector.recordStageCompleted(
        20,
        1,
        successful = true,
        Seq(
          AccumulatorValue(1L, "number of files read", 3L),
          AccumulatorValue(2L, "number of files written", 2L)
        )
      )
      collector.recordStageCompleted(
        21,
        0,
        successful = true,
        Seq(
          AccumulatorValue(1L, "number of files read", 3L),
          AccumulatorValue(3L, "files scanned", 4L)
        )
      )
      collector.recordJobEnd(2, 190L)

      val metrics = collector.finish("metrics-request", 200L)
      metrics.wallDurationMs shouldBe Some(80L)
      metrics.jobCount shouldBe Some(1L)
      metrics.stageCount shouldBe Some(2L)
      metrics.taskCount shouldBe Some(2L)
      metrics.inputRecords shouldBe Some(30L)
      metrics.inputBytes shouldBe Some(300L)
      metrics.outputRecords shouldBe Some(8L)
      metrics.outputBytes shouldBe Some(80L)
      metrics.shuffleReadRecords shouldBe Some(4L)
      metrics.shuffleReadBytes shouldBe Some(40L)
      metrics.shuffleWriteRecords shouldBe Some(5L)
      metrics.shuffleWriteBytes shouldBe Some(50L)
      metrics.memorySpillBytes shouldBe Some(7L)
      metrics.diskSpillBytes shouldBe Some(9L)
      metrics.filesRead shouldBe Some(7L)
      metrics.filesWritten shouldBe Some(2L)
      collector.activeRequestCount shouldBe 0
    }

    it("leaves unavailable task and accumulator metrics absent") {
      val collector = new OpenIvmRequestMetricsCollector
      collector.begin("absent")
      collector.recordJobStart(1, 10L, Seq(1), properties("absent"))
      collector.recordTaskEnd(
        1,
        0,
        0,
        1L,
        0,
        15L,
        successful = true,
        TaskMetricValues()
      )
      collector.recordStageCompleted(
        1,
        0,
        successful = true,
        Seq(AccumulatorValue(1L, "number of output rows", 0L))
      )
      collector.recordJobEnd(1, 20L)

      val metrics = collector.finish("absent", 25L)
      metrics.jobCount shouldBe Some(1L)
      metrics.stageCount shouldBe Some(1L)
      metrics.taskCount shouldBe Some(1L)
      metrics.inputRecords shouldBe None
      metrics.inputBytes shouldBe None
      metrics.outputRecords shouldBe None
      metrics.outputBytes shouldBe None
      metrics.shuffleReadRecords shouldBe None
      metrics.shuffleWriteRecords shouldBe None
      metrics.memorySpillBytes shouldBe None
      metrics.diskSpillBytes shouldBe None
      metrics.filesRead shouldBe None
      metrics.filesWritten shouldBe None
    }

    it("bounds stage accumulator inspection and deduplicates exposed accumulator IDs") {
      val collector = new OpenIvmRequestMetricsCollector
      collector.begin("bounded-accumulators")
      collector.recordJobStart(1, 10L, Seq(1), properties("bounded-accumulators"))
      val withinBound =
        (0 until 4096).map(index => AccumulatorValue(index.toLong, "number of files read", 1L))
      collector.recordStageCompleted(
        1,
        0,
        successful = true,
        withinBound :+ AccumulatorValue(5000L, "number of files written", 99L)
      )
      collector.recordJobEnd(1, 20L)

      val metrics = collector.finish("bounded-accumulators", 25L)
      metrics.filesRead shouldBe Some(4096L)
      metrics.filesWritten shouldBe None
    }

    it("isolates concurrent requests and cleans all request ownership") {
      val collector = new OpenIvmRequestMetricsCollector
      collector.begin("concurrent-a")
      collector.begin("concurrent-b")
      val executor = Executors.newFixedThreadPool(2)
      try {
        val requests = Seq("concurrent-a" -> 10, "concurrent-b" -> 20).map { case (requestId, base) =>
          executor.submit(new Callable[Unit] {
            override def call(): Unit = {
              collector.recordJobStart(base, base.toLong, Seq(base), properties(requestId))
              collector.recordTaskEnd(
                base,
                0,
                0,
                base.toLong,
                0,
                base.toLong + 1L,
                successful = true,
                TaskMetricValues(inputRecords = Some(base.toLong))
              )
              collector.recordStageCompleted(base, 0, successful = true, Seq.empty)
              collector.recordJobEnd(base, base.toLong + 5L)
            }
          })
        }
        requests.foreach(_.get(20, TimeUnit.SECONDS))
      } finally executor.shutdownNow()

      collector.finish("concurrent-a", 100L).inputRecords shouldBe Some(10L)
      collector.finish("concurrent-b", 100L).inputRecords shouldBe Some(20L)
      collector.activeRequestCount shouldBe 0

      collector.recordTaskEnd(
        10,
        0,
        1,
        99L,
        0,
        99L,
        successful = true,
        TaskMetricValues(inputRecords = Some(999L))
      )
      collector.finish("concurrent-a", 100L).nonEmpty shouldBe false
    }

    it("keeps the no-request listener path within its constant-time budget") {
      val collector     = new OpenIvmRequestMetricsCollector
      var propertyReads = 0
      val observingProperties = new Properties {
        override def getProperty(key: String): String = {
          propertyReads += 1
          super.getProperty(key)
        }
      }
      val started = System.nanoTime()
      (0 until 100000).foreach { jobId =>
        collector.recordJobStart(jobId, jobId.toLong, Seq(jobId), observingProperties)
        collector.recordJobEnd(jobId, jobId.toLong + 1L)
      }
      val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)

      propertyReads shouldBe 0
      elapsedMs should be < 2000L
      collector.activeRequestCount shouldBe 0
    }
  }

  private def properties(requestId: String): Properties = {
    val result = new Properties
    result.setProperty(OpenIvmInsightsContract.RequestIdProperty, requestId)
    result
  }
}
