package org.openivm.spark.telemetry

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.UUID

class OneLakeSpanSinkSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  private val roots = scala.collection.mutable.ArrayBuffer.empty[File]

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    OneLakeSpanSink.resetHealth()
  }

  override protected def afterEach(): Unit = {
    roots.foreach(deleteDir)
    roots.clear()
    OneLakeSpanSink.resetHealth()
    super.afterEach()
  }

  private def newRoot(): File = {
    val root = new File(s"target/openivm-span-sink-${UUID.randomUUID().toString}")
    root.mkdirs() shouldBe true
    roots += root
    root
  }

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  describe("OneLakeSpanSink") {
    it("mirrors a span to its own object and records a published health signal") {
      val root = newRoot()
      val sink = OneLakeSpanSink.forDir(root.toURI.toString, new Configuration())

      noException should be thrownBy sink.write("req-1", "OPENIVM_EXECUTION_SPAN {\"a\":1}")

      val written = Option(root.listFiles()).getOrElse(Array.empty).filter(_.getName.endsWith(".jsonl"))
      written should have size 1
      val body = new String(java.nio.file.Files.readAllBytes(written.head.toPath), StandardCharsets.UTF_8)
      body.trim shouldBe "OPENIVM_EXECUTION_SPAN {\"a\":1}"

      val health = OneLakeSpanSink.health
      health.published shouldBe 1L
      health.dropped shouldBe 0L
      health.lastError shouldBe None
    }

    it("logs-loud-and-continues without throwing when the mirror cannot be published") {
      // A regular file used as the sink's parent directory makes every publish
      // attempt fail (cannot create a child under a file), emulating a OneLake
      // outage. The sink must NOT throw — a telemetry mirror failure must never
      // fail the model that produced the span.
      val blocker = new File(newRoot(), "blocker")
      blocker.createNewFile() shouldBe true
      val unwritableDir = new File(blocker, "child")
      val sink          = OneLakeSpanSink.forDir(unwritableDir.toURI.toString, new Configuration())

      noException should be thrownBy sink.write("req-2", "OPENIVM_EXECUTION_SPAN {\"b\":2}")

      // The drop is explicit and retrievable, never silent.
      val health = OneLakeSpanSink.health
      health.dropped shouldBe 1L
      health.published shouldBe 0L
      health.lastError.isDefined shouldBe true
    }

    it("keeps a cumulative, retrievable health signal across mixed outcomes") {
      val good     = newRoot()
      val goodSink = OneLakeSpanSink.forDir(good.toURI.toString, new Configuration())

      val blocker = new File(newRoot(), "blocker")
      blocker.createNewFile() shouldBe true
      val badSink = OneLakeSpanSink.forDir(new File(blocker, "child").toURI.toString, new Configuration())

      goodSink.write("ok-1", "OPENIVM_EXECUTION_SPAN {\"c\":1}")
      badSink.write("bad-1", "OPENIVM_EXECUTION_SPAN {\"c\":2}")
      goodSink.write("ok-2", "OPENIVM_EXECUTION_SPAN {\"c\":3}")

      val health = OneLakeSpanSink.health
      health.published shouldBe 2L
      health.dropped shouldBe 1L
    }
  }
}
