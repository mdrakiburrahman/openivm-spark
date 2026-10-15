package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.funspec.AnyFunSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class StreamingTableCreationRecoverySpec extends AnyFunSpec with StreamingTableTestFixture {

  private def tableProperties(name: String): Map[String, String] =
    spark
      .sql(s"SHOW TBLPROPERTIES `$name`")
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toMap

  private def createTarget(source: String, target: String): StreamingTableStatus =
    createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")

  private def interruptedCreate(source: String, target: String, interruption: String): Unit = {
    val failure = intercept[Exception](createTarget(source, target))
    failure.getMessage should include(interruption)
  }

  describe("interrupted streaming-table creation") {

    it("finishes a target interrupted before its ownership binding instead of refusing the retry") {
      val source = "strt_interrupted_bind_source"
      val target = "strt_interrupted_bind_target"
      createSource(source)
      appendRows(source, "(1, 'a', 'x'), (2, 'b', 'y')")
      StreamingTableMetadata.setAfterTargetCreatedHookForTesting(_ =>
        throw new IllegalStateException("interrupted after CREATE TABLE")
      )
      interruptedCreate(source, target, "interrupted after CREATE TABLE")
      StreamingTableMetadata.setAfterTargetCreatedHookForTesting(_ => ())

      val leftover = tableProperties(target)
      leftover.get(StreamingTableMetadata.OwnerMarkerKey) shouldBe Some(StreamingTableMetadata.OwnerMarkerValue)
      leftover.contains(StreamingTableMetadata.DeltaIdMarkerKey) shouldBe false

      process(createTarget(source, target))
      assertBagEqual(target, s"SELECT id, value, part FROM $source")
      hasDefinition(target) shouldBe true
      tableProperties(target).get(StreamingTableMetadata.TableIdMarkerKey) shouldBe
        leftover.get(StreamingTableMetadata.TableIdMarkerKey)
    }

    it("finishes a bound target interrupted before its manifest was written") {
      val source = "strt_interrupted_manifest_source"
      val target = "strt_interrupted_manifest_target"
      createSource(source)
      appendRows(source, "(1, 'a', 'x')")
      StreamingTableMetadata.setAfterTargetBoundHookForTesting(_ =>
        throw new IllegalStateException("interrupted before the manifest")
      )
      interruptedCreate(source, target, "interrupted before the manifest")
      StreamingTableMetadata.setAfterTargetBoundHookForTesting(_ => ())
      hasDefinition(target) shouldBe false

      process(createTarget(source, target))
      assertBagEqual(target, s"SELECT id, value, part FROM $source")
      hasDefinition(target) shouldBe true
    }

    it("waits for a newly created target to become readable") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val source                                      = "strt_delayed_visibility_source"
      val target                                      = "strt_delayed_visibility_target"
      createSource(source)
      appendRows(source, "(1, 'a', 'x')")
      val restore = new AtomicReference[Option[Future[Boolean]]](None)
      StreamingTableMetadata.setAfterTargetCreatedHookForTesting { _ =>
        val location = new Path(spark.sessionState.catalog.getTableMetadata(TableIdentifier(target)).location)
        val log      = new Path(location, "_delta_log")
        val hidden   = new Path(location, "_delta_log.hidden")
        val fs       = log.getFileSystem(spark.sessionState.newHadoopConf())
        fs.rename(log, hidden) shouldBe true
        restore.set(Some(Future {
          Thread.sleep(300)
          fs.rename(hidden, log)
        }))
      }
      spark.conf.set(StreamingTableMetadata.TargetReadRetryTimeoutMsKey, "5000")
      spark.conf.set(StreamingTableMetadata.TargetReadRetryIntervalMsKey, "25")
      try {
        val status = createTarget(source, target)
        Await.result(restore.get.get, 5.seconds) shouldBe true
        process(status)
        assertBagEqual(target, s"SELECT id, value, part FROM $source")
      } finally {
        spark.conf.unset(StreamingTableMetadata.TargetReadRetryTimeoutMsKey)
        spark.conf.unset(StreamingTableMetadata.TargetReadRetryIntervalMsKey)
      }
    }

    it("still refuses an unbound target that already holds data") {
      val source = "strt_interrupted_data_source"
      val target = "strt_interrupted_data_target"
      createSource(source)
      StreamingTableMetadata.setAfterTargetCreatedHookForTesting(_ =>
        throw new IllegalStateException("interrupted after CREATE TABLE")
      )
      interruptedCreate(source, target, "interrupted after CREATE TABLE")
      StreamingTableMetadata.setAfterTargetCreatedHookForTesting(_ => ())
      appendRows(target, "(9, 'z', 'q')")

      val refusal = intercept[Exception](createTarget(source, target))
      refusal.getMessage should include("ownership marker does not match its current Delta table id")
    }
  }
}
