package org.openivm.spark.parity

import org.openivm.spark.parity.base.InterceptMode
import org.openivm.spark.common.{FeatureGate, RefreshSqlLogAsyncFlusher}
import org.openivm.spark.common.rocksdb.OpenIvmStateSync
import org.openivm.spark.commands.CommandConcurrencyInjection
import org.openivm.spark.testkit.ParkedCommandBarrier

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.zip.ZipFile
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

/** Intercept-mode entry point for [[ConcurrencyScenarios]]. */
class ConcurrencySpec extends ConcurrencyScenarios with InterceptMode

class ConcurrencyStateSyncSpec extends ConcurrencyScenarios with InterceptMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.StateSyncUriKey          -> new File(warehouseDir, "state-sync-remote").toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey -> "false",
    FeatureGate.ProfileRefreshKey        -> "true",
    FeatureGate.QueryLogEnabledKey       -> "true"
  )

  override protected val RefreshObservationBudget: FiniteDuration = 45.seconds

  it("publishes an unrelated refresh while a CREATE is between data write and metadata") {
    sql("CREATE TABLE state_sync_create_src(id INT) USING DELTA")
    sql("CREATE TABLE state_sync_refresh_src(id INT) USING DELTA")
    sql("INSERT INTO state_sync_create_src VALUES (1)")
    sql("INSERT INTO state_sync_refresh_src VALUES (1)")
    val query = "SELECT id FROM state_sync_refresh_src"
    sql(s"CREATE MATERIALIZED VIEW state_sync_refresh_mv AS $query")
    sql("INSERT INTO state_sync_refresh_src VALUES (2)")
    val commits = new File(warehouseDir, "state-sync-remote/.openivm-state-sync/commits")
    val before  = Option(commits.list()).getOrElse(Array.empty[String]).filter(_.endsWith(".commit")).sorted.last
    val barrier = ParkedCommandBarrier.forObservation(RefreshObservationBudget)
    CommandConcurrencyInjection.withAfterCreateDataWrite(barrier.park()) {
      withPool(2) { implicit ec =>
        barrier.use {
          val create = Future(
            sql("CREATE MATERIALIZED VIEW state_sync_create_mv AS SELECT id FROM state_sync_create_src").collect()
          )
          withClue("CREATE did not reach its post-write boundary: ") {
            barrier.awaitEntered() shouldBe true
          }
          Await.result(Future(refreshMv("state_sync_refresh_mv")), RefreshObservationBudget)
          val after = Option(commits.list()).getOrElse(Array.empty[String]).filter(_.endsWith(".commit")).sorted.last
          after should not be before
          assertStillParked(barrier, create)
          barrier.release()
          Await.result(create, CreateCompletionBudget)
        }
      }
    }
    assertMvCorrect("state_sync_refresh_mv", query)
    assertMvCorrect("state_sync_create_mv", "SELECT id FROM state_sync_create_src")
  }

  it("allows sibling refreshes to hold shared upstream reads with synchronous snapshots") {
    sql("CREATE TABLE state_sync_chain_src(id INT, amount INT) USING DELTA")
    sql("INSERT INTO state_sync_chain_src VALUES (1, 10)")
    sql(
      "CREATE MATERIALIZED VIEW state_sync_upstream AS SELECT id, SUM(amount) AS total FROM state_sync_chain_src GROUP BY id"
    )
    sql("CREATE MATERIALIZED VIEW state_sync_left AS SELECT id, total FROM state_sync_upstream")
    sql("CREATE MATERIALIZED VIEW state_sync_right AS SELECT id, total FROM state_sync_upstream")
    sql("INSERT INTO state_sync_chain_src VALUES (1, 20)")
    refreshMv("state_sync_upstream")
    val entered = new CountDownLatch(2)
    val release = new CountDownLatch(1)
    CommandConcurrencyInjection.withBeforeRefreshApply {
      entered.countDown()
      if (!release.await(120, TimeUnit.SECONDS))
        throw new IllegalStateException("Timed out waiting to release sibling refreshes")
    } {
      withPool(2) { implicit ec =>
        try {
          val left  = Future(refreshMv("state_sync_left"))
          val right = Future(refreshMv("state_sync_right"))
          withClue("sibling refreshes did not enter together: ") {
            entered.await(60, TimeUnit.SECONDS) shouldBe true
          }
          release.countDown()
          Await.result(left, 180.seconds)
          Await.result(right, 180.seconds)
        } finally release.countDown()
      }
    }
    assertMvCorrect("state_sync_left", "SELECT id, total FROM state_sync_upstream")
    assertMvCorrect("state_sync_right", "SELECT id, total FROM state_sync_upstream")
    RefreshSqlLogAsyncFlusher.awaitQuiescence(60000L) shouldBe true
    OpenIvmStateSync.backupNow(spark)
    val snapshots = new File(warehouseDir, "state-sync-remote/.openivm-state-sync/snapshots")
    val newest    = snapshots.listFiles().filter(_.isDirectory).sortBy(_.getName).last
    val archive   = new ZipFile(new File(newest, "snapshot.zip"))
    try {
      val entries = archive.entries().asScala.map(_.getName).toSet
      entries.exists(_.startsWith("refresh_profile/rocksdb/")) shouldBe true
      entries.exists(_.startsWith("refresh_sql_log/rocksdb/")) shouldBe true
    } finally archive.close()
  }
}
