package org.openivm.spark.parity

import org.openivm.spark.parity.base.InterceptMode
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.commands.CommandConcurrencyInjection
import org.openivm.spark.testkit.ParkedCommandBarrier

import java.io.File
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/** Intercept-mode entry point for [[ConcurrencyScenarios]]. */
class ConcurrencySpec extends ConcurrencyScenarios with InterceptMode

class ConcurrencyStateSyncSpec extends ConcurrencyScenarios with InterceptMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.StateSyncUriKey -> new File(warehouseDir, "state-sync-remote").toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey -> "false"
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
    val before = Option(commits.list()).getOrElse(Array.empty[String]).filter(_.endsWith(".commit")).sorted.last
    val barrier = ParkedCommandBarrier.forObservation(RefreshObservationBudget)
    CommandConcurrencyInjection.withAfterCreateDataWrite(barrier.park()) {
      withPool(2) { implicit ec =>
        barrier.use {
          val create = Future(sql("CREATE MATERIALIZED VIEW state_sync_create_mv AS SELECT id FROM state_sync_create_src").collect())
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
}
