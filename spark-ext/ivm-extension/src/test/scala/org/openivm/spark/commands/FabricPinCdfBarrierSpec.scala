package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{DeltaTableVersion, MvCatalog, MvMetadata, StagingCatalog}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics
import org.openivm.spark.testkit.{ParkedCommandBarrier, TestPools}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/** Consumer-visibility barrier for a pinned upstream MV and its downstream CDF
  * consumer.
  *
  * An upstream pinned MV refresh publishes its Delta table — making its change
  * feed visible — at the boundary BEFORE its post-apply pinned-source identity
  * gate. Previously an ordinary REFRESH held only its own MV key, so a
  * concurrent downstream CDF/staging refresh could consume that intermediate,
  * about-to-be-restored commit in the window before the upstream detected a
  * source rebind and RESTOREd it, leaving the downstream ahead of a frontier
  * that no longer exists.
  *
  * [[MvCommandHelper.directMvSourceLockKeys]] makes an ordinary refresh also hold
  * every DIRECT upstream MV source key, so the two are mutually exclusive against
  * the shared upstream MV. These tests drive the exact interleaving
  * deterministically: the upstream is parked at the pre-gate boundary
  * ([[CommandConcurrencyInjection.withBeforeRefreshFinalize]]) while a downstream
  * refresh is proven to queue on the shared lock (via the `refresh.queued`
  * gauge) rather than consume the intermediate commit.
  */
class FabricPinCdfBarrierSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val db = "cdf_barrier_db"

  private val warehouseDir: String = {
    val directory = new File(s"target/test-warehouse-cdf-barrier-${UUID.randomUUID().toString.take(8)}")
    directory.mkdirs()
    directory.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("openivm-spark-FabricPinCdfBarrierSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
      deleteDir(new File(warehouseDir))
    } finally super.afterAll()

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  private def withPool[A](parallelism: Int)(body: ExecutionContext => A): A =
    TestPools.withPool(parallelism)(body)

  private def awaitResult[A](future: Future[A], timeout: FiniteDuration = 600.seconds): A =
    Await.result(future, timeout)

  private def lookup(name: String): MvMetadata =
    MvCatalog.lookup(spark, TableIdentifier(name)).getOrElse(fail(s"missing MV metadata for $name"))

  /** SUM of the `total` column for `id = 1` in a materialized view, or `-1` when
    * the view has no such row. */
  private def total(mv: String): Long =
    spark.sql(s"SELECT COALESCE(SUM(total), -1) FROM $mv WHERE id = 1").collect().head.getLong(0)

  /** Generous ceiling for a single command observed while another is parked in a
    * concurrency-injection hook; exceeding it means the observed command is
    * genuinely blocked, not merely slow under an oversubscribed suite. */
  private val ObservationBudget: FiniteDuration = 300.seconds

  private def assertStillParked(barrier: ParkedCommandBarrier, parked: Future[_]): Unit =
    if (!barrier.isParked) {
      val outcome = parked.value
        .map {
          case scala.util.Failure(t) => s"failed with ${t.getClass.getName}: ${t.getMessage}"
          case scala.util.Success(_) => "completed successfully"
        }
        .getOrElse("still running")
      fail(
        s"the parked upstream refresh left the pre-gate hook before the observation finished " +
          s"(${barrier.describe}); it $outcome. The observation is void."
      )
    }

  /** Block until `predicate` holds or `budget` elapses; returns the final value
    * of the predicate. Used to observe the deterministic `refresh.queued`
    * transition rather than race a wall clock. */
  private def awaitTrue(budget: FiniteDuration)(predicate: => Boolean): Boolean = {
    val deadline = System.nanoTime() + budget.toNanos
    while (!predicate && System.nanoTime() < deadline) Thread.sleep(25L)
    predicate
  }

  private def createPinnedUpstream(pinned: String, live: String, upMv: String, downMv: String): Unit = {
    spark.sql(s"CREATE TABLE $db.$pinned(id INT, grp STRING) USING DELTA")
    spark.sql(s"CREATE TABLE $db.$live(id INT, amount INT) USING DELTA")
    spark.sql(s"INSERT INTO $db.$pinned VALUES (1, 'x')")
    spark.sql(s"INSERT INTO $db.$live VALUES (1, 10)")
    val pv = DeltaTableVersion.requireLatest(spark, s"$db.$pinned")
    spark
      .sql(
        s"CREATE MATERIALIZED VIEW $upMv AS " +
          s"SELECT p.id AS id, SUM(live.amount) AS total FROM `$db`.`$pinned` VERSION AS OF $pv AS p " +
          s"JOIN `$db`.`$live` AS live ON p.id = live.id GROUP BY p.id"
      )
      .collect()
    spark.sql(s"CREATE MATERIALIZED VIEW $downMv AS SELECT id, total FROM $upMv").collect()

    // Drive both views to a committed steady state of total = 15.
    spark.sql(s"INSERT INTO $db.$live VALUES (1, 5)")
    spark.sql(s"REFRESH MATERIALIZED VIEW $upMv").collect()
    spark.sql(s"REFRESH MATERIALIZED VIEW $downMv").collect()
    total(upMv) shouldBe 15L
    total(downMv) shouldBe 15L
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  describe("Fabric pinned-MV CDF consumer-visibility barrier") {

    it(
      "blocks a downstream CDF consumer while a pinned upstream MV is mid-refresh, then lets it " +
        "advance against the restored frontier once the upstream rebind rolls back"
    ) {
      createPinnedUpstream("up_pinned", "up_live", "up_mv", "down_mv")

      val preRejectUpVersion = DeltaTableVersion.requireLatest(spark, lookup("up_mv").location)
      // A pending delta the upstream refresh will apply (total 15 -> 22) before
      // its pinned-identity gate runs.
      spark.sql(s"INSERT INTO $db.up_live VALUES (1, 7)")

      val barrier = ParkedCommandBarrier.forObservation(ObservationBudget)
      CommandConcurrencyInjection.withBeforeRefreshFinalize(barrier.park()) {
        withPool(2) { implicit ec =>
          barrier.use {
            // Upstream applies its Delta mutation (its change feed now shows the
            // intermediate total = 22) and parks at the boundary BEFORE the gate,
            // still holding the up_mv refresh lock.
            val upstream = Future(spark.sql("REFRESH MATERIALIZED VIEW up_mv").collect())
            withClue(s"upstream must reach the pre-gate hook (${barrier.describe}): ") {
              barrier.awaitEntered() shouldBe true
            }
            val queuedBefore = OpenIvmMetrics.RefreshQueued.get()

            // The downstream consumer must not observe the intermediate commit:
            // it has to queue on the shared up_mv lock the parked upstream holds.
            val downstream = Future(spark.sql("REFRESH MATERIALIZED VIEW down_mv").collect())
            withClue("downstream refresh must queue on the shared upstream MV lock: ") {
              awaitTrue(ObservationBudget)(OpenIvmMetrics.RefreshQueued.get() > queuedBefore) shouldBe true
            }
            assertStillParked(barrier, upstream)
            downstream.isCompleted shouldBe false
            total("down_mv") shouldBe 15L

            // Rebind the upstream's pinned source (new physical Delta identity),
            // then release: the gate detects the rebind and RESTOREs up_mv.
            spark.sql(s"DROP TABLE $db.up_pinned")
            spark.sql(s"CREATE TABLE $db.up_pinned(id INT, grp STRING) USING DELTA")
            spark.sql(s"INSERT INTO $db.up_pinned VALUES (1, 'x')")
            barrier.release()

            intercept[SourceIdentityRebindingException](awaitResult(upstream))
            awaitResult(downstream)
          }
        }
      }

      // Upstream rolled back to its pre-reject content; its tracked version is
      // synchronized to the (new) RESTORE commit.
      total("up_mv") shouldBe 15L
      val restoredVersion = DeltaTableVersion.requireLatest(spark, lookup("up_mv").location)
      lookup("up_mv").lastVersion shouldBe restoredVersion
      restoredVersion should be > preRejectUpVersion

      // The downstream consumer advanced only against the committed frontier: it
      // never consumed the intermediate total = 22, and matches the restored
      // upstream exactly.
      total("down_mv") shouldBe 15L
      total("down_mv") shouldBe total("up_mv")
    }

    it("releases the upstream refresh lock on a successful refresh so the downstream consumer then advances") {
      createPinnedUpstream("up2_pinned", "up2_live", "up2_mv", "down2_mv")
      spark.sql(s"INSERT INTO $db.up2_live VALUES (1, 7)")

      val barrier = ParkedCommandBarrier.forObservation(ObservationBudget)
      CommandConcurrencyInjection.withBeforeRefreshFinalize(barrier.park()) {
        withPool(2) { implicit ec =>
          barrier.use {
            val upstream = Future(spark.sql("REFRESH MATERIALIZED VIEW up2_mv").collect())
            withClue(s"upstream must reach the pre-gate hook (${barrier.describe}): ") {
              barrier.awaitEntered() shouldBe true
            }
            val queuedBefore = OpenIvmMetrics.RefreshQueued.get()
            val downstream   = Future(spark.sql("REFRESH MATERIALIZED VIEW down2_mv").collect())
            withClue("downstream refresh must queue on the shared upstream MV lock: ") {
              awaitTrue(ObservationBudget)(OpenIvmMetrics.RefreshQueued.get() > queuedBefore) shouldBe true
            }
            assertStillParked(barrier, upstream)
            downstream.isCompleted shouldBe false

            // No rebind: releasing lets the upstream commit successfully and free
            // the lock, so the downstream consumer proceeds and advances.
            barrier.release()
            awaitResult(upstream)
            awaitResult(downstream)
          }
        }
      }

      total("up2_mv") shouldBe 22L
      total("down2_mv") shouldBe 22L
      total("down2_mv") shouldBe total("up2_mv")
    }

    it("keeps independent materialized views concurrent (no upstream over-locking)") {
      spark.sql(s"CREATE TABLE $db.ind_a(id INT, amount INT) USING DELTA")
      spark.sql(s"CREATE TABLE $db.ind_b(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.ind_a VALUES (1, 3)")
      spark.sql(s"INSERT INTO $db.ind_b VALUES (1, 4)")
      spark
        .sql(s"CREATE MATERIALIZED VIEW ind_a_mv AS SELECT id, SUM(amount) AS total FROM `$db`.`ind_a` GROUP BY id")
        .collect()
      spark
        .sql(s"CREATE MATERIALIZED VIEW ind_b_mv AS SELECT id, SUM(amount) AS total FROM `$db`.`ind_b` GROUP BY id")
        .collect()
      spark.sql(s"INSERT INTO $db.ind_a VALUES (1, 30)")
      spark.sql(s"INSERT INTO $db.ind_b VALUES (1, 40)")

      // Park ONLY the first refresh to reach the pre-gate hook. ind_a_mv is
      // launched alone and awaited so it is deterministically that first refresh;
      // the hook then no-ops for every later refresh, letting ind_b_mv through.
      val entered   = new CountDownLatch(1)
      val release   = new CountDownLatch(1)
      val parkFirst = new AtomicBoolean(true)
      CommandConcurrencyInjection.withBeforeRefreshFinalize({
        if (parkFirst.compareAndSet(true, false)) {
          entered.countDown()
          release.await(ObservationBudget.toMillis, TimeUnit.MILLISECONDS)
          ()
        }
      }) {
        withPool(2) { implicit ec =>
          // Launch ind_a_mv ALONE and wait until it is the refresh parked at the
          // finalize boundary (holding only its own ind_a_mv lock). Being the sole
          // in-flight refresh until `entered` fires makes it deterministically the
          // parked one, regardless of thread scheduling.
          val parked = Future(spark.sql("REFRESH MATERIALIZED VIEW ind_a_mv").collect())
          try {
            withClue("the parked refresh must reach the pre-gate hook: ") {
              entered.await(ObservationBudget.toMillis, TimeUnit.MILLISECONDS) shouldBe true
            }
            // Only now start the independent MV. It shares no MV source with the
            // parked ind_a_mv, so it must not queue on ind_a_mv's lock and must
            // refresh to completion while ind_a_mv stays parked.
            val independent = Future(spark.sql("REFRESH MATERIALIZED VIEW ind_b_mv").collect())
            awaitResult(independent)
            parked.isCompleted shouldBe false
          } finally release.countDown()
          awaitResult(parked)
        }
      }

      total("ind_a_mv") shouldBe 33L
      total("ind_b_mv") shouldBe 44L
    }
  }
}
