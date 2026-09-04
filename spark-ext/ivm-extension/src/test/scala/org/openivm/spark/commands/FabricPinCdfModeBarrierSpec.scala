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

/** Real `changeFeed.mode = cdf` proof of the authoritative direct managed-MV
  * dependency barrier.
  *
  * Under CDF mode change propagation is backed by the upstream MV's OWN Delta
  * change feed (`requiresMvCdf`), NOT the intercept staging catalog. The MV data
  * tables are created with `delta.enableChangeDataFeed = true`, so an upstream
  * pinned MV refresh publishes a CDF-visible Delta commit at the boundary BEFORE
  * its post-apply pinned-source identity gate. This is exactly the window the
  * consumer-visibility barrier must close: a downstream CDF consumer
  * (`collectChanges` under the refresh lock) must not observe that intermediate,
  * about-to-be-restored commit.
  *
  * The barrier keys are taken from the downstream MV's AUTHORITATIVE persisted
  * dependency map — resolved at CREATE by physical Delta identity, never by
  * lexical source-name matching — so the same barrier engages regardless of the
  * qualifier form the downstream used to reference the upstream MV
  * ([[MvCommandHelper.resolveDirectMvDependencies]] /
  * [[MvCommandHelper.resolveUpstreamMvDependencyLockKeys]]).
  */
class FabricPinCdfModeBarrierSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val db = "cdf_mode_barrier_db"

  private val warehouseDir: String = {
    val directory = new File(s"target/test-warehouse-cdf-mode-barrier-${UUID.randomUUID().toString.take(8)}")
    directory.mkdirs()
    directory.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // A dedicated session pinned to cdf mode. `changeFeed.mode` is read from the
    // immutable SparkConf at session build, so it cannot be toggled per-test;
    // ivmExtension specs run sequentially in one forked JVM (Test /
    // parallelExecution := false) and each clears its session in afterAll, so
    // this cdf-mode session never collides with the intercept-mode specs.
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("openivm-spark-FabricPinCdfModeBarrierSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.openivm.changeFeed.mode", "cdf")
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

  private def total(mv: String): Long =
    spark.sql(s"SELECT COALESCE(SUM(total), -1) FROM $mv WHERE id = 1").collect().head.getLong(0)

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

  private def awaitTrue(budget: FiniteDuration)(predicate: => Boolean): Boolean = {
    val deadline = System.nanoTime() + budget.toNanos
    while (!predicate && System.nanoTime() < deadline) Thread.sleep(25L)
    predicate
  }

  /** Physical Delta identity `(normalized dataPath, metadata.id)` of a managed
    * MV's backing table, read the same way the production barrier reads it. */
  private def backingIdentity(mvName: String): (String, String) =
    MvCommandHelper
      .mvBackingIdentity(spark, lookup(mvName))
      .getOrElse(fail(s"$mvName has no readable backing Delta identity"))

  /** The parsed authoritative dependency map persisted on `mvName`. */
  private def dependencyMap(mvName: String): Seq[MvCommandHelper.DirectMvDependency] =
    MvCommandHelper.readMvDependencyMap(lookup(mvName).properties) match {
      case Right(deps)  => deps
      case Left(reason) => fail(s"$mvName has no well-formed dependency map: $reason")
    }

  /** A CDF-enabled base source, a pinned upstream MV over it, and a downstream MV
    * referencing that upstream by `downstreamUpstreamRef` (a possibly
    * schema/catalog-qualified spelling of `upMv`). Driven to a committed steady
    * state of total = 15. */
  private def createPinnedUpstream(
      pinned: String,
      live: String,
      upMv: String,
      downMv: String,
      downstreamUpstreamRef: String
  ): Unit = {
    spark.sql(
      s"CREATE TABLE $db.$pinned(id INT, grp STRING) USING DELTA " +
        s"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )
    spark.sql(
      s"CREATE TABLE $db.$live(id INT, amount INT) USING DELTA " +
        s"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )
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
    spark.sql(s"CREATE MATERIALIZED VIEW $downMv AS SELECT id, total FROM $downstreamUpstreamRef").collect()

    spark.sql(s"INSERT INTO $db.$live VALUES (1, 5)")
    spark.sql(s"REFRESH MATERIALIZED VIEW $upMv").collect()
    spark.sql(s"REFRESH MATERIALIZED VIEW $downMv").collect()
    total(upMv) shouldBe 15L
    total(downMv) shouldBe 15L
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  describe("Fabric pinned-MV consumer-visibility barrier under changeFeed.mode = cdf") {

    it(
      "blocks a downstream CDF consumer while a pinned upstream MV holds a CDF-visible intermediate commit, " +
        "then lets it consume only the restored frontier after the upstream rebind rolls back"
    ) {
      createPinnedUpstream("a_pinned", "a_live", "a_up_mv", "a_down_mv", "a_up_mv")

      val preRejectUpVersion = DeltaTableVersion.requireLatest(spark, lookup("a_up_mv").location)
      spark.sql(s"INSERT INTO $db.a_live VALUES (1, 7)")

      val barrier = ParkedCommandBarrier.forObservation(ObservationBudget)
      CommandConcurrencyInjection.withBeforeRefreshFinalize(barrier.park()) {
        withPool(2) { implicit ec =>
          barrier.use {
            // Upstream applies its Delta mutation — its change feed now carries the
            // intermediate total = 22 — and parks at the boundary BEFORE the gate,
            // still holding the a_up_mv refresh lock.
            val upstream = Future(spark.sql("REFRESH MATERIALIZED VIEW a_up_mv").collect())
            withClue(s"upstream must reach the pre-gate hook (${barrier.describe}): ") {
              barrier.awaitEntered() shouldBe true
            }
            val queuedBefore = OpenIvmMetrics.RefreshQueued.get()

            // The downstream CDF consumer must not collect the intermediate change
            // feed: it has to queue on the shared a_up_mv lock the parked upstream
            // holds.
            val downstream = Future(spark.sql("REFRESH MATERIALIZED VIEW a_down_mv").collect())
            withClue("downstream CDF refresh must queue on the shared upstream MV lock: ") {
              awaitTrue(ObservationBudget)(OpenIvmMetrics.RefreshQueued.get() > queuedBefore) shouldBe true
            }
            assertStillParked(barrier, upstream)
            downstream.isCompleted shouldBe false
            total("a_down_mv") shouldBe 15L

            spark.sql(s"DROP TABLE $db.a_pinned")
            spark.sql(
              s"CREATE TABLE $db.a_pinned(id INT, grp STRING) USING DELTA " +
                s"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
            )
            spark.sql(s"INSERT INTO $db.a_pinned VALUES (1, 'x')")
            barrier.release()

            intercept[SourceIdentityRebindingException](awaitResult(upstream))
            awaitResult(downstream)
          }
        }
      }

      total("a_up_mv") shouldBe 15L
      val restoredVersion = DeltaTableVersion.requireLatest(spark, lookup("a_up_mv").location)
      restoredVersion should be > preRejectUpVersion

      // The downstream consumer never consumed the intermediate total = 22; it is
      // consistent with the restored upstream frontier.
      total("a_down_mv") shouldBe 15L
      total("a_down_mv") shouldBe total("a_up_mv")
    }

    it("lets the downstream CDF consumer advance to the committed frontier after a successful upstream refresh") {
      createPinnedUpstream("b_pinned", "b_live", "b_up_mv", "b_down_mv", "b_up_mv")
      spark.sql(s"INSERT INTO $db.b_live VALUES (1, 7)")

      val barrier = ParkedCommandBarrier.forObservation(ObservationBudget)
      CommandConcurrencyInjection.withBeforeRefreshFinalize(barrier.park()) {
        withPool(2) { implicit ec =>
          barrier.use {
            val upstream = Future(spark.sql("REFRESH MATERIALIZED VIEW b_up_mv").collect())
            withClue(s"upstream must reach the pre-gate hook (${barrier.describe}): ") {
              barrier.awaitEntered() shouldBe true
            }
            val queuedBefore = OpenIvmMetrics.RefreshQueued.get()
            val downstream   = Future(spark.sql("REFRESH MATERIALIZED VIEW b_down_mv").collect())
            withClue("downstream CDF refresh must queue on the shared upstream MV lock: ") {
              awaitTrue(ObservationBudget)(OpenIvmMetrics.RefreshQueued.get() > queuedBefore) shouldBe true
            }
            assertStillParked(barrier, upstream)
            downstream.isCompleted shouldBe false

            barrier.release()
            awaitResult(upstream)
            awaitResult(downstream)
          }
        }
      }

      total("b_up_mv") shouldBe 22L
      total("b_down_mv") shouldBe 22L
      total("b_down_mv") shouldBe total("b_up_mv")
    }

    it(
      "persists an authoritative identity map that binds a schema/catalog-qualified upstream reference to the same " +
        "canonical lock key, and an explicit empty map for a base-only MV"
    ) {
      val currentDb = spark.catalog.currentDatabase
      // The downstream references the upstream MV by a fully catalog/schema
      // qualified name — a different lexical spelling of the same physical Delta
      // table. Authoritative resolution binds it to the SAME canonical key.
      createPinnedUpstream(
        "c_pinned",
        "c_live",
        "c_up_mv",
        "c_down_mv",
        s"spark_catalog.$currentDb.c_up_mv"
      )

      // Base-only upstream: explicit EMPTY map positively proves base-only (not a
      // legacy MV that never persisted one).
      dependencyMap("c_up_mv") shouldBe empty
      lookup("c_up_mv").properties.keySet should contain(MvCommandHelper.MvDependencyMapPropertyKey)

      // Downstream: exactly one dependency, keyed by the canonical MV name and
      // carrying the upstream's physical Delta identity — resolved from the
      // qualified reference by physical identity, not by the qualifier text.
      val upKey = MvCommandHelper.metaName(lookup("c_up_mv").name)
      val deps  = dependencyMap("c_down_mv")
      deps.map(_.key) shouldBe Seq(upKey)
      val (path, id) = backingIdentity("c_up_mv")
      deps.head.deltaLogDataPath shouldBe path
      deps.head.deltaTableMetadataId shouldBe id

      // The barrier engages on the qualified-reference downstream exactly as on a
      // plain reference: its resolved upstream lock key is the upstream's own key.
      val (keys, _) = MvCommandHelper.resolveUpstreamMvDependencyLockKeys(spark, TableIdentifier("c_down_mv"))
      keys shouldBe Seq(upKey)

      spark.sql(s"INSERT INTO $db.c_live VALUES (1, 7)")
      val barrier = ParkedCommandBarrier.forObservation(ObservationBudget)
      CommandConcurrencyInjection.withBeforeRefreshFinalize(barrier.park()) {
        withPool(2) { implicit ec =>
          barrier.use {
            val upstream = Future(spark.sql("REFRESH MATERIALIZED VIEW c_up_mv").collect())
            withClue(s"upstream must reach the pre-gate hook (${barrier.describe}): ") {
              barrier.awaitEntered() shouldBe true
            }
            val queuedBefore = OpenIvmMetrics.RefreshQueued.get()
            val downstream   = Future(spark.sql("REFRESH MATERIALIZED VIEW c_down_mv").collect())
            withClue("qualified-reference downstream must queue on the shared upstream MV lock: ") {
              awaitTrue(ObservationBudget)(OpenIvmMetrics.RefreshQueued.get() > queuedBefore) shouldBe true
            }
            assertStillParked(barrier, upstream)
            downstream.isCompleted shouldBe false
            barrier.release()
            awaitResult(upstream)
            awaitResult(downstream)
          }
        }
      }
      total("c_up_mv") shouldBe 22L
      total("c_down_mv") shouldBe 22L
    }

    it(
      "binds an encoded Fabric V1 operational name for the upstream MV to the same canonical lock key by " +
        "physical Delta identity, never lexically"
    ) {
      // base -> e_up_mv, a managed MV whose CDF-enabled backing table is the
      // physical target the encoded operational name will resolve to.
      spark.sql(
        s"CREATE TABLE $db.e_src(id INT, amount INT) USING DELTA " +
          s"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
      )
      spark.sql(s"INSERT INTO $db.e_src VALUES (1, 5)")
      spark
        .sql(s"CREATE MATERIALIZED VIEW e_up_mv AS SELECT id, SUM(amount) AS total FROM `$db`.`e_src` GROUP BY id")
        .collect()

      val upKey          = MvCommandHelper.metaName(lookup("e_up_mv").name)
      val (upPath, upId) = backingIdentity("e_up_mv")

      // The Fabric V1 encoded operational name: an external Delta table registered
      // under a `__fabric_encoded__…` database that points at the SAME physical
      // Delta location as the managed MV's backing table. Fabric presents exactly
      // this shape — an opaque encoded identifier resolving to the MV's storage —
      // and it shares NO lexical token with `e_up_mv`.
      val encodedDb = "__fabric_encoded__arc_sql_db_bi_7d1f"
      spark.sql(s"CREATE DATABASE IF NOT EXISTS `$encodedDb`")
      val encodedRef = s"`$encodedDb`.`e_up_mv_enc`"
      spark.sql(s"CREATE TABLE $encodedRef USING DELTA LOCATION '${lookup("e_up_mv").location}'")

      // The encoded operational name resolves by physical Delta identity to the
      // managed MV's backing identity — name-independent canonicalization.
      MvCommandHelper.deltaPhysicalIdentity(spark, encodedRef) shouldBe Some((upPath, upId))

      // The authoritative CREATE-time resolver maps that encoded operational name
      // to the canonical upstream MV key; the strings never match lexically.
      val deps =
        MvCommandHelper.resolveDirectMvDependencies(spark, TableIdentifier("e_enc_down_mv"), Seq(encodedRef))
      deps.map(_.key) shouldBe Seq(upKey)
      deps.head.deltaLogDataPath shouldBe upPath
      deps.head.deltaTableMetadataId shouldBe upId
    }

    it("keeps only DIRECT upstream MV keys (no transitive over-locking) and independent base MVs concurrent") {
      // A three-level chain base -> l1_mv -> l2_mv -> l3_mv. l3_mv's map records
      // ONLY its direct upstream l2_mv (never the transitive l1_mv), proving the
      // sorted multi-key acquisition stays minimal and deadlock-free.
      spark.sql(
        s"CREATE TABLE $db.chain_src(id INT, amount INT) USING DELTA " +
          s"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
      )
      spark.sql(s"INSERT INTO $db.chain_src VALUES (1, 3)")
      spark
        .sql(s"CREATE MATERIALIZED VIEW l1_mv AS SELECT id, SUM(amount) AS total FROM `$db`.`chain_src` GROUP BY id")
        .collect()
      spark.sql(s"CREATE MATERIALIZED VIEW l2_mv AS SELECT id, total FROM l1_mv").collect()
      spark.sql(s"CREATE MATERIALIZED VIEW l3_mv AS SELECT id, total FROM l2_mv").collect()

      dependencyMap("l1_mv") shouldBe empty
      dependencyMap("l2_mv").map(_.key) shouldBe Seq(MvCommandHelper.metaName(lookup("l1_mv").name))
      dependencyMap("l3_mv").map(_.key) shouldBe Seq(MvCommandHelper.metaName(lookup("l2_mv").name))

      spark.sql(s"INSERT INTO $db.chain_src VALUES (1, 30)")
      // Refreshing the whole chain acquires only direct-upstream keys per hop and
      // must not deadlock (sorted, reentrant withLocks).
      spark.sql("REFRESH MATERIALIZED VIEW l1_mv").collect()
      spark.sql("REFRESH MATERIALIZED VIEW l2_mv").collect()
      spark.sql("REFRESH MATERIALIZED VIEW l3_mv").collect()
      total("l3_mv") shouldBe 33L

      // Two base-only MVs share no MV source: one parked at the finalize boundary
      // must not block the other.
      spark.sql(
        s"CREATE TABLE $db.ind_a(id INT, amount INT) USING DELTA " +
          s"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
      )
      spark.sql(
        s"CREATE TABLE $db.ind_b(id INT, amount INT) USING DELTA " +
          s"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
      )
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
      // the hook then no-ops for every later refresh, letting the independent
      // ind_b_mv through. A global barrier.park() would park BOTH refreshes and
      // could never prove independence.
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
          // finalize boundary (holding only its own ind_a_mv lock).
          val parked = Future(spark.sql("REFRESH MATERIALIZED VIEW ind_a_mv").collect())
          try {
            withClue("the parked refresh must reach the pre-gate hook: ") {
              entered.await(ObservationBudget.toMillis, TimeUnit.MILLISECONDS) shouldBe true
            }
            // The independent MV shares no upstream MV key with the parked
            // ind_a_mv (both base-only, so disjoint {ind_a_mv} / {ind_b_mv} lock
            // sets), so it must refresh to completion while ind_a_mv stays parked.
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
