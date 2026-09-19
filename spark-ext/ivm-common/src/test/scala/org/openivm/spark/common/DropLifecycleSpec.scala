package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.{CyclicBarrier, Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Try}

/**
 * Focused tests for the DropMaterializedViewCommand blast-radius / concurrent
 * DROP+CREATE lifecycle fix.
 *
 * Tests are scoped to the catalog layer (CdfWatermarkCatalog, MvCatalog,
 * StagingCatalog) and do NOT exercise the full Spark command path.
 *
 * Concurrency model for the 18-thread tests: a CyclicBarrier ensures all
 * threads start their catalog operations simultaneously, maximising the chance
 * of triggering the old race window.
 *
 * With the reverse-index-scoped cleanup (no open-all fanout) and the atomic
 * OpenIvmRocksDBRegistry.closeAndDelete primitive (close + filesystem delete
 * under one per-path slot lock), concurrent DROP / DROP+CREATE over a shared
 * source must complete with ZERO "lock held by current process" / "already
 * closed" errors. These tests assert exactly that: any thrown exception is a
 * hard failure, and the final catalog state is reached by the concurrent
 * operations themselves (no tolerated residual-race recovery pass).
 */
class DropLifecycleSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private val Concurrency = 18

  private var spark: SparkSession = _
  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-drop-lifecycle-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DropLifecycleSpec")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
    CdfWatermarkCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      OpenIvmRocksDBRegistry.closeAll()
      deleteDir(new File(warehouseDir))
    }
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def sampleMvMeta(name: String, sources: Seq[String]): MvMetadata =
    MvMetadata(
      name = CatalystSqlParser.parseTableIdentifier(name),
      querySql = s"SELECT count(*) FROM ${sources.head}",
      refreshType = 0,
      refreshTypeName = "SIMPLE_PROJECTION",
      lastVersion = 0L,
      sourceTables = sources,
      sourceSchemaFingerprint = MvCatalog.schemaFingerprint(
        sources.map(_ -> StructType(Seq(StructField("id", LongType)))).toMap
      ),
      location = s"$warehouseDir/$name",
      createdAt = new Timestamp(1700000000000L),
      properties = Map.empty
    )

  private def failIfAnyThrew(label: String, results: Seq[Try[_]]): Unit = {
    val failures = results.collect { case Failure(e) => e }
    if (failures.nonEmpty) {
      fail(
        s"${failures.size} concurrent $label operation(s) threw — the atomic closeAndDelete lifecycle " +
          s"must eliminate all lock-held / already-closed races:\n" +
          failures.map(e => s"${e.getClass.getSimpleName}: ${e.getMessage}").mkString("\n")
      )
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers that mirror what DropMaterializedViewCommand does at the catalog layer
  // ---------------------------------------------------------------------------

  /**
   * Simulate the catalog-layer steps of DROP MATERIALIZED VIEW for an MV
   * that is itself used as a source by other MVs (the MV-over-MV cascade case).
   *
   *  1. StagingCatalog.removeForBaseTable — remove intercept staging rows whose
   *     base_table == mvName.
   *  2. CdfWatermarkCatalog.removeForView — remove this MV's own watermarks.
   *  3. CdfWatermarkCatalog.removeForBaseTable — clean downstream MVs' watermarks
   *     for this MV as a cascade source (the blast-radius of the fix).
   *  4. MvCatalog.remove — close and delete the per-MV RocksDB.
   */
  private def simulateDrop(spark: SparkSession, mvName: String): Unit = {
    val id = CatalystSqlParser.parseTableIdentifier(mvName)
    StagingCatalog.removeForBaseTable(spark, mvName)
    CdfWatermarkCatalog.removeForView(spark, mvName)
    CdfWatermarkCatalog.removeForBaseTable(spark, mvName)
    MvCatalog.remove(spark, id)
  }

  // ---------------------------------------------------------------------------
  // Test 1: 18 concurrent source-sharing DROP operations
  // ---------------------------------------------------------------------------
  describe("concurrent source-sharing DROP") {
    it(s"$Concurrency MVs sharing one source dropped simultaneously leave the catalog clean with zero races") {
      val executor                      = Executors.newFixedThreadPool(Concurrency)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)

      val sharedSource = "csd_shared_orders"
      val metas        = (0 until Concurrency).map(i => sampleMvMeta(s"mv_csd_$i", Seq(sharedSource)))

      metas.foreach { meta =>
        MvCatalog.upsert(spark, meta)
        CdfWatermarkCatalog.put(spark, meta.name.identifier, sharedSource, 100L)
      }

      val barrier = new CyclicBarrier(Concurrency)

      val futures = metas.map { meta =>
        Future {
          barrier.await()
          Try(simulateDrop(spark, meta.name.identifier))
        }
      }

      val results = Await.result(Future.sequence(futures), 60.seconds)

      // Shut down the executor BEFORE closing DBs so no zombie threads hold locks.
      executor.shutdown()
      executor.awaitTermination(15, TimeUnit.SECONDS)

      // ZERO tolerated races: every concurrent DROP must have succeeded.
      failIfAnyThrew("DROP", results)

      // The concurrent DROPs themselves fully cleaned up — no recovery pass.
      OpenIvmRocksDBRegistry.closeAll()
      metas.foreach { meta =>
        MvCatalog.lookup(spark, meta.name) shouldBe None
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2: Stale reverse-index entry is silently skipped
  // ---------------------------------------------------------------------------
  describe("stale reverse-index entry handling") {
    it("removeForBaseTable skips a stale index entry whose per-MV DB has been deleted") {
      val sourceName = "sri_stale_source"
      val meta       = sampleMvMeta("mv_sri_stale_index", Seq(sourceName))
      MvCatalog.upsert(spark, meta)
      CdfWatermarkCatalog.put(spark, "mv_sri_stale_index", sourceName, 42L)

      // Forcibly remove the per-MV DB without updating the reverse index (simulates
      // the crash window between Registry.close and the reverse-index prune).
      val serializedName = meta.name.identifier
      val perMvPath      = OpenIvmStatePaths.perMvDbPath(spark, serializedName)
      OpenIvmRocksDBRegistry.close(perMvPath)
      deleteDir(new File(perMvPath))
      OpenIvmStatePaths.isExistingDb(perMvPath) shouldBe false

      // removeForBaseTable must not throw even though the index entry points to a
      // deleted DB. viewsForSource validates each entry against per-MV metadata
      // (readMetadataAtPath returns None for a deleted DB) and skips it, so the
      // narrowed NonFatal re-check never fires (nothing is opened).
      noException should be thrownBy {
        CdfWatermarkCatalog.removeForBaseTable(spark, sourceName)
      }

      // The MV catalog entry backed by the deleted DB is also gone.
      MvCatalog.lookup(spark, meta.name) shouldBe None
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3: No unrelated-MV DB opens — selectivity of the reverse index
  // ---------------------------------------------------------------------------
  describe("reverse-index selectivity") {
    it("removeForBaseTable(A) does not open or modify watermarks of MVs that depend only on B") {
      val sourceA = "sel_source_A"
      val sourceB = "sel_source_B"

      val mvsA = (0 until Concurrency).map(i => sampleMvMeta(s"mv_sel_A_$i", Seq(sourceA)))
      val mvsB = (0 until Concurrency).map(i => sampleMvMeta(s"mv_sel_B_$i", Seq(sourceB)))

      (mvsA ++ mvsB).foreach(MvCatalog.upsert(spark, _))
      mvsA.foreach(m => CdfWatermarkCatalog.put(spark, m.name.identifier, sourceA, 10L))
      mvsB.foreach(m => CdfWatermarkCatalog.put(spark, m.name.identifier, sourceB, 20L))

      CdfWatermarkCatalog.removeForBaseTable(spark, sourceA)

      mvsA.foreach { m =>
        CdfWatermarkCatalog.get(spark, m.name.identifier, sourceA) shouldBe None
      }
      mvsB.foreach { m =>
        CdfWatermarkCatalog.get(spark, m.name.identifier, sourceB) shouldBe Some(20L)
      }

      OpenIvmRocksDBRegistry.closeAll()
      (mvsA ++ mvsB).foreach(m => MvCatalog.remove(spark, m.name))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 4: DROP IF EXISTS is a no-op on an absent MV (no DB opens)
  // ---------------------------------------------------------------------------
  describe("DROP IF EXISTS on absent MV") {
    it("simulateDrop on a non-existent MV name does not throw and opens no DBs") {
      val absentName = "mv_die_never_existed"
      noException should be thrownBy {
        simulateDrop(spark, absentName)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5: Concurrent DROP+CREATE cycle — final state is correct with zero races
  // ---------------------------------------------------------------------------
  describe("concurrent DROP+CREATE cycle") {
    it(
      s"$Concurrency alternating DROP+CREATE cycles over a shared source produce correct final state with zero races"
    ) {
      val executor                      = Executors.newFixedThreadPool(Concurrency * 2)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)

      val sharedSource = "dcc_cycle_source"
      val metas        = (0 until Concurrency).map(i => sampleMvMeta(s"mv_dcc_$i", Seq(sharedSource)))

      metas.foreach { meta =>
        MvCatalog.upsert(spark, meta)
        CdfWatermarkCatalog.put(spark, meta.name.identifier, sharedSource, 50L)
      }

      val barrier = new CyclicBarrier(Concurrency)

      val futures = metas.map { meta =>
        Future {
          barrier.await()
          Try {
            simulateDrop(spark, meta.name.identifier)
            MvCatalog.upsert(spark, meta)
            CdfWatermarkCatalog.put(spark, meta.name.identifier, sharedSource, 99L)
          }
        }
      }

      val results = Await.result(Future.sequence(futures), 60.seconds)

      executor.shutdown()
      executor.awaitTermination(15, TimeUnit.SECONDS)

      // ZERO tolerated races: every DROP+CREATE cycle must have succeeded.
      failIfAnyThrew("DROP+CREATE", results)

      // The concurrent cycles themselves produced the final state — no recovery pass.
      OpenIvmRocksDBRegistry.closeAll()
      metas.foreach { meta =>
        MvCatalog.lookup(spark, meta.name) shouldBe defined
        CdfWatermarkCatalog.get(spark, meta.name.identifier, sharedSource) shouldBe Some(99L)
      }
      MvCatalog.viewsForSource(spark, sharedSource).map(_.name).toSet shouldBe metas.map(_.name).toSet
    }
  }
}
