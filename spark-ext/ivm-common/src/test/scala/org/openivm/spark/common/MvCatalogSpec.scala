package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CyclicBarrier
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class MvCatalogSpec extends AnyFunSpec with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {

  private var spark: SparkSession = _

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-mv-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-MvCatalogSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    MvCatalog.list(spark).foreach(meta => MvCatalog.remove(spark, meta.name))
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      deleteDir(new File(warehouseDir))
    }
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def withNewSession[A](f: SparkSession => A): A = {
    val session = spark.newSession()
    SparkSession.setActiveSession(session)
    try f(session)
    finally SparkSession.clearActiveSession()
  }

  private def sampleMeta(suffix: String, sources: Seq[String] = Seq("orders")): MvMetadata =
    MvMetadata(
      name = CatalystSqlParser.parseTableIdentifier(s"db.mv_$suffix"),
      querySql = s"SELECT count(*) FROM ${sources.headOption.getOrElse("orders")} WHERE id = '$suffix'",
      refreshType = 0,
      refreshTypeName = "SIMPLE_PROJECTION",
      lastVersion = 0L,
      sourceTables = sources,
      sourceSchemaFingerprint =
        MvCatalog.schemaFingerprint(sources.map(_ -> StructType(Seq(StructField("id", StringType)))).toMap),
      location = s"$warehouseDir/mv_$suffix",
      createdAt = new Timestamp(1700000000000L),
      properties = Map("owner" -> "alice", "tier" -> "gold")
    )

  // ---------------------------------------------------------------------------
  // Test 1: ensureTables is idempotent
  // ---------------------------------------------------------------------------
  describe("MvCatalog.ensureTables") {
    it("is idempotent — calling twice does not throw") {
      // ensureTables already called in beforeAll; calling again must not throw
      MvCatalog.ensureTables(spark)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2: upsert insert + lookup round-trip
  // ---------------------------------------------------------------------------
  describe("MvCatalog.upsert + lookup") {
    it("preserves every field including properties map and source_tables order") {
      val original = sampleMeta("rt", sources = Seq("orders", "products", "customers"))
      MvCatalog.upsert(spark, original)

      val result = MvCatalog.lookup(spark, original.name)
      result shouldBe defined
      val m = result.get
      m.querySql shouldBe original.querySql
      m.refreshType shouldBe original.refreshType
      m.refreshTypeName shouldBe original.refreshTypeName
      m.lastVersion shouldBe original.lastVersion
      m.sourceTables shouldBe Seq("orders", "products", "customers")
      m.sourceSchemaFingerprint shouldBe original.sourceSchemaFingerprint
      m.location shouldBe original.location
      m.createdAt shouldBe original.createdAt
      m.properties shouldBe Map("owner" -> "alice", "tier" -> "gold")
      Files.exists(Paths.get(OpenIvmStatePaths.indexDbPath(spark), "CURRENT")) shouldBe false
      OpenIvmStatePaths.isExistingDb(OpenIvmStatePaths.sourceDependencyDbPath(spark, "orders")) shouldBe true
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3: upsert on existing name updates fields without duplicating rows
  // ---------------------------------------------------------------------------
  describe("MvCatalog.upsert (update path)") {
    it("updates last_version and other mutable fields without duplicating the row") {
      val original = sampleMeta("upd")
      MvCatalog.upsert(spark, original)

      val updated = original.copy(lastVersion = 42L, refreshTypeName = "AGGREGATE_GROUP")
      MvCatalog.upsert(spark, updated)

      val allRows = MvCatalog.list(spark).filter(_.name == original.name)
      allRows should have size 1
      allRows.head.lastVersion shouldBe 42L
      allRows.head.refreshTypeName shouldBe "AGGREGATE_GROUP"
    }

    it("syncs each touched RocksDB shard once while publishing metadata") {
      val meta           = sampleMeta("wal_sync", sources = Seq("orders", "products", "customers"))
      val serializedName = meta.name.database.fold(meta.name.identifier)(db => s"$db.${meta.name.identifier}")
      val perMvDb = OpenIvmRocksDBRegistry.getOrOpen(
        spark,
        OpenIvmStatePaths.perMvDbPath(spark, serializedName),
        OpenIvmStatePaths.PerMvColumnFamilies
      )
      val sourceDbs = meta.sourceTables.map(source =>
        OpenIvmRocksDBRegistry.getOrOpen(
          spark,
          OpenIvmStatePaths.sourceDependencyDbPath(spark, source),
          OpenIvmStatePaths.SourceDependencyColumnFamilies
        )
      )
      val touchedDbs    = (perMvDb +: sourceDbs).distinct
      val syncCountByDb = touchedDbs.map(_ -> new AtomicInteger(0)).toMap

      try {
        touchedDbs.foreach { db =>
          db.setBeforeSyncWalHookForTesting(() => syncCountByDb(db).incrementAndGet())
        }

        MvCatalog.upsert(spark, meta)

        MvCatalog.lookup(spark, meta.name) shouldBe Some(meta)
        touchedDbs.foreach(db => syncCountByDb(db).get() shouldBe 1)
      } finally {
        touchedDbs.foreach(_.setBeforeSyncWalHookForTesting(() => ()))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 4: viewsForSource filters correctly
  // ---------------------------------------------------------------------------
  describe("MvCatalog.viewsForSource") {
    it("returns only MVs whose source_tables contains the given table") {
      val mvOrders   = sampleMeta("src_o", sources = Seq("orders"))
      val mvProducts = sampleMeta("src_p", sources = Seq("products"))
      val mvBoth     = sampleMeta("src_b", sources = Seq("orders", "products"))
      Seq(mvOrders, mvProducts, mvBoth).foreach(MvCatalog.upsert(spark, _))

      val forOrders = MvCatalog.viewsForSource(spark, "orders")
      forOrders.map(_.name).toSet shouldBe Set(mvOrders.name, mvBoth.name)

      val forProducts = MvCatalog.viewsForSource(spark, "products")
      forProducts.map(_.name).toSet shouldBe Set(mvProducts.name, mvBoth.name)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5: advance bumps last_version for the named MV only
  // ---------------------------------------------------------------------------
  describe("MvCatalog.advance") {
    it("bumps last_version for the named MV only") {
      val mv1 = sampleMeta("adv1")
      val mv2 = sampleMeta("adv2")
      MvCatalog.upsert(spark, mv1)
      MvCatalog.upsert(spark, mv2)

      MvCatalog.advance(spark, mv1.name, newVersion = 99L)

      MvCatalog.lookup(spark, mv1.name).get.lastVersion shouldBe 99L
      MvCatalog.lookup(spark, mv2.name).get.lastVersion shouldBe 0L
    }
  }

  // ---------------------------------------------------------------------------
  // Test 6: remove is idempotent
  // ---------------------------------------------------------------------------
  describe("MvCatalog.remove") {
    it("is idempotent — no error when removing a non-existent MV") {
      val mv = sampleMeta("rem")
      MvCatalog.upsert(spark, mv)

      MvCatalog.remove(spark, mv.name)
      MvCatalog.lookup(spark, mv.name) shouldBe None

      // Second call must not throw
      noException should be thrownBy MvCatalog.remove(spark, mv.name)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 7: schemaFingerprint determinism and sensitivity
  // ---------------------------------------------------------------------------
  describe("MvCatalog.schemaFingerprint") {
    it("is deterministic across calls with the same input") {
      val sources = Map(
        "orders" -> StructType(Seq(StructField("id", IntegerType), StructField("amount", DoubleType)))
      )
      MvCatalog.schemaFingerprint(sources) shouldBe MvCatalog.schemaFingerprint(sources)
    }

    it("changes when a column type changes") {
      val before = Map(
        "orders" -> StructType(Seq(StructField("id", IntegerType), StructField("amount", DoubleType)))
      )
      val after = Map(
        "orders" -> StructType(Seq(StructField("id", LongType), StructField("amount", DoubleType)))
      )
      MvCatalog.schemaFingerprint(before) should not equal MvCatalog.schemaFingerprint(after)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 8: persisted cascade-delta capability override
  // ---------------------------------------------------------------------------
  describe("MvMetadata.emitsCascadeViewDelta") {
    it("falls back to the refresh-type capability when no property is stored") {
      sampleMeta("cascade_default")
        .copy(
          refreshType = RefreshTypeCode.WindowPartition,
          refreshTypeName = "WINDOW_PARTITION",
          properties = Map.empty
        )
        .emitsCascadeViewDelta shouldBe true

      sampleMeta("cascade_full")
        .copy(
          refreshType = RefreshTypeCode.FullRefresh,
          refreshTypeName = "FULL_REFRESH",
          properties = Map.empty
        )
        .emitsCascadeViewDelta shouldBe false

      sampleMeta("cascade_scalar")
        .copy(
          refreshType = RefreshTypeCode.SimpleAggregate,
          refreshTypeName = "SIMPLE_AGGREGATE",
          properties = Map.empty
        )
        .emitsCascadeViewDelta shouldBe true
    }

    it("honors the persisted per-MV override when present") {
      sampleMeta("cascade_override")
        .copy(
          refreshType = RefreshTypeCode.WindowPartition,
          refreshTypeName = "WINDOW_PARTITION",
          properties = MvMetadata.cascadeViewDeltaProperties(false)
        )
        .emitsCascadeViewDelta shouldBe false
    }

    it("honors a verified FULL_REFRESH cascade capability recorded at CREATE") {
      sampleMeta("cascade_full_verified")
        .copy(
          refreshType = RefreshTypeCode.FullRefresh,
          refreshTypeName = "FULL_REFRESH",
          properties = MvMetadata.cascadeViewDeltaProperties(true)
        )
        .emitsCascadeViewDelta shouldBe true

      sampleMeta("cascade_full_unverified")
        .copy(
          refreshType = RefreshTypeCode.FullRefresh,
          refreshTypeName = "FULL_REFRESH",
          properties = MvMetadata.cascadeViewDeltaProperties(false)
        )
        .emitsCascadeViewDelta shouldBe false
    }
  }

  describe("MvMetadata.queryHasJoin") {
    it("fails closed for legacy or malformed metadata") {
      sampleMeta("join_shape_legacy").copy(properties = Map.empty).queryHasJoin shouldBe true
      sampleMeta("join_shape_malformed")
        .copy(properties = Map(MvMetadata.QueryHasJoinKey -> "unknown"))
        .queryHasJoin shouldBe true
    }

    it("uses the analyzed-plan fact persisted at CREATE") {
      sampleMeta("join_shape_false")
        .copy(properties = MvMetadata.queryShapeProperties(hasJoin = false))
        .queryHasJoin shouldBe false
      sampleMeta("join_shape_true")
        .copy(properties = MvMetadata.queryShapeProperties(hasJoin = true))
        .queryHasJoin shouldBe true
    }
  }

  describe("MvMetadata time-travel pin telemetry") {
    it("round-trips the pin status, reason and identity through the catalog") {
      val pins = Seq(
        "db.customer_address=VERSION AS OF 7",
        "db.customer=VERSION AS OF 3"
      )
      val original = sampleMeta("pin_rt").copy(
        properties = MvMetadata.timeTravelPinProperties(
          TimeTravelPinStatus.Applied,
          pins,
          TimeTravelPinReason.PinsResolved
        )
      )
      MvCatalog.upsert(spark, original)

      val stored = MvCatalog.lookup(spark, original.name).get
      stored.timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.Applied)
      stored.timeTravelPinReason shouldBe Some(TimeTravelPinReason.PinsResolved)
      stored.timeTravelPins shouldBe Seq("db.customer=VERSION AS OF 3", "db.customer_address=VERSION AS OF 7")
      stored.properties(MvMetadata.TimeTravelPinsKey) shouldBe
        "db.customer=VERSION AS OF 3;db.customer_address=VERSION AS OF 7"
    }

    it("stores a quoted clause verbatim so REFRESH can compare it byte for byte") {
      val pins = Seq("db.events=TIMESTAMP AS OF '2024-01-01'")
      val props = MvMetadata.timeTravelPinProperties(
        TimeTravelPinStatus.Applied,
        pins,
        TimeTravelPinReason.PinsResolved
      )
      val original = sampleMeta("pin_quoted").copy(properties = props)
      MvCatalog.upsert(spark, original)

      MvCatalog.lookup(spark, original.name).get.timeTravelPins shouldBe pins
    }

    it("omits the pin list when the view has no resolved pin") {
      val props = MvMetadata.timeTravelPinProperties(
        TimeTravelPinStatus.NotApplicable,
        Seq.empty,
        TimeTravelPinReason.NoUserPin
      )
      props shouldBe Map(
        MvMetadata.TimeTravelPinStatusKey -> TimeTravelPinStatus.NotApplicable,
        MvMetadata.TimeTravelPinReasonKey -> TimeTravelPinReason.NoUserPin
      )
      sampleMeta("pin_none").copy(properties = props).timeTravelPins shouldBe empty
    }

    it("treats a legacy status as absent but a corrupt one as refused") {
      sampleMeta("pin_legacy").copy(properties = Map.empty).timeTravelPinStatus shouldBe None

      // Fail-closed: a property outside the vocabulary must not read as
      // "not persisted" (which silently re-derives) nor as NOT_APPLICABLE.
      val corrupt = sampleMeta("pin_bogus")
        .copy(properties = Map(MvMetadata.TimeTravelPinStatusKey -> "MAYBE"))
      corrupt.timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.CompileFailed)
      corrupt.timeTravelPinStatusRaw shouldBe Some("MAYBE")

      MvMetadata.timeTravelPinProperties(
        "MAYBE",
        Seq("db.t=VERSION AS OF 1"),
        "who_knows"
      ) shouldBe Map(MvMetadata.TimeTravelPinsKey -> "db.t=VERSION AS OF 1")
    }
  }

  // ---------------------------------------------------------------------------
  // Test 11: concurrent writers don't double-insert
  // ---------------------------------------------------------------------------
  describe("MvCatalog concurrent writers") {
    it("4 synchronized writers on distinct MV and source shards produce exactly 4 rows") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val barrier                                     = new CyclicBarrier(4)
      val futures = (1 to 4).map { i =>
        Future {
          barrier.await()
          withNewSession(session => MvCatalog.upsert(session, sampleMeta(s"conc_$i", sources = Seq(s"orders_conc_$i"))))
        }
      }
      futures.foreach(Await.result(_, 30.seconds))

      val concRows = MvCatalog
        .list(spark)
        .filter(m => m.name.identifier.startsWith("mv_conc_"))
      concRows should have size 4
    }

    it("keeps lastVersion monotonic while properties race on the same MV") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val barrier                                     = new CyclicBarrier(2)
      val entry                                       = sampleMeta("property_race")
      MvCatalog.upsert(spark, entry)

      val advances = Future {
        barrier.await()
        withNewSession(session => (2L to 20L).foreach(version => MvCatalog.advance(session, entry.name, version)))
      }
      val propertyUpdates = Future {
        barrier.await()
        withNewSession(session =>
          (1 to 20).foreach(index => MvCatalog.updateProperties(session, entry.name, Map("revision" -> index.toString)))
        )
      }

      Await.result(Future.sequence(Seq(advances, propertyUpdates)), 2.minutes)

      val updated = MvCatalog.lookup(spark, entry.name).get
      updated.lastVersion shouldBe 20L
      updated.properties.keySet shouldBe Set("revision")
    }
  }
  describe("MvMetadata compile cache keys") {
    it("key compiled SQL by schema fingerprint and facts tier") {
      val fp1 = MvCatalog.schemaFingerprint(
        Map("orders" -> StructType(Seq(StructField("id", IntegerType))))
      )
      val fp2 = MvCatalog.schemaFingerprint(
        Map("orders" -> StructType(Seq(StructField("id", LongType))))
      )
      val tier1 = MvMetadata.compileCacheTier(WorkloadFacts(deltaShape = Map("orders" -> DeltaShape.InsertOnly)))
      val tier2 = MvMetadata.compileCacheTier(WorkloadFacts(deltaShape = Map("orders" -> DeltaShape.General)))
      val tier3 = MvMetadata.compileCacheTier(
        WorkloadFacts(deltaShape = Map("orders" -> DeltaShape.InsertOnly), scd2RangeJoinAccel = true)
      )

      tier1 should not equal tier2
      tier1 should not equal tier3
      val props = MvMetadata.compiledProperties(fp1, tier1, "SQL", "INIT", 0, "AGGREGATE_GROUP")

      MvMetadata.cachedCompiledSql(props, fp1, tier1) shouldBe Some("SQL")
      MvMetadata.cachedInitialLoadSql(props, fp1, tier1) shouldBe Some("INIT")
      MvMetadata.cachedCompiledSql(props, fp2, tier1) shouldBe None
      MvMetadata.cachedCompiledSql(props, fp1, tier2) shouldBe None
    }

    it("separates declareRelyFk compile tiers without changing compile facts JSON") {
      val off = WorkloadFacts(fkRelations = Seq(ForeignKeyRelation("child", Seq("parent_id"), "parent", Seq("id"))))
      val on  = off.copy(declareRelyFk = true)

      MvMetadata.compileCacheTier(off) should not equal MvMetadata.compileCacheTier(on)
      off.toJson shouldBe on.toJson
    }
  }

}
