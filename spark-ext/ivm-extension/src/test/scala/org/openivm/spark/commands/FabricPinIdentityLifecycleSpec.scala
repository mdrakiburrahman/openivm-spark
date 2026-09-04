package org.openivm.spark.commands

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{
  DeltaTableVersion,
  MvCatalog,
  MvMetadata,
  StagingCatalog,
  TimeTravelPinReason,
  TimeTravelPinStatus
}
import org.openivm.spark.compiler.SparkTimeTravelSql
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** End-to-end lifecycle coverage for the resolved snapshot-pin binding wired
  * through CREATE / REFRESH / ADVANCE. On local hive the SQL-visible and
  * resolved names coincide, so these lock the operation-variance contract (a
  * pinned MV stays APPLIED across refresh recompiles), the CREATE-time physical
  * identity persistence, and the fail-closed rebind/legacy behavior.
  */
class FabricPinIdentityLifecycleSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val db = "pil_db"
  private val warehouseDir = {
    val directory = new File(s"target/test-warehouse-pin-identity-${UUID.randomUUID().toString.take(8)}")
    directory.mkdirs()
    directory.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-FabricPinIdentityLifecycleSpec")
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

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  private def lookup(name: String): MvMetadata =
    MvCatalog.lookup(spark, TableIdentifier(name)).getOrElse(fail(s"missing MV metadata for $name"))

  /** A persisted pin over a real committed Delta table, resolved to its physical
    * identity, for driving [[MvCommandHelper.resolveAdvanceRequest]] directly. */
  private def advancePin(alias: String, clause: String): SparkTimeTravelSql.ResolvedSnapshotPin = {
    val (path, id) =
      MvCommandHelper.deltaPhysicalIdentity(spark, alias).getOrElse(fail(s"expected Delta identity for $alias"))
    val identity = SparkTimeTravelSql.SourceIdentity(alias, path, id)
    SparkTimeTravelSql.ResolvedSnapshotPin(SparkTimeTravelSql.SnapshotPin(alias, clause), identity, identity)
  }

  private def countSubstring(haystack: String, needle: String): Int =
    if (needle.isEmpty) 0
    else {
      @annotation.tailrec
      def loop(from: Int, acc: Int): Int = {
        val idx = haystack.indexOf(needle, from)
        if (idx < 0) acc else loop(idx + needle.length, acc + 1)
      }
      loop(0, 0)
    }

  describe("Fabric snapshot-pin identity lifecycle") {
    it("keeps a pinned MV APPLIED across two refresh recompiles and persists its physical identity") {
      spark.sql(s"CREATE TABLE $db.a_pinned(id INT, grp STRING) USING DELTA")
      spark.sql(s"CREATE TABLE $db.a_live(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.a_pinned VALUES (1, 'x')")
      spark.sql(s"INSERT INTO $db.a_live VALUES (1, 10)")
      val pinnedVersion = DeltaTableVersion.requireLatest(spark, s"$db.a_pinned")

      val query =
        s"SELECT p.id, p.grp, live.amount FROM `$db`.`a_pinned` VERSION AS OF $pinnedVersion AS p " +
          s"JOIN `$db`.`a_live` AS live ON p.id = live.id"
      spark.sql(s"CREATE MATERIALIZED VIEW a_pinned_mv AS $query").collect()

      val created = lookup("a_pinned_mv")
      created.timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.Applied)
      created.properties.keySet should contain(SparkTimeTravelSql.PinnedSourceIdentitiesPropertyKey)
      created.properties.getOrElse(MvMetadata.CompileRefreshTypeKey, "") should not be "COMPILE_FAILED"
      created.querySql should include(s"`$db`.`a_pinned` VERSION AS OF $pinnedVersion")

      spark.sql(s"INSERT INTO $db.a_live VALUES (1, 11)")
      spark.sql("REFRESH MATERIALIZED VIEW a_pinned_mv").collect()
      lookup("a_pinned_mv").timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.Applied)

      spark.sql(s"INSERT INTO $db.a_live VALUES (1, 12)")
      spark.sql("REFRESH MATERIALIZED VIEW a_pinned_mv").collect()
      val refreshed = lookup("a_pinned_mv")
      refreshed.timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.Applied)
      refreshed.properties.getOrElse(MvMetadata.CompileRefreshTypeKey, "") should not be "COMPILE_FAILED"
    }

    it(
      "hard-fails a legacy pinned MV lacking persisted identities at REFRESH and ADVANCE, leaving unpinned MVs working"
    ) {
      spark.sql(s"CREATE TABLE $db.c_src(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.c_src VALUES (1, 10)")
      val version = DeltaTableVersion.requireLatest(spark, s"$db.c_src")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW c_pinned_mv AS " +
            s"SELECT id, SUM(amount) AS total FROM `$db`.`c_src` VERSION AS OF $version GROUP BY id"
        )
        .collect()

      val meta = lookup("c_pinned_mv")
      meta.properties.keySet should contain(SparkTimeTravelSql.PinnedSourceIdentitiesPropertyKey)

      // Simulate a view created before the physical-identity property existed.
      MvCatalog.upsert(
        spark,
        meta.copy(properties = meta.properties - SparkTimeTravelSql.PinnedSourceIdentitiesPropertyKey)
      )

      val refreshError = intercept[Exception] {
        spark.sql("REFRESH MATERIALIZED VIEW c_pinned_mv").collect()
      }
      refreshError.getMessage.toLowerCase should include("recreate")

      val advanceError = intercept[Exception] {
        spark.sql(s"ALTER MATERIALIZED VIEW c_pinned_mv ADVANCE SOURCE VERSIONS ($db.c_src = $version)").collect()
      }
      advanceError.getMessage.toLowerCase should include("recreate")

      // A legacy UNPINNED MV has no identity property and must refresh normally.
      spark.sql(s"CREATE TABLE $db.c_unpinned(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.c_unpinned VALUES (1, 10)")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW c_unpinned_mv AS SELECT id, SUM(amount) AS total FROM `$db`.`c_unpinned` GROUP BY id"
        )
        .collect()
      lookup("c_unpinned_mv").properties.keySet should not contain
        SparkTimeTravelSql.PinnedSourceIdentitiesPropertyKey
      spark.sql(s"INSERT INTO $db.c_unpinned VALUES (1, 5)")
      noException should be thrownBy spark.sql("REFRESH MATERIALIZED VIEW c_unpinned_mv").collect()
    }

    it("rejects ADVANCE onto a source recreated at a new Delta metadata id under the same name") {
      spark.sql(s"CREATE TABLE $db.d_src(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.d_src VALUES (1, 10)")
      val version = DeltaTableVersion.requireLatest(spark, s"$db.d_src")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW d_pinned_mv AS " +
            s"SELECT id, SUM(amount) AS total FROM `$db`.`d_src` VERSION AS OF $version GROUP BY id"
        )
        .collect()

      // Drop and recreate the pinned source at the same name -> new Delta id.
      spark.sql(s"DROP TABLE $db.d_src")
      spark.sql(s"CREATE TABLE $db.d_src(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.d_src VALUES (1, 10)")
      spark.sql(s"INSERT INTO $db.d_src VALUES (1, 5)")
      val newVersion = DeltaTableVersion.requireLatest(spark, s"$db.d_src")

      val error = intercept[Exception] {
        spark.sql(s"ALTER MATERIALIZED VIEW d_pinned_mv ADVANCE SOURCE VERSIONS ($db.d_src = $newVersion)").collect()
      }
      error.getMessage.toLowerCase should (include("metadata.id") or include("recreate"))
    }

    it("advances the persisted pinned identity version while preserving its dataPath and metadata id") {
      spark.sql(s"CREATE TABLE $db.e_src(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.e_src VALUES (1, 10)")
      val v0 = DeltaTableVersion.requireLatest(spark, s"$db.e_src")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW e_pinned_mv AS " +
            s"SELECT id, SUM(amount) AS total FROM `$db`.`e_src` VERSION AS OF $v0 GROUP BY id"
        )
        .collect()

      val persistedBeforeMeta = lookup("e_pinned_mv")
      val persistedBefore = MvCommandHelper
        .persistedPinnedSources(persistedBeforeMeta.properties, persistedBeforeMeta.sourceTables)
        .getOrElse(fail("expected persisted pinned identities before ADVANCE"))
      persistedBefore should have size 1

      spark.sql(s"INSERT INTO $db.e_src VALUES (1, 5)")
      val v1 = DeltaTableVersion.requireLatest(spark, s"$db.e_src")
      spark.sql(s"ALTER MATERIALIZED VIEW e_pinned_mv ADVANCE SOURCE VERSIONS ($db.e_src = $v1)").collect()

      val persistedAfterMeta = lookup("e_pinned_mv")
      val persistedAfter = MvCommandHelper
        .persistedPinnedSources(persistedAfterMeta.properties, persistedAfterMeta.sourceTables)
        .getOrElse(fail("expected persisted pinned identities after ADVANCE"))
      persistedAfter should have size 1
      persistedAfter.head.pin.clause shouldBe s"VERSION AS OF $v1"
      persistedAfter.head.operationalSource.deltaLogDataPath shouldBe
        persistedBefore.head.operationalSource.deltaLogDataPath
      persistedAfter.head.operationalSource.deltaTableMetadataId shouldBe
        persistedBefore.head.operationalSource.deltaTableMetadataId
    }

    it("hard-fails REFRESH of a pinned MV whose source was dropped, instead of demoting to FULL_REFRESH") {
      spark.sql(s"CREATE TABLE $db.f_src(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.f_src VALUES (1, 10)")
      val v = DeltaTableVersion.requireLatest(spark, s"$db.f_src")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW f_pinned_mv AS " +
            s"SELECT id, SUM(amount) AS total FROM `$db`.`f_src` VERSION AS OF $v GROUP BY id"
        )
        .collect()
      spark.sql(s"DROP TABLE $db.f_src")

      val error = intercept[Exception] {
        spark.sql("REFRESH MATERIALIZED VIEW f_pinned_mv").collect()
      }
      error.getMessage.toLowerCase should (include("cannot maintain") or include("readable"))
      error.getMessage.toUpperCase should not include "FULL_REFRESH"
    }

    it("allows a same-version self-join of one pinned source at CREATE") {
      spark.sql(s"CREATE TABLE $db.g_src(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.g_src VALUES (1, 10)")
      val v = DeltaTableVersion.requireLatest(spark, s"$db.g_src")
      noException should be thrownBy spark
        .sql(
          s"CREATE MATERIALIZED VIEW g_selfjoin_mv AS " +
            s"SELECT l.id FROM `$db`.`g_src` VERSION AS OF $v AS l " +
            s"JOIN `$db`.`g_src` VERSION AS OF $v AS r ON l.id = r.id"
        )
        .collect()
      lookup("g_selfjoin_mv").timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.Applied)
    }

    it(
      "keeps an identical-version self-join APPLIED with ONE canonical identity across CREATE, two REFRESHes, and ADVANCE"
    ) {
      spark.sql(s"CREATE TABLE $db.sj_dim(id INT, label STRING) USING DELTA")
      spark.sql(s"CREATE TABLE $db.sj_facts(fid INT, cur_id INT, prev_id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.sj_dim VALUES (1, 'a'), (2, 'b')")
      spark.sql(s"INSERT INTO $db.sj_facts VALUES (10, 1, 2)")
      val v0 = DeltaTableVersion.requireLatest(spark, s"$db.sj_dim")
      val (dimPath, dimMetaId) =
        MvCommandHelper.deltaPhysicalIdentity(spark, s"$db.sj_dim").getOrElse(fail("expected sj_dim identity"))

      // Two SQL-visible aliases (ct, pt) of the SAME physical Delta table at the
      // SAME pinned version, joined to a live driver so each REFRESH applies a
      // real upstream delta and the MV is an incrementally maintained
      // SIMPLE_PROJECTION that ADVANCE accepts.
      val query =
        s"SELECT f.fid, ct.label AS cur_label, pt.label AS prev_label " +
          s"FROM `$db`.`sj_facts` AS f " +
          s"JOIN `$db`.`sj_dim` VERSION AS OF $v0 AS ct ON f.cur_id = ct.id " +
          s"JOIN `$db`.`sj_dim` VERSION AS OF $v0 AS pt ON f.prev_id = pt.id"
      spark.sql(s"CREATE MATERIALIZED VIEW sj_selfjoin_mv AS $query").collect()

      val created = lookup("sj_selfjoin_mv")
      val key     = SparkTimeTravelSql.PinnedSourceIdentitiesPropertyKey
      created.timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.Applied)

      // (a) The persisted identity is EXACTLY ONE canonical record even though the
      // body reads the pinned source twice: verified path + metadata.id + version.
      created.properties(key) should not include "},{"
      val persistedAtCreate = MvCommandHelper
        .persistedPinnedSources(created.properties, created.sourceTables)
        .getOrElse(fail("expected persisted pinned identities at CREATE"))
      persistedAtCreate should have size 1
      persistedAtCreate.head.operationalSource.deltaLogDataPath shouldBe dimPath
      persistedAtCreate.head.operationalSource.deltaTableMetadataId shouldBe dimMetaId
      persistedAtCreate.head.pin.clause shouldBe s"VERSION AS OF $v0"

      // (b) BOTH SQL-visible occurrences bind to the verified delta path on the
      // executed full-query surface; no logical temporal read and no unbound
      // memory.main read survive; the compiler-emit surface collapses to one path.
      val binding = MvCommandHelper.requirePinBinding(
        spark,
        SparkTimeTravelSql.PinIdentityOperation.Create,
        TableIdentifier("sj_selfjoin_mv"),
        query,
        created.sourceTables,
        created.sourceTables.map(source => source.split("\\.").last -> source).toMap,
        Map.empty
      )
      binding.resolvedPins should have size 2
      val bound = MvCommandHelper.pathBindUserFullQuery(
        TableIdentifier("sj_selfjoin_mv"),
        query,
        binding,
        created.sourceTables,
        binding.resolvedPins
      )
      countSubstring(bound, s"delta.`$dimPath`") shouldBe 2
      bound should not include s"`$db`.`sj_dim` VERSION AS OF"
      bound.toLowerCase should not include "memory.main."
      bound should include(s"VERSION AS OF $v0")
      MvCommandHelper.pinnedPathByShort(binding) shouldBe Map("sj_dim" -> dimPath)

      // Data reflects the frozen self-join joined to the live fact row.
      spark
        .table("sj_selfjoin_mv")
        .collect()
        .map(r => (r.getInt(0), r.getString(1), r.getString(2)))
        .toSet shouldBe Set((10, "a", "b"))

      // (c) Identity survives TWO REFRESH round trips; each applies a live delta,
      // and the persisted property is byte-stable (never re-serialized into a
      // duplicate the strict reader would later reject).
      spark.sql(s"INSERT INTO $db.sj_facts VALUES (20, 2, 1)")
      spark.sql("REFRESH MATERIALIZED VIEW sj_selfjoin_mv").collect()
      val afterR1 = lookup("sj_selfjoin_mv")
      afterR1.timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.Applied)
      afterR1.properties(key) shouldBe created.properties(key)

      spark.sql(s"INSERT INTO $db.sj_facts VALUES (30, 1, 1)")
      spark.sql("REFRESH MATERIALIZED VIEW sj_selfjoin_mv").collect()
      val afterR2 = lookup("sj_selfjoin_mv")
      afterR2.timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.Applied)
      afterR2.properties(key) shouldBe created.properties(key)
      spark
        .table("sj_selfjoin_mv")
        .collect()
        .map(r => (r.getInt(0), r.getString(1), r.getString(2)))
        .toSet shouldBe Set((10, "a", "b"), (20, "b", "a"), (30, "a", "a"))

      // (d) ADVANCE moves the pinned version, keeps ONE canonical record, same
      // dataPath + metadata.id, and rewrites BOTH occurrences in the stored body.
      // The advanced-past row (id 99) is unreferenced, so the signed old-state vs
      // new-state delta is empty: the identity contract is verified in isolation
      // from self-join delta expansion, and both old-/new-state reads stay
      // path-bound (a logical/rebound old-state read would fail identity checks).
      spark.sql(s"INSERT INTO $db.sj_dim VALUES (99, 'z')")
      val v1 = DeltaTableVersion.requireLatest(spark, s"$db.sj_dim")
      v1 should be > v0
      spark.sql(s"ALTER MATERIALIZED VIEW sj_selfjoin_mv ADVANCE SOURCE VERSIONS ($db.sj_dim = $v1)").collect()

      val advanced = lookup("sj_selfjoin_mv")
      val persistedAfterAdvance = MvCommandHelper
        .persistedPinnedSources(advanced.properties, advanced.sourceTables)
        .getOrElse(fail("expected persisted pinned identities after ADVANCE"))
      persistedAfterAdvance should have size 1
      persistedAfterAdvance.head.pin.clause shouldBe s"VERSION AS OF $v1"
      persistedAfterAdvance.head.operationalSource.deltaLogDataPath shouldBe dimPath
      persistedAfterAdvance.head.operationalSource.deltaTableMetadataId shouldBe dimMetaId
      advanced.properties(key) should not include "},{"
      countSubstring(advanced.querySql, s"VERSION AS OF $v1 ") shouldBe 2
      advanced.querySql should not include s"VERSION AS OF $v0 "
      spark
        .table("sj_selfjoin_mv")
        .collect()
        .map(r => (r.getInt(0), r.getString(1), r.getString(2)))
        .toSet shouldBe Set((10, "a", "b"), (20, "b", "a"), (30, "a", "a"))
    }

    it("collapses an identical TIMESTAMP self-join to ONE canonical persisted record with the clause verbatim") {
      spark.sql(s"CREATE TABLE $db.sjt_dim(id INT, label STRING) USING DELTA")
      spark.sql(s"INSERT INTO $db.sjt_dim VALUES (1, 'a')")
      val (path, metaId) =
        MvCommandHelper.deltaPhysicalIdentity(spark, s"$db.sjt_dim").getOrElse(fail("expected sjt_dim identity"))
      val clause  = "TIMESTAMP AS OF '2026-09-04 05:00:00'"
      val sources = Seq(s"$db.sjt_dim")
      val query =
        s"SELECT l.id FROM `$db`.`sjt_dim` $clause AS l " +
          s"JOIN `$db`.`sjt_dim` $clause AS r ON l.id = r.id"

      // Physical identity resolves without executing the (unresolvable-on-a-fresh
      // table) historical read, so this exercises the binding + serialization
      // seam the same way the live TIMESTAMP path does.
      val binding = MvCommandHelper.requirePinBinding(
        spark,
        SparkTimeTravelSql.PinIdentityOperation.Create,
        TableIdentifier("sjt_probe"),
        query,
        sources,
        Map("sjt_dim" -> s"$db.sjt_dim"),
        Map.empty
      )
      // Both SQL-visible occurrences are retained for execution rewriting...
      binding.resolvedPins should have size 2
      MvCommandHelper.pinnedPathBindingsByShort(binding) shouldBe Map("sjt_dim" -> ((path, clause)))

      // ...but the persisted identity records the one physical source ONCE, with
      // the exact TIMESTAMP clause verbatim (never folded to a version).
      val props = SparkTimeTravelSql.pinnedSourceIdentityProperties(MvCommandHelper.bindingsView(binding, sources))
      val key   = SparkTimeTravelSql.PinnedSourceIdentitiesPropertyKey
      props(key) should not include "},{"
      props(key) should include("\"timestamp\":\"2026-09-04 05:00:00\"")
      props(key) should not include "\"version\""
      val readBack = MvCommandHelper
        .persistedPinnedSources(props, sources)
        .getOrElse(fail("expected the canonical TIMESTAMP record to round-trip"))
      readBack should have size 1
      readBack.head.operationalSource.deltaLogDataPath shouldBe path
      readBack.head.operationalSource.deltaTableMetadataId shouldBe metaId
      readBack.head.pin.clause shouldBe clause
    }

    it("still rejects malformed persisted duplicate identity records for a self-join at REFRESH") {
      spark.sql(s"CREATE TABLE $db.sjm_dim(id INT, label STRING) USING DELTA")
      spark.sql(s"CREATE TABLE $db.sjm_facts(fid INT, cur_id INT, prev_id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.sjm_dim VALUES (1, 'a'), (2, 'b')")
      spark.sql(s"INSERT INTO $db.sjm_facts VALUES (10, 1, 2)")
      val v0 = DeltaTableVersion.requireLatest(spark, s"$db.sjm_dim")
      val query =
        s"SELECT f.fid, ct.label AS cur_label, pt.label AS prev_label " +
          s"FROM `$db`.`sjm_facts` AS f " +
          s"JOIN `$db`.`sjm_dim` VERSION AS OF $v0 AS ct ON f.cur_id = ct.id " +
          s"JOIN `$db`.`sjm_dim` VERSION AS OF $v0 AS pt ON f.prev_id = pt.id"
      spark.sql(s"CREATE MATERIALIZED VIEW sjm_selfjoin_mv AS $query").collect()

      val created   = lookup("sjm_selfjoin_mv")
      val key       = SparkTimeTravelSql.PinnedSourceIdentitiesPropertyKey
      val canonical = created.properties(key)
      canonical should not include "},{"

      // Re-introduce the PRE-FIX duplicate serialization (one record per textual
      // occurrence). The strict reader must still refuse it: duplicates in
      // PERSISTED input remain invalid even though canonical OUTPUT is deduped.
      val duplicated = canonical.dropRight(1) + "," + canonical.drop(1)
      duplicated should include("},{")
      MvCommandHelper.persistedPinnedSources(
        created.properties + (key -> duplicated),
        created.sourceTables
      ) shouldBe None
      MvCatalog.upsert(spark, created.copy(properties = created.properties + (key -> duplicated)))

      spark.sql(s"INSERT INTO $db.sjm_facts VALUES (20, 2, 1)")
      val refreshError = intercept[Exception] {
        spark.sql("REFRESH MATERIALIZED VIEW sjm_selfjoin_mv").collect()
      }
      refreshError.getMessage.toLowerCase should include("recreate")
    }

    it("hard-fails CREATE of a pinned self-join short name shared by two DISTINCT physical sources") {
      val leftDb  = "pil_dshort_l"
      val rightDb = "pil_dshort_r"
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $leftDb")
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $rightDb")
      spark.sql(s"CREATE TABLE $leftDb.dim(id INT) USING DELTA")
      spark.sql(s"CREATE TABLE $rightDb.dim(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $leftDb.dim VALUES (1)")
      spark.sql(s"INSERT INTO $rightDb.dim VALUES (1)")
      val lv = DeltaTableVersion.requireLatest(spark, s"$leftDb.dim")
      val rv = DeltaTableVersion.requireLatest(spark, s"$rightDb.dim")

      // Two DISTINCT physical `dim` tables, BOTH pinned, colliding on the compiler
      // short name `dim` -- rejected before any persist, never fused to one
      // identity record.
      val error = intercept[Exception] {
        spark
          .sql(
            s"CREATE MATERIALIZED VIEW dshort_mv AS " +
              s"SELECT l.id FROM `$leftDb`.`dim` VERSION AS OF $lv AS l " +
              s"JOIN `$rightDb`.`dim` VERSION AS OF $rv AS r ON l.id = r.id"
          )
          .collect()
      }
      error.getMessage.toLowerCase should include("duplicate short")
    }

    it("refuses a same-physical two-version self-join as an unsupported pin shape, persisting no pin identity") {
      spark.sql(s"CREATE TABLE $db.sjx_dim(id INT, label STRING) USING DELTA")
      spark.sql(s"INSERT INTO $db.sjx_dim VALUES (1, 'a')")
      val v0 = DeltaTableVersion.requireLatest(spark, s"$db.sjx_dim")
      spark.sql(s"INSERT INTO $db.sjx_dim VALUES (2, 'b')")
      val v1 = DeltaTableVersion.requireLatest(spark, s"$db.sjx_dim")
      v1 should be > v0
      val sources = Seq(s"$db.sjx_dim")
      val query =
        s"SELECT l.id FROM `$db`.`sjx_dim` VERSION AS OF $v0 AS l " +
          s"JOIN `$db`.`sjx_dim` VERSION AS OF $v1 AS r ON l.id = r.id"

      // One physical source read at TWO versions is an ambiguous cross-version
      // shape: the resolver yields no verified pins (so it can never persist a
      // conflicting identity) and the pin telemetry reports it refused.
      val telemetry = SparkTimeTravelSql.pinTelemetry(query, sources)
      telemetry.status shouldBe TimeTravelPinStatus.CompileFailed
      telemetry.reason shouldBe TimeTravelPinReason.UnsupportedPinShape
      MvCommandHelper.resolvePinnedSources(spark, query, sources) shouldBe MvCommandHelper.PinResolution.NoPin
    }

    it("hard-fails CREATE when a pinned source and a live source share a compiler short name") {
      val leftDb  = "pil_left"
      val rightDb = "pil_right"
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $leftDb")
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $rightDb")
      spark.sql(s"CREATE TABLE $leftDb.foo(id INT) USING DELTA")
      spark.sql(s"CREATE TABLE $rightDb.foo(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $leftDb.foo VALUES (1)")
      spark.sql(s"INSERT INTO $rightDb.foo VALUES (1)")
      val v = DeltaTableVersion.requireLatest(spark, s"$leftDb.foo")

      val error = intercept[Exception] {
        spark
          .sql(
            s"CREATE MATERIALIZED VIEW h_collision_mv AS " +
              s"SELECT l.id FROM `$leftDb`.`foo` VERSION AS OF $v AS l JOIN `$rightDb`.`foo` AS r ON l.id = r.id"
          )
          .collect()
      }
      error.getMessage.toLowerCase should include("duplicate short")
    }

    it("hard-fails ADVANCE naming a source the view does not pin") {
      spark.sql(s"CREATE TABLE $db.i_src(id INT, amount INT) USING DELTA")
      spark.sql(s"CREATE TABLE $db.i_other(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.i_src VALUES (1, 10)")
      spark.sql(s"INSERT INTO $db.i_other VALUES (1, 10)")
      val v = DeltaTableVersion.requireLatest(spark, s"$db.i_src")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW i_pinned_mv AS " +
            s"SELECT id, SUM(amount) AS total FROM `$db`.`i_src` VERSION AS OF $v GROUP BY id"
        )
        .collect()
      spark.sql(s"INSERT INTO $db.i_other VALUES (1, 5)")
      val otherVersion = DeltaTableVersion.requireLatest(spark, s"$db.i_other")

      val error = intercept[Exception] {
        spark
          .sql(s"ALTER MATERIALIZED VIEW i_pinned_mv ADVANCE SOURCE VERSIONS ($db.i_other = $otherVersion)")
          .collect()
      }
      error.getMessage.toLowerCase should (include("persisted pinned") or include("expected exactly one"))
    }

    it("detects a recreated pinned source by metadata.id read at the verified Delta path (TOCTOU)") {
      spark.sql(s"CREATE TABLE $db.j_src(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.j_src VALUES (1)")
      val (path, id) = MvCommandHelper
        .deltaPhysicalIdentity(spark, s"$db.j_src")
        .getOrElse(fail("expected a Delta physical identity"))

      // No rebind yet: the path-bound check passes.
      MvCommandHelper.verifyPinnedSourceIdentitiesAtPath(spark, TableIdentifier("j_check"), Seq(path -> id))

      // Drop and recreate at the same path -> new metadata.id -> rebinding.
      spark.sql(s"DROP TABLE $db.j_src")
      spark.sql(s"CREATE TABLE $db.j_src(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.j_src VALUES (1)")

      intercept[SourceIdentityRebindingException] {
        MvCommandHelper.verifyPinnedSourceIdentitiesAtPath(spark, TableIdentifier("j_check"), Seq(path -> id))
      }
    }

    it("rolls back CREATE and cleans owned artifacts when a pinned source is recreated during the CTAS (CREATE POST)") {
      spark.sql(s"CREATE TABLE $db.k_src(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.k_src VALUES (1, 10)")
      val v = DeltaTableVersion.requireLatest(spark, s"$db.k_src")

      var ctasAttempts = 0
      val error = intercept[SourceIdentityRebindingException] {
        // Count CTAS attempts; recreate the pinned source at its managed path
        // (new metadata.id) AFTER the write but BEFORE the POST check.
        CommandConcurrencyInjection.withBeforeCreateDataWrite { ctasAttempts += 1 } {
          CommandConcurrencyInjection.withAfterCreateDataWrite {
            spark.sql(s"DROP TABLE $db.k_src")
            spark.sql(s"CREATE TABLE $db.k_src(id INT, amount INT) USING DELTA")
            spark.sql(s"INSERT INTO $db.k_src VALUES (1, 10)")
          } {
            spark
              .sql(
                s"CREATE MATERIALIZED VIEW k_pinned_mv AS " +
                  s"SELECT id, SUM(amount) AS total FROM `$db`.`k_src` VERSION AS OF $v GROUP BY id"
              )
              .collect()
          }
        }
      }
      error.getMessage.toLowerCase should include("metadata.id")
      // Exactly one CTAS attempt: the rebinding is non-retryable, never demoted.
      ctasAttempts shouldBe 1
      // Owned artifacts cleaned: neither the MV metadata nor a registered table remain.
      MvCatalog.lookup(spark, TableIdentifier("k_pinned_mv")) shouldBe None
      spark.catalog.tableExists(db, "k_pinned_mv") shouldBe false
    }

    it(
      "path-binds the pinned FULL_REFRESH CREATE read to the verified Delta path even when the alias is repointed mid-write"
    ) {
      val pathA = s"$warehouseDir/n_src_a_${UUID.randomUUID().toString.take(8)}"
      val pathB = s"$warehouseDir/n_src_b_${UUID.randomUUID().toString.take(8)}"
      spark.sql(s"CREATE TABLE $db.n_src(id INT, tag STRING) USING DELTA LOCATION '$pathA'")
      spark.sql(s"INSERT INTO $db.n_src VALUES (1, 'A')")
      val v = DeltaTableVersion.requireLatest(spark, s"$db.n_src")

      CommandConcurrencyInjection.withBeforeCreateDataWrite {
        // Repoint the catalog NAME to a different physical table (different rows)
        // just before the CTAS reads it. Path binding must ignore this rebind.
        spark.sql(s"DROP TABLE $db.n_src")
        spark.sql(s"CREATE TABLE $db.n_src(id INT, tag STRING) USING DELTA LOCATION '$pathB'")
        spark.sql(s"INSERT INTO $db.n_src VALUES (1, 'B')")
      } {
        // `input_file_name()` is Spark-only, so openivm demotes this to
        // FULL_REFRESH: the initial CTAS uses the path-bound USER body.
        spark
          .sql(
            s"CREATE MATERIALIZED VIEW n_mv AS " +
              s"SELECT id, tag, input_file_name() AS src_file FROM `$db`.`n_src` VERSION AS OF $v"
          )
          .collect()
      }

      lookup("n_mv").properties.getOrElse(MvMetadata.CompileRefreshTypeKey, "") shouldBe "COMPILE_FAILED"
      // The MV read the VERIFIED path (pathA, tag 'A'), not the repointed pathB.
      spark.table("n_mv").collect().map(_.getString(1)).toSet shouldBe Set("A")
    }

    it(
      "rolls a pinned MV back to its pre-refresh version when a pinned source is recreated during apply (REFRESH POST)"
    ) {
      spark.sql(s"CREATE TABLE $db.m_pinned(id INT, grp STRING) USING DELTA")
      spark.sql(s"CREATE TABLE $db.m_live(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.m_pinned VALUES (1, 'x')")
      spark.sql(s"INSERT INTO $db.m_live VALUES (1, 10)")
      val pv = DeltaTableVersion.requireLatest(spark, s"$db.m_pinned")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW m_mv AS " +
            s"SELECT p.id, p.grp, live.amount FROM `$db`.`m_pinned` VERSION AS OF $pv AS p " +
            s"JOIN `$db`.`m_live` AS live ON p.id = live.id"
        )
        .collect()

      val beforeVersion = lookup("m_mv").lastVersion
      val beforeRows    = spark.table("m_mv").collect().map(_.mkString("|")).sorted.toList

      // Queue a real live delta, then recreate the pinned source at its path
      // (new metadata.id) after the apply loop but before the POST check.
      spark.sql(s"INSERT INTO $db.m_live VALUES (1, 11)")
      val error = intercept[SourceIdentityRebindingException] {
        CommandConcurrencyInjection.withBeforeRefreshFinalize {
          spark.sql(s"DROP TABLE $db.m_pinned")
          spark.sql(s"CREATE TABLE $db.m_pinned(id INT, grp STRING) USING DELTA")
          spark.sql(s"INSERT INTO $db.m_pinned VALUES (1, 'x')")
        } {
          spark.sql("REFRESH MATERIALIZED VIEW m_mv").collect()
        }
      }
      error.getMessage.toLowerCase should include("metadata.id")

      // MV data rolled back to its pre-refresh content; the live delta was NOT
      // consumed (no marker drift) and no cascade/staging row was published.
      spark.table("m_mv").collect().map(_.mkString("|")).sorted.toList shouldBe beforeRows
      // The tracked version is synchronized to the RESTORE commit (a new Delta
      // version), never left stale at the pre-refresh value.
      val restoredVersion = DeltaTableVersion.requireLatest(spark, lookup("m_mv").location)
      lookup("m_mv").lastVersion shouldBe restoredVersion
      restoredVersion should be > beforeVersion
    }

    it("incremental REFRESH reads the pinned source at its verified path even when the alias is repointed mid-apply") {
      val pathA = s"$warehouseDir/ir_pinned_a_${UUID.randomUUID().toString.take(8)}"
      val pathB = s"$warehouseDir/ir_pinned_b_${UUID.randomUUID().toString.take(8)}"
      spark.sql(s"CREATE TABLE $db.ir_pinned(id INT, tag STRING) USING DELTA LOCATION '$pathA'")
      spark.sql(s"CREATE TABLE $db.ir_live(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.ir_pinned VALUES (1, 'A')")
      spark.sql(s"INSERT INTO $db.ir_live VALUES (1, 10)")
      val pv = DeltaTableVersion.requireLatest(spark, s"$db.ir_pinned")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW ir_mv AS " +
            s"SELECT p.id, p.tag, live.amount FROM `$db`.`ir_pinned` VERSION AS OF $pv AS p " +
            s"JOIN `$db`.`ir_live` AS live ON p.id = live.id"
        )
        .collect()
      spark.table("ir_mv").collect().map(_.getString(1)).toSet shouldBe Set("A")

      // Queue a live delta that joins the pinned row, then repoint the pinned
      // ALIAS to a different physical table (tag 'B') before the incremental
      // apply reads it. The emitted incremental SQL is path-bound to pathA, so
      // the new MV row must carry tag 'A', not 'B'.
      spark.sql(s"INSERT INTO $db.ir_live VALUES (1, 100)")
      CommandConcurrencyInjection.withBeforeRefreshApply {
        spark.sql(s"DROP TABLE $db.ir_pinned")
        spark.sql(s"CREATE TABLE $db.ir_pinned(id INT, tag STRING) USING DELTA LOCATION '$pathB'")
        spark.sql(s"INSERT INTO $db.ir_pinned VALUES (1, 'B')")
      } {
        spark.sql("REFRESH MATERIALIZED VIEW ir_mv").collect()
      }

      val amounts = spark.table("ir_mv").collect().map(r => (r.getString(1), r.getInt(2))).toSet
      // Every row read the verified path (tag 'A'); the incremental amount=100 row is present.
      amounts.map(_._1) shouldBe Set("A")
      amounts should contain((("A"), 100))
      lookup("ir_mv").properties.getOrElse(MvMetadata.CompileRefreshTypeKey, "") should not be "COMPILE_FAILED"
    }

    it("path-binds the ADVANCE old-state read so advancing a pinned source recomputes the correct signed delta") {
      spark.sql(s"CREATE TABLE $db.av_src(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.av_src VALUES (1, 10)")
      val v0 = DeltaTableVersion.requireLatest(spark, s"$db.av_src")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW av_mv AS " +
            s"SELECT id, SUM(amount) AS total FROM `$db`.`av_src` VERSION AS OF $v0 GROUP BY id"
        )
        .collect()
      spark.table("av_mv").collect().map(r => (r.getInt(0), r.getLong(1))).toSet shouldBe Set((1, 10L))

      // Add rows, then ADVANCE the pin v0 -> v1. The signed delta is
      // (new-state av@v1) - (old-state av@v0); both reads are path-bound. A
      // correct total proves the OLD-state read used the verified snapshot.
      spark.sql(s"INSERT INTO $db.av_src VALUES (1, 5)")
      val v1 = DeltaTableVersion.requireLatest(spark, s"$db.av_src")
      spark.sql(s"ALTER MATERIALIZED VIEW av_mv ADVANCE SOURCE VERSIONS ($db.av_src = $v1)").collect()

      spark.table("av_mv").collect().map(r => (r.getInt(0), r.getLong(1))).toSet shouldBe Set((1, 15L))
      val advanced = MvCommandHelper
        .persistedPinnedSources(lookup("av_mv").properties, lookup("av_mv").sourceTables)
        .getOrElse(fail("expected persisted pinned identities after ADVANCE"))
      advanced.head.pin.clause shouldBe s"VERSION AS OF $v1"
    }

    it("rolls a FULL_REFRESH pinned MV back and publishes no cascade/metadata when a source is recreated (FULL POST)") {
      spark.sql(s"CREATE TABLE $db.fr_src(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.fr_src VALUES (1, 10)")
      val v = DeltaTableVersion.requireLatest(spark, s"$db.fr_src")
      // `input_file_name()` is Spark-only, so the MV is FULL_REFRESH: every
      // REFRESH re-executes the pinned body via INSERT OVERWRITE.
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW fr_mv AS " +
            s"SELECT id, amount, input_file_name() AS f FROM `$db`.`fr_src` VERSION AS OF $v"
        )
        .collect()
      lookup("fr_mv").properties.getOrElse(MvMetadata.CompileRefreshTypeKey, "") shouldBe "COMPILE_FAILED"
      val beforeRows    = spark.table("fr_mv").collect().map(r => (r.getInt(0), r.getInt(1))).toSet
      val beforeVersion = DeltaTableVersion.requireLatest(spark, lookup("fr_mv").location)

      val error = intercept[SourceIdentityRebindingException] {
        CommandConcurrencyInjection.withBeforeRefreshFinalize {
          spark.sql(s"DROP TABLE $db.fr_src")
          spark.sql(s"CREATE TABLE $db.fr_src(id INT, amount INT) USING DELTA")
          spark.sql(s"INSERT INTO $db.fr_src VALUES (1, 10)")
        } {
          spark.sql("REFRESH MATERIALIZED VIEW fr_mv").collect()
        }
      }
      error.getMessage.toLowerCase should include("metadata.id")

      // Rolled back: data unchanged, tracked version synced to the RESTORE commit,
      // and the persisted pin status is still the pre-refresh APPLIED (no
      // success-shaped metadata publication for the rejected refresh).
      spark.table("fr_mv").collect().map(r => (r.getInt(0), r.getInt(1))).toSet shouldBe beforeRows
      val restoredVersion = DeltaTableVersion.requireLatest(spark, lookup("fr_mv").location)
      lookup("fr_mv").lastVersion shouldBe restoredVersion
      restoredVersion should be > beforeVersion
      lookup("fr_mv").timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.Applied)
    }

    it(
      "marks a pinned MV repair-required when rollback fails, blocking REFRESH/ADVANCE until recreate"
    ) {
      spark.sql(s"CREATE TABLE $db.rr_pinned(id INT, grp STRING) USING DELTA")
      spark.sql(s"CREATE TABLE $db.rr_live(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.rr_pinned VALUES (1, 'x')")
      spark.sql(s"INSERT INTO $db.rr_live VALUES (1, 10)")
      val pv = DeltaTableVersion.requireLatest(spark, s"$db.rr_pinned")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW rr_mv AS " +
            s"SELECT p.id, p.grp, live.amount FROM `$db`.`rr_pinned` VERSION AS OF $pv AS p " +
            s"JOIN `$db`.`rr_live` AS live ON p.id = live.id"
        )
        .collect()

      spark.sql(s"INSERT INTO $db.rr_live VALUES (1, 11)")
      // A rebind is detected AND the rollback RESTORE is forced to fail.
      val error = intercept[SourceIdentityRebindingException] {
        CommandConcurrencyInjection.withForcedPinRollbackRestoreFailure {
          CommandConcurrencyInjection.withBeforeRefreshFinalize {
            spark.sql(s"DROP TABLE $db.rr_pinned")
            spark.sql(s"CREATE TABLE $db.rr_pinned(id INT, grp STRING) USING DELTA")
            spark.sql(s"INSERT INTO $db.rr_pinned VALUES (1, 'x')")
          } {
            spark.sql("REFRESH MATERIALIZED VIEW rr_mv").collect()
          }
        }
      }
      // The failure surfaces both the identity mismatch and the rollback failure.
      error.getMessage.toLowerCase should include("rollback failed")
      error.getMessage.toLowerCase should include("repair-required")

      // The durable journal is persisted and now blocks REFRESH and ADVANCE.
      lookup("rr_mv").properties.keySet should contain(MvCommandHelper.PinRebindRepairRequiredKey)
      val refreshBlocked = intercept[Exception](spark.sql("REFRESH MATERIALIZED VIEW rr_mv").collect())
      refreshBlocked.getMessage.toLowerCase should include("repair-required")
      val liveV = DeltaTableVersion.requireLatest(spark, s"$db.rr_live")
      val advanceBlocked =
        intercept[Exception](
          spark.sql(s"ALTER MATERIALIZED VIEW rr_mv ADVANCE SOURCE VERSIONS ($db.rr_live = $liveV)").collect()
        )
      advanceBlocked.getMessage.toLowerCase should include("repair-required")

      // Recreate clears the journal: a fresh CREATE + REFRESH works again.
      spark.sql("DROP MATERIALIZED VIEW rr_mv")
      val pv2 = DeltaTableVersion.requireLatest(spark, s"$db.rr_pinned")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW rr_mv AS " +
            s"SELECT p.id, p.grp, live.amount FROM `$db`.`rr_pinned` VERSION AS OF $pv2 AS p " +
            s"JOIN `$db`.`rr_live` AS live ON p.id = live.id"
        )
        .collect()
      lookup("rr_mv").properties.keySet should not contain MvCommandHelper.PinRebindRepairRequiredKey
      spark.sql(s"INSERT INTO $db.rr_live VALUES (1, 12)")
      noException should be thrownBy spark.sql("REFRESH MATERIALIZED VIEW rr_mv").collect()
    }

    it("aborts a pinned refresh before any mutation when the pre-apply version cannot be captured") {
      spark.sql(s"CREATE TABLE $db.pvc_pinned(id INT, grp STRING) USING DELTA")
      spark.sql(s"CREATE TABLE $db.pvc_live(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.pvc_pinned VALUES (1, 'x')")
      spark.sql(s"INSERT INTO $db.pvc_live VALUES (1, 10)")
      val pv = DeltaTableVersion.requireLatest(spark, s"$db.pvc_pinned")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW pvc_mv AS " +
            s"SELECT p.id, p.grp, live.amount FROM `$db`.`pvc_pinned` VERSION AS OF $pv AS p " +
            s"JOIN `$db`.`pvc_live` AS live ON p.id = live.id"
        )
        .collect()
      val beforeVersion = DeltaTableVersion.requireLatest(spark, lookup("pvc_mv").location)
      val beforeRows    = spark.table("pvc_mv").collect().map(_.mkString("|")).sorted.toList

      spark.sql(s"INSERT INTO $db.pvc_live VALUES (1, 11)")
      val error = intercept[Exception] {
        CommandConcurrencyInjection.withForcedPreApplyVersionCaptureFailure {
          spark.sql("REFRESH MATERIALIZED VIEW pvc_mv").collect()
        }
      }
      error.getMessage.toLowerCase should include("before any mutation")
      // No mutation happened: MV version and rows are unchanged.
      DeltaTableVersion.requireLatest(spark, lookup("pvc_mv").location) shouldBe beforeVersion
      spark.table("pvc_mv").collect().map(_.mkString("|")).sorted.toList shouldBe beforeRows
    }

    it("establishes a blocking repair state when the post-RESTORE latest-version read fails") {
      spark.sql(s"CREATE TABLE $db.prr_pinned(id INT, grp STRING) USING DELTA")
      spark.sql(s"CREATE TABLE $db.prr_live(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.prr_pinned VALUES (1, 'x')")
      spark.sql(s"INSERT INTO $db.prr_live VALUES (1, 10)")
      val pv = DeltaTableVersion.requireLatest(spark, s"$db.prr_pinned")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW prr_mv AS " +
            s"SELECT p.id, p.grp, live.amount FROM `$db`.`prr_pinned` VERSION AS OF $pv AS p " +
            s"JOIN `$db`.`prr_live` AS live ON p.id = live.id"
        )
        .collect()

      spark.sql(s"INSERT INTO $db.prr_live VALUES (1, 11)")
      val error = intercept[SourceIdentityRebindingException] {
        CommandConcurrencyInjection.withForcedPostRestoreVersionReadFailure {
          CommandConcurrencyInjection.withBeforeRefreshFinalize {
            spark.sql(s"DROP TABLE $db.prr_pinned")
            spark.sql(s"CREATE TABLE $db.prr_pinned(id INT, grp STRING) USING DELTA")
            spark.sql(s"INSERT INTO $db.prr_pinned VALUES (1, 'x')")
          } {
            spark.sql("REFRESH MATERIALIZED VIEW prr_mv").collect()
          }
        }
      }
      error.getMessage.toLowerCase should include("post-restore version read failed")
      error.getMessage.toLowerCase should include("repair-required")
      // The blocking repair state persists: a subsequent REFRESH is refused.
      intercept[Exception](
        spark.sql("REFRESH MATERIALIZED VIEW prr_mv").collect()
      ).getMessage.toLowerCase should include(
        "repair-required"
      )
    }

    it("writes a filesystem fallback journal when the catalog repair-marker upsert fails, blocking until recreate") {
      spark.sql(s"CREATE TABLE $db.fb_pinned(id INT, grp STRING) USING DELTA")
      spark.sql(s"CREATE TABLE $db.fb_live(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.fb_pinned VALUES (1, 'x')")
      spark.sql(s"INSERT INTO $db.fb_live VALUES (1, 10)")
      val pv = DeltaTableVersion.requireLatest(spark, s"$db.fb_pinned")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW fb_mv AS " +
            s"SELECT p.id, p.grp, live.amount FROM `$db`.`fb_pinned` VERSION AS OF $pv AS p " +
            s"JOIN `$db`.`fb_live` AS live ON p.id = live.id"
        )
        .collect()

      spark.sql(s"INSERT INTO $db.fb_live VALUES (1, 11)")
      // Rollback RESTORE fails AND the catalog repair-marker upsert fails -> the
      // verified filesystem fallback journal must carry the durable repair state.
      intercept[SourceIdentityRebindingException] {
        CommandConcurrencyInjection.withForcedCatalogRepairMarkerUpsertFailure {
          CommandConcurrencyInjection.withForcedPinRollbackRestoreFailure {
            CommandConcurrencyInjection.withBeforeRefreshFinalize {
              spark.sql(s"DROP TABLE $db.fb_pinned")
              spark.sql(s"CREATE TABLE $db.fb_pinned(id INT, grp STRING) USING DELTA")
              spark.sql(s"INSERT INTO $db.fb_pinned VALUES (1, 'x')")
            } {
              spark.sql("REFRESH MATERIALIZED VIEW fb_mv").collect()
            }
          }
        }
      }
      // Catalog property marker absent (upsert failed) but the FS fallback present.
      lookup("fb_mv").properties.keySet should not contain MvCommandHelper.PinRebindRepairRequiredKey
      MvCommandHelper.hasPinRepairFallbackMarker(spark, TableIdentifier("fb_mv")) shouldBe true
      // REFRESH and ADVANCE are blocked by the fallback journal.
      intercept[Exception](
        spark.sql("REFRESH MATERIALIZED VIEW fb_mv").collect()
      ).getMessage.toLowerCase should include(
        "repair-required"
      )
      val liveV = DeltaTableVersion.requireLatest(spark, s"$db.fb_live")
      intercept[Exception](
        spark.sql(s"ALTER MATERIALIZED VIEW fb_mv ADVANCE SOURCE VERSIONS ($db.fb_live = $liveV)").collect()
      ).getMessage.toLowerCase should include("repair-required")
      // DROP + recreate deterministically clears the fallback journal.
      spark.sql("DROP MATERIALIZED VIEW fb_mv")
      val pv2 = DeltaTableVersion.requireLatest(spark, s"$db.fb_pinned")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW fb_mv AS " +
            s"SELECT p.id, p.grp, live.amount FROM `$db`.`fb_pinned` VERSION AS OF $pv2 AS p " +
            s"JOIN `$db`.`fb_live` AS live ON p.id = live.id"
        )
        .collect()
      MvCommandHelper.hasPinRepairFallbackMarker(spark, TableIdentifier("fb_mv")) shouldBe false
      spark.sql(s"INSERT INTO $db.fb_live VALUES (1, 12)")
      noException should be thrownBy spark.sql("REFRESH MATERIALIZED VIEW fb_mv").collect()
    }

    it("publishes no downstream cascade/staging row when a pinned upstream refresh is rejected (intercept mode)") {
      spark.sql(s"CREATE TABLE $db.up_pinned(id INT, grp STRING) USING DELTA")
      spark.sql(s"CREATE TABLE $db.up_live(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.up_pinned VALUES (1, 'x')")
      spark.sql(s"INSERT INTO $db.up_live VALUES (1, 10)")
      val pv = DeltaTableVersion.requireLatest(spark, s"$db.up_pinned")
      spark
        .sql(
          s"CREATE MATERIALIZED VIEW up_mv AS " +
            s"SELECT p.id AS id, SUM(live.amount) AS total FROM `$db`.`up_pinned` VERSION AS OF $pv AS p " +
            s"JOIN `$db`.`up_live` AS live ON p.id = live.id GROUP BY p.id"
        )
        .collect()
      spark.sql("CREATE MATERIALIZED VIEW down_mv AS SELECT id, total FROM up_mv").collect()

      def downstreamCascadeRows: Int =
        StagingCatalog.collectFor(spark, "down_mv", Seq("up_mv", s"default.up_mv")).size

      // Control: a normal upstream refresh publishes a cascade staging row that
      // down_mv's next refresh would consume.
      spark.sql(s"INSERT INTO $db.up_live VALUES (1, 5)")
      spark.sql("REFRESH MATERIALIZED VIEW up_mv").collect()
      val afterSuccess = downstreamCascadeRows
      afterSuccess should be >= 1

      // Reject: a rebind during the next upstream refresh must publish NO new
      // cascade/staging row, and roll the upstream back.
      val upVersionBeforeReject = DeltaTableVersion.requireLatest(spark, lookup("up_mv").location)
      spark.sql(s"INSERT INTO $db.up_live VALUES (1, 7)")
      intercept[SourceIdentityRebindingException] {
        CommandConcurrencyInjection.withBeforeRefreshFinalize {
          spark.sql(s"DROP TABLE $db.up_pinned")
          spark.sql(s"CREATE TABLE $db.up_pinned(id INT, grp STRING) USING DELTA")
          spark.sql(s"INSERT INTO $db.up_pinned VALUES (1, 'x')")
        } {
          spark.sql("REFRESH MATERIALIZED VIEW up_mv").collect()
        }
      }
      // No new downstream cascade row; the upstream data rolled back (version
      // synchronized to the RESTORE commit, no forward MERGE published).
      downstreamCascadeRows shouldBe afterSuccess
      val upVersionAfterReject = DeltaTableVersion.requireLatest(spark, lookup("up_mv").location)
      lookup("up_mv").lastVersion shouldBe upVersionAfterReject
      upVersionAfterReject should be > upVersionBeforeReject
    }
  }

  // The exact `short -> (verifiedPath, clause)` map the disjoint compiler-emit
  // agent consumes (via CompileRequest.sourceSnapshotPinnedPaths and
  // SparkRefreshRewriter.rewrite) to path-bind every emitted pinned read. Locking
  // it here de-risks that integration: the command layer already produces the
  // precise data every compile/rewrite site will pass.
  describe("pinnedPathBindingsByShort compiler-emit seam") {
    def probeBinding(view: String, querySql: String, sources: Seq[String]): MvCommandHelper.PinnedSourceBinding =
      MvCommandHelper.requirePinBinding(
        spark,
        SparkTimeTravelSql.PinIdentityOperation.Create,
        TableIdentifier(view),
        querySql,
        sources,
        sources.map(s => s.split("\\.").last -> s).toMap,
        Map.empty
      )

    it("collapses a self-joined pinned source to one verified path + VERSION clause entry") {
      spark.sql(s"CREATE TABLE $db.sm1(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.sm1 VALUES (1)")
      val v         = DeltaTableVersion.requireLatest(spark, s"$db.sm1")
      val (path, _) = MvCommandHelper.deltaPhysicalIdentity(spark, s"$db.sm1").getOrElse(fail("expected identity"))

      val binding = probeBinding(
        "sm1_probe",
        s"SELECT a.id FROM `$db`.`sm1` VERSION AS OF $v AS a JOIN `$db`.`sm1` VERSION AS OF $v AS b ON a.id = b.id",
        Seq(s"$db.sm1")
      )
      binding.resolvedPins should have size 2
      val bound = MvCommandHelper.pinnedPathBindingsByShort(binding)
      bound should have size 1
      bound("sm1") shouldBe ((path, s"VERSION AS OF $v"))
    }

    it("preserves a TIMESTAMP AS OF clause verbatim, keyed by the verified path") {
      spark.sql(s"CREATE TABLE $db.sm2(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.sm2 VALUES (1)")
      val (path, _) = MvCommandHelper.deltaPhysicalIdentity(spark, s"$db.sm2").getOrElse(fail("expected identity"))
      val clause    = "TIMESTAMP AS OF '2020-01-01 00:00:00'"

      val binding = probeBinding("sm2_probe", s"SELECT id FROM `$db`.`sm2` $clause", Seq(s"$db.sm2"))
      val bound   = MvCommandHelper.pinnedPathBindingsByShort(binding)
      bound should have size 1
      bound("sm2") shouldBe ((path, clause))
    }

    it("maps two distinct pinned sources each to its own verified path and clause") {
      spark.sql(s"CREATE TABLE $db.sm3a(id INT) USING DELTA")
      spark.sql(s"CREATE TABLE $db.sm3b(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.sm3a VALUES (1)")
      spark.sql(s"INSERT INTO $db.sm3b VALUES (1)")
      val va         = DeltaTableVersion.requireLatest(spark, s"$db.sm3a")
      val vb         = DeltaTableVersion.requireLatest(spark, s"$db.sm3b")
      val (pathA, _) = MvCommandHelper.deltaPhysicalIdentity(spark, s"$db.sm3a").getOrElse(fail("a"))
      val (pathB, _) = MvCommandHelper.deltaPhysicalIdentity(spark, s"$db.sm3b").getOrElse(fail("b"))

      val binding = probeBinding(
        "sm3_probe",
        s"SELECT a.id FROM `$db`.`sm3a` VERSION AS OF $va AS a " +
          s"JOIN `$db`.`sm3b` VERSION AS OF $vb AS b ON a.id = b.id",
        Seq(s"$db.sm3a", s"$db.sm3b")
      )
      val bound = MvCommandHelper.pinnedPathBindingsByShort(binding)
      bound.keySet shouldBe Set("sm3a", "sm3b")
      bound("sm3a") shouldBe ((pathA, s"VERSION AS OF $va"))
      bound("sm3b") shouldBe ((pathB, s"VERSION AS OF $vb"))
      pathA should not be pathB
    }
  }

  // BLOCKER 1: `split` lifts no pins for BOTH a provably unpinned body and an
  // unsupported snapshot shape it refuses to rewrite (a source read at two
  // versions, or pinned in one place and read live in another). The command
  // layer must hard-fail the latter as a fail-closed AnalysisException -- never
  // let it fall through to NoPin and a COMPILE_FAILED/FULL_REFRESH demotion that
  // could silently maintain a frozen relation from live rows.
  describe("Tri-state snapshot-pin resolution refuses unsupported shapes (BLOCKER 1)") {
    it("resolves a source read at two different versions to Failure, not NoPin") {
      spark.sql(s"CREATE TABLE $db.ts_xver(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.ts_xver VALUES (1)")
      val v0 = DeltaTableVersion.requireLatest(spark, s"$db.ts_xver")
      spark.sql(s"INSERT INTO $db.ts_xver VALUES (2)")
      val v1 = DeltaTableVersion.requireLatest(spark, s"$db.ts_xver")

      val body =
        s"SELECT a.id FROM `$db`.`ts_xver` VERSION AS OF $v0 AS a " +
          s"JOIN `$db`.`ts_xver` VERSION AS OF $v1 AS b ON a.id = b.id"
      MvCommandHelper.resolvePinnedSources(spark, body, Seq(s"$db.ts_xver")) match {
        case MvCommandHelper.PinResolution.Failure(reason) =>
          reason.toLowerCase should include("unsupported snapshot-pin shape")
        case other => fail(s"expected Failure, got $other")
      }
    }

    it("resolves a source pinned in one place and read live in another to Failure, not NoPin") {
      spark.sql(s"CREATE TABLE $db.ts_live(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.ts_live VALUES (1)")
      val v = DeltaTableVersion.requireLatest(spark, s"$db.ts_live")

      val body =
        s"SELECT a.id FROM `$db`.`ts_live` VERSION AS OF $v AS a " +
          s"JOIN `$db`.`ts_live` AS b ON a.id = b.id"
      MvCommandHelper.resolvePinnedSources(spark, body, Seq(s"$db.ts_live")) match {
        case MvCommandHelper.PinResolution.Failure(reason) =>
          reason.toLowerCase should include("unsupported snapshot-pin shape")
        case other => fail(s"expected Failure, got $other")
      }
    }

    it("resolves a body with no temporal clause to NoPin") {
      spark.sql(s"CREATE TABLE $db.ts_none(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.ts_none VALUES (1)")
      MvCommandHelper.resolvePinnedSources(
        spark,
        s"SELECT id FROM `$db`.`ts_none`",
        Seq(s"$db.ts_none")
      ) shouldBe MvCommandHelper.PinResolution.NoPin
    }

    it("resolves a same-version self-join to Verified") {
      spark.sql(s"CREATE TABLE $db.ts_same(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.ts_same VALUES (1)")
      val v = DeltaTableVersion.requireLatest(spark, s"$db.ts_same")

      val body =
        s"SELECT a.id FROM `$db`.`ts_same` VERSION AS OF $v AS a " +
          s"JOIN `$db`.`ts_same` VERSION AS OF $v AS b ON a.id = b.id"
      MvCommandHelper.resolvePinnedSources(spark, body, Seq(s"$db.ts_same")) match {
        case MvCommandHelper.PinResolution.Verified(resolvedPins, _) =>
          resolvedPins should not be empty
        case other => fail(s"expected Verified, got $other")
      }
    }

    it("hard-fails CREATE for a cross-version self-join before compile, leaving no view") {
      spark.sql(s"CREATE TABLE $db.cv_src(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.cv_src VALUES (1)")
      val v0 = DeltaTableVersion.requireLatest(spark, s"$db.cv_src")
      spark.sql(s"INSERT INTO $db.cv_src VALUES (2)")
      val v1 = DeltaTableVersion.requireLatest(spark, s"$db.cv_src")

      val error = intercept[AnalysisException] {
        spark
          .sql(
            s"CREATE MATERIALIZED VIEW cv_mv AS SELECT a.id FROM `$db`.`cv_src` VERSION AS OF $v0 AS a " +
              s"JOIN `$db`.`cv_src` VERSION AS OF $v1 AS b ON a.id = b.id"
          )
          .collect()
      }
      error.getMessage.toLowerCase should include("unsupported snapshot-pin shape")
      classOf[org.openivm.spark.compiler.OpenIvmCompileException].isInstance(error) shouldBe false
      MvCatalog.lookup(spark, TableIdentifier("cv_mv")) shouldBe None
    }

    it("hard-fails CREATE for a pinned+live read before compile, leaving no view") {
      spark.sql(s"CREATE TABLE $db.pl_src(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.pl_src VALUES (1)")
      val v = DeltaTableVersion.requireLatest(spark, s"$db.pl_src")

      val error = intercept[AnalysisException] {
        spark
          .sql(
            s"CREATE MATERIALIZED VIEW pl_mv AS SELECT a.id FROM `$db`.`pl_src` VERSION AS OF $v AS a " +
              s"JOIN `$db`.`pl_src` AS b ON a.id = b.id"
          )
          .collect()
      }
      error.getMessage.toLowerCase should include("unsupported snapshot-pin shape")
      classOf[org.openivm.spark.compiler.OpenIvmCompileException].isInstance(error) shouldBe false
      MvCatalog.lookup(spark, TableIdentifier("pl_mv")) shouldBe None
    }

    it("hard-fails the dry compile for a cross-version self-join before compile") {
      spark.sql(s"CREATE TABLE $db.dry_src(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.dry_src VALUES (1)")
      val v0 = DeltaTableVersion.requireLatest(spark, s"$db.dry_src")
      spark.sql(s"INSERT INTO $db.dry_src VALUES (2)")
      val v1 = DeltaTableVersion.requireLatest(spark, s"$db.dry_src")

      val body =
        s"SELECT a.id FROM `$db`.`dry_src` VERSION AS OF $v0 AS a " +
          s"JOIN `$db`.`dry_src` VERSION AS OF $v1 AS b ON a.id = b.id"
      val error = intercept[AnalysisException] {
        MvDryCompile.dryCompile(spark, TableIdentifier("dry_mv"), body)
      }
      error.getMessage.toLowerCase should include("unsupported snapshot-pin shape")
      classOf[org.openivm.spark.compiler.OpenIvmCompileException].isInstance(error) shouldBe false
    }
  }

  // BLOCKER 4: `resolveAdvanceRequest` used to `versionsBySource.map` straight
  // onto the resolved operational alias, so two textually distinct request
  // identifiers over ONE physical Delta source collapsed to a single entry
  // (last-writer-wins, iteration-order dependent) EVEN when their versions
  // disagreed. Duplicate physical identity must be rejected before the map is
  // keyed; distinct sources that merely share a leaf name must not be fused.
  describe("ADVANCE request resolution rejects duplicate physical identity (BLOCKER 4)") {
    it("rejects two distinct identifiers resolving to one physical source with equal versions") {
      spark.sql(s"CREATE TABLE $db.adv_dup(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.adv_dup VALUES (1)")
      val v         = DeltaTableVersion.requireLatest(spark, s"$db.adv_dup")
      val plain     = s"$db.adv_dup"
      val quoted    = s"`$db`.`adv_dup`"
      val persisted = Seq(advancePin(plain, s"VERSION AS OF $v"))

      // Both spellings parse to the same TableIdentifier and therefore the same
      // physical Delta identity, but remain distinct request-map keys.
      MvCommandHelper.deltaPhysicalIdentity(spark, quoted) shouldBe
        MvCommandHelper.deltaPhysicalIdentity(spark, plain)

      val error = intercept[AnalysisException] {
        MvCommandHelper.resolveAdvanceRequest(
          spark,
          TableIdentifier("adv_dup_mv"),
          Map(plain -> v, quoted -> v),
          persisted
        )
      }
      error.getMessage should include(plain)
      error.getMessage should include(quoted)
    }

    it("rejects two distinct identifiers resolving to one physical source when versions differ") {
      spark.sql(s"CREATE TABLE $db.adv_diff(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.adv_diff VALUES (1)")
      val v         = DeltaTableVersion.requireLatest(spark, s"$db.adv_diff")
      val plain     = s"$db.adv_diff"
      val quoted    = s"`$db`.`adv_diff`"
      val persisted = Seq(advancePin(plain, s"VERSION AS OF $v"))

      val error = intercept[AnalysisException] {
        MvCommandHelper.resolveAdvanceRequest(
          spark,
          TableIdentifier("adv_diff_mv"),
          Map(plain -> v, quoted -> (v + 1L)),
          persisted
        )
      }
      error.getMessage should include(plain)
      error.getMessage should include(quoted)
    }

    it("accepts two distinct physical sources that share a leaf name") {
      spark.sql(s"CREATE TABLE default.adv_leaf(id INT) USING DELTA")
      spark.sql(s"INSERT INTO default.adv_leaf VALUES (1)")
      spark.sql(s"CREATE TABLE $db.adv_leaf(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.adv_leaf VALUES (1)")
      val vLeft  = DeltaTableVersion.requireLatest(spark, "default.adv_leaf")
      val vRight = DeltaTableVersion.requireLatest(spark, s"$db.adv_leaf")
      val persisted = Seq(
        advancePin("default.adv_leaf", s"VERSION AS OF $vLeft"),
        advancePin(s"$db.adv_leaf", s"VERSION AS OF $vRight")
      )

      val resolved = MvCommandHelper.resolveAdvanceRequest(
        spark,
        TableIdentifier("leaf_mv"),
        Map("default.adv_leaf" -> vLeft, s"$db.adv_leaf" -> vRight),
        persisted
      )
      resolved shouldBe Map("default.adv_leaf" -> vLeft, s"$db.adv_leaf" -> vRight)
    }

    it("accepts a single exact request identifier") {
      spark.sql(s"CREATE TABLE $db.adv_solo(id INT) USING DELTA")
      spark.sql(s"INSERT INTO $db.adv_solo VALUES (1)")
      val v         = DeltaTableVersion.requireLatest(spark, s"$db.adv_solo")
      val persisted = Seq(advancePin(s"$db.adv_solo", s"VERSION AS OF $v"))

      MvCommandHelper.resolveAdvanceRequest(
        spark,
        TableIdentifier("solo_mv"),
        Map(s"$db.adv_solo" -> v),
        persisted
      ) shouldBe Map(s"$db.adv_solo" -> v)
    }
  }
}
