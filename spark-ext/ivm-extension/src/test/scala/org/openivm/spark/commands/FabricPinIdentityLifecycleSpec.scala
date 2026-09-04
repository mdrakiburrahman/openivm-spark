package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{DeltaTableVersion, MvCatalog, MvMetadata, TimeTravelPinStatus}
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
  }
}
