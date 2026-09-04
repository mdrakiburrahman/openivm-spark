package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.openivm.spark.common.{
  CdfWatermarkCatalog,
  ChangeWatermark,
  FeatureGate,
  MvCatalog,
  MvMetadata,
  RefreshTypeCode,
  StagingCatalog,
  TimeTravelPinStatus
}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.UUID

class FabricPinAliasObservationSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val friendlyDatabase = "fpa_friendly_source"
  private val encodedDatabase  = "__fabric_encoded__arc_sql_db_bi_7d1f"
  private val warehouseDir = {
    val directory = new File(s"target/test-warehouse-fabric-pin-alias-${UUID.randomUUID().toString.take(8)}")
    directory.mkdirs()
    directory.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-FabricPinAliasObservationSpec")
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
    MvCatalog.ensureTables(spark)
    CdfWatermarkCatalog.ensureTables(spark)
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
    } finally {
      super.afterAll()
    }

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  private def withEncodedFabricDatabase(plan: LogicalPlan): LogicalPlan = {
    val withEncodedRelations = plan.transformDown {
      case relation: LogicalRelation
          if relation.catalogTable.exists(_.identifier.database.contains(friendlyDatabase)) =>
        val catalogTable = relation.catalogTable.get
        relation.copy(
          catalogTable = Some(
            catalogTable.copy(identifier = catalogTable.identifier.copy(database = Some(encodedDatabase)))
          )
        )
    }
    withEncodedRelations.transformAllExpressions { case subquery: SubqueryExpression =>
      subquery.withNewPlan(withEncodedFabricDatabase(subquery.plan))
    }
  }

  private def locationUri(plan: LogicalPlan): String =
    plan
      .collectFirst {
        case relation: LogicalRelation if relation.catalogTable.isDefined =>
          relation.catalogTable.flatMap(_.storage.locationUri).map(_.toString)
      }
      .flatten
      .getOrElse(fail("missing V1 catalogTable.storage.locationUri"))

  private def deltaLogDataPath(table: String): String =
    DeltaLog.forTable(spark, spark.sessionState.sqlParser.parseTableIdentifier(table)).dataPath.toString

  private def deltaTableMetadataId(table: String): String =
    spark.sql(s"DESCRIBE DETAIL $table").select("id").head().getString(0)

  private def latestVersion(table: String): Long =
    spark.sql(s"DESCRIBE HISTORY $table").selectExpr("max(version)").head().getLong(0)

  private def withSparkConf[A](key: String, value: String)(body: => A): A = {
    val previous = spark.conf.getOption(key)
    spark.conf.set(key, value)
    try body
    finally
      previous match {
        case Some(existing) => spark.conf.set(key, existing)
        case None           => spark.conf.unset(key)
      }
  }

  describe("Fabric V1 source observation") {
    it("keeps resolved operational names through temp and global-temp expansions, catalog, and CDF") {
      val tempSource   = s"$friendlyDatabase.fpa_temp_source"
      val globalSource = s"$friendlyDatabase.fpa_global_source"
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $friendlyDatabase")
      spark.sql(s"CREATE TABLE $tempSource(id INT) USING DELTA")
      spark.sql(s"CREATE TABLE $globalSource(id INT) USING DELTA")
      spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW fpa_temp_alias AS SELECT id FROM $tempSource")
      spark.sql(s"CREATE OR REPLACE GLOBAL TEMPORARY VIEW fpa_global_alias AS SELECT id FROM $globalSource")

      val friendlyV1Plan =
        spark.sql(s"SELECT id FROM `$friendlyDatabase`.`fpa_temp_source`").queryExecution.analyzed
      val encodedV1Plan = withEncodedFabricDatabase(friendlyV1Plan)
      val dataPath      = deltaLogDataPath(tempSource)
      locationUri(friendlyV1Plan) shouldBe dataPath
      locationUri(encodedV1Plan) shouldBe dataPath
      deltaTableMetadataId(tempSource) should not be empty

      val query =
        """WITH temp_cte AS (
          |  SELECT id FROM fpa_temp_alias
          |)
          |SELECT t.id,
          |       (SELECT MAX(g.id) FROM global_temp.fpa_global_alias g) AS global_max
          |FROM temp_cte t
          |WHERE EXISTS (
          |  SELECT 1 FROM global_temp.fpa_global_alias nested_global WHERE nested_global.id = t.id
          |)""".stripMargin
      val (sourceTables, sourceSchemas, compileSchemas, shortToOperational) =
        MvCommandHelper.collectSourceSchemas(withEncodedFabricDatabase(spark.sql(query).queryExecution.analyzed))

      val expectedSources = Set(
        s"$encodedDatabase.fpa_temp_source",
        s"$encodedDatabase.fpa_global_source"
      )
      sourceTables.toSet shouldBe expectedSources
      sourceSchemas.keySet shouldBe expectedSources
      compileSchemas.keySet shouldBe Set("fpa_temp_source", "fpa_global_source")
      shortToOperational.values.toSet shouldBe expectedSources

      val watermarks = sourceTables.map(_ -> ChangeWatermark.DeltaVersion(46094L)).toMap
      val metadata = MvMetadata(
        name = TableIdentifier("fpa_alias_observation"),
        querySql = query,
        refreshType = RefreshTypeCode.SimpleProjection,
        refreshTypeName = "SIMPLE_PROJECTION",
        lastVersion = 0L,
        sourceTables = sourceTables,
        sourceSchemaFingerprint = MvCatalog.schemaFingerprint(sourceSchemas),
        location = s"$warehouseDir/fpa_alias_observation",
        createdAt = new Timestamp(System.currentTimeMillis()),
        properties = MvMetadata.changeWatermarkProperties(watermarks)
      )

      try {
        MvCatalog.upsert(spark, metadata)
        val persisted = MvCatalog.lookup(spark, metadata.name).getOrElse(fail("missing persisted MV metadata"))
        persisted.sourceTables.toSet shouldBe expectedSources
        persisted.changeWatermarks shouldBe watermarks
        expectedSources.foreach { source =>
          MvCatalog.viewsForSource(spark, source).map(_.name) should contain(metadata.name)
        }

        val viewName = MvCommandHelper.metaName(metadata.name)
        CdfWatermarkCatalog.putAll(spark, viewName, sourceTables.map(_ -> 46094L).toMap)
        CdfWatermarkCatalog.getAll(spark, viewName, sourceTables) shouldBe sourceTables.map(_ -> 46094L).toMap
      } finally {
        CdfWatermarkCatalog.removeForView(spark, MvCommandHelper.metaName(metadata.name))
        MvCatalog.remove(spark, metadata.name)
      }
    }

    it("keeps same-leaf non-pinned sources distinct in operational metadata and CDF keys") {
      val leftDatabase  = "fpa_left_source"
      val rightDatabase = "fpa_right_source"
      val leftSource    = s"$leftDatabase.foo"
      val rightSource   = s"$rightDatabase.foo"

      spark.sql(s"CREATE DATABASE IF NOT EXISTS $leftDatabase")
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $rightDatabase")
      spark.sql(s"CREATE TABLE $leftSource(id INT) USING DELTA")
      spark.sql(s"CREATE TABLE $rightSource(id INT) USING DELTA")
      val query =
        s"""SELECT l.id AS left_id, r.id AS right_id
           |FROM `$leftDatabase`.`foo` AS l
           |JOIN `$rightDatabase`.`foo` AS r ON l.id = r.id""".stripMargin
      val (sourceTables, sourceSchemas, _, _) =
        MvCommandHelper.collectSourceSchemas(spark.sql(query).queryExecution.analyzed)
      sourceTables.toSet shouldBe Set(leftSource, rightSource)
      sourceSchemas.keySet shouldBe Set(leftSource, rightSource)

      val watermarks = sourceTables.map(_ -> ChangeWatermark.DeltaVersion(46094L)).toMap
      val metadata = MvMetadata(
        name = TableIdentifier("fpa_non_pinned_same_leaf"),
        querySql = query,
        refreshType = RefreshTypeCode.SimpleProjection,
        refreshTypeName = "SIMPLE_PROJECTION",
        lastVersion = 0L,
        sourceTables = sourceTables,
        sourceSchemaFingerprint = MvCatalog.schemaFingerprint(sourceSchemas),
        location = s"$warehouseDir/fpa_non_pinned_same_leaf",
        createdAt = new Timestamp(System.currentTimeMillis()),
        properties = MvMetadata.changeWatermarkProperties(watermarks)
      )

      try {
        MvCatalog.upsert(spark, metadata)
        MvCatalog.lookup(spark, metadata.name).getOrElse(fail("missing MV metadata")).sourceTables.toSet shouldBe
          Set(leftSource, rightSource)
        val viewName = MvCommandHelper.metaName(metadata.name)
        CdfWatermarkCatalog.putAll(spark, viewName, sourceTables.map(_ -> 46094L).toMap)
        CdfWatermarkCatalog.getAll(spark, viewName, sourceTables) shouldBe sourceTables.map(_ -> 46094L).toMap
      } finally {
        CdfWatermarkCatalog.removeForView(spark, MvCommandHelper.metaName(metadata.name))
        MvCatalog.remove(spark, metadata.name)
      }
    }

    it("recompiles the same friendly pinned MV twice and keeps both refreshes APPLIED") {
      val pinnedSource = s"$friendlyDatabase.fpa_recompile_pinned"
      val liveSource   = s"$friendlyDatabase.fpa_recompile_live"
      val materialized = "fpa_recompile_pinned_mv"

      spark.sql(s"CREATE DATABASE IF NOT EXISTS $friendlyDatabase")
      spark.sql(s"CREATE TABLE $pinnedSource(id INT, grp STRING) USING DELTA")
      spark.sql(s"CREATE TABLE $liveSource(id INT, amount INT) USING DELTA")
      spark.sql(s"INSERT INTO $pinnedSource VALUES (1, 'a')")
      spark.sql(s"INSERT INTO $liveSource VALUES (1, 10)")
      val pinnedVersion = latestVersion(pinnedSource)
      val query =
        s"""SELECT p.id, p.grp, live.amount
           |FROM `$friendlyDatabase`.`fpa_recompile_pinned` VERSION AS OF $pinnedVersion AS p
           |JOIN `$friendlyDatabase`.`fpa_recompile_live` AS live ON p.id = live.id""".stripMargin

      withSparkConf(FeatureGate.CompileClassificationCacheEnabledKey, "false") {
        val compilerCount = OpenIvmMetrics.counter("compiler.compile.count")
        val beforeCreate  = compilerCount.getCount
        spark.sql(s"CREATE MATERIALIZED VIEW $materialized AS $query")
        val afterCreate = compilerCount.getCount
        afterCreate shouldBe beforeCreate + 1L

        def assertRecompiled(expectedCompilerCount: Long): Unit = {
          compilerCount.getCount shouldBe expectedCompilerCount
          val metadata = MvCatalog.lookup(spark, TableIdentifier(materialized)).getOrElse(fail("missing MV metadata"))
          metadata.timeTravelPinStatus shouldBe Some(TimeTravelPinStatus.Applied)
          metadata.properties.getOrElse(MvMetadata.CompileRefreshTypeKey, "") should not be "COMPILE_FAILED"
          metadata.querySql should include(s"`$friendlyDatabase`.`fpa_recompile_pinned` VERSION AS OF $pinnedVersion")
        }

        spark.sql(s"INSERT INTO $liveSource VALUES (1, 11)")
        spark.sql(s"REFRESH MATERIALIZED VIEW $materialized")
        assertRecompiled(afterCreate + 1L)

        spark.sql(s"INSERT INTO $liveSource VALUES (1, 12)")
        spark.sql(s"REFRESH MATERIALIZED VIEW $materialized")
        assertRecompiled(afterCreate + 2L)
      }
    }
  }
}
