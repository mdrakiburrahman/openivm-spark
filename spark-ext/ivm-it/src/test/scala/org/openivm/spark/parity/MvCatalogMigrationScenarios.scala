package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types._
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry
import org.openivm.spark.common.{MvCatalog, MvMetadata, RefreshTypeCode, StagingCatalog}

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.util.UUID

abstract class MvCatalogMigrationScenarios extends IvmParitySpecBase("mv-catalog-migration") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected val warehouseRoot: File = {
    val d = new File(s"target/test-warehouse-mvc-migration-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d
  }

  protected val legacyMvSchema = StructType(
    Array(
      StructField("name", StringType, nullable = false),
      StructField("query_sql", StringType, nullable = false),
      StructField("refresh_type", IntegerType, nullable = false),
      StructField("refresh_type_name", StringType, nullable = false),
      StructField("last_version", LongType, nullable = false),
      StructField("source_tables", ArrayType(StringType, containsNull = false), nullable = false),
      StructField("source_schema_fingerprint", StringType, nullable = false),
      StructField("location", StringType, nullable = false),
      StructField("created_at", TimestampType, nullable = false),
      StructField("properties", MapType(StringType, StringType), nullable = true)
    )
  )

  protected val legacyStagingSchema = StructType(
    Array(
      StructField("base_table", StringType, nullable = false),
      StructField("op_type", StringType, nullable = false),
      StructField("staging_path", StringType, nullable = false),
      StructField("txn_ts", TimestampType, nullable = false),
      StructField("consumed_by", ArrayType(StringType, containsNull = false), nullable = false)
    )
  )

  override def afterAll(): Unit =
    try {
      OpenIvmRocksDBRegistry.closeAll()
      deleteDir(warehouseRoot)
    } finally {
      super.afterAll()
    }

  override protected def deleteDir(f: File): Unit = {
    if (f.exists()) {
      if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
      f.delete()
    }
    ()
  }

  protected def withSpark(warehouseName: String)(f: (SparkSession, String) => Unit): Unit = {
    val warehouseDir = new File(warehouseRoot, warehouseName)
    warehouseDir.mkdirs()

    var spark: SparkSession = null
    try {
      spark = SparkSession
        .builder()
        .master("local[1]")
        .appName(s"openivm-spark-MvCatalogMigrationSpec-$warehouseName")
        .config(
          "spark.sql.extensions",
          "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.openivm.enabled", "true")
        .config("spark.sql.warehouse.dir", warehouseDir.getAbsolutePath)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
      f(spark, warehouseDir.getAbsolutePath)
    } finally {
      try {
        if (spark != null) spark.stop()
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        OpenIvmRocksDBRegistry.closeAll()
      }
    }
  }

  protected def legacyMvMetaPath(warehouseDir: String): String =
    Paths.get(warehouseDir, "_ivm", "_meta", "mv_metadata").toString

  protected def legacyStagingPath(warehouseDir: String): String =
    Paths.get(warehouseDir, "_ivm", "_meta", "staging").toString

  protected def pathExists(path: String): Boolean = Files.exists(Paths.get(path))

  protected def writeDeltaRows(spark: SparkSession, path: String, schema: StructType, rows: Seq[Row]): Unit =
    spark
      .createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write
      .format("delta")
      .mode("overwrite")
      .save(path)

  protected def sampleMeta(warehouseDir: String, name: String): MvMetadata =
    MvMetadata(
      name = TableIdentifier(name),
      querySql = "SELECT id FROM orders",
      refreshType = RefreshTypeCode.SimpleProjection,
      refreshTypeName = "SIMPLE_PROJECTION",
      lastVersion = 7L,
      sourceTables = Seq("orders"),
      sourceSchemaFingerprint = MvCatalog.schemaFingerprint(
        Map("orders" -> StructType(Seq(StructField("id", IntegerType, nullable = false))))
      ),
      location = Paths.get(warehouseDir, s"$name-location").toString,
      createdAt = new Timestamp(1700001000000L),
      properties = Map("owner" -> "migration-spec")
    )

  describe("MvCatalog legacy Delta migration") {
    itIntercept("fresh-no-legacy") {
      withSpark("fresh-no-legacy") { (spark, warehouseDir) =>
        MvCatalog.ensureTables(spark)
        StagingCatalog.ensureTables(spark)

        pathExists(legacyMvMetaPath(warehouseDir)) shouldBe false
        pathExists(legacyStagingPath(warehouseDir)) shouldBe false
        MvCatalog.list(spark) shouldBe empty
      }
    }

    itIntercept("fresh-with-legacy") {
      withSpark("fresh-with-legacy") { (spark, warehouseDir) =>
        val legacyMvMeta    = legacyMvMetaPath(warehouseDir)
        val legacyStaging   = legacyStagingPath(warehouseDir)
        val consumedPath    = Paths.get(warehouseDir, "legacy-staging", "consumed").toString
        val pendingPath     = Paths.get(warehouseDir, "legacy-staging", "pending").toString
        val mvLocation      = Paths.get(warehouseDir, "mv-mig-location").toString
        val createdAt       = new Timestamp(1700000005000L)
        val consumedStaging = new Timestamp(1700000001000L)
        val pendingStaging  = new Timestamp(1700000002000L)

        writeDeltaRows(
          spark,
          legacyMvMeta,
          legacyMvSchema,
          Seq(
            Row(
              "mv_mig",
              "SELECT id FROM orders",
              RefreshTypeCode.SimpleProjection,
              "SIMPLE_PROJECTION",
              5L,
              Seq("orders"),
              "fp-orders",
              mvLocation,
              createdAt,
              Map("owner" -> "legacy")
            )
          )
        )
        writeDeltaRows(
          spark,
          legacyStaging,
          legacyStagingSchema,
          Seq(
            Row("orders", "INSERT", consumedPath, consumedStaging, Seq("mv_mig")),
            Row("orders", "INSERT", pendingPath, pendingStaging, Seq.empty[String])
          )
        )

        pathExists(legacyMvMeta) shouldBe true
        pathExists(legacyStaging) shouldBe true

        MvCatalog.ensureTables(spark)
        StagingCatalog.ensureTables(spark)

        val migrated = MvCatalog.lookup(spark, TableIdentifier("mv_mig")).getOrElse(fail("mv_mig not migrated"))
        migrated.lastVersion shouldBe 5L
        migrated.sourceTables shouldBe Seq("orders")

        val pending = StagingCatalog.collectFor(spark, "mv_mig", Seq("orders"))
        pending.map(_.stagingPath) shouldBe Seq(pendingPath)

        pathExists(legacyMvMeta) shouldBe false
        pathExists(legacyStaging) shouldBe false

        noException should be thrownBy MvCatalog.ensureTables(spark)
        StagingCatalog.collectFor(spark, "mv_mig", Seq("orders")).map(_.stagingPath) shouldBe Seq(pendingPath)
      }
    }

    itIntercept("already-migrated") {
      withSpark("already-migrated") { (spark, warehouseDir) =>
        val legacyMvMeta = legacyMvMetaPath(warehouseDir)
        val liveMeta     = sampleMeta(warehouseDir, "mv_live")

        MvCatalog.ensureTables(spark)
        StagingCatalog.ensureTables(spark)
        MvCatalog.upsert(spark, liveMeta)

        writeDeltaRows(
          spark,
          legacyMvMeta,
          legacyMvSchema,
          Seq(
            Row(
              "mv_bogus",
              "SELECT 1",
              RefreshTypeCode.FullRefresh,
              "FULL_REFRESH",
              999L,
              Seq("bogus_source"),
              "fp-bogus",
              Paths.get(warehouseDir, "mv-bogus-location").toString,
              new Timestamp(1700002000000L),
              Map("owner" -> "bogus")
            )
          )
        )

        MvCatalog.ensureTables(spark)

        MvCatalog.lookup(spark, liveMeta.name) shouldBe Some(liveMeta)
        MvCatalog.lookup(spark, TableIdentifier("mv_bogus")) shouldBe None
        // Once RocksDB already has MV rows, migration short-circuits and leaves any later
        // legacy Delta tables untouched rather than re-clobbering live state.
        pathExists(legacyMvMeta) shouldBe true
      }
    }
  }
}
