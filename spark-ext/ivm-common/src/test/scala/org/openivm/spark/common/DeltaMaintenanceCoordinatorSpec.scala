package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.UUID

class DeltaMaintenanceCoordinatorSpec extends AnyFunSpec with BeforeAndAfterEach with Matchers {

  private var spark: SparkSession = _
  private var warehouseDir: File  = _

  override def afterEach(): Unit = {
    DeltaMaintenanceCoordinator.shutdown()
    if (spark != null) {
      spark.stop()
      spark = null
    }
    OpenIvmRocksDBRegistry.closeAll()
    if (warehouseDir != null) {
      deleteDir(warehouseDir)
      warehouseDir = null
    }
    super.afterEach()
  }

  private def newSpark(name: String, conf: (String, String)*): SparkSession = {
    warehouseDir = new File(s"target/test-warehouse-delta-maint-$name-${UUID.randomUUID().toString.take(8)}")
    warehouseDir.mkdirs()
    val builder = SparkSession
      .builder()
      .master("local[1]")
      .appName(s"openivm-spark-DeltaMaintenanceCoordinatorSpec-$name")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir.getAbsolutePath)
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
    conf.foreach { case (k, v) => builder.config(k, v) }
    spark = builder.getOrCreate()
    MvCatalog.ensureTables(spark)
    spark
  }

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  private def meta(name: String, location: String, properties: Map[String, String] = Map.empty): MvMetadata =
    MvMetadata(
      name = CatalystSqlParser.parseTableIdentifier(s"default.$name"),
      querySql = "SELECT id FROM source_table",
      refreshType = RefreshTypeCode.SimpleProjection,
      refreshTypeName = "SIMPLE_PROJECTION",
      lastVersion = 0L,
      sourceTables = Seq("default.source_table"),
      sourceSchemaFingerprint = MvCatalog.schemaFingerprint(
        Map("default.source_table" -> StructType(Seq(StructField("id", IntegerType))))
      ),
      location = location,
      createdAt = new Timestamp(1700000000000L),
      properties = properties
    )

  private def writeSmallDeltaFiles(location: String, count: Int): Unit =
    (1 to count).foreach { i =>
      spark
        .range(i.toLong, i.toLong + 1L)
        .coalesce(1)
        .write
        .format("delta")
        .mode(if (i == 1) "overwrite" else "append")
        .save(location)
    }

  private def fileCount(location: String): Long =
    spark
      .sql(s"DESCRIBE DETAIL delta.`${location.replace("`", "``")}`")
      .select("numFiles")
      .head()
      .get(0) match {
      case n: java.lang.Number => n.longValue()
      case other               => other.toString.toLong
    }

  describe("DeltaMaintenanceCoordinator") {
    it("does not start when all maintenance flags are off") {
      val s = newSpark("off")

      DeltaMaintenanceCoordinator.ensureStarted(s)

      DeltaMaintenanceCoordinator.isRunning shouldBe false
    }

    it("builds compaction OPTIMIZE and VACUUM SQL") {
      val loc = "/warehouse/mv_sql"
      DeltaMaintenanceCoordinator.optimizeSql(loc) shouldBe
        "OPTIMIZE delta.`/warehouse/mv_sql`"
      DeltaMaintenanceCoordinator.vacuumSql(loc, 168) shouldBe
        "VACUUM delta.`/warehouse/mv_sql` RETAIN 168 HOURS"
    }

    it("compacts Delta tables above the configured file threshold") {
      val s = newSpark(
        "optimize",
        FeatureGate.MaintenanceOptimizeEnabledKey  -> "true",
        FeatureGate.MaintenanceOptimizeMinFilesKey -> "2"
      )
      val location = new File(warehouseDir, "mv_optimize").getAbsolutePath
      writeSmallDeltaFiles(location, 6)
      val before = fileCount(location)
      before should be > 2L
      MvCatalog.upsert(s, meta("mv_optimize", location))

      DeltaMaintenanceCoordinator.runOnceForTesting(s)

      fileCount(location) should be < before
    }

    it("skips in-progress MV locations") {
      val s = newSpark(
        "inprogress",
        FeatureGate.MaintenanceOptimizeEnabledKey  -> "true",
        FeatureGate.MaintenanceOptimizeMinFilesKey -> "2"
      )
      val location = new File(warehouseDir, "mv_inprogress").getAbsolutePath
      writeSmallDeltaFiles(location, 6)
      val before = fileCount(location)
      MvCatalog.upsert(s, meta("mv_inprogress", location))
      DeltaMaintenanceCoordinator.markRefreshInProgress(location)

      try {
        DeltaMaintenanceCoordinator.runOnceForTesting(s)
        fileCount(location) shouldBe before
      } finally {
        DeltaMaintenanceCoordinator.clearRefreshInProgress(location)
      }
    }
  }
}
