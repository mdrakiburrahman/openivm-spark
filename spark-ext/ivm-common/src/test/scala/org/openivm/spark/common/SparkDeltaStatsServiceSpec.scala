package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class SparkDeltaStatsServiceSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-stats-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }
  private val tableName = s"stats_orders_${UUID.randomUUID().toString.replace("-", "").take(8)}"

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-SparkDeltaStatsServiceSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.databricks.delta.stats.collect", "true")
      .config("spark.databricks.delta.dataSkipping.numIndexedCols", "32")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        spark.stop()
      }
    } finally {
      deleteDir(new File(warehouseDir))
    }
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  describe("SparkDeltaStatsService") {
    it("extracts Delta file stats, table totals, catalog stats, and deletion effects") {
      spark.sql(s"""
        CREATE TABLE $tableName (
          id BIGINT,
          amount DOUBLE,
          region STRING
        )
        USING delta
        PARTITIONED BY (region)
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (1L, 10.0D, 'east'),
          (2L, 20.0D, 'east'),
          (3L, 30.0D, 'west'),
          (4L, 40.0D, 'west')
      """)
      val identifier = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
      spark.sessionState.catalog.alterTableStats(
        identifier,
        Some(
          CatalogStatistics(
            sizeInBytes = BigInt(2048),
            rowCount = Some(BigInt(4)),
            colStats = Map(
              "id" -> CatalogColumnStat(
                distinctCount = Some(BigInt(4)),
                min = Some("1"),
                max = Some("4"),
                nullCount = Some(BigInt(0)),
                avgLen = None,
                maxLen = None,
                histogram = None
              ),
              "amount" -> CatalogColumnStat(
                distinctCount = Some(BigInt(4)),
                min = Some("10.0"),
                max = Some("40.0"),
                nullCount = Some(BigInt(0)),
                avgLen = None,
                maxLen = None,
                histogram = None
              )
            )
          )
        )
      )

      val service = SparkDeltaStatsService.forRefresh()
      val initial = service.statsFor(spark, tableName)

      initial.table shouldBe tableName
      initial.tableStats.rowCount shouldBe 4L
      initial.tableStats.numFiles should be > 0L
      initial.tableStats.sizeBytes should be > 0L
      initial.tableStats.partitionColumns shouldBe Seq("region")
      initial.catalogTableStats shouldBe defined
      initial.catalogTableStats.flatMap(_.rowCount) should contain(4L)
      initial.columnStats("id").min should contain("1")
      initial.columnStats("id").max should contain("4")
      initial.columnStats("amount").min should contain("10.0")
      initial.columnStats("amount").max should contain("40.0")
      initial.files.flatMap(_.minValues.get("id")).map(_.toLong).min shouldBe 1L
      initial.files.flatMap(_.maxValues.get("id")).map(_.toLong).max shouldBe 4L
      initial.files.map(_.numRecords).sum shouldBe 4L
      initial.files.flatMap(_.partitionValues.get("region")).toSet shouldBe Set("east", "west")
      service.statsFor(spark, tableName) should be theSameInstanceAs initial
      val workloadFacts = service.workloadFactsFor(spark, Seq(tableName))
      workloadFacts.tableStats(tableName).rowCount should contain(4L)
      workloadFacts.tableStats(tableName).partitionColumns shouldBe Seq("region")
      workloadFacts.columnStats(s"$tableName.id").ndv should contain(4L)
      workloadFacts.columnStats(s"$tableName.id").rowCount should contain(4L)
      workloadFacts.columnStats(s"$tableName.amount").min should contain("10.0")

      spark.sql(s"DELETE FROM $tableName WHERE id = 2L")

      val afterDelete = SparkDeltaStatsService.forRefresh().statsFor(spark, tableName)
      afterDelete.tableStats.rowCount shouldBe 3L
      afterDelete.files.map(file => file.numRecords - file.dvCardinality).sum shouldBe 3L
      if (afterDelete.hasDeletionVectors) {
        afterDelete.files.map(_.dvCardinality).sum should be > 0L
      }
    }
  }
}
