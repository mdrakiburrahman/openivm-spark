package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import scala.collection.mutable

trait StreamingTableTestFixture extends BeforeAndAfterAll with BeforeAndAfterEach with Matchers { self: Suite =>

  private val warehouse = new File(
    s"target/test-warehouse-streaming-${UUID.randomUUID().toString.take(8)}"
  )
  private val ownedQueryIds = mutable.Set.empty[String]

  protected var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    warehouse.mkdirs()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(s"${getClass.getSimpleName}-${UUID.randomUUID().toString.take(8)}")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override protected def afterAll(): Unit =
    try {
      if (spark != null) {
        ownedQueryIds.foreach { id =>
          Option(spark.streams.get(id)).filter(_.isActive).foreach(_.stop())
        }
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
      deleteRecursively(warehouse)
    } finally {
      super.afterAll()
    }

  override protected def afterEach(): Unit =
    try {
      if (spark != null)
        ownedQueryIds.foreach { id =>
          Option(spark.streams.get(id)).filter(_.isActive).foreach(_.stop())
        }
    } finally {
      super.afterEach()
    }

  protected def createSource(name: String, schema: String = "id INT, value STRING, part STRING"): Unit =
    spark.sql(s"CREATE TABLE `$name` ($schema) USING DELTA").collect()

  protected def appendRows(name: String, values: String): Unit =
    spark.sql(s"INSERT INTO `$name` VALUES $values").collect()

  protected def readStream(name: String): DataFrame =
    spark.readStream.format("delta").table(name)

  protected def createStreaming(
      name: String,
      query: DataFrame,
      queryText: String,
      location: Option[String] = None,
      partitions: Seq[String] = Seq.empty,
      clusters: Seq[Seq[String]] = Seq.empty,
      properties: Map[String, String] = Map.empty,
      options: Map[String, String] = Map.empty
  ): StreamingTableStatus = {
    val status = StreamingTableManager.create(
      spark,
      StreamingTableSpec(
        name = Seq(name),
        queryText = queryText,
        query = query.queryExecution.logical,
        location = location,
        partitionColumns = partitions,
        clusterColumns = clusters,
        tableProperties = properties,
        options = options
      )
    )
    status.queryId.foreach(ownedQueryIds += _)
    status
  }

  protected def process(status: StreamingTableStatus): StreamingQuery = {
    val query = status.queryId.flatMap(id => Option(spark.streams.get(id))).getOrElse {
      fail(s"No active native streaming query for ${status.tableName}")
    }
    query.processAllAvailable()
    query
  }

  protected def assertBagEqual(table: String, expectedSql: String): Unit = {
    val actual   = spark.table(table)
    val expected = spark.sql(expectedSql)
    actual.exceptAll(expected).count() shouldBe 0L
    expected.exceptAll(actual).count() shouldBe 0L
  }

  protected def hasDefinition(name: String): Boolean = {
    val target = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(name), requireTableIdMarker = true)
    val path   = StreamingTableMetadata.definitionPath(target)
    path.getFileSystem(spark.sessionState.newHadoopConf()).exists(path)
  }

  protected def targetPath(name: String): String =
    StreamingTableMetadata
      .resolveDeltaTarget(spark, Seq(name), requireTableIdMarker = true)
      .dataPath

  protected def pathExists(path: String): Boolean = {
    val hadoopPath = new Path(path)
    hadoopPath.getFileSystem(spark.sessionState.newHadoopConf()).exists(hadoopPath)
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()
  }
}
