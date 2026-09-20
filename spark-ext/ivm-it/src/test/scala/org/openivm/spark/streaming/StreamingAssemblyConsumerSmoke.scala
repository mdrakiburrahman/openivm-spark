package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.streaming.StreamingQuery

import java.io.File
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, blocking}

/**
 * Assembly-only consumer smoke entry.
 *
 * Launch this class with ivm-it test classes plus dependency jars and the
 * ivmextension assembly, while excluding every module's main class directory.
 */
object StreamingAssemblyConsumerSmoke {

  def main(args: Array[String]): Unit = {
    val extensionClass = Class.forName("org.openivm.spark.OpenIvmSparkExtensions")
    val extensionSource = Option(extensionClass.getProtectionDomain)
      .flatMap(domain => Option(domain.getCodeSource))
      .map(_.getLocation.toString)
      .getOrElse("")
    require(
      extensionSource.endsWith(".jar") && extensionSource.contains("assembly"),
      s"OpenIvmSparkExtensions was not loaded from an assembly jar: $extensionSource"
    )

    val warehouse = args.headOption
      .map(new File(_))
      .getOrElse(
        new File(
          s"target/streaming-assembly-smoke-${UUID.randomUUID().toString.take(8)}"
        )
      )
      .getCanonicalFile
    val suffix                = UUID.randomUUID().toString.replace("-", "").take(8)
    val source                = s"streaming_smoke_source_$suffix"
    val target                = s"streaming_smoke_target_$suffix"
    var spark: SparkSession   = null
    var query: StreamingQuery = null

    try {
      spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("StreamingAssemblyConsumerSmoke")
        .config(
          "spark.sql.extensions",
          "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.openivm.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
        .config("spark.ui.enabled", "false")
        .getOrCreate()

      spark
        .sql(
          s"""CREATE TABLE `$source` (
             |  id INT,
             |  event_time TIMESTAMP,
             |  group_key STRING
             |) USING DELTA""".stripMargin
        )
        .collect()

      val created = spark
        .sql(
          s"""CREATE STREAMING TABLE `$target`
             |CLUSTER BY (group_key, window_start)
             |AS
             |SELECT
             |  window(e.event_time, '10 minutes').start AS window_start,
             |  e.group_key,
             |  COUNT(*) AS event_count
             |FROM STREAM `$source`
             |WATERMARK e.event_time DELAY OF INTERVAL 1 MINUTE AS e
             |GROUP BY window(e.event_time, '10 minutes'), e.group_key""".stripMargin
        )
        .collect()
        .head
      val expectedTargetName =
        Seq(spark.catalog.currentCatalog, spark.catalog.currentDatabase, target)
          .map(quoteIdentifier)
          .mkString(".")
      require(created.getAs[String]("table_name") == expectedTargetName)
      Seq("run_id", "status", "checkpoint_location", "definition_hash").foreach { column =>
        require(
          Option(created.getAs[String](column)).exists(_.nonEmpty),
          s"CREATE STREAMING TABLE returned no $column"
        )
      }
      val queryId = Option(created.getAs[String]("query_id")).filter(_.nonEmpty).getOrElse {
        throw new IllegalStateException("CREATE STREAMING TABLE returned no query_id")
      }
      query = Option(spark.streams.get(queryId)).getOrElse {
        throw new IllegalStateException(s"Native query $queryId is not visible in spark.streams")
      }

      spark
        .sql(
          s"""INSERT INTO `$source` VALUES
             |(1, TIMESTAMP '2024-01-01 00:01:00', 'east'),
             |(2, TIMESTAMP '2024-01-01 00:02:00', 'east')""".stripMargin
        )
        .collect()
      process(query)
      spark
        .sql(
          s"INSERT INTO `$source` VALUES (3, TIMESTAMP '2024-01-01 01:00:00', 'advance')"
        )
        .collect()
      process(query)
      spark
        .sql(
          s"INSERT INTO `$source` VALUES (4, TIMESTAMP '2024-01-01 02:00:00', 'advance')"
        )
        .collect()
      process(query)

      require(
        query.recentProgress.exists(_.stateOperators.nonEmpty),
        "Stateful watermark query reported no native state operators"
      )
      val targetLocation = spark
        .sql(s"DESCRIBE DETAIL `$target`")
        .select("location")
        .head()
        .getString(0)
      val clusteringColumns = ClusteringMetadataDomain
        .fromSnapshot(DeltaLog.forTable(spark, new Path(targetLocation)).update())
        .map(_.clusteringColumns)
        .getOrElse(Seq.empty)
      require(
        clusteringColumns == Seq(Seq("group_key"), Seq("window_start")),
        s"Unexpected clustering columns: $clusteringColumns"
      )
      assertBagEqual(
        spark.table(target).where("window_start = TIMESTAMP '2024-01-01 00:00:00'"),
        spark.sql(
          """SELECT
            |  TIMESTAMP '2024-01-01 00:00:00' AS window_start,
            |  'east' AS group_key,
            |  CAST(2 AS BIGINT) AS event_count""".stripMargin
        )
      )

      val stopped = spark
        .sql(s"ALTER STREAMING TABLE `$target` STOP")
        .collect()
        .head
      require(stopped.getAs[String]("status") == "stopped")
      val shown = spark
        .sql("SHOW STREAMING TABLES")
        .collect()
        .find(_.getAs[String]("table_name") == expectedTargetName)
        .getOrElse(throw new IllegalStateException(s"SHOW omitted $expectedTargetName"))
      require(shown.getAs[String]("query_id") == queryId)
      require(!shown.getAs[Boolean]("is_active"))
      spark.sql(s"DROP STREAMING TABLE `$target`").collect()
      spark.sql(s"DROP TABLE `$source`").collect()
    } finally {
      if (query != null && query.isActive) query.stop()
      if (spark != null) {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
      deleteRecursively(warehouse)
    }
  }

  private def process(query: StreamingQuery): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    Await.result(Future(blocking(query.processAllAvailable())), 90.seconds)
  }

  private def assertBagEqual(actual: DataFrame, expected: DataFrame): Unit = {
    require(actual.exceptAll(expected).count() == 0L)
    require(expected.exceptAll(actual).count() == 0L)
  }

  private def quoteIdentifier(value: String): String =
    s"`${value.replace("`", "``")}`"

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()
  }
}
