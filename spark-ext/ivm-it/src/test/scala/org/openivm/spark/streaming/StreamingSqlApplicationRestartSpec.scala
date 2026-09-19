package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.util.control.NonFatal

private[streaming] final case class RestartCreateStatus(
    queryId: String,
    runId: String,
    checkpointLocation: String,
    definitionHash: String
)

class StreamingSqlApplicationRestartSpec extends AnyFunSpec with Matchers {

  private val StateProviderKey = "spark.sql.streaming.stateStore.providerClass"
  private val RocksDbProvider =
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"

  describe("SQL streaming-table recovery across Spark applications") {
    it("reuses the durable checkpoint without replay after a fresh SparkContext") {
      withRestartRoot("projection") { root =>
        val source     = "strsql_restart_projection_source"
        val target     = "strsql_restart_projection_target"
        val sourcePath = new File(root, "source").getCanonicalPath
        val targetPath = new File(root, "target").getCanonicalPath
        val sessions   = mutable.ArrayBuffer.empty[SparkSession]
        val first      = newSession(root, "projection-first", new File(root, "local-first"), None)
        sessions += first

        try {
          createLocatedSource(first, source, sourcePath, "id INT, value STRING")
          first.sql(s"INSERT INTO `${source}` VALUES (1, 'before-restart')").collect()
          val declaration =
            s"""CREATE STREAMING TABLE `$target`
               |USING DELTA
               |LOCATION ${sqlString(targetPath)}
               |AS SELECT id, value FROM STREAM `$source`""".stripMargin

          val original      = create(first, declaration)
          val originalQuery = query(first, original.queryId)
          process(originalQuery)
          assertBagEqual(
            first,
            target,
            "SELECT 1 AS id, 'before-restart' AS value"
          )
          first.sql(s"ALTER STREAMING TABLE `$target` STOP").collect()
          val firstContext = first.sparkContext
          stopSession(first)

          val second = newSession(root, "projection-second", new File(root, "local-second"), None)
          sessions += second
          (second.sparkContext eq firstContext) shouldBe false
          registerLocatedTable(second, source, sourcePath)
          registerLocatedTable(second, target, targetPath)

          val resumed = create(second, declaration)
          resumed.queryId shouldBe original.queryId
          resumed.runId should not be original.runId
          resumed.checkpointLocation shouldBe original.checkpointLocation
          resumed.definitionHash shouldBe original.definitionHash
          second.sql(s"INSERT INTO `${source}` VALUES (2, 'after-restart')").collect()
          process(query(second, resumed.queryId))

          assertBagEqual(
            second,
            target,
            "SELECT 1 AS id, 'before-restart' AS value UNION ALL " +
              "SELECT 2, 'after-restart'"
          )
        } finally {
          sessions.reverseIterator.foreach(stopSession)
        }
      }
    }

    it("restores RocksDB-backed aggregate state with a fresh local directory") {
      withRestartRoot("rocksdb") { root =>
        val source      = "strsql_restart_rocks_source"
        val target      = "strsql_restart_rocks_target"
        val sourcePath  = new File(root, "source").getCanonicalPath
        val targetPath  = new File(root, "target").getCanonicalPath
        val firstLocal  = new File(root, "local-first")
        val secondLocal = new File(root, "local-second")
        val sessions    = mutable.ArrayBuffer.empty[SparkSession]
        val first       = newSession(root, "rocks-first", firstLocal, Some(RocksDbProvider))
        sessions += first

        try {
          createLocatedSource(
            first,
            source,
            sourcePath,
            "id INT, value STRING, part STRING"
          )
          first
            .sql(s"INSERT INTO `$source` VALUES (1, 'one', 'east'), (2, 'two', 'west')")
            .collect()
          val declaration =
            s"""CREATE STREAMING TABLE `$target`
               |USING DELTA
               |LOCATION ${sqlString(targetPath)}
               |OPTIONS ('outputMode' = 'complete')
               |AS
               |SELECT part, COUNT(*) AS event_count
               |FROM STREAM `$source`
               |GROUP BY part""".stripMargin

          val original = create(first, declaration)
          process(query(first, original.queryId))
          assertBagEqual(
            first,
            target,
            "SELECT 'east' AS part, CAST(1 AS BIGINT) AS event_count UNION ALL " +
              "SELECT 'west', CAST(1 AS BIGINT)"
          )
          val statePath = new Path(targetPath, "_openivm-checkpoint/state")
          statePath.getFileSystem(first.sessionState.newHadoopConf()).exists(statePath) shouldBe true
          first.sql(s"ALTER STREAMING TABLE `$target` STOP").collect()
          val firstContext = first.sparkContext
          stopSession(first)
          deleteRecursively(firstLocal)
          firstLocal.exists() shouldBe false

          val second =
            newSession(root, "rocks-second", secondLocal, Some(RocksDbProvider))
          sessions += second
          (second.sparkContext eq firstContext) shouldBe false
          second.conf.get(StateProviderKey) shouldBe RocksDbProvider
          second.sparkContext.getConf.get("spark.local.dir") shouldBe secondLocal.getCanonicalPath
          registerLocatedTable(second, source, sourcePath)
          registerLocatedTable(second, target, targetPath)

          val resumed = create(second, declaration)
          resumed.queryId shouldBe original.queryId
          resumed.runId should not be original.runId
          resumed.checkpointLocation shouldBe original.checkpointLocation
          resumed.definitionHash shouldBe original.definitionHash
          second.sql(s"INSERT INTO `$source` VALUES (3, 'three', 'east')").collect()
          val resumedQuery = query(second, resumed.queryId)
          process(resumedQuery)

          resumedQuery.recentProgress.exists(_.stateOperators.nonEmpty) shouldBe true
          statePath.getFileSystem(second.sessionState.newHadoopConf()).exists(statePath) shouldBe true
          assertBagEqual(
            second,
            target,
            "SELECT 'east' AS part, CAST(2 AS BIGINT) AS event_count UNION ALL " +
              "SELECT 'west', CAST(1 AS BIGINT)"
          )
        } finally {
          sessions.reverseIterator.foreach(stopSession)
        }
      }
    }
  }

  private def newSession(
      root: File,
      suffix: String,
      localDirectory: File,
      stateProvider: Option[String]
  ): SparkSession = {
    localDirectory.mkdirs()
    val builder = SparkSession
      .builder()
      .master("local[1]")
      .appName(s"StreamingSqlApplicationRestartSpec-$suffix")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", new File(root, "warehouse").getCanonicalPath)
      .config("spark.local.dir", localDirectory.getCanonicalPath)
      .config("spark.ui.enabled", "false")
    stateProvider.foreach(value => builder.config(StateProviderKey, value))
    builder.getOrCreate()
  }

  private def createLocatedSource(spark: SparkSession, name: String, location: String, schema: String): Unit =
    spark
      .sql(
        s"""CREATE TABLE `$name` ($schema)
           |USING DELTA
           |LOCATION ${sqlString(location)}""".stripMargin
      )
      .collect()

  private def registerLocatedTable(spark: SparkSession, name: String, location: String): Unit =
    spark
      .sql(s"CREATE TABLE IF NOT EXISTS `$name` USING DELTA LOCATION ${sqlString(location)}")
      .collect()

  private def create(spark: SparkSession, declaration: String): RestartCreateStatus = {
    val row = spark.sql(declaration).collect().head
    RestartCreateStatus(
      queryId = requiredString(row, "query_id"),
      runId = requiredString(row, "run_id"),
      checkpointLocation = requiredString(row, "checkpoint_location"),
      definitionHash = requiredString(row, "definition_hash")
    )
  }

  private def query(spark: SparkSession, queryId: String): StreamingQuery =
    Option(spark.streams.get(queryId)).getOrElse {
      fail(s"Native query $queryId is not active in the fresh caller session")
    }

  private def process(query: StreamingQuery): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    Await.result(Future(blocking(query.processAllAvailable())), 90.seconds)
  }

  private def assertBagEqual(spark: SparkSession, table: String, expectedSql: String): Unit = {
    val actual   = spark.table(table)
    val expected = spark.sql(expectedSql)
    actual.exceptAll(expected).count() shouldBe 0L
    expected.exceptAll(actual).count() shouldBe 0L
  }

  private def requiredString(row: Row, column: String): String =
    Option(row.getAs[String](column)).filter(_.nonEmpty).getOrElse {
      fail(s"CREATE STREAMING TABLE returned an empty $column")
    }

  private def stopSession(spark: SparkSession): Unit =
    if (spark != null) {
      try spark.streams.active.foreach(_.stop())
      catch {
        case NonFatal(_) =>
      }
      try spark.stop()
      catch {
        case NonFatal(_) =>
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }

  private def withRestartRoot(name: String)(body: File => Unit): Unit = {
    val root = new File(
      s"target/test-streaming-application-restart-$name-${UUID.randomUUID().toString.take(8)}"
    ).getCanonicalFile
    root.mkdirs()
    try body(root)
    finally deleteRecursively(root)
  }

  private def sqlString(value: String): String =
    s"'${value.replace("'", "''")}'"

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()
  }
}
