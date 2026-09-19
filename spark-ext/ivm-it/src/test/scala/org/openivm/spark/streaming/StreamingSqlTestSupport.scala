package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, StreamingQueryProgress}
import org.scalatest.Suite

import java.io.File
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, blocking}

private[streaming] final case class SqlStreamingStatus(
    tableName: String,
    queryId: String,
    runId: String,
    status: String,
    checkpointLocation: String,
    definitionHash: String
)

private[streaming] final case class SqlCompletedRun(
    status: SqlStreamingStatus,
    progress: Seq[StreamingQueryProgress],
    exception: Option[String]
) {

  def inputRows: Long = progress.map(_.numInputRows).sum
}

private[streaming] final class SqlCompletionListener extends StreamingQueryListener {

  private final class RunEvents {
    val progress                            = new ConcurrentLinkedQueue[StreamingQueryProgress]()
    val terminated                          = new CountDownLatch(1)
    @volatile var started: Boolean          = false
    @volatile var exception: Option[String] = None
  }

  private val runs = new ConcurrentHashMap[String, RunEvents]()

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
    run(event.id.toString, event.runId.toString).started = true

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress
    run(progress.id.toString, progress.runId.toString).progress.add(progress)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val events = run(event.id.toString, event.runId.toString)
    events.exception = event.exception
    events.terminated.countDown()
  }

  def await(status: SqlStreamingStatus, timeout: FiniteDuration): SqlCompletedRun = {
    val events    = run(status.queryId, status.runId)
    val completed = events.terminated.await(timeout.toMillis, TimeUnit.MILLISECONDS)
    if (!completed)
      throw new java.util.concurrent.TimeoutException(
        s"Timed out waiting for query ${status.queryId}, run ${status.runId} to terminate"
      )
    if (!events.started)
      throw new IllegalStateException(
        s"Query ${status.queryId}, run ${status.runId} terminated without a start event"
      )
    SqlCompletedRun(status, events.progress.iterator().asScala.toVector, events.exception)
  }

  private def run(queryId: String, runId: String): RunEvents = {
    val key       = s"$queryId\n$runId"
    val candidate = new RunEvents
    val existing  = runs.putIfAbsent(key, candidate)
    if (existing == null) candidate else existing
  }
}

private[streaming] trait StreamingSqlTestSupport extends StreamingTableTestFixture {
  self: Suite =>

  private val sqlQueryIds  = mutable.Set.empty[String]
  private val scratchPaths = mutable.Set.empty[String]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
  }

  override protected def afterEach(): Unit =
    try {
      stopOwnedQueries()
      deleteScratchPaths()
    } finally {
      super.afterEach()
    }

  override protected def afterAll(): Unit =
    try {
      stopOwnedQueries()
      deleteScratchPaths()
    } finally {
      super.afterAll()
    }

  protected def createStreamingSql(sqlText: String): SqlStreamingStatus = {
    val row = spark.sql(sqlText).collect().head
    val result = SqlStreamingStatus(
      tableName = row.getAs[String]("table_name"),
      queryId = requiredString(row, "query_id"),
      runId = requiredString(row, "run_id"),
      status = row.getAs[String]("status"),
      checkpointLocation = requiredString(row, "checkpoint_location"),
      definitionHash = requiredString(row, "definition_hash")
    )
    sqlQueryIds += result.queryId
    result
  }

  protected def createStreamingSqlToCompletion(
      sqlText: String,
      timeout: FiniteDuration = 120.seconds
  ): SqlCompletedRun = {
    val listener = new SqlCompletionListener
    spark.streams.addListener(listener)
    val startedAt = System.nanoTime()
    try {
      val status = createStreamingSql(sqlText)
      val completed =
        try listener.await(status, timeout)
        catch {
          case error: java.util.concurrent.TimeoutException =>
            val elapsedMs = (System.nanoTime() - startedAt) / 1000000L
            fail(s"${error.getMessage} after ${elapsedMs}ms", error)
        }
      completed.exception.foreach { message =>
        fail(
          s"Query ${status.queryId}, run ${status.runId} terminated with an exception: $message"
        )
      }
      completed
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  protected def nativeQuery(status: SqlStreamingStatus): StreamingQuery =
    Option(spark.streams.get(status.queryId)).getOrElse {
      fail(s"Native query ${status.queryId} for ${status.tableName} is not active")
    }

  protected def process(status: SqlStreamingStatus): StreamingQuery = {
    val query = nativeQuery(status)
    process(query)
    query
  }

  protected def process(query: StreamingQuery, timeout: FiniteDuration = 90.seconds): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val processing                                  = Future(blocking(query.processAllAvailable()))
    try Await.result(processing, timeout)
    catch {
      case timeoutError: java.util.concurrent.TimeoutException =>
        if (query.isActive) query.stop()
        fail(s"Timed out processing native query ${query.id}", timeoutError)
    }
  }

  protected def stopStreamingSql(name: String): Row =
    spark.sql(s"ALTER STREAMING TABLE ${quoteIdentifier(name)} STOP").collect().head

  protected def showStreamingSql(namespace: Option[String] = None): Seq[Row] = {
    val suffix = namespace.map(value => s" IN ${quoteIdentifier(value)}").getOrElse("")
    spark.sql(s"SHOW STREAMING TABLES$suffix").collect().toSeq
  }

  protected def createDeltaSource(name: String, schema: String, partitions: Seq[String] = Seq.empty): Unit = {
    val partitionClause =
      if (partitions.isEmpty) ""
      else partitions.map(quoteIdentifier).mkString(" PARTITIONED BY (", ", ", ")")
    spark.sql(s"CREATE TABLE ${quoteIdentifier(name)} ($schema) USING DELTA$partitionClause").collect()
  }

  protected def insertRows(name: String, values: String): Unit =
    spark.sql(s"INSERT INTO ${quoteIdentifier(name)} VALUES $values").collect()

  protected def sourceVersion(name: String): Long =
    spark
      .sql(s"DESCRIBE HISTORY ${quoteIdentifier(name)}")
      .select("version")
      .collect()
      .map(_.getLong(0))
      .max

  protected def sourceVersionTimestamp(name: String, version: Long): Timestamp =
    spark
      .sql(s"DESCRIBE HISTORY ${quoteIdentifier(name)}")
      .where(s"version = $version")
      .select("timestamp")
      .head()
      .getTimestamp(0)

  protected def assertFramesBagEqual(actual: DataFrame, expected: DataFrame): Unit = {
    actual.exceptAll(expected).count() shouldBe 0L
    expected.exceptAll(actual).count() shouldBe 0L
  }

  protected def scratchPath(slug: String): String = {
    val file = new File(
      "target",
      s"streaming-sql-$slug-${UUID.randomUUID().toString.take(8)}"
    )
    val path = file.getCanonicalPath
    scratchPaths += path
    path
  }

  protected def track(query: StreamingQuery): StreamingQuery = {
    sqlQueryIds += query.id.toString
    query
  }

  protected def quoteIdentifier(name: String): String =
    s"`${name.replace("`", "``")}`"

  private def requiredString(row: Row, column: String): String =
    Option(row.getAs[String](column)).filter(_.nonEmpty).getOrElse {
      fail(s"CREATE STREAMING TABLE returned an empty $column")
    }

  private def stopOwnedQueries(): Unit = {
    if (spark != null) {
      sqlQueryIds.foreach { id =>
        Option(spark.streams.get(id)).filter(_.isActive).foreach(_.stop())
      }
    }
    sqlQueryIds.clear()
  }

  private def deleteScratchPaths(): Unit = {
    if (spark != null) {
      scratchPaths.foreach { raw =>
        val path = new Path(raw)
        val fs   = path.getFileSystem(spark.sessionState.newHadoopConf())
        if (fs.exists(path)) fs.delete(path, true)
      }
    }
    scratchPaths.clear()
  }
}
