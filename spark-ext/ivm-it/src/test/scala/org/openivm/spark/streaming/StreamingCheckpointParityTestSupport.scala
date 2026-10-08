package org.openivm.spark.streaming

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.Suite

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.collection.mutable
import scala.concurrent.duration._

private[streaming] final case class DeltaCheckpointSourceOffset(
    batchId: Long,
    sourceIndex: Int,
    sourceVersion: Long,
    reservoirVersion: Long,
    index: Long,
    isStartingVersion: Boolean
)

private[streaming] final case class StreamingCheckpointSemantics(
    offsetBatches: Vector[Long],
    commitBatches: Vector[Long],
    sourceOffsets: Vector[DeltaCheckpointSourceOffset]
)

private[streaming] final case class StreamingCheckpointSnapshot(
    semantics: StreamingCheckpointSemantics,
    files: Vector[(String, String)]
)

private[streaming] trait StreamingCheckpointParityTestSupport extends StreamingSqlTestSupport { self: Suite =>

  private val Json            = new ObjectMapper()
  private val DeltaErrorClass = "\\bDELTA_[A-Z0-9_]+\\b".r

  protected def awaitStreamingRun(
      start: => SqlStreamingStatus,
      timeout: FiniteDuration = 120.seconds
  ): SqlCompletedRun = {
    val listener = new SqlCompletionListener
    spark.streams.addListener(listener)
    try listener.await(start, timeout)
    finally spark.streams.removeListener(listener)
  }

  protected def checkpointSnapshot(location: String): StreamingCheckpointSnapshot = {
    val root = new Path(location)
    val fs   = root.getFileSystem(spark.sessionState.newHadoopConf())
    require(fs.exists(root), s"Checkpoint does not exist: $location")
    val offsets = batchFiles(fs, new Path(root, "offsets"))
    val commits = batchFiles(fs, new Path(root, "commits"))
    val sourceOffsets = offsets.flatMap { case (batchId, path) =>
      readLines(fs, path).iterator
        .filter(_.startsWith("{"))
        .map(Json.readTree)
        .filter(_.has("reservoirVersion"))
        .zipWithIndex
        .map { case (node, sourceIndex) =>
          DeltaCheckpointSourceOffset(
            batchId = batchId,
            sourceIndex = sourceIndex,
            sourceVersion = node.path("sourceVersion").asLong(),
            reservoirVersion = node.path("reservoirVersion").asLong(),
            index = node.path("index").asLong(),
            isStartingVersion = node.path("isStartingVersion").asBoolean(false)
          )
        }
        .toVector
    }
    StreamingCheckpointSnapshot(
      StreamingCheckpointSemantics(
        offsetBatches = offsets.map(_._1),
        commitBatches = commits.map(_._1),
        sourceOffsets = sourceOffsets
      ),
      checkpointDigests(fs, root)
    )
  }

  protected def truncateDeltaHistoryBeforeLatestCheckpoint(table: String, requiredVersion: Long): Long = {
    val logDirectory   = new Path(deltaTableLocation(table), "_delta_log")
    val fs             = logDirectory.getFileSystem(spark.sessionState.newHadoopConf())
    val lastCheckpoint = new Path(logDirectory, "_last_checkpoint")
    require(fs.exists(lastCheckpoint), s"Delta source $table did not write _last_checkpoint")
    val latestCheckpointVersion = Json.readTree(readText(fs, lastCheckpoint)).path("version").asLong(-1L)
    require(
      latestCheckpointVersion > requiredVersion,
      s"Delta source $table checkpoint $latestCheckpointVersion does not supersede required version $requiredVersion"
    )

    fs.listStatus(logDirectory).foreach { status =>
      val name = status.getPath.getName
      leadingVersion(name)
        .filter(_ < latestCheckpointVersion)
        .filter(_ => name.endsWith(".json") || name.contains(".checkpoint."))
        .foreach(_ => fs.delete(status.getPath, true))
    }
    DeltaLog.clearCache()
    spark.catalog.clearCache()
    deltaTableVersion(table) shouldBe latestCheckpointVersion
    latestCheckpointVersion
  }

  protected def deltaTableLocation(table: String): String =
    spark
      .sql(s"DESCRIBE DETAIL ${quoteIdentifier(table)}")
      .select("location")
      .head()
      .getString(0)

  protected def deltaTableVersion(table: String): Long =
    DeltaLog.forTable(spark, new Path(deltaTableLocation(table))).update().version

  protected def deltaTableId(table: String): String =
    DeltaLog.forTable(spark, new Path(deltaTableLocation(table))).update().metadata.id

  protected def deltaErrorClasses(run: SqlCompletedRun): Set[String] =
    run.exception.toSeq.flatMap(DeltaErrorClass.findAllIn).toSet

  private def batchFiles(fs: FileSystem, directory: Path): Vector[(Long, Path)] = {
    if (!fs.exists(directory)) Vector.empty
    else
      fs.listStatus(directory)
        .iterator
        .filter(_.isFile)
        .map(_.getPath)
        .filter(path => path.getName.nonEmpty && path.getName.forall(_.isDigit))
        .map(path => path.getName.toLong -> path)
        .toVector
        .sortBy(_._1)
  }

  private def checkpointDigests(fs: FileSystem, root: Path): Vector[(String, String)] = {
    val files    = mutable.ArrayBuffer.empty[(String, String)]
    val iterator = fs.listFiles(root, true)
    val prefix   = root.toString.stripSuffix("/") + "/"
    while (iterator.hasNext) {
      val path     = iterator.next().getPath
      val relative = path.toString.stripPrefix(prefix)
      if (!relative.split('/').exists(_.startsWith(".")))
        files += relative -> sha256(fs, path)
    }
    files.toVector.sortBy(_._1)
  }

  private def sha256(fs: FileSystem, path: Path): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val input  = fs.open(path)
    val buffer = new Array[Byte](8192)
    try {
      var count = input.read(buffer)
      while (count >= 0) {
        if (count > 0) digest.update(buffer, 0, count)
        count = input.read(buffer)
      }
    } finally input.close()
    digest.digest().map(value => f"${value & 0xff}%02x").mkString
  }

  private def readLines(fs: FileSystem, path: Path): Vector[String] = {
    val reader = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))
    val lines  = Vector.newBuilder[String]
    try {
      var line = reader.readLine()
      while (line != null) {
        lines += line
        line = reader.readLine()
      }
    } finally reader.close()
    lines.result()
  }

  private def readText(fs: FileSystem, path: Path): String =
    readLines(fs, path).mkString("\n")

  private def leadingVersion(name: String): Option[Long] =
    if (name.length >= 20 && name.take(20).forall(_.isDigit)) Some(name.take(20).toLong)
    else None
}
