package org.openivm.spark.telemetry

import java.nio.charset.StandardCharsets
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging

/** Mirrors each completed `OPENIVM_EXECUTION_SPAN` line to its own object under
  * a Hadoop-filesystem directory (an OneLake `Files/...` path on managed Fabric
  * Spark).
  *
  * Fabric keeps the driver log4j behind a browser-authenticated Spark UI, so
  * the benchmark harness cannot scrape it the way it scrapes a local driver
  * log. Writing every span to its own object lets the harness fetch the exact
  * same `OPENIVM_EXECUTION_SPAN <json>` lines it already parses, preserving the
  * compile/effective refresh type, refresh reason and time-travel pin
  * classification the A/B guard verifies.
  *
  *  - One object per span (`<key>__<uuid>.jsonl`) so concurrent CREATE/REFRESH
  *    writers never contend on a shared append target.
  *  - A temp object is written then atomically renamed into place, so a
  *    concurrent listing/read never observes a half-written line.
  *  - A write that cannot be published after retries throws
  *    [[OpenIvmTelemetrySinkException]]: a configured sink that fails must
  *    surface an explicit error, never a silently missing classification.
  */
final class OneLakeSpanSink private (dir: Path, hadoopConf: Configuration) extends Logging {

  import OneLakeSpanSink._

  def write(key: String, line: String): Unit = {
    val base      = s"${sanitize(key)}__${UUID.randomUUID().toString}"
    val finalPath = new Path(dir, s"$base$Suffix")
    val tmpPath   = new Path(dir, s"$base$Suffix$TmpSuffix")
    val bytes     = (stripTrailingNewline(line) + "\n").getBytes(StandardCharsets.UTF_8)

    var attempt              = 0
    var lastError: Throwable = null
    while (attempt < MaxAttempts) {
      try {
        val fs = finalPath.getFileSystem(hadoopConf)
        if (!fs.exists(dir)) fs.mkdirs(dir)
        writeBytes(fs, tmpPath, bytes)
        if (fs.rename(tmpPath, finalPath) || fs.exists(finalPath)) return
        deleteQuietly(fs, tmpPath)
        lastError = new IllegalStateException(s"rename to $finalPath returned false")
      } catch {
        case t: Throwable => lastError = t
      }
      attempt += 1
      if (attempt < MaxAttempts) {
        logWarning(
          s"[openivm-telemetry-sink] publish attempt $attempt/$MaxAttempts to " +
            s"$finalPath failed; retrying",
          lastError
        )
        Thread.sleep(RetryBackoffMs * attempt)
      }
    }
    throw new OpenIvmTelemetrySinkException(
      s"OpenIVM telemetry sink could not publish a completed span object under " +
        s"$dir after $MaxAttempts attempts",
      lastError
    )
  }

  private def writeBytes(fs: FileSystem, path: Path, bytes: Array[Byte]): Unit = {
    val out = fs.create(path, /* overwrite = */ true)
    try out.write(bytes)
    finally out.close()
  }

  private def deleteQuietly(fs: FileSystem, path: Path): Unit =
    try fs.delete(path, false)
    catch { case _: Throwable => () }
}

object OneLakeSpanSink {

  private val Suffix         = ".jsonl"
  private val TmpSuffix      = ".tmp"
  private val MaxAttempts    = 5
  private val RetryBackoffMs = 200L

  def forDir(dir: String, hadoopConf: Configuration): OneLakeSpanSink =
    new OneLakeSpanSink(new Path(dir), hadoopConf)

  private def sanitize(key: String): String = {
    val trimmed = Option(key).getOrElse("").trim
    val safe    = trimmed.replaceAll("[^A-Za-z0-9_.-]", "_")
    if (safe.isEmpty) "span" else safe.take(80)
  }

  private def stripTrailingNewline(s: String): String =
    if (s.endsWith("\n")) s.dropRight(1) else s
}

/** Raised when a configured [[OneLakeSpanSink]] cannot publish a span. */
final class OpenIvmTelemetrySinkException(message: String, cause: Throwable) extends RuntimeException(message, cause)
