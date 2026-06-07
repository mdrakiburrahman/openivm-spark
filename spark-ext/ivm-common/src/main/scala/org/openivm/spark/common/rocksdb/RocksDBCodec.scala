package org.openivm.spark.common.rocksdb

import java.io.File
import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object RocksDBCodec {
  private val RemoteSchemes = Set("hdfs", "s3", "s3a", "abfs", "abfss", "wasb", "wasbs", "gs")

  private val EscapeLead: Byte      = 0.toByte
  private val SeparatorTail: Byte   = 0.toByte
  private val EscapedZeroTail: Byte = 0xff.toByte

  def utf8(value: String): Array[Byte] =
    value.getBytes(StandardCharsets.UTF_8)

  def fromUtf8(value: Array[Byte]): String =
    new String(value, StandardCharsets.UTF_8)

  def encodeLongBE(value: Long): Array[Byte] =
    ByteBuffer.allocate(java.lang.Long.BYTES).putLong(value).array()

  def decodeLongBE(value: Array[Byte]): Long = {
    require(
      value.length == java.lang.Long.BYTES,
      s"Expected ${java.lang.Long.BYTES} bytes for a big-endian Long, found ${value.length}"
    )
    ByteBuffer.wrap(value).getLong
  }

  def compositeKey(parts: Seq[Array[Byte]]): Array[Byte] = {
    if (parts.isEmpty) return Array.emptyByteArray

    val out = ArrayBuffer.empty[Byte]
    parts.zipWithIndex.foreach { case (part, index) =>
      part.foreach { byte =>
        if (byte == EscapeLead) {
          out += EscapeLead
          out += EscapedZeroTail
        } else {
          out += byte
        }
      }
      if (index < parts.length - 1) {
        out += EscapeLead
        out += SeparatorTail
      }
    }
    out.toArray
  }

  def splitComposite(value: Array[Byte], maxParts: Int = -1): Seq[Array[Byte]] = {
    if (maxParts == 0) return Seq.empty

    val limit   = if (maxParts < 0) Int.MaxValue else maxParts
    val parts   = ArrayBuffer.empty[Array[Byte]]
    val current = ArrayBuffer.empty[Byte]
    var index   = 0

    while (index < value.length) {
      if (value(index) == EscapeLead) {
        if (index + 1 < value.length && value(index + 1) == EscapedZeroTail) {
          current += EscapeLead
          index += 2
        } else if (parts.length + 1 < limit && index + 1 < value.length && value(index + 1) == SeparatorTail) {
          parts += current.toArray
          current.clear()
          index += 2
        } else if (parts.length + 1 < limit) {
          // Backward-compatible decode path for the earlier single-byte separator scheme.
          parts += current.toArray
          current.clear()
          index += 1
        } else {
          current += value(index)
          index += 1
        }
      } else {
        current += value(index)
        index += 1
      }
    }

    parts += current.toArray
    parts.toSeq
  }

  def requireLocalPath(uriOrPath: String): String = {
    val raw = Option(uriOrPath).map(_.trim).getOrElse("")
    require(raw.nonEmpty, "OpenIVM RocksDB requires a non-empty local filesystem path.")

    val maybeUri = Try(new URI(raw)).toOption
    maybeUri.flatMap(uri => Option(uri.getScheme).map(_.toLowerCase(java.util.Locale.ROOT))) match {
      case Some("file") =>
        val file = new File(new URI(raw))
        require(
          file.isAbsolute,
          s"OpenIVM RocksDB requires an absolute local filesystem path, but got '$uriOrPath'."
        )
        file.getAbsolutePath

      case Some(scheme) if RemoteSchemes.contains(scheme) || scheme.nonEmpty =>
        throw new IllegalArgumentException(
          s"OpenIVM RocksDB requires a local filesystem path, but got '$uriOrPath' with unsupported scheme '$scheme'. Supported forms are an absolute path or a file:/ URI."
        )

      case _ =>
        val file = new File(raw)
        if (!file.isAbsolute) {
          throw new IllegalArgumentException(
            s"OpenIVM RocksDB requires an absolute local filesystem path, but got '$uriOrPath'."
          )
        }
        file.getAbsolutePath
    }
  }

  def safePathSegment(name: String): String =
    Base64.getUrlEncoder.withoutPadding.encodeToString(utf8(name))
}
