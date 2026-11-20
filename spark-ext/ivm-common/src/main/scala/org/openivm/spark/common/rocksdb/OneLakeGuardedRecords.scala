package org.openivm.spark.common.rocksdb

import java.io.IOException
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.Arrays

private[rocksdb] object OneLakeGuardedRecords {
  import GuardedSnapshotDecisions._

  val MaxRecordBytes = 1024
  private val Format = "OPENIVM_GUARDED_CHECKPOINT_V1"

  def receiptBytes(artifact: Finished): Array[Byte] = encode("FINISHED", artifact)

  def decisionBytes(decision: Decision): Array[Byte] = decision match {
    case Published(artifact) => encode("PUBLISHED", artifact)
    case Retired(artifact)   => encode("RETIRED", artifact)
  }

  def receipt(body: Array[Byte], root: String, generation: Generation): Finished =
    decode(body, root, generation) match {
      case ("FINISHED", artifact) => artifact
      case _                      => throw new IOException("expected a sealed checkpoint receipt")
    }

  def decision(body: Array[Byte], root: String, generation: Generation): Decision =
    decode(body, root, generation) match {
      case ("PUBLISHED", artifact) => Published(artifact)
      case ("RETIRED", artifact)   => Retired(artifact)
      case _                       => throw new IOException("expected a terminal checkpoint decision")
    }

  def namespaceMarker(root: String): Array[Byte] = {
    OneLakeGuardedPaths.requireCanonicalRoot(root)
    ascii(s"$Format\nNAMESPACE\n$root\n")
  }

  private def ascii(record: String): Array[Byte] = {
    if (record.length > MaxRecordBytes || !record.forall(c => c == '\n' || (c >= '!' && c <= '~')))
      throw new IOException("invalid guarded checkpoint record")
    record.getBytes(US_ASCII)
  }

  private def encode(kind: String, a: Finished): Array[Byte] = {
    OneLakeGuardedPaths.requireCanonicalRoot(a.root)
    OneLakeGuardedPaths.exactVersion(a.version)
    ascii(s"$Format\n$kind\n${a.root}\n${a.generation.value}\n${a.bytes}\n${a.sha256}\n${a.version}\n")
  }

  private def decode(body: Array[Byte], root: String, generation: Generation): (String, Finished) = {
    if (body.length > MaxRecordBytes) throw new IOException("guarded checkpoint record exceeds its limit")
    new String(body, US_ASCII).split("\n", -1) match {
      case Array(Format, kind, boundRoot, id, size, digest, version, "")
          if boundRoot == root && id == generation.value && size.matches("[1-9][0-9]{0,18}") =>
        val artifact =
          try Finished(root, generation, size.toLong, digest, version)
          catch { case e: IllegalArgumentException => throw new IOException("invalid checkpoint receipt fields", e) }
        if (!Arrays.equals(encode(kind, artifact), body)) throw new IOException("noncanonical checkpoint record")
        kind -> artifact
      case _ => throw new IOException("guarded checkpoint record has the wrong format or binding")
    }
  }
}
