package org.openivm.spark.common

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{
  OpenIvmRocksDB,
  OpenIvmRocksDBBatchOps,
  OpenIvmRocksDBRegistry,
  OpenIvmStateSync,
  RocksDBCodec
}

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

final case class StreamingDependencySource(
    parentIdentity: String,
    parentPath: String,
    parentDeltaTableId: String,
    parentTableId: String,
    formatVersion: Int
)

final case class StreamingDependencyTarget(
    identity: String,
    name: Seq[String],
    dataPath: String,
    deltaTableId: String,
    tableId: String,
    definitionHash: String,
    formatVersion: Int,
    sources: Seq[StreamingDependencySource]
)

/** Durable target registry and reverse dependency index for managed streaming tables. */
object StreamingDependencyCatalog {

  private val TargetsCf  = "targets"
  private val ChildrenCf = "children"
  private val ColumnFamilies = Seq(
    TargetsCf,
    ChildrenCf
  )
  private val Mapper     = new ObjectMapper()
  private val EmptyBytes = Array.emptyByteArray

  def materializedIdentity(name: String): String = s"materialized:$name"

  def lifecycleLockKey(identity: String): String =
    identity.stripPrefix("materialized:")

  def exists(spark: SparkSession): Boolean =
    Files.exists(Paths.get(OpenIvmStatePaths.streamingDependencyDbPath(spark), "CURRENT"))

  def publish(spark: SparkSession, target: StreamingDependencyTarget): Unit = {
    validateTarget(target)
    val db = open(spark)
    db.withSession {
      val previous = readTarget(db, target.identity)
      db.withBatch { batch =>
        previous.toSeq.flatMap(_.sources).foreach { source =>
          OpenIvmRocksDBBatchOps.delete(
            db,
            batch,
            ChildrenCf,
            childKey(source.parentIdentity, target.identity)
          )
        }
        target.sources.foreach { source =>
          OpenIvmRocksDBBatchOps.put(
            db,
            batch,
            ChildrenCf,
            childKey(source.parentIdentity, target.identity),
            EmptyBytes
          )
        }
        OpenIvmRocksDBBatchOps.put(
          db,
          batch,
          TargetsCf,
          RocksDBCodec.utf8(target.identity),
          RocksDBCodec.utf8(targetJson(target))
        )
      }
    }
    OpenIvmStateSync.backupAsync(spark)
  }

  def lookup(spark: SparkSession, identity: String): Option[StreamingDependencyTarget] =
    if (!exists(spark)) None else readTarget(open(spark), identity)

  def directChildren(spark: SparkSession, parentIdentity: String): Seq[StreamingDependencyTarget] = {
    if (!exists(spark)) return Seq.empty
    val db     = open(spark)
    val prefix = childPrefix(parentIdentity)
    val rows = db.withSession {
      val iterator = db.prefixScan(ChildrenCf, prefix)
      try {
        iterator.map { case (key, _) =>
          val parts = RocksDBCodec.splitComposite(key, maxParts = 2)
          if (parts.size != 2 || RocksDBCodec.fromUtf8(parts.head) != parentIdentity)
            invalid(s"Corrupt streaming dependency key for parent '$parentIdentity'")
          RocksDBCodec.fromUtf8(parts(1))
        }.toVector
      } finally iterator.asInstanceOf[AutoCloseable].close()
    }
    rows.distinct.sorted.map { childIdentity =>
      val child = readTarget(db, childIdentity).getOrElse(
        invalid(
          s"Streaming dependency index for '$parentIdentity' references missing child '$childIdentity'"
        )
      )
      val matching = child.sources.filter(_.parentIdentity == parentIdentity)
      if (matching.size != 1)
        invalid(
          s"Streaming dependency child '$childIdentity' has ${matching.size} records for parent '$parentIdentity'"
        )
      child
    }
  }

  def remove(spark: SparkSession, childIdentity: String): Unit = {
    if (!exists(spark)) return
    val db = open(spark)
    db.withSession {
      readTarget(db, childIdentity).foreach { target =>
        db.withBatch { batch =>
          target.sources.foreach { source =>
            OpenIvmRocksDBBatchOps.delete(
              db,
              batch,
              ChildrenCf,
              childKey(source.parentIdentity, childIdentity)
            )
          }
          OpenIvmRocksDBBatchOps.delete(
            db,
            batch,
            TargetsCf,
            RocksDBCodec.utf8(childIdentity)
          )
        }
      }
    }
    OpenIvmStateSync.backupAsync(spark)
  }

  def backupNow(spark: SparkSession): Unit =
    OpenIvmStateSync.backupNow(spark)

  private[openivm] def removeTargetRecordForTesting(
      spark: SparkSession,
      identity: String
  ): Unit = {
    val db = open(spark)
    db.withBatch { batch =>
      OpenIvmRocksDBBatchOps.delete(
        db,
        batch,
        TargetsCf,
        RocksDBCodec.utf8(identity)
      )
    }
  }

  private def open(spark: SparkSession): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(
      spark,
      OpenIvmStatePaths.streamingDependencyDbPath(spark),
      ColumnFamilies
    )

  private def readTarget(db: OpenIvmRocksDB, identity: String): Option[StreamingDependencyTarget] =
    db.get(TargetsCf, RocksDBCodec.utf8(identity)).map { bytes =>
      val target = parseTarget(RocksDBCodec.fromUtf8(bytes), identity)
      if (target.identity != identity)
        invalid(s"Streaming dependency target '$identity' contains identity '${target.identity}'")
      target
    }

  private def childKey(parentIdentity: String, childIdentity: String): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(parentIdentity), RocksDBCodec.utf8(childIdentity)))

  private def childPrefix(parentIdentity: String): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(parentIdentity))) ++ Array(0.toByte, 0.toByte)

  private def targetJson(target: StreamingDependencyTarget): String = {
    val root = Mapper.createObjectNode()
    root.put("identity", target.identity)
    val name = root.putArray("name")
    target.name.foreach(name.add)
    root.put("path", target.dataPath)
    root.put("deltaTableId", target.deltaTableId)
    root.put("tableId", target.tableId)
    root.put("definitionHash", target.definitionHash)
    root.put("formatVersion", target.formatVersion)
    val sources = root.putArray("sources")
    target.sources.sortBy(source => (source.parentIdentity, source.parentDeltaTableId)).foreach { source =>
      val node = sources.addObject()
      node.put("parentIdentity", source.parentIdentity)
      node.put("parentPath", source.parentPath)
      node.put("parentDeltaTableId", source.parentDeltaTableId)
      node.put("parentTableId", source.parentTableId)
      node.put("formatVersion", source.formatVersion)
    }
    Mapper.writeValueAsString(root)
  }

  private def parseTarget(json: String, identity: String): StreamingDependencyTarget =
    try {
      val root     = Mapper.readTree(json)
      val nameNode = required(root, "name", identity)
      if (!nameNode.isArray)
        invalid(s"Streaming dependency target '$identity' has invalid name")
      val sourcesNode = required(root, "sources", identity)
      if (!sourcesNode.isArray)
        invalid(s"Streaming dependency target '$identity' has invalid sources")
      val target = StreamingDependencyTarget(
        identity = text(root, "identity", identity),
        name = nameNode.elements().asScala.toSeq.map { node =>
          if (!node.isTextual || node.asText().isEmpty)
            invalid(s"Streaming dependency target '$identity' has invalid name")
          node.asText()
        },
        dataPath = text(root, "path", identity),
        deltaTableId = text(root, "deltaTableId", identity),
        tableId = text(root, "tableId", identity),
        definitionHash = text(root, "definitionHash", identity),
        formatVersion = integer(root, "formatVersion", identity),
        sources = sourcesNode.elements().asScala.toSeq.map { node =>
          StreamingDependencySource(
            parentIdentity = text(node, "parentIdentity", identity),
            parentPath = text(node, "parentPath", identity),
            parentDeltaTableId = text(node, "parentDeltaTableId", identity),
            parentTableId = text(node, "parentTableId", identity),
            formatVersion = integer(node, "formatVersion", identity)
          )
        }
      )
      validateTarget(target)
      target
    } catch {
      case error: org.apache.spark.sql.AnalysisException => throw error
      case NonFatal(error) =>
        invalid(
          s"Cannot parse streaming dependency target '$identity': " +
            Option(error.getMessage).getOrElse(error.toString)
        )
    }

  private def validateTarget(target: StreamingDependencyTarget): Unit = {
    if (
      target.identity.isEmpty ||
      target.name.isEmpty ||
      target.name.exists(_.isEmpty) ||
      target.dataPath.isEmpty ||
      target.deltaTableId.isEmpty ||
      target.tableId.isEmpty ||
      !target.definitionHash.matches("[0-9a-f]{64}") ||
      target.formatVersion <= 0
    )
      invalid(s"Invalid streaming dependency target '${target.identity}'")
    val duplicateParents = target.sources.groupBy(_.parentIdentity).collectFirst {
      case (identity, sources) if sources.size > 1 => identity
    }
    duplicateParents.foreach(parent =>
      invalid(s"Streaming dependency target '${target.identity}' has duplicate parent '$parent'")
    )
    target.sources.foreach { source =>
      if (
        source.parentIdentity.isEmpty ||
        source.parentPath.isEmpty ||
        source.parentDeltaTableId.isEmpty ||
        source.parentTableId.isEmpty ||
        source.formatVersion <= 0 ||
        source.parentIdentity == target.identity
      )
        invalid(s"Invalid streaming dependency source for target '${target.identity}'")
    }
  }

  private def required(node: JsonNode, field: String, identity: String): JsonNode =
    Option(node.get(field))
      .filterNot(_.isNull)
      .getOrElse(invalid(s"Streaming dependency target '$identity' is missing '$field'"))

  private def text(node: JsonNode, field: String, identity: String): String =
    Option(node.get(field))
      .filter(_.isTextual)
      .map(_.asText())
      .filter(_.nonEmpty)
      .getOrElse(invalid(s"Streaming dependency target '$identity' has invalid '$field'"))

  private def integer(node: JsonNode, field: String, identity: String): Int =
    Option(node.get(field))
      .filter(_.canConvertToInt)
      .map(_.asInt())
      .getOrElse(invalid(s"Streaming dependency target '$identity' has invalid '$field'"))

  private def invalid(message: String): Nothing =
    throw new org.apache.spark.sql.AnalysisException(
      "_LEGACY_ERROR_TEMP_2273",
      Map("message" -> s"Streaming dependency catalog: $message")
    )
}
