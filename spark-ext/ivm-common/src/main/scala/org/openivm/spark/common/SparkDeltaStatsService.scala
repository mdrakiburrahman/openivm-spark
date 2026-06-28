package org.openivm.spark.common

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile

import scala.collection.JavaConverters._
import scala.collection.mutable

final case class FileStat(
    path: String,
    numRecords: Long,
    minValues: Map[String, String],
    maxValues: Map[String, String],
    nullCount: Map[String, Long],
    dvCardinality: Long,
    partitionValues: Map[String, String],
    tightBounds: Boolean
)

final case class TableStats(
    rowCount: Long,
    numFiles: Long,
    sizeBytes: Long,
    partitionColumns: Seq[String]
)

final case class CatalogTableStats(
    sizeBytes: Long,
    rowCount: Option[Long]
)

final case class ColumnStats(
    ndv: Option[Long],
    min: Option[String],
    max: Option[String],
    nulls: Option[Long]
)

final case class SourceStats(
    table: String,
    tableStats: TableStats,
    catalogTableStats: Option[CatalogTableStats],
    columnStats: Map[String, ColumnStats],
    files: Seq[FileStat],
    hasDeletionVectors: Boolean
)

final class SparkDeltaStatsService {
  import SparkDeltaStatsService._

  private val cache = mutable.HashMap.empty[CacheKey, SourceStats]

  def statsFor(spark: SparkSession, tableNameOrPath: String): SourceStats =
    cache.synchronized {
      cache.getOrElseUpdate(
        CacheKey(System.identityHashCode(spark), tableNameOrPath),
        readStats(spark, tableNameOrPath)
      )
    }

  def clear(): Unit =
    cache.synchronized {
      cache.clear()
    }
}

object SparkDeltaStatsService {
  private val Json = new ObjectMapper()

  private final case class CacheKey(sparkId: Int, table: String)
  private final case class ResolvedDeltaTable(identifier: Option[TableIdentifier], log: DeltaLog)
  private final case class ParsedDeltaStats(
      numRecords: Long,
      minValues: Map[String, String],
      maxValues: Map[String, String],
      nullCount: Map[String, Long]
  )

  def forRefresh(): SparkDeltaStatsService = new SparkDeltaStatsService

  private def readStats(spark: SparkSession, tableNameOrPath: String): SourceStats = {
    val resolved = resolveDeltaTable(spark, tableNameOrPath)
    val snapshot = resolved.log.update()
    val files    = snapshot.allFiles.collect().toVector.map(fileStat)

    val rowCount           = files.map(file => file.numRecords - file.dvCardinality).sum
    val hasDeletionVectors = files.exists(_.dvCardinality > 0L)
    val tableStats = TableStats(
      rowCount = rowCount,
      numFiles = snapshot.numOfFiles,
      sizeBytes = snapshot.sizeInBytes,
      partitionColumns = snapshot.metadata.partitionColumns
    )
    val catalogStats = resolved.identifier.flatMap(catalogTableStats(spark, _))
    SourceStats(
      table = tableNameOrPath,
      tableStats = tableStats,
      catalogTableStats = catalogStats.map { stats =>
        CatalogTableStats(sizeBytes = bigIntToLong(stats.sizeInBytes), rowCount = stats.rowCount.map(bigIntToLong))
      },
      columnStats = mergedColumnStats(catalogStats, files),
      files = files,
      hasDeletionVectors = hasDeletionVectors
    )
  }

  private def resolveDeltaTable(spark: SparkSession, tableNameOrPath: String): ResolvedDeltaTable = {
    val maybeIdentifier = parseExistingTableIdentifier(spark, tableNameOrPath)
    maybeIdentifier match {
      case Some(identifier) => ResolvedDeltaTable(Some(identifier), DeltaLog.forTable(spark, identifier))
      case None             => ResolvedDeltaTable(None, DeltaLog.forTable(spark, new Path(tableNameOrPath)))
    }
  }

  private def parseExistingTableIdentifier(spark: SparkSession, tableNameOrPath: String): Option[TableIdentifier] =
    try {
      val identifier = spark.sessionState.sqlParser.parseTableIdentifier(tableNameOrPath)
      if (spark.sessionState.catalog.tableExists(identifier)) Some(identifier) else None
    } catch {
      case _: Exception => None
    }

  private def catalogTableStats(spark: SparkSession, identifier: TableIdentifier) =
    spark.sessionState.catalog.getTableMetadata(identifier).stats

  private def fileStat(file: AddFile): FileStat = {
    val stats         = parseDeltaStats(file.stats)
    val dvCardinality = Option(file.deletionVector).map(_.cardinality).getOrElse(0L)
    FileStat(
      path = file.path,
      numRecords = stats.numRecords,
      minValues = stats.minValues,
      maxValues = stats.maxValues,
      nullCount = stats.nullCount,
      dvCardinality = dvCardinality,
      partitionValues = file.partitionValues.toMap,
      // Delta marks tightBounds=false once deletion vectors make min/max conservative bounds.
      tightBounds = file.tightBounds.getOrElse(true)
    )
  }

  private def parseDeltaStats(rawStats: String): ParsedDeltaStats = {
    if (rawStats == null || rawStats.trim.isEmpty) {
      ParsedDeltaStats(numRecords = 0L, minValues = Map.empty, maxValues = Map.empty, nullCount = Map.empty)
    } else {
      val root = Json.readTree(rawStats)
      ParsedDeltaStats(
        numRecords = longField(root, "numRecords").getOrElse(0L),
        minValues = stringFields(root.get("minValues")),
        maxValues = stringFields(root.get("maxValues")),
        nullCount = longFields(root.get("nullCount"))
      )
    }
  }

  private def mergedColumnStats(
      catalogStats: Option[org.apache.spark.sql.catalyst.catalog.CatalogStatistics],
      files: Seq[FileStat]
  ): Map[String, ColumnStats] = {
    val catalog = catalogStats.toSeq
      .flatMap(_.colStats.toSeq)
      .map { case (name, stat) =>
        name -> fromCatalogColumnStat(stat)
      }
      .toMap
    val delta = deltaColumnStats(files)
    (catalog.keySet ++ delta.keySet).map { name =>
      val c = catalog.get(name)
      val d = delta.get(name)
      name -> ColumnStats(
        ndv = c.flatMap(_.ndv),
        min = c.flatMap(_.min).orElse(d.flatMap(_.min)),
        max = c.flatMap(_.max).orElse(d.flatMap(_.max)),
        nulls = c.flatMap(_.nulls).orElse(d.flatMap(_.nulls))
      )
    }.toMap
  }

  private def fromCatalogColumnStat(stat: CatalogColumnStat): ColumnStats =
    ColumnStats(
      ndv = stat.distinctCount.map(bigIntToLong),
      min = stat.min,
      max = stat.max,
      nulls = stat.nullCount.map(bigIntToLong)
    )

  private def deltaColumnStats(files: Seq[FileStat]): Map[String, ColumnStats] = {
    val columnNames =
      files.flatMap(file => file.minValues.keySet ++ file.maxValues.keySet ++ file.nullCount.keySet).toSet
    columnNames.map { name =>
      name -> ColumnStats(
        ndv = None,
        min = files.flatMap(_.minValues.get(name)).reduceOption(minValue),
        max = files.flatMap(_.maxValues.get(name)).reduceOption(maxValue),
        nulls = Some(files.flatMap(_.nullCount.get(name)).sum)
      )
    }.toMap
  }

  private def stringFields(node: JsonNode): Map[String, String] =
    if (node == null || node.isNull) Map.empty
    else node.fields().asScala.map(entry => entry.getKey -> jsonValueToString(entry.getValue)).toMap

  private def longFields(node: JsonNode): Map[String, Long] =
    if (node == null || node.isNull) Map.empty
    else node.fields().asScala.flatMap(entry => longNode(entry.getValue).map(entry.getKey -> _)).toMap

  private def longField(node: JsonNode, field: String): Option[Long] =
    Option(node).flatMap(root => Option(root.get(field))).flatMap(longNode)

  private def longNode(node: JsonNode): Option[Long] =
    if (node == null || node.isNull) None
    else if (node.isNumber) Some(node.longValue())
    else if (node.isTextual) scala.util.Try(node.asText().toLong).toOption
    else None

  private def jsonValueToString(node: JsonNode): String =
    if (node == null || node.isNull) "null"
    else if (node.isTextual) node.asText()
    else node.toString

  private def bigIntToLong(value: BigInt): Long =
    if (value > BigInt(Long.MaxValue)) Long.MaxValue
    else if (value < BigInt(Long.MinValue)) Long.MinValue
    else value.toLong

  private def minValue(left: String, right: String): String =
    if (compareValues(left, right) <= 0) left else right

  private def maxValue(left: String, right: String): String =
    if (compareValues(left, right) >= 0) left else right

  private def compareValues(left: String, right: String): Int =
    (scala.util.Try(BigDecimal(left)).toOption, scala.util.Try(BigDecimal(right)).toOption) match {
      case (Some(l), Some(r)) => l.compare(r)
      case _                  => left.compareTo(right)
    }
}
