package org.openivm.spark.common

final case class WorkloadTableStats(
    rowCount: Option[Long] = None,
    numFiles: Option[Long] = None,
    sizeBytes: Option[Long] = None,
    partitionColumns: Seq[String] = Seq.empty
)

final case class WorkloadColumnStats(
    ndv: Option[Long] = None,
    min: Option[String] = None,
    max: Option[String] = None,
    nulls: Option[Long] = None,
    rowCount: Option[Long] = None
)

final case class WorkloadDeltaStats(
    rowCount: Option[Long] = None,
    numFiles: Option[Long] = None,
    sizeBytes: Option[Long] = None,
    min: Map[String, String] = Map.empty,
    max: Map[String, String] = Map.empty,
    nulls: Map[String, Long] = Map.empty
)

/** The `WorkloadFacts` payload threaded into `openivm_compile_with_facts(view,
  * facts_json)`. openivm ignores unknown keys, so newly-added quantitative
  * facts default empty and are forward-compatible.
  */
final case class WorkloadFacts(
    targetDialect: String = "spark",
    compileOnly: Boolean = true,
    forceViewDeltaCascade: Boolean = true,
    assumeInsertOnly: Boolean = false,
    runningWindowIncremental: Boolean = false,
    scd2RangeJoinAccel: Boolean = false,
    declareRelyFk: Boolean = false,
    deltaShape: Map[String, DeltaShape] = Map.empty,
    fkRelations: Seq[ForeignKeyRelation] = Seq.empty,
    uniqueKeys: Seq[UniqueKey] = Seq.empty,
    tableStats: Map[String, WorkloadTableStats] = Map.empty,
    columnStats: Map[String, WorkloadColumnStats] = Map.empty,
    deltaStats: Map[String, WorkloadDeltaStats] = Map.empty,
    schemaVersion: Int = 2
) {

  def toJson: String = {
    import WorkloadFacts._

    val fks = fkRelations
      .map { fk =>
        s"""{"child_table":${q(fk.childTable)},"child_columns":${stringArray(fk.childColumns)},""" +
          s""""parent_table":${q(fk.parentTable)},"parent_columns":${stringArray(fk.parentColumns)},""" +
          s""""rely":${bool(fk.rely)}}"""
      }
      .mkString("[", ",", "]")
    val uniques = uniqueKeys
      .map(key => s"""{"table":${q(key.table)},"columns":${stringArray(key.columns)},"rely":${bool(key.rely)}}""")
      .mkString("[", ",", "]")
    val shapes = deltaShape.toSeq
      .sortBy(_._1)
      .map { case (table, shape) => s"${q(table)}:${q(shape.compileFactValue)}" }
      .mkString("{", ",", "}")

    s"""{"schema_version":$schemaVersion,"target_dialect":${q(targetDialect)},""" +
      s""""compile_only":${bool(compileOnly)},"force_view_delta_cascade":${bool(forceViewDeltaCascade)},""" +
      s""""assume_insert_only":${bool(assumeInsertOnly)},""" +
      s""""running_window_incremental":${bool(runningWindowIncremental)},""" +
      s""""scd2_range_join_accel":${bool(scd2RangeJoinAccel)},"delta_shape":$shapes,""" +
      s""""fk_relations":$fks,"unique_keys":$uniques,""" +
      s""""table_stats":${tableStatsJson(tableStats)},"column_stats":${columnStatsJson(columnStats)},""" +
      s""""delta_stats":${deltaStatsJson(deltaStats)}}"""
  }
}

object WorkloadFacts {
  def fromSourceStats(stats: Seq[SourceStats]): WorkloadFacts = {
    val tableStats = stats.map { source =>
      source.table -> WorkloadTableStats(
        rowCount = Some(source.tableStats.rowCount),
        numFiles = Some(source.tableStats.numFiles),
        sizeBytes = Some(source.tableStats.sizeBytes),
        partitionColumns = source.tableStats.partitionColumns
      )
    }.toMap

    val columnStats = stats.flatMap { source =>
      source.columnStats.map { case (column, stat) =>
        s"${source.table}.$column" -> WorkloadColumnStats(
          ndv = stat.ndv,
          min = stat.min,
          max = stat.max,
          nulls = stat.nulls,
          rowCount = Some(source.tableStats.rowCount)
        )
      }
    }.toMap

    WorkloadFacts(tableStats = tableStats, columnStats = columnStats)
  }

  def deltaStatsFromFiles(files: Seq[FileStat]): WorkloadDeltaStats = {
    val columns = files.flatMap(file => file.minValues.keySet ++ file.maxValues.keySet ++ file.nullCount.keySet).toSet
    WorkloadDeltaStats(
      rowCount = Some(files.map(file => file.numRecords - file.dvCardinality).sum),
      numFiles = Some(files.size.toLong),
      min = columns.flatMap(name => files.flatMap(_.minValues.get(name)).reduceOption(minValue).map(name -> _)).toMap,
      max = columns.flatMap(name => files.flatMap(_.maxValues.get(name)).reduceOption(maxValue).map(name -> _)).toMap,
      nulls = columns.map(name => name -> files.flatMap(_.nullCount.get(name)).sum).toMap
    )
  }

  private[common] def bool(x: Boolean): String = if (x) "true" else "false"

  private[common] def q(x: String): String = "\"" + escapeJson(x) + "\""

  private[common] def stringArray(xs: Seq[String]): String = xs.map(q).mkString("[", ",", "]")

  private def tableStatsJson(stats: Map[String, WorkloadTableStats]): String =
    objectJson(stats) { s =>
      fields(
        "row_count"         -> s.rowCount.map(_.toString),
        "num_files"         -> s.numFiles.map(_.toString),
        "size_bytes"        -> s.sizeBytes.map(_.toString),
        "partition_columns" -> nonEmptyArray(s.partitionColumns)
      )
    }

  private def columnStatsJson(stats: Map[String, WorkloadColumnStats]): String =
    objectJson(stats) { s =>
      fields(
        "ndv"       -> s.ndv.map(_.toString),
        "min"       -> s.min.map(q),
        "max"       -> s.max.map(q),
        "nulls"     -> s.nulls.map(_.toString),
        "row_count" -> s.rowCount.map(_.toString)
      )
    }

  private def deltaStatsJson(stats: Map[String, WorkloadDeltaStats]): String =
    objectJson(stats) { s =>
      fields(
        "row_count"  -> s.rowCount.map(_.toString),
        "num_files"  -> s.numFiles.map(_.toString),
        "size_bytes" -> s.sizeBytes.map(_.toString),
        "min"        -> nonEmptyStringMap(s.min),
        "max"        -> nonEmptyStringMap(s.max),
        "nulls"      -> nonEmptyLongMap(s.nulls)
      )
    }

  private def objectJson[A](values: Map[String, A])(valueJson: A => String): String =
    values.toSeq.sortBy(_._1).map { case (key, value) => s"${q(key)}:${valueJson(value)}" }.mkString("{", ",", "}")

  private def fields(values: (String, Option[String])*): String =
    values.collect { case (key, Some(value)) => s"${q(key)}:$value" }.mkString("{", ",", "}")

  private def nonEmptyArray(values: Seq[String]): Option[String] =
    if (values.nonEmpty) Some(stringArray(values)) else None

  private def nonEmptyStringMap(values: Map[String, String]): Option[String] =
    if (values.nonEmpty) Some(objectJson(values)(q)) else None

  private def nonEmptyLongMap(values: Map[String, Long]): Option[String] =
    if (values.nonEmpty) Some(objectJson(values)(_.toString)) else None

  private def escapeJson(raw: String): String =
    raw.flatMap {
      case '"'              => "\\\""
      case '\\'             => "\\\\"
      case '\b'             => "\\b"
      case '\f'             => "\\f"
      case '\n'             => "\\n"
      case '\r'             => "\\r"
      case '\t'             => "\\t"
      case c if c.isControl => f"\\u${c.toInt}%04x"
      case c                => c.toString
    }

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
