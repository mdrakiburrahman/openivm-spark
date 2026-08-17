package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * [[ChangePropagation]] implementation that reads Delta Change Data Feed
 * (`spark.read.format("delta").option("readChangeFeed","true")...`) for
 * every tracked source at refresh time.
 *
 * Writers other than this Spark extension can update the base tables — we
 * never intercept DML; we always re-derive the change set from the Delta
 * log's CDF rows between the last persisted version and the current
 * version.
 *
 * Per-(view, source) last-consumed Delta version is tracked in
 * [[CdfWatermarkCatalog]].
 */
final class CdfChangePropagation extends ChangePropagation {

  private val log = LoggerFactory.getLogger(getClass)

  override def mode: ChangeFeedMode = ChangeFeedMode.Cdf

  override val requiresDmlInterception: Boolean = false
  override val requiresMvCdf: Boolean           = true

  override def validateSources(spark: SparkSession, sources: Seq[String]): Unit = {
    val missing = sources.distinct.flatMap { src =>
      val enabled =
        try CdfChangePropagation.tableHasCdf(spark, src)
        catch {
          case t: Throwable =>
            log.warn(s"[openivm-cdf] could not read table properties for $src: ${t.getMessage}")
            true
        }
      if (enabled) None else Some(src)
    }
    if (missing.nonEmpty) {
      val missingList = missing.mkString(", ")
      val msg =
        s"openivm-spark: changeFeed.mode = 'cdf' requires every source Delta " +
          s"table to have 'delta.enableChangeDataFeed' = 'true', but the following " +
          s"sources do NOT: [$missingList]. " +
          s"Fix: ALTER TABLE <table> SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
      throw new AnalysisException("_LEGACY_ERROR_TEMP_2273", Map("message" -> msg))
    }
  }

  override def currentWatermarks(spark: SparkSession, sources: Seq[String]): Map[String, ChangeWatermark] =
    sources.distinct.flatMap { src =>
      CdfChangePropagation.tableLatestVersion(spark, src).map(v => src -> ChangeWatermark.DeltaVersion(v))
    }.toMap

  override def hasPendingChanges(
      spark: SparkSession,
      viewName: String,
      sources: Seq[String],
      persisted: Map[String, ChangeWatermark]
  ): Boolean = {
    val effective = effectivePersistedVersions(spark, viewName, sources, persisted)
    sources.distinct.exists { src =>
      val currentOpt   = CdfChangePropagation.tableLatestVersion(spark, src)
      val persistedVer = effective.get(src)
      currentOpt match {
        case None => false
        case Some(cur) =>
          persistedVer match {
            case Some(p) => cur > p
            case None    => true
          }
      }
    }
  }

  override def collectChanges(
      spark: SparkSession,
      viewName: String,
      sources: Seq[String],
      persisted: Map[String, ChangeWatermark]
  ): Seq[ChangeBatch] = {
    val effective = effectivePersistedVersions(spark, viewName, sources, persisted)
    sources.distinct.flatMap { src =>
      val currentOpt = CdfChangePropagation.tableLatestVersion(spark, src)
      currentOpt.flatMap { cur =>
        val from = effective.getOrElse(src, -1L)
        if (cur > from) Some(CdfChangeBatch(src, startVersionExclusive = from, endVersionInclusive = cur))
        else None
      }
    }
  }

  override def buildSourceDeltaViewSql(
      sourceTable: String,
      sourceSchema: StructType,
      batches: Seq[ChangeBatch]
  ): String = {
    val short    = sourceTable.split("\\.").last
    val viewName = s"`openivm_delta_$short`"
    val cols     = sourceSchema.fieldNames.map(n => s"`${n.replace("`", "``")}`").mkString(", ")

    val cdfBatch = batches.collectFirst { case b: CdfChangeBatch => b }

    cdfBatch match {
      case Some(b) =>
        val from = b.startVersionExclusive + 1L
        val to   = b.endVersionInclusive
        val tbl  = sourceTable.replace("'", "''")
        s"""-- registered via DataFrame API: readChangeFeed startingVersion=$from endingVersion=$to
           |-- equivalent SQL form retained for diagnostic logs only:
           |CREATE OR REPLACE TEMP VIEW $viewName AS
           |SELECT $cols,
           |       CAST(`_commit_timestamp` AS TIMESTAMP) AS openivm_timestamp,
           |       CASE
           |         WHEN `_change_type` IN ('insert', 'update_postimage') THEN CAST(1 AS INT)
           |         WHEN `_change_type` IN ('delete', 'update_preimage')  THEN CAST(-1 AS INT)
           |       END AS openivm_multiplicity
           |FROM table_changes('$tbl', $from, $to)
           |WHERE `_change_type` IN ('insert', 'delete', 'update_preimage', 'update_postimage')""".stripMargin

      case None =>
        // Diagnostic-only SQL rendering of the empty view; the actual empty
        // view is registered via the DataFrame API in registerSourceDeltaView
        // (see ChangePropagation.registerEmptyDeltaView) so struct-typed
        // columns keep their exact native type.
        val nullCols = sourceSchema.fieldNames.map(n => s"NULL AS `${n.replace("`", "``")}`").mkString(", ")
        s"""CREATE OR REPLACE TEMP VIEW $viewName AS
           |SELECT $cols, CURRENT_TIMESTAMP() AS openivm_timestamp, CAST(0 AS INT) AS openivm_multiplicity
           |FROM (SELECT $nullCols) WHERE 1=0""".stripMargin
    }
  }

  override def registerSourceDeltaView(
      spark: SparkSession,
      sourceTable: String,
      sourceSchema: StructType,
      batches: Seq[ChangeBatch]
  ): String = {
    val short    = sourceTable.split("\\.").last
    val viewName = s"openivm_delta_$short"

    val cdfBatch = batches.collectFirst { case b: CdfChangeBatch => b }
    val topKMeta = CdfChangePropagation.topKBackingMeta(spark, sourceTable)

    cdfBatch match {
      case Some(b) if topKMeta.nonEmpty =>
        val meta             = topKMeta.get
        val escapedLocation  = meta.location.replace("`", "``")
        val suffix           = meta.backingViewSuffix.get
        val userCols         = sourceSchema.fieldNames.map(n => s"`${n.replace("`", "``")}`").mkString(", ")
        val oldVisible =
          if (b.startVersionExclusive >= 0)
            s"""SELECT $userCols,
               |       CURRENT_TIMESTAMP() AS openivm_timestamp,
               |       CAST(-1 AS INT) AS openivm_multiplicity
               |FROM (
               |  SELECT $userCols
               |  FROM delta.`$escapedLocation` VERSION AS OF ${b.startVersionExclusive} $suffix
               |) __openivm_top_k_old""".stripMargin
          else ""
        val newVisible =
          s"""SELECT $userCols,
             |       CURRENT_TIMESTAMP() AS openivm_timestamp,
             |       CAST(1 AS INT) AS openivm_multiplicity
             |FROM (
             |  SELECT $userCols
             |  FROM delta.`$escapedLocation` VERSION AS OF ${b.endVersionInclusive} $suffix
             |) __openivm_top_k_new""".stripMargin
        val body = if (oldVisible.nonEmpty) s"$oldVisible\nUNION ALL\n$newVisible" else newVisible
        val sql  = s"CREATE OR REPLACE TEMP VIEW `$viewName` AS\n$body"
        spark.sql(sql)
        sql

      case Some(b) =>
        val from = b.startVersionExclusive + 1L
        val to   = b.endVersionInclusive
        val raw =
          spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", from)
            .option("endingVersion", to)
            .table(sourceTable)

        val userCols         = sourceSchema.fieldNames.map(n => s"`${n.replace("`", "``")}`").mkString(", ")
        val transformedAlias = s"_openivm_cdf_raw_$short"
        raw.createOrReplaceTempView(transformedAlias)
        val sql = s"""CREATE OR REPLACE TEMP VIEW `$viewName` AS
                     |SELECT $userCols,
                     |       CAST(`_commit_timestamp` AS TIMESTAMP) AS openivm_timestamp,
                     |       CASE
                     |         WHEN `_change_type` IN ('insert', 'update_postimage') THEN CAST(1 AS INT)
                     |         WHEN `_change_type` IN ('delete', 'update_preimage')  THEN CAST(-1 AS INT)
                     |       END AS openivm_multiplicity
                     |FROM `$transformedAlias`
                     |WHERE `_change_type` IN ('insert', 'delete', 'update_preimage', 'update_postimage')""".stripMargin
        spark.sql(sql)
        sql

      case None =>
        registerEmptyDeltaView(spark, sourceTable, sourceSchema)
    }
  }

  override def markConsumed(spark: SparkSession, viewName: String, batches: Seq[ChangeBatch]): Unit =
    CdfWatermarkCatalog.putAll(
      spark,
      viewName,
      batches.collect { case b: CdfChangeBatch => b.baseTable -> b.endVersionInclusive }.toMap
    )

  override def pruneConsumed(spark: SparkSession, viewsByTable: Map[String, Seq[String]]): Unit = ()

  override def removeForBaseTable(spark: SparkSession, baseTable: String): Unit =
    CdfWatermarkCatalog.removeForBaseTable(spark, baseTable)

  /**
   * Per-(view, source) last-consumed Delta version: prefer the value
   * persisted in [[CdfWatermarkCatalog]] (carries cross-refresh state),
   * fall back to the value captured at MV CREATE time (carried in
   * `persisted`), and finally to "no version persisted" (consume everything
   * since the start).
   */
  private def effectivePersistedVersions(
      spark: SparkSession,
      viewName: String,
      sources: Seq[String],
      persisted: Map[String, ChangeWatermark]
  ): Map[String, Long] = {
    val liveBySource = CdfWatermarkCatalog.getAll(spark, viewName, sources)
    sources.distinct.flatMap { src =>
      val live = liveBySource.get(src)
      val seed = persisted.get(src).collect { case ChangeWatermark.DeltaVersion(v) => v }
      (live, seed) match {
        case (Some(l), Some(s)) => Some(src -> math.max(l, s))
        case (Some(l), None)    => Some(src -> l)
        case (None, Some(s))    => Some(src -> s)
        case (None, None)       => None
      }
    }.toMap
  }
}

object CdfChangePropagation {

  private[common] def topKBackingMeta(spark: SparkSession, name: String): Option[MvMetadata] = {
    val candidates = MvCatalog.list(spark).filter(_.backingViewSuffix.nonEmpty)
    val exact = candidates.find { meta =>
      val qualified = meta.name.database.fold(meta.name.identifier)(db => s"$db.${meta.name.identifier}")
      qualified.equalsIgnoreCase(name)
    }
    exact.orElse {
      val short   = name.split("\\.").last
      val matches = candidates.filter(_.name.identifier.equalsIgnoreCase(short))
      if (matches.size == 1) matches.headOption else None
    }
  }

  /**
   * `true` when the Delta table identified by `name` has
   * `delta.enableChangeDataFeed` set to `true`.  Names are resolved through
   * Spark's catalog: bare names are looked up in the active database, and
   * `db.table` is resolved via [[DeltaTable.forName]].  Returns `false` when
   * the table cannot be resolved (caller handles that as a "missing source"
   * upstream).
   */
  def tableHasCdf(spark: SparkSession, name: String): Boolean = {
    topKBackingMeta(spark, name) match {
      case Some(meta) =>
        val detailRow = DeltaTable
          .forPath(spark, meta.location)
          .detail()
          .collect()
          .head
        val properties = detailRow.getMap[String, String](detailRow.fieldIndex("properties"))
        return properties.exists { case (k, v) =>
          k.equalsIgnoreCase("delta.enableChangeDataFeed") && v.trim.equalsIgnoreCase("true")
        }
      case None => ()
    }
    val identifier = CatalystSqlParser.parseTableIdentifier(name)
    val resolved = identifier.database match {
      case Some(_) => name
      case None    => name
    }
    val df =
      try spark.sql(s"SHOW TBLPROPERTIES ${quoteForCatalog(resolved)}")
      catch { case _: Throwable => return false }
    val rows = df.collect()
    rows.exists { row =>
      val k = row.getAs[String]("key")
      val v = row.getAs[String]("value")
      k != null && k.equalsIgnoreCase("delta.enableChangeDataFeed") &&
      v != null && v.trim.equalsIgnoreCase("true")
    }
  }

  /** Current Delta `version` of `name`, or `None` if the table cannot be loaded as Delta. */
  def tableLatestVersion(spark: SparkSession, name: String): Option[Long] = {
    try {
      val dt = topKBackingMeta(spark, name)
        .map(meta => DeltaTable.forPath(spark, meta.location))
        .getOrElse(DeltaTable.forName(spark, name))
      val hist = dt.history(1).collect()
      hist.headOption.map(_.getAs[Long]("version"))
    } catch {
      case _: Throwable => None
    }
  }

  private def quoteForCatalog(name: String): String =
    name.split("\\.").map(p => s"`${p.replace("`", "``")}`").mkString(".")
}
