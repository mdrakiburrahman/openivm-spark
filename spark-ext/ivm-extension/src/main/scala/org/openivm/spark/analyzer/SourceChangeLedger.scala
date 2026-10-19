package org.openivm.spark.analyzer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, Metadata, RemoveFile}
import org.apache.spark.sql.functions.col
import org.openivm.spark.common.{
  DeltaTableVersion,
  MvCatalog,
  SourceLedgerEntry,
  SourceVersionDelta,
  StagingCatalog,
  StagingDelta
}

import java.util.Locale
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Keeps the staging records of every base table that feeds materialized views complete with
 * respect to its Delta log.
 *
 * A base table's ledger position is the committed Delta version up to which its staging records
 * describe every change. Intercepted writes publish their records together with the position they
 * commit, in one RocksDB write under the table's mutation lock. Commits past the position, such as
 * native streaming appends, writers without the OpenIVM extension, or writes whose records a
 * restarted driver lost by restoring an older state snapshot, are captured from the Delta log
 * before materialized views read the table's changes: appended files as INSERT rows, other changes
 * as the exact multiset difference of the two versions, and changes that cannot be compared as an
 * OVERWRITE that dependents apply as a full refresh. A table whose position was never recorded
 * publishes an OVERWRITE for its existing dependents once, since nothing proves what they reflect.
 */
private[spark] object SourceChangeLedger extends Logging {

  /** Capture the unrecorded commits of every base-table source. Materialized-view sources, matched
    * by short name as their refreshes match downstream sources, are kept complete by those
    * refreshes.
    *
    * @param spark   active session
    * @param sources qualified source names of a materialized view
    */
  def reconcile(spark: SparkSession, sources: Seq[String]): Unit = {
    val materialized = MvCatalog.list(spark).map(meta => lower(meta.name.identifier)).toSet
    sources.distinct.filterNot(source => materialized.contains(shortName(source))).foreach { source =>
      StagedDmlNode.withBaseTableMutationLock(source)(reconcileLocked(spark, source))
    }
  }

  /** Capture the unrecorded commits of `baseTable`, then return its committed position, or `None`
    * when it is not an existing Delta table. The caller holds the table's mutation lock.
    *
    * @param spark     active session
    * @param baseTable qualified base-table name
    */
  def reconcileLocked(spark: SparkSession, baseTable: String): Option[SourceLedgerEntry] =
    DeltaTableVersion.deltaLogOption(spark, baseTable).flatMap { log =>
      position(log).map { live =>
        StagingCatalog.ledger(spark, baseTable) match {
          case Some(recorded) if recorded == live =>
            ()
          case Some(recorded) if recorded.tableId == live.tableId && recorded.version < live.version =>
            publish(spark, baseTable, live, capture(spark, log, baseTable, recorded.version, live.version))
          case Some(recorded) =>
            logWarning(
              s"[openivm] $baseTable moved from version ${recorded.version} of Delta table ${recorded.tableId} " +
                s"to version ${live.version} of Delta table ${live.tableId} outside OpenIVM; dependents refresh fully"
            )
            publish(spark, baseTable, live, overwrite(spark, baseTable))
          case None if hasDependents(spark, baseTable) =>
            logWarning(
              s"[openivm] $baseTable has no recorded change position; its materialized views refresh fully once"
            )
            publish(spark, baseTable, live, overwrite(spark, baseTable))
          case None =>
            StagingCatalog.publish(spark, baseTable, Seq.empty, live)
        }
        live
      }
    }

  /** Publish the records of an intercepted write that started at position `before`. When the
    * write was not the only commit since `before`, its records cannot be attributed and the
    * interval is captured from the Delta log instead. Hold the table's mutation lock.
    *
    * @param spark     active session
    * @param baseTable qualified base-table name
    * @param before    position returned by [[reconcileLocked]] before the write ran
    * @param deltas    records the write staged
    * @return the records published
    */
  def publishWrite(
      spark: SparkSession,
      baseTable: String,
      before: Option[SourceLedgerEntry],
      deltas: Seq[StagingDelta]
  ): Seq[StagingDelta] =
    DeltaTableVersion.deltaLogOption(spark, baseTable).flatMap(log => position(log).map(log -> _)) match {
      case None =>
        deltas.foreach(StagingCatalog.record(spark, _))
        deltas
      case Some((_, after))
          if before.exists(start => start.tableId == after.tableId && start.version == after.version) =>
        // The write committed nothing, so it changed no rows.
        deltas.foreach(delta => StagedDmlNode.deleteStagingPath(spark, delta.stagingPath))
        Seq.empty
      case Some((_, after))
          if before.exists(start => start.tableId == after.tableId && start.version + 1L == after.version) =>
        publish(spark, baseTable, after, deltas)
        deltas
      case Some((log, after)) =>
        deltas.foreach(delta => StagedDmlNode.deleteStagingPath(spark, delta.stagingPath))
        val captured = before match {
          case Some(start) if start.tableId == after.tableId =>
            logInfo(
              s"[openivm] $baseTable advanced ${start.version}->${after.version} during an intercepted write; " +
                "capturing the interval from the Delta log"
            )
            capture(spark, log, baseTable, start.version, after.version)
          case _ => overwrite(spark, baseTable)
        }
        publish(spark, baseTable, after, captured)
        captured
    }

  /** Publish the exact records of a table replacement, which describe every change since the
    * position the replacement started from. Hold the table's mutation lock.
    *
    * @param spark     active session
    * @param baseTable qualified base-table name
    * @param deltas    records describing the replacement
    */
  def publishReplacement(spark: SparkSession, baseTable: String, deltas: Seq[StagingDelta]): Unit =
    DeltaTableVersion.deltaLogOption(spark, baseTable).flatMap(position) match {
      case Some(after) => publish(spark, baseTable, after, deltas)
      case None        => deltas.foreach(StagingCatalog.record(spark, _))
    }

  /** Whether versions `oldVersion` and `newVersion` of `baseTable` admit an exact row-level
    * change set. Schemas are compared by DDL, the identity dependents fingerprint.
    */
  private[analyzer] def comparable(
      spark: SparkSession,
      baseTable: String,
      oldVersion: Long,
      newVersion: Long
  ): Boolean =
    try {
      val schema = snapshot(spark, baseTable, oldVersion).schema
      val same   = schema.toDDL == snapshot(spark, baseTable, newVersion).schema.toDDL
      if (!same) logInfo(s"[openivm] $baseTable changed its schema between versions $oldVersion and $newVersion")
      same && SourceVersionDelta.supportsBagDiff(schema)
    } catch {
      case NonFatal(error) =>
        logWarning(s"[openivm] cannot compare $baseTable versions $oldVersion and $newVersion", error)
        false
    }

  /** Stage `old EXCEPT ALL new` as DELETE and `new EXCEPT ALL old` as INSERT. */
  private[analyzer] def exactChange(
      spark: SparkSession,
      baseTable: String,
      oldVersion: Long,
      newVersion: Long
  ): Seq[StagingDelta] = {
    val oldRows = snapshot(spark, baseTable, oldVersion)
    val newRows = snapshot(spark, baseTable, newVersion)
    stage(
      spark,
      baseTable,
      Seq(
        (oldRows.exceptAll(newRows), StagingDelta.OpTypes.Delete, false),
        (newRows.exceptAll(oldRows), StagingDelta.OpTypes.Insert, false)
      )
    )
  }

  /** Rows of a committed version, read through SQL time travel so that version's schema applies. */
  private[analyzer] def snapshot(spark: SparkSession, baseTable: String, version: Long): DataFrame =
    spark.sql(s"SELECT * FROM ${quote(baseTable)} VERSION AS OF $version")

  /** Stage each `(rows, opType, keepEmpty)` in order; on failure no staged output survives. */
  private[analyzer] def stage(
      spark: SparkSession,
      baseTable: String,
      ops: Seq[(DataFrame, String, Boolean)]
  ): Seq[StagingDelta] = {
    val staged = mutable.ArrayBuffer.empty[StagingDelta]
    try {
      ops.foreach { case (rows, opType, keepEmpty) =>
        val path  = IvmDmlInterceptorRule.stagingPath(spark, baseTable, opType)
        val delta = StagedDmlNode.writeStaging(spark, rows, baseTable, path, opType)
        if (keepEmpty || !spark.read.format("delta").load(path).isEmpty) staged += delta
        else StagedDmlNode.deleteStagingPath(spark, path)
      }
      staged.toVector
    } catch {
      case failure: Throwable =>
        staged.foreach(delta => StagedDmlNode.cleanupStagingPath(spark, delta.stagingPath, failure))
        throw failure
    }
  }

  /** Records describing every commit of `baseTable` in `(from, to]`. */
  private def capture(
      spark: SparkSession,
      log: DeltaLog,
      baseTable: String,
      from: Long,
      to: Long
  ): Seq[StagingDelta] = {
    val commits =
      try
        log
          .getChanges(from + 1L, failOnDataLoss = false)
          .takeWhile(_._1 <= to)
          .map { case (version, actions) => version -> actions.toVector }
          .toVector
      catch {
        case NonFatal(error) =>
          logWarning(s"[openivm] cannot read the Delta log of $baseTable after version $from", error)
          return overwrite(spark, baseTable)
      }
    if (commits.map(_._1) != (from + 1L to to))
      return overwrite(spark, baseTable)
    val actions = commits.flatMap(_._2)
    if (actions.exists(_.isInstanceOf[Metadata]) && !sameSchema(spark, baseTable, from, to))
      overwrite(spark, baseTable)
    else if (!actions.exists(changesData))
      Seq.empty
    else
      appendedFiles(actions) match {
        case Some(added) =>
          try appended(spark, log, baseTable, to, added)
          catch {
            case NonFatal(error) =>
              logWarning(s"[openivm] cannot read files appended to $baseTable in ($from, $to]", error)
              exactOrOverwrite(spark, baseTable, from, to)
          }
        case None => exactOrOverwrite(spark, baseTable, from, to)
      }
  }

  private def appended(
      spark: SparkSession,
      log: DeltaLog,
      baseTable: String,
      to: Long,
      added: Seq[AddFile]
  ): Seq[StagingDelta] = {
    val rows    = log.createDataFrame(log.getSnapshotAt(to), added.toList, isStreaming = false)
    val columns = snapshot(spark, baseTable, to).columns.map(name => col(s"`${name.replace("`", "``")}`"))
    stage(spark, baseTable, Seq((rows.select(columns.toIndexedSeq: _*), StagingDelta.OpTypes.Insert, false)))
  }

  private def exactOrOverwrite(spark: SparkSession, baseTable: String, from: Long, to: Long): Seq[StagingDelta] =
    if (!comparable(spark, baseTable, from, to)) overwrite(spark, baseTable)
    else
      try exactChange(spark, baseTable, from, to)
      catch {
        case NonFatal(error) =>
          logWarning(s"[openivm] exact change set of $baseTable in ($from, $to] failed", error)
          overwrite(spark, baseTable)
      }

  /** An empty OVERWRITE record: dependents recompute from the live table. */
  private def overwrite(spark: SparkSession, baseTable: String): Seq[StagingDelta] = {
    val schema = spark.table(quote(baseTable)).schema
    val empty  = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    stage(spark, baseTable, Seq((empty, StagingDelta.OpTypes.Overwrite, true)))
  }

  private def publish(
      spark: SparkSession,
      baseTable: String,
      live: SourceLedgerEntry,
      deltas: Seq[StagingDelta]
  ): Unit =
    try StagingCatalog.publish(spark, baseTable, deltas, live)
    catch {
      case failure: Throwable =>
        deltas.foreach(delta => StagedDmlNode.cleanupStagingPath(spark, delta.stagingPath, failure))
        throw failure
    }

  private def position(log: DeltaLog): Option[SourceLedgerEntry] = {
    val snapshot = log.update()
    if (snapshot.version < 0L) None else Some(SourceLedgerEntry(snapshot.metadata.id, snapshot.version))
  }

  private def sameSchema(spark: SparkSession, baseTable: String, from: Long, to: Long): Boolean =
    try snapshot(spark, baseTable, from).schema.toDDL == snapshot(spark, baseTable, to).schema.toDDL
    catch { case NonFatal(_) => false }

  private def changesData(action: Action): Boolean =
    action match {
      case add: AddFile       => add.dataChange
      case remove: RemoveFile => remove.dataChange
      case _: AddCDCFile      => true
      case _                  => false
    }

  /** The data files of an interval that only appended whole files, or `None`. Files rewritten
    * without a data change (compaction) are excluded; their rows were already present.
    */
  private def appendedFiles(actions: Seq[Action]): Option[Seq[AddFile]] =
    if (
      actions.forall {
        case add: AddFile       => !add.dataChange || add.deletionVector == null
        case remove: RemoveFile => !remove.dataChange
        case _: AddCDCFile      => false
        case _                  => true
      }
    ) Some(actions.collect { case add: AddFile if add.dataChange => add })
    else None

  private def hasDependents(spark: SparkSession, baseTable: String): Boolean =
    MvCatalog.list(spark).exists(_.sourceTables.exists(_.equalsIgnoreCase(baseTable)))

  private def shortName(source: String): String =
    lower(source.split("\\.").last.stripPrefix("`").stripSuffix("`"))

  private def quote(table: String): String =
    table.split("\\.").map(part => s"`${part.replace("`", "``")}`").mkString(".")

  private def lower(value: String): String = value.toLowerCase(Locale.ROOT)
}
