package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDB, OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}
import java.sql.Timestamp
import scala.collection.mutable

/**
 * A single DML delta written by the DML interceptor for one base table.
 *
 * @param baseTable   qualified name of the base table being modified
 * @param opType      operation type. See [[StagingDelta.OpTypes]] for the
 *                    formal set + multiplicity-handling contract.
 * @param stagingPath path of the Delta table holding the staged rows
 * @param txnTs       wall-clock timestamp of the originating DML transaction
 * @param consumedBy  MV names that have already applied this delta (idempotency guard)
 */
final case class StagingDelta(
    baseTable: String,
    opType: String,
    stagingPath: String,
    txnTs: Timestamp,
    consumedBy: Seq[String]
)

/** Formal set of supported `opType` values + their multiplicity-handling
  * contract.
  *
  * `StagingDeltaView.buildSourceDeltaViewSql` decides how to assemble the
  * `openivm_delta_<source>` temp view per opType:
  *
  *  - `INSERT`, `OVERWRITE`, `UPDATE_AFTER` — synthesise
  *    `openivm_multiplicity = +1` for every row at the staging path.
  *  - `DELETE`, `UPDATE_BEFORE` — synthesise `openivm_multiplicity = -1`.
  *  - `MERGE_SRC` — currently dropped (returns None from the multiplicity
  *    helper); rows fall through to the empty-view fallback.
  *  - `MV_VIEW_DELTA` — **preserves the existing multiplicity column at the
  *    staging path** rather than synthesising one. Used by the MV-over-MV
  *    cascade: an upstream MV's incremental refresh writes a view-delta
  *    Delta table with `openivm_multiplicity` + `openivm_timestamp` columns
  *    already populated; the downstream's refresh consumes it as-is.
  *
  * Invariants:
  *  - Any code that introduces a new opType MUST also extend
  *    `StagingDeltaView.buildSourceDeltaViewSql` and add a docstring entry
  *    here.
  *  - Never treat `MV_VIEW_DELTA` like an `INSERT`/`OVERWRITE` — doing so
  *    would overwrite the upstream's signed multiplicities to +1 and silently
  *    corrupt downstream aggregates.
  */
object StagingDelta {
  object OpTypes {
    val Insert       = "INSERT"
    val Delete       = "DELETE"
    val UpdateBefore = "UPDATE_BEFORE"
    val UpdateAfter  = "UPDATE_AFTER"
    val MergeSrc     = "MERGE_SRC"
    val Overwrite    = "OVERWRITE"

    /** Marker for an upstream MV's persisted view-delta. The staging path
      * IS a Delta table whose columns are `<userCols> + openivm_timestamp +
      * openivm_multiplicity`. Downstream's `openivm_delta_<src>` temp view
      * preserves these columns verbatim.
      */
    val MvViewDelta = "MV_VIEW_DELTA"
  }
}

/** Committed Delta version of a base table up to which staging records describe every change.
  *
  * @param tableId Delta table identity (`metaData.id`) the version belongs to
  * @param version committed Delta version
  */
final case class SourceLedgerEntry(tableId: String, version: Long)

/** RocksDB-backed catalog for DML staging records. */
object StagingCatalog {

  private val MvDbColumnFamilies   = OpenIvmStatePaths.PerMvColumnFamilies
  private val BaseDbColumnFamilies = OpenIvmStatePaths.BaseTableColumnFamilies

  private val StagingCf  = "staging"
  private val ConsumedCf = "consumed"
  private val LedgerCf   = "ledger"
  private val GcCf       = "gc"
  private val LedgerKey  = RocksDBCodec.utf8("source")

  private def baseTableDbPath(spark: SparkSession, baseTable: String): String =
    OpenIvmStatePaths.baseTableDbPath(spark, baseTable)

  private def openBaseTableDb(spark: SparkSession, baseTable: String): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(spark, baseTableDbPath(spark, baseTable), BaseDbColumnFamilies)

  private def openTrackedMvDb(spark: SparkSession, viewName: String): Option[OpenIvmRocksDB] = {
    val path = OpenIvmStatePaths.perMvDbPath(spark, viewName)
    if (OpenIvmStatePaths.isExistingDb(path)) {
      Some(OpenIvmRocksDBRegistry.getOrOpen(spark, path, MvDbColumnFamilies))
    } else None
  }

  private def decodeStagingKey(key: Array[Byte]): (Long, String) = {
    val parts = RocksDBCodec.splitComposite(key, 2)
    require(parts.length == 2, s"Expected 2-part staging key, found ${parts.length} part(s).")
    RocksDBCodec.decodeLongBE(parts.head) -> RocksDBCodec.fromUtf8(parts(1))
  }

  private def decodeOpType(value: Array[Byte]): String = {
    val parts = RocksDBCodec.splitComposite(value, 2)
    require(parts.nonEmpty, "Expected at least one part in staging value.")
    RocksDBCodec.fromUtf8(parts.head)
  }

  private def deleteRecursively(path: Path): Unit =
    if (Files.exists(path)) {
      Files.walkFileTree(
        path,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.deleteIfExists(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
            if (exc != null) throw exc
            Files.deleteIfExists(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
    }

  private def stagingPathStillTracked(spark: SparkSession, stagingPath: String): Boolean =
    OpenIvmStatePaths.existingBaseTableDbPaths(spark).exists { dbPath =>
      val baseDb = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, BaseDbColumnFamilies)
      baseDb.prefixScan(StagingCf, Array.emptyByteArray).exists { case (key, _) =>
        decodeStagingKey(key)._2 == stagingPath
      }
    }

  def ensureTables(spark: SparkSession): Unit = {
    Files.createDirectories(OpenIvmStatePaths.tablesRoot(spark))
    ()
  }

  // `consumedBy` is intentionally not persisted: consumed state lives in each MV's dedicated
  // RocksDB under the `consumed` column family.
  private def stagingKey(delta: StagingDelta): Array[Byte] =
    RocksDBCodec.compositeKey(
      Seq(RocksDBCodec.encodeLongBE(delta.txnTs.getTime), RocksDBCodec.utf8(delta.stagingPath))
    )

  private def stagingValue(delta: StagingDelta): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(delta.opType)))

  def record(spark: SparkSession, delta: StagingDelta): Unit = {
    val baseDb = openBaseTableDb(spark, delta.baseTable)
    baseDb.withBatch { batch =>
      OpenIvmRocksDBBatchOps.put(baseDb, batch, StagingCf, stagingKey(delta), stagingValue(delta))
    }
  }

  /** Ledger position of `baseTable`, if one was ever published. */
  def ledger(spark: SparkSession, baseTable: String): Option[SourceLedgerEntry] = {
    val dbPath = baseTableDbPath(spark, baseTable)
    if (!OpenIvmStatePaths.isExistingDb(dbPath)) None
    else
      openBaseTableDb(spark, baseTable).get(LedgerCf, LedgerKey).map { value =>
        val parts = RocksDBCodec.splitComposite(value, 2)
        require(parts.length == 2, s"Expected 2-part ledger value for $baseTable, found ${parts.length} part(s).")
        SourceLedgerEntry(RocksDBCodec.fromUtf8(parts.head), RocksDBCodec.decodeLongBE(parts(1)))
      }
  }

  private[openivm] def clearLedgerForTesting(spark: SparkSession, baseTable: String): Unit = {
    val baseDb = openBaseTableDb(spark, baseTable)
    baseDb.withBatch(batch => OpenIvmRocksDBBatchOps.delete(baseDb, batch, LedgerCf, LedgerKey))
  }

  /** Atomically publish the records describing changes of `baseTable` up to `ledger` and advance
    * its ledger to that position.
    *
    * @param spark     active session
    * @param baseTable qualified name of the changed base table
    * @param deltas    records for every change after the previous ledger position
    * @param ledger    committed position the records bring the table to
    */
  def publish(spark: SparkSession, baseTable: String, deltas: Seq[StagingDelta], ledger: SourceLedgerEntry): Unit = {
    val baseDb = openBaseTableDb(spark, baseTable)
    val value =
      RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(ledger.tableId), RocksDBCodec.encodeLongBE(ledger.version)))
    baseDb.withBatch { batch =>
      deltas.foreach { delta =>
        OpenIvmRocksDBBatchOps.put(baseDb, batch, StagingCf, stagingKey(delta), stagingValue(delta))
      }
      OpenIvmRocksDBBatchOps.put(baseDb, batch, LedgerCf, LedgerKey, value)
    }
  }

  def collectFor(
      spark: SparkSession,
      viewName: String,
      sources: Seq[String],
      watermarks: Map[String, Timestamp] = Map.empty
  ): Seq[StagingDelta] = {
    if (sources.isEmpty) return Seq.empty

    val maybeMvDb = openTrackedMvDb(spark, viewName)
    val deltas = sources.distinct.iterator.flatMap { source =>
      val dbPath = baseTableDbPath(spark, source)
      if (!Files.exists(Paths.get(dbPath))) {
        Iterator.empty
      } else {
        val baseDb = openBaseTableDb(spark, source)
        baseDb.prefixScan(StagingCf, Array.emptyByteArray).flatMap { case (key, value) =>
          val (txnTsMillis, stagingPath) = decodeStagingKey(key)
          val watermarkPassed            = watermarks.get(source).forall(wm => txnTsMillis > wm.getTime)
          val alreadyConsumed            = maybeMvDb.exists(_.get(ConsumedCf, RocksDBCodec.utf8(stagingPath)).isDefined)
          if (watermarkPassed && !alreadyConsumed) {
            Iterator.single(
              StagingDelta(
                baseTable = source,
                opType = decodeOpType(value),
                stagingPath = stagingPath,
                txnTs = new Timestamp(txnTsMillis),
                // The RocksDB layout tracks consumption per MV, not per staging row. Verified
                // callers only consult `collectFor` for presence/path/op/timestamp, so the
                // informational `consumedBy` field stays empty on read-back.
                consumedBy = Seq.empty
              )
            )
          } else {
            Iterator.empty
          }
        }
      }
    }.toVector

    deltas.sortBy(_.txnTs.getTime)
  }

  def currentWatermarks(spark: SparkSession, sources: Seq[String]): Map[String, Timestamp] = {
    if (sources.isEmpty) return Map.empty

    sources.distinct.iterator.flatMap { source =>
      val dbPath = baseTableDbPath(spark, source)
      if (!Files.exists(Paths.get(dbPath))) {
        None
      } else {
        val baseDb = openBaseTableDb(spark, source)
        var found  = false
        var maxTs  = 0L
        baseDb.prefixScan(StagingCf, Array.emptyByteArray).foreach { case (key, _) =>
          found = true
          maxTs = math.max(maxTs, decodeStagingKey(key)._1)
        }
        if (found) Some(source -> new Timestamp(maxTs)) else None
      }
    }.toMap
  }

  def markConsumed(spark: SparkSession, viewName: String, paths: Seq[String]): Unit = {
    if (paths.isEmpty) return

    openTrackedMvDb(spark, viewName).foreach { mvDb =>
      mvDb.withBatch { batch =>
        paths.distinct.foreach { path =>
          OpenIvmRocksDBBatchOps.put(mvDb, batch, ConsumedCf, RocksDBCodec.utf8(path), Array.emptyByteArray)
        }
      }
    }
  }

  def pruneFullyConsumed(spark: SparkSession, viewsByTable: Map[String, Seq[String]]): Unit = {
    if (viewsByTable.isEmpty) return

    val mvDbCache = mutable.HashMap.empty[String, Option[OpenIvmRocksDB]]

    def trackedMvDb(viewName: String): Option[OpenIvmRocksDB] =
      mvDbCache.getOrElseUpdate(viewName, openTrackedMvDb(spark, viewName))

    viewsByTable.foreach { case (baseTable, rawMvs) =>
      val mvs    = rawMvs.distinct
      val dbPath = baseTableDbPath(spark, baseTable)
      if (mvs.nonEmpty && Files.exists(Paths.get(dbPath))) {
        val baseDb = openBaseTableDb(spark, baseTable)
        val toDelete = baseDb
          .prefixScan(StagingCf, Array.emptyByteArray)
          .flatMap { case (key, _) =>
            val (_, stagingPath) = decodeStagingKey(key)
            val consumedByAll = mvs.forall { mvName =>
              trackedMvDb(mvName).exists(_.get(ConsumedCf, RocksDBCodec.utf8(stagingPath)).isDefined)
            }
            if (consumedByAll) Iterator.single(key.clone() -> stagingPath) else Iterator.empty
          }
          .toList

        deleteRecords(spark, baseDb, toDelete)
      }
    }
  }

  /** Prune every record of `baseTable`, a key no materialized view reads, such as the view-delta
    * records a view without consumers leaves under its own name. A view created later starts
    * after them, so none is ever consumed.
    *
    * @param spark     active session
    * @param baseTable staging key without dependents
    */
  def pruneUnconsumed(spark: SparkSession, baseTable: String): Unit = {
    val dbPath = baseTableDbPath(spark, baseTable)
    if (OpenIvmStatePaths.isExistingDb(dbPath)) {
      val baseDb = openBaseTableDb(spark, baseTable)
      deleteRecords(
        spark,
        baseDb,
        baseDb
          .prefixScan(StagingCf, Array.emptyByteArray)
          .map { case (key, _) => key.clone() -> decodeStagingKey(key)._2 }
          .toList
      )
    }
  }

  /** Delete `records` and tombstone their locations in one write. Locations stay on disk until a
    * published state no longer references them; see [[pendingGarbage]].
    */
  private def deleteRecords(spark: SparkSession, baseDb: OpenIvmRocksDB, records: List[(Array[Byte], String)]): Unit =
    if (records.nonEmpty) {
      baseDb.withBatch { batch =>
        records.foreach { case (key, stagingPath) =>
          OpenIvmRocksDBBatchOps.delete(baseDb, batch, StagingCf, key)
          if (StagingDeltaView.CachedViewDeltaRef.decode(stagingPath).isEmpty)
            OpenIvmRocksDBBatchOps.put(baseDb, batch, GcCf, RocksDBCodec.utf8(stagingPath), Array.emptyByteArray)
        }
      }
      records
        .map { case (_, stagingPath) => stagingPath }
        .flatMap(p => StagingDeltaView.CachedViewDeltaRef.decode(p).map(p -> _))
        .filterNot { case (stagingPath, _) => stagingPathStillTracked(spark, stagingPath) }
        .foreach { case (_, globalView) =>
          try spark.catalog.uncacheTable(s"global_temp.$globalView")
          catch { case _: Throwable => () }
          try spark.catalog.dropGlobalTempView(globalView)
          catch { case _: Throwable => () }
          StagingDeltaView.CachedViewDeltaRef.forgetInputs(globalView)
        }
    }

  /** Staging locations whose records were pruned and that await deletion. A location may only be
    * deleted once a durable state that no longer references it exists: with state sync enabled,
    * after the next published snapshot; otherwise immediately, since local RocksDB is durable.
    */
  def pendingGarbage(spark: SparkSession): Set[String] =
    OpenIvmStatePaths
      .existingBaseTableDbPaths(spark)
      .iterator
      .flatMap { dbPath =>
        val baseDb = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, BaseDbColumnFamilies)
        baseDb.prefixScan(GcCf, Array.emptyByteArray).map { case (key, _) => RocksDBCodec.fromUtf8(key) }.toVector
      }
      .toSet

  /** Staging locations referenced by any published staging record. */
  def referencedStagingPaths(spark: SparkSession): Set[String] =
    OpenIvmStatePaths
      .existingBaseTableDbPaths(spark)
      .iterator
      .flatMap { dbPath =>
        val baseDb = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, BaseDbColumnFamilies)
        baseDb.prefixScan(StagingCf, Array.emptyByteArray).map { case (key, _) => decodeStagingKey(key)._2 }.toVector
      }
      .toSet

  /** Drop the tombstones of deleted staging locations.
    *
    * @param spark active session
    * @param paths staging locations no record references any more and that no longer exist
    */
  def forgetGarbage(spark: SparkSession, paths: Set[String]): Unit = {
    if (paths.isEmpty) return
    OpenIvmStatePaths.existingBaseTableDbPaths(spark).foreach { dbPath =>
      val baseDb = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, BaseDbColumnFamilies)
      val victims = baseDb
        .prefixScan(GcCf, Array.emptyByteArray)
        .collect { case (key, _) if paths.contains(RocksDBCodec.fromUtf8(key)) => key.clone() }
        .toList
      if (victims.nonEmpty)
        baseDb.withBatch(batch => victims.foreach(key => OpenIvmRocksDBBatchOps.delete(baseDb, batch, GcCf, key)))
    }
  }

  /** Drop `viewName`'s consumption markers for staging locations no record of its `sources`
    * references any more, so markers do not outlive the records they guard. Hold the view's
    * refresh lock.
    *
    * @param spark    active session
    * @param viewName serialized materialized-view name
    * @param sources  the view's qualified source names
    */
  def forgetConsumed(spark: SparkSession, viewName: String, sources: Seq[String]): Unit =
    openTrackedMvDb(spark, viewName).foreach { mvDb =>
      val referenced = sources.distinct.iterator.flatMap { source =>
        val dbPath = baseTableDbPath(spark, source)
        if (!OpenIvmStatePaths.isExistingDb(dbPath)) Iterator.empty
        else
          openBaseTableDb(spark, source)
            .prefixScan(StagingCf, Array.emptyByteArray)
            .map { case (key, _) => decodeStagingKey(key)._2 }
            .toVector
      }.toSet
      val stale = mvDb
        .prefixScan(ConsumedCf, Array.emptyByteArray)
        .collect { case (key, _) if !referenced.contains(RocksDBCodec.fromUtf8(key)) => key.clone() }
        .toList
      if (stale.nonEmpty)
        mvDb.withBatch(batch => stale.foreach(key => OpenIvmRocksDBBatchOps.delete(mvDb, batch, ConsumedCf, key)))
    }

  /** Remove every staging and per-MV consumption record for one exact staging
    * path. Used by source-version advancement rollback before the operation is
    * published as committed metadata.
    */
  def removeStagingPathEverywhere(spark: SparkSession, stagingPath: String): Unit = {
    OpenIvmStatePaths.existingBaseTableDbPaths(spark).foreach { dbPath =>
      val baseDb = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, BaseDbColumnFamilies)
      val victims = baseDb
        .prefixScan(StagingCf, Array.emptyByteArray)
        .flatMap { case (key, _) =>
          if (decodeStagingKey(key)._2 == stagingPath) Iterator.single(key.clone()) else Iterator.empty
        }
        .toList
      if (victims.nonEmpty)
        baseDb.withBatch { batch =>
          victims.foreach(key => OpenIvmRocksDBBatchOps.delete(baseDb, batch, StagingCf, key))
        }
    }

    val consumedKey = RocksDBCodec.utf8(stagingPath)
    OpenIvmStatePaths.existingMvDbPaths(spark).foreach { dbPath =>
      val mvDb = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, MvDbColumnFamilies)
      if (mvDb.get(ConsumedCf, consumedKey).nonEmpty)
        mvDb.withBatch(batch => OpenIvmRocksDBBatchOps.delete(mvDb, batch, ConsumedCf, consumedKey))
    }

    StagingDeltaView.CachedViewDeltaRef.decode(stagingPath).foreach { globalView =>
      try spark.catalog.uncacheTable(s"global_temp.$globalView")
      catch { case _: Throwable => () }
      try spark.catalog.dropGlobalTempView(globalView)
      catch { case _: Throwable => () }
      StagingDeltaView.CachedViewDeltaRef.forgetInputs(globalView)
    }
  }

  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit = {
    val dbPath = baseTableDbPath(spark, baseTable)
    if (OpenIvmStatePaths.isExistingDb(dbPath)) {
      // Close and filesystem removal form one atomic lifecycle step; see
      // OpenIvmRocksDBRegistry.closeAndDelete for the per-path exclusion contract.
      OpenIvmRocksDBRegistry.closeAndDelete(dbPath)(canonical => deleteRecursively(Paths.get(canonical)))
    }
  }
}
