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

/** RocksDB-backed catalog for DML staging records. */
object StagingCatalog {

  private val MvDbColumnFamilies   = OpenIvmStatePaths.PerMvColumnFamilies
  private val BaseDbColumnFamilies = OpenIvmStatePaths.BaseTableColumnFamilies

  private val StagingCf  = "staging"
  private val ConsumedCf = "consumed"

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

  def record(spark: SparkSession, delta: StagingDelta): Unit = {
    val baseDb = openBaseTableDb(spark, delta.baseTable)
    // `consumedBy` is intentionally ignored on write: consumed state now lives in each MV's
    // dedicated RocksDB under the `consumed` column family.
    val stagingKey = RocksDBCodec.compositeKey(
      Seq(RocksDBCodec.encodeLongBE(delta.txnTs.getTime), RocksDBCodec.utf8(delta.stagingPath))
    )
    val stagingValue = RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(delta.opType)))

    // `IvmDmlInterceptorRule.stagingPath` already uses millisecond-resolution timestamps, so a
    // same-millisecond same-table same-op collision is a pre-existing path-generation bug that is
    // intentionally left to follow-up work.
    baseDb.withBatch { batch =>
      OpenIvmRocksDBBatchOps.put(baseDb, batch, StagingCf, stagingKey, stagingValue)
    }
  }

  def hasPendingDeltas(
      spark: SparkSession,
      viewName: String,
      sources: Seq[String],
      watermarks: Map[String, Timestamp] = Map.empty
  ): Boolean = {
    if (sources.isEmpty) return false

    val maybeMvDb = openTrackedMvDb(spark, viewName)
    sources.distinct.exists { source =>
      val dbPath = baseTableDbPath(spark, source)
      Files.exists(Paths.get(dbPath)) && {
        val baseDb = openBaseTableDb(spark, source)
        baseDb.prefixScan(StagingCf, Array.emptyByteArray).exists { case (key, _) =>
          val (txnTsMillis, stagingPath) = decodeStagingKey(key)
          val watermarkPassed            = watermarks.get(source).forall(wm => txnTsMillis > wm.getTime)
          val alreadyConsumed            = maybeMvDb.exists(_.get(ConsumedCf, RocksDBCodec.utf8(stagingPath)).isDefined)
          watermarkPassed && !alreadyConsumed
        }
      }
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

        if (toDelete.nonEmpty) {
          baseDb.withBatch { batch =>
            toDelete.foreach { case (key, _) => OpenIvmRocksDBBatchOps.delete(baseDb, batch, StagingCf, key) }
          }
          toDelete
            .map { case (_, stagingPath) => stagingPath }
            .flatMap(p => StagingDeltaView.CachedViewDeltaRef.decode(p).map(p -> _))
            .filterNot { case (stagingPath, _) => stagingPathStillTracked(spark, stagingPath) }
            .foreach { case (_, globalView) =>
              try spark.catalog.uncacheTable(s"global_temp.$globalView")
              catch { case _: Throwable => () }
              try spark.catalog.dropGlobalTempView(globalView)
              catch { case _: Throwable => () }
            }
        }
      }
    }
  }

  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit = {
    val dbPath = baseTableDbPath(spark, baseTable)
    if (OpenIvmStatePaths.isExistingDb(dbPath)) {
      OpenIvmRocksDBRegistry.close(dbPath)
      deleteRecursively(Paths.get(dbPath))
    }
  }
}
