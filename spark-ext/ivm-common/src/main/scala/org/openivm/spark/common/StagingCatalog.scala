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

  private val IndexDbColumnFamilies = org.openivm.spark.common.IndexDbColumnFamilies.All
  private val MvDbColumnFamilies    = Seq("meta", "properties", "consumed")
  private val BaseDbColumnFamilies  = Seq("staging")

  private val MvIndexCf    = "mv_index"
  private val TableIndexCf = "table_index"
  private val StagingCf    = "staging"
  private val ConsumedCf   = "consumed"

  private def warehouseDir(spark: SparkSession): String =
    RocksDBCodec.requireLocalPath(FeatureGate.stateWarehouse(spark).stripSuffix("/"))

  private def indexDbPath(spark: SparkSession): String =
    Paths.get(warehouseDir(spark), "_openivm", "index", "rocksdb").toString

  private def baseTableDbPath(spark: SparkSession, baseTable: String): String =
    Paths
      .get(warehouseDir(spark), "_openivm", "tables", RocksDBCodec.safePathSegment(baseTable), "rocksdb")
      .toString

  private def openIndexDb(spark: SparkSession): OpenIvmRocksDB = {
    // Request ALL column families hosted by the shared index DB; the registry rejects widening
    // the CF set on a later reopen of the same path.
    OpenIvmRocksDBRegistry.getOrOpen(spark, indexDbPath(spark), IndexDbColumnFamilies)
  }

  private def openBaseTableDb(spark: SparkSession, baseTable: String): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(spark, baseTableDbPath(spark, baseTable), BaseDbColumnFamilies)

  private def openTrackedMvDb(
      spark: SparkSession,
      indexDb: OpenIvmRocksDB,
      viewName: String
  ): Option[OpenIvmRocksDB] =
    indexDb.get(MvIndexCf, RocksDBCodec.utf8(viewName)).map { pathBytes =>
      val mvDbPath = RocksDBCodec.requireLocalPath(RocksDBCodec.fromUtf8(pathBytes))
      // Request ALL column families hosted by the per-MV DB even though StagingCatalog only
      // touches `consumed`; the registry enforces subset-safe reopen semantics per path.
      OpenIvmRocksDBRegistry.getOrOpen(spark, mvDbPath, MvDbColumnFamilies)
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

  private def stagingPathStillTracked(spark: SparkSession, indexDb: OpenIvmRocksDB, stagingPath: String): Boolean =
    indexDb.prefixScan(TableIndexCf, Array.emptyByteArray).exists { case (_, pathBytes) =>
      val dbPath = RocksDBCodec.requireLocalPath(RocksDBCodec.fromUtf8(pathBytes))
      Files.exists(Paths.get(dbPath)) && {
        val baseDb = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, BaseDbColumnFamilies)
        baseDb.prefixScan(StagingCf, Array.emptyByteArray).exists { case (key, _) =>
          decodeStagingKey(key)._2 == stagingPath
        }
      }
    }

  def ensureTables(spark: SparkSession): Unit = {
    openIndexDb(spark)
    ()
  }

  def record(spark: SparkSession, delta: StagingDelta): Unit = {
    val baseDbPath = baseTableDbPath(spark, delta.baseTable)
    val baseDb     = openBaseTableDb(spark, delta.baseTable)
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

    val indexDb = openIndexDb(spark)
    if (indexDb.get(TableIndexCf, RocksDBCodec.utf8(delta.baseTable)).isEmpty) {
      indexDb.withBatch { batch =>
        OpenIvmRocksDBBatchOps.put(
          indexDb,
          batch,
          TableIndexCf,
          RocksDBCodec.utf8(delta.baseTable),
          RocksDBCodec.utf8(baseDbPath)
        )
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

    val indexDb   = openIndexDb(spark)
    val maybeMvDb = openTrackedMvDb(spark, indexDb, viewName)
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

    val indexDb = openIndexDb(spark)
    openTrackedMvDb(spark, indexDb, viewName).foreach { mvDb =>
      mvDb.withBatch { batch =>
        paths.distinct.foreach { path =>
          OpenIvmRocksDBBatchOps.put(mvDb, batch, ConsumedCf, RocksDBCodec.utf8(path), Array.emptyByteArray)
        }
      }
    }
    // Corner case: if `mv_index` does not yet contain `viewName`, we intentionally skip the mark.
    // There is no back-fill mechanism today; the owning MvCatalog rewrite writes that index entry.
  }

  def pruneFullyConsumed(spark: SparkSession, viewsByTable: Map[String, Seq[String]]): Unit = {
    if (viewsByTable.isEmpty) return

    val indexDb   = openIndexDb(spark)
    val mvDbCache = mutable.HashMap.empty[String, Option[OpenIvmRocksDB]]

    def trackedMvDb(viewName: String): Option[OpenIvmRocksDB] =
      mvDbCache.getOrElseUpdate(viewName, openTrackedMvDb(spark, indexDb, viewName))

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
            .filterNot { case (stagingPath, _) => stagingPathStillTracked(spark, indexDb, stagingPath) }
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
    val indexDb = openIndexDb(spark)
    indexDb.get(TableIndexCf, RocksDBCodec.utf8(baseTable)).foreach { pathBytes =>
      val dbPath = RocksDBCodec.requireLocalPath(RocksDBCodec.fromUtf8(pathBytes))
      OpenIvmRocksDBRegistry.close(dbPath)
      deleteRecursively(Paths.get(dbPath))
      indexDb.withBatch { batch =>
        OpenIvmRocksDBBatchOps.delete(indexDb, batch, TableIndexCf, RocksDBCodec.utf8(baseTable))
      }
    }
  }
}
