package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * [[ChangePropagation]] implementation that delegates to the existing
 * [[StagingCatalog]] / [[StagingDeltaView]] machinery written by the DML
 * interceptor.  All semantics of the pre-trait code path are preserved
 * verbatim — this class is a pure forwarding shim.
 */
final class InterceptChangePropagation extends ChangePropagation {

  override def mode: ChangeFeedMode = ChangeFeedMode.Intercept

  override val requiresDmlInterception: Boolean = true
  override val requiresMvCdf: Boolean           = false

  override def validateSources(spark: SparkSession, sources: Seq[String]): Unit = ()

  override def currentWatermarks(spark: SparkSession, sources: Seq[String]): Map[String, ChangeWatermark] =
    StagingCatalog.currentWatermarks(spark, sources).map { case (src, ts) =>
      src -> ChangeWatermark.TxnTs(ts)
    }

  override def collectChanges(
      spark: SparkSession,
      viewName: String,
      sources: Seq[String],
      persisted: Map[String, ChangeWatermark]
  ): Seq[ChangeBatch] = {
    val deltas = StagingCatalog.collectFor(spark, viewName, sources, toTsMap(persisted))
    deltas
      .groupBy(_.baseTable)
      .toSeq
      .map { case (baseTable, tableDeltas) =>
        val maxTs = tableDeltas.map(_.txnTs.getTime).max
        StagingChangeBatch(
          baseTable = baseTable,
          deltas = tableDeltas,
          endWatermark = ChangeWatermark.TxnTs(new java.sql.Timestamp(maxTs))
        )
      }
  }

  override def buildSourceDeltaViewSql(
      sourceTable: String,
      sourceSchema: StructType,
      batches: Seq[ChangeBatch]
  ): String = {
    val deltas = batches.flatMap {
      case StagingChangeBatch(_, ds, _) => ds
      case other =>
        throw new IllegalStateException(
          s"InterceptChangePropagation cannot render batch of type ${other.getClass.getName}"
        )
    }
    StagingDeltaView.buildSourceDeltaViewSql(sourceTable, sourceSchema, deltas)
  }

  override def markConsumed(spark: SparkSession, viewName: String, batches: Seq[ChangeBatch]): Unit = {
    val paths = batches.flatMap {
      case StagingChangeBatch(_, ds, _) => ds.map(_.stagingPath)
      case _                            => Seq.empty
    }
    StagingCatalog.markConsumed(spark, viewName, paths)
  }

  override def pruneConsumed(spark: SparkSession, viewsByTable: Map[String, Seq[String]]): Unit =
    StagingCatalog.pruneFullyConsumed(spark, viewsByTable)

  override def removeForBaseTable(spark: SparkSession, baseTable: String): Unit =
    StagingCatalog.removeForBaseTable(spark, baseTable)

  private def toTsMap(persisted: Map[String, ChangeWatermark]): Map[String, java.sql.Timestamp] =
    persisted.collect { case (src, ChangeWatermark.TxnTs(ts)) =>
      src -> ts
    }
}
