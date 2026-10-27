package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project, SubqueryAlias, Union, View}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSourceOffset
import org.apache.spark.sql.delta.util.JsonUtils
import org.openivm.spark.common.DeltaTableVersion
import org.openivm.spark.insights.OpenIvmInsightsContract

import scala.util.control.NonFatal

private[streaming] final case class CommittedReplayBatch(
    batchId: Long,
    previous: Seq[Option[String]],
    current: Seq[Option[String]],
    advance: () => Unit
)

/** Only the discarded V1 last-committed-batch replay is bypassed, never unread source data. */
private[streaming] object StreamingCommittedReplayRepair extends Logging {
  private val CommitFile = "([0-9]{20})\\.json".r

  // Caller must hold the table lifecycle admission locks and have stopped any old writer.
  def beforeStart(
      spark: SparkSession,
      frame: DataFrame,
      target: StreamingTableTarget,
      operationId: Option[String] = None
  ): Unit = {
    if (!StreamingLifecycleSettings.fromSpark(spark).repairExpiredCommittedBatchReplay) return
    val checkpoint = new Path(target.checkpointLocation)
    val conf       = spark.sessionState.newHadoopConf()
    val fs         = checkpoint.getFileSystem(conf)
    if (!fs.exists(checkpoint) || fs.exists(new Path(checkpoint, "state"))) return

    // Spark inlines CTEs before collecting streaming sources; inspect the same expanded structure.
    val plan = InlineCTE(alwaysInline = true)(frame.queryExecution.analyzed)
    // An allow-list is deliberately narrower than Spark's evolving set of stateful operators.
    // Unknown operators, static joins, aggregates, watermarks and deduplication are not repaired.
    if (
      plan.exists { node =>
        node match {
          case _: Project | _: Filter | _: Union | _: SubqueryAlias | _: View => false
          case other =>
            !StreamingPlanCompatibility.extract(other).exists {
              case source: CompatibleV1StreamingRelation => source.dataSource.className.equalsIgnoreCase("delta")
              case _                                     => false
            }
        }
      }
    ) return

    val batch = StreamingReplayCheckpointLog
      .latestCommitted(spark, target.checkpointLocation)
      .getOrElse(
        return
      )
    val relations = plan.collect {
      case node if StreamingPlanCompatibility.extract(node).exists(_.isInstanceOf[CompatibleV1StreamingRelation]) =>
        node
    }.distinct
    if (batch.previous.size != batch.current.size || batch.current.size != relations.size) return

    val logs = relations.flatMap { node =>
      StreamingPlanCompatibility
        .extract(node)
        .collect { case source: CompatibleV1StreamingRelation =>
          source.dataSource.catalogTable
            .flatMap(table => DeltaTableVersion.deltaLogOption(spark, table.identifier.quotedString))
            .orElse(
              source.dataSource.paths.headOption
                .orElse(source.dataSource.options.get("path"))
                .map(path => DeltaLog.forTable(spark, new Path(path)))
            )
        }
        .flatten
        .toSeq
    }
    if (logs.size != relations.size) return
    val identifiedLogs = logs.map(log => log.update().metadata.id -> log)
    val byId           = identifiedLogs.toMap
    val parsed = batch.previous.zip(batch.current).map { case (start, end) =>
      for {
        startJson <- start
        endJson   <- end
        previous  <- deltaOffset(startJson)
        current   <- deltaOffset(endJson)
        if previous.reservoirId == current.reservoirId && byId.contains(current.reservoirId)
        if !previous.isInitialSnapshot && !current.isInitialSnapshot
        if previous.compareTo(current) <= 0
      } yield (previous, current)
    }
    if (parsed.exists(_.isEmpty)) return
    val ranges = parsed.flatten
    if (ranges.map(_._2.reservoirId).sorted != identifiedLogs.map(_._1).sorted) return

    val earliest = byId.map { case (id, log) =>
      val version = log.store
        .listFrom(new Path(log.logPath, "00000000000000000000.json"), conf)
        .collectFirst {
          case file if CommitFile.pattern.matcher(file.getPath.getName).matches() =>
            file.getPath.getName.take(20).toLong
        }
      id -> version
    }
    val details = ranges
      .map { case (start, end) =>
        s"${end.reservoirId}: ${start.reservoirVersion} -> ${end.reservoirVersion} " +
          s"(earliest retained=${earliest(end.reservoirId)})"
      }
      .mkString(", ")
    if (ranges.exists { case (_, end) => earliest(end.reservoirId).forall(_ > end.reservoirVersion) })
      StreamingTableErrors.invalid(
        s"Cannot resume streaming table ${target.sqlIdentifier}: committed END offset is outside " +
          s"Delta log retention; unread data may have been lost. Rebuild the table with a full refresh. $details"
      )
    if (
      ranges.exists { case (start, end) =>
        start != end && earliest(start.reservoirId).exists(_ > start.reservoirVersion)
      }
    ) {
      batch.advance()
      operationId.foreach { id =>
        StreamingInsightEvents.action(
          spark,
          id,
          target.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = target.identity,
          causedBy = None,
          stage = "checkpoint",
          code = OpenIvmInsightsContract.Code.StreamingCommittedReplayRepaired,
          message = "Advanced the expired, fully committed Delta replay horizon without changing source positions.",
          level = OpenIvmInsightsContract.Level.Warn,
          details = Seq(
            "committed_batch_id" -> batch.batchId,
            "replay_batch_id"    -> (batch.batchId + 1L),
            "source_versions"    -> details
          )
        )
      }
      logWarning(
        s"Repaired expired committed-batch replay for ${target.sqlIdentifier}: " +
          s"batch ${batch.batchId} -> ${batch.batchId + 1L}; $details"
      )
    }
  }

  private def deltaOffset(json: String): Option[DeltaSourceOffset] =
    try Some(JsonUtils.fromJson[DeltaSourceOffset](json))
    catch { case NonFatal(_) => None }
}
