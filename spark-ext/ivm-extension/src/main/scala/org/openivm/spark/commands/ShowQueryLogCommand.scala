package org.openivm.spark.commands

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._
import org.openivm.spark.common.{RefreshSqlLogAsyncFlusher, RefreshSqlLogCatalog, RefreshSqlLogRow}

/** SQL surface for the per-statement query log produced by
  * [[RefreshSqlLog]] / persisted by [[RefreshSqlLogCatalog]].
  *
  * Before scanning the catalog this command blocks (up to 60 s) on
  * [[RefreshSqlLogAsyncFlusher.awaitQuiescence]] so any in-flight
  * end-of-refresh batches are drained. This barrier runs OUTSIDE the bench
  * timer (the ivm-bench `_export_spark_openivm_query_log` is called after
  * `stream_progress` returns, with `batch.duration_s` already stopped) so
  * we can afford it.
  */
case class ShowQueryLogCommand() extends LeafRunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("refresh_id", StringType, nullable = false)(),
    AttributeReference("view_name", StringType, nullable = false)(),
    AttributeReference("profile_timestamp", TimestampType, nullable = false)(),
    AttributeReference("stmt_order", IntegerType, nullable = false)(),
    AttributeReference("attempt_idx", IntegerType, nullable = false)(),
    AttributeReference("mode", StringType, nullable = false)(),
    AttributeReference("category", StringType, nullable = false)(),
    AttributeReference("stmt_kind", StringType, nullable = false)(),
    AttributeReference("duration_ms", LongType, nullable = false)(),
    AttributeReference("sql_text", StringType, nullable = false)()
  )

  override def run(spark: SparkSession): Seq[Row] = {
    // Drain the async-flusher queue so the catalog scan reflects everything
    // recorded by every refresh that completed before this SHOW.
    // 60-second budget is generous; in steady-state the queue is empty.
    RefreshSqlLogAsyncFlusher.awaitQuiescence(timeoutMs = 60000L)
    RefreshSqlLogCatalog.scanAll(spark).map { row: RefreshSqlLogRow =>
      Row(
        row.refreshId,
        row.viewName,
        row.profileTimestamp,
        row.stmtOrder,
        row.attemptIdx,
        row.mode,
        row.category,
        row.stmtKind,
        row.durationMs,
        Option(row.sqlText).getOrElse("")
      )
    }
  }
}
