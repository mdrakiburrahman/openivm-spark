package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.checkpointing.{CommitLog, OffsetSeqLog}

private[streaming] object StreamingReplayCheckpointLog {
  def latestCommitted(spark: SparkSession, checkpoint: String): Option[CommittedReplayBatch] = {
    val offsets = new OffsetSeqLog(spark, new Path(checkpoint, "offsets").toString)
    val commits = new CommitLog(spark, new Path(checkpoint, "commits").toString)
    for {
      latest <- offsets.getLatest()
      (batchId, current) = latest
      if batchId >= 1L && batchId < Long.MaxValue
      committed <- commits.getLatest()
      if committed._1 == batchId
      previous <- offsets.get(batchId - 1L)
    } yield CommittedReplayBatch(
      batchId,
      previous.offsets.map(_.map(_.json)),
      current.offsets.map(_.map(_.json)),
      () => {
        // Do not overwrite a concurrent writer. If interrupted between adds, Spark sees an
        // uncommitted empty batch, which is safe to execute normally on the next restart.
        if (!offsets.add(batchId + 1L, current))
          throw new IllegalStateException(s"Committed replay repair offset batch ${batchId + 1L} already exists")
        if (!commits.add(batchId + 1L, committed._2))
          throw new IllegalStateException(s"Committed replay repair commit batch ${batchId + 1L} already exists")
      }
    )
  }
}
