package org.openivm.spark.streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.streaming.CheckpointFileManager

private[streaming] object StreamingCheckpointFileManager {
  type CancellableOutputStream = CheckpointFileManager.CancellableFSDataOutputStream

  def create(path: Path, hadoopConf: Configuration): CheckpointFileManager =
    CheckpointFileManager.create(path, hadoopConf)
}
