package org.apache.spark.sql.delta.openivm

import org.apache.spark.sql.delta.DeltaOptions

/** Narrow package bridge for Delta 3.2's protected[delta] raw option map. */
object DeltaOptionsAccess {

  def rawOptions(options: DeltaOptions): Map[String, String] = options.options.originalMap
}
