package org.openivm.spark.executor

/**
 * Reserved namespace for executor-side operators (DeltaStagingExec,
 * MergeWriterExec, etc.) that must travel to Spark task threads via
 * serialization-safe class loading.
 */
private[executor] object _Marker
