package org.openivm.spark.executor

/**
 * Reserved namespace for executor-side operators (DeltaStagingExec,
 * MergeWriterExec, etc.) that must travel to Spark task threads via
 * serialization-safe class loading. Kept deliberately empty in the P3
 * scaffold so the module compiles before P4.dml lands.
 */
private[executor] object _Marker
