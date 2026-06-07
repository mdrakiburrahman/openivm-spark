package org.openivm.spark.common

/** Constants for Spark + Delta Lake exception patterns that should trigger an
  * automatic retry.  Adapted from the upstream `azurearcdata.spark.SparkExceptions`
  * pattern; openivm-specific additions are noted inline.
  *
  * Patterns are matched against the exception class name AND the exception
  * message via regex `.findFirstIn`, so a bare substring is fine.
  */
object SparkExceptions {

  /** Delta Lake metadata changed by a concurrent update — typical under
    * concurrent `ALTER TABLE … SET TBLPROPERTIES` / schema change. */
  val EXCEPTION_DELTA_METADATA_CHANGED: String = "DELTA_METADATA_CHANGED"

  /** Delta Lake protocol version bumped by a concurrent writer to an empty
    * directory.  Common when multiple specs in parallel JVMs race to create
    * the same MV-data Delta path. */
  val EXCEPTION_DELTA_PROTOCOL_CHANGED: String = "DELTA_PROTOCOL_CHANGED"

  /** Delta Lake concurrent append — files were added to the root of the table
    * by another writer between our snapshot and our commit.  Surfaces most
    * often during MV `REFRESH` MERGEs when the DML interceptor is racing.
    *
    * Example:
    * {{{
    * io.delta.exceptions.ConcurrentAppendException: [DELTA_CONCURRENT_APPEND]
    * ConcurrentAppendException: Files were added to the root of the table by a
    * concurrent update.  Please try the operation again.
    * Conflicting commit: {"timestamp":…,"operation":"MERGE","operationParameters":…}
    * Refer to https://docs.delta.io/latest/concurrency-control.html for more details.
    * }}}
    */
  val EXCEPTION_DELTA_CONCURRENT_APPEND: String = "DELTA_CONCURRENT_APPEND"

  /** Delta Lake concurrent delete vs. read — the row(s) we tried to MERGE/DELETE
    * were deleted by a concurrent writer between snapshot read and commit. */
  val EXCEPTION_DELTA_CONCURRENT_DELETE_READ: String = "DELTA_CONCURRENT_DELETE_READ"

  /** Delta Lake concurrent delete vs. delete — both writers tried to remove
    * the same row set.  Standard OCC failure mode. */
  val EXCEPTION_DELTA_CONCURRENT_DELETE_DELETE: String = "DELTA_CONCURRENT_DELETE_DELETE"

  /** Delta Lake concurrent transaction — a sibling commit happened against the
    * same table version we were reading. */
  val EXCEPTION_DELTA_CONCURRENT_TRANSACTION: String = "DELTA_CONCURRENT_TRANSACTION"

  /** Hive metastore "already exists" — race condition when two concurrent
    * threads CREATE the same managed table. */
  val EXCEPTION_HIVE_TABLE_ALREADY_EXISTS: String = "AlreadyExistsException"

  /** Cannot CREATE TABLE at a non-empty location — typically a partial-failure
    * leftover from a previous attempt. */
  val EXCEPTION_DELTA_NON_EMPTY_LOCATION: String =
    "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION"

  /** File not found error during cached plan execution.  Surfaces when a
    * concurrent writer rewrote / cleaned up Parquet files between plan
    * preparation and scan execution; harmless once we retry against the
    * fresh snapshot. */
  val EXCEPTION_SPARK_FILE_NOT_FOUND: String = "SparkFileNotFoundException"

  /** Default retry patterns for Delta + Spark concurrency conflicts.
    *
    * The list is biased towards the OCC failure modes openivm-spark hits in
    * tests when many JVM forks race on the same Delta tables.  Add new
    * patterns here when you find a new flake.
    */
  val DefaultDeltaRetryPatterns: Array[String] = Array(
    EXCEPTION_DELTA_METADATA_CHANGED,
    EXCEPTION_DELTA_PROTOCOL_CHANGED,
    EXCEPTION_DELTA_CONCURRENT_APPEND,
    EXCEPTION_DELTA_CONCURRENT_DELETE_READ,
    EXCEPTION_DELTA_CONCURRENT_DELETE_DELETE,
    EXCEPTION_DELTA_CONCURRENT_TRANSACTION,
    EXCEPTION_HIVE_TABLE_ALREADY_EXISTS,
    EXCEPTION_DELTA_NON_EMPTY_LOCATION,
    EXCEPTION_SPARK_FILE_NOT_FOUND
  )
}
