package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

sealed trait RefreshTransactionState { def value: String }
object RefreshTransactionState {
  case object Prepared      extends RefreshTransactionState { val value = "PREPARED"       }
  case object DataCommitted extends RefreshTransactionState { val value = "DATA_COMMITTED" }
  case object Committed     extends RefreshTransactionState { val value = "COMMITTED"      }

  def fromString(value: String): RefreshTransactionState = value match {
    case Prepared.value      => Prepared
    case DataCommitted.value => DataCommitted
    case Committed.value     => Committed
    case other               => throw new IllegalArgumentException(s"Unknown refresh transaction state '$other'.")
  }
}

final case class RefreshTransaction(
    refreshId: String,
    viewName: String,
    state: RefreshTransactionState,
    dataVersion: Option[Long]
)

/** Durable refresh commit protocol used by the Delta catalog backend. */
object RefreshTransactionCatalog extends DeltaRetrySupport {
  private val RefreshId   = "refresh_id"
  private val ViewName    = "view_name"
  private val State       = "state"
  private val DataVersion = "data_version"
  private val UpdatedAt   = "updated_at"

  private val schema = StructType(
    Seq(
      StructField(RefreshId, StringType, nullable = false),
      StructField(ViewName, StringType, nullable = false),
      StructField(State, StringType, nullable = false),
      StructField(DataVersion, LongType, nullable = true),
      StructField(UpdatedAt, TimestampType, nullable = false)
    )
  )

  private def enabled(spark: SparkSession): Boolean = FeatureGate.deltaCatalogEnabled(spark)

  private def path(spark: SparkSession): String =
    FeatureGate.catalogPath(spark).stripSuffix("/") + "/refresh_transactions"

  def ensureTable(spark: SparkSession): Unit = {
    if (!enabled(spark)) return
    withDeltaRetry {
      DeltaTable.createIfNotExists(spark).location(path(spark)).addColumns(schema).execute()
    }
  }

  def prepare(spark: SparkSession, refreshId: String, viewName: String): Unit = {
    if (!enabled(spark)) return
    withDeltaRetry {
      ensureTable(spark)
      val row = Row(
        refreshId,
        viewName,
        RefreshTransactionState.Prepared.value,
        null,
        new java.sql.Timestamp(System.currentTimeMillis())
      )
      val incoming = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row), 1), schema)
      DeltaTable
        .forPath(spark, path(spark))
        .as("target")
        .merge(incoming.as("incoming"), s"target.$RefreshId = incoming.$RefreshId")
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    requireState(spark, refreshId, Set(RefreshTransactionState.Prepared))
  }

  def markDataCommitted(spark: SparkSession, refreshId: String, dataVersion: Long): Unit = {
    if (!enabled(spark)) return
    withDeltaRetry {
      ensureTable(spark)
      DeltaTable
        .forPath(spark, path(spark))
        .update(
          col(RefreshId) === lit(refreshId) && (
            col(State) === lit(RefreshTransactionState.Prepared.value) ||
              (col(State) === lit(RefreshTransactionState.DataCommitted.value) &&
                col(DataVersion) === lit(dataVersion))
          ),
          Map(
            State       -> lit(RefreshTransactionState.DataCommitted.value),
            DataVersion -> lit(dataVersion),
            UpdatedAt   -> current_timestamp()
          )
        )
    }
    val transaction = requireState(spark, refreshId, Set(RefreshTransactionState.DataCommitted))
    require(
      transaction.dataVersion.contains(dataVersion),
      s"Refresh transaction '$refreshId' has data version ${transaction.dataVersion}, expected $dataVersion."
    )
  }

  def commit(spark: SparkSession, refreshId: String): Unit = {
    if (!enabled(spark)) return
    withDeltaRetry {
      ensureTable(spark)
      DeltaTable
        .forPath(spark, path(spark))
        .update(
          col(RefreshId) === lit(refreshId) && col(State).isin(
            RefreshTransactionState.DataCommitted.value,
            RefreshTransactionState.Committed.value
          ),
          Map(State -> lit(RefreshTransactionState.Committed.value), UpdatedAt -> current_timestamp())
        )
    }
    requireState(spark, refreshId, Set(RefreshTransactionState.Committed))
  }

  def lookup(spark: SparkSession, refreshId: String): Option[RefreshTransaction] = {
    if (!enabled(spark)) return None
    ensureTable(spark)
    spark.read
      .format("delta")
      .load(path(spark))
      .filter(col(RefreshId) === lit(refreshId))
      .take(1)
      .headOption
      .map(decode)
  }

  def incompleteForView(spark: SparkSession, viewName: String): Seq[RefreshTransaction] = {
    if (!enabled(spark)) return Seq.empty
    ensureTable(spark)
    spark.read
      .format("delta")
      .load(path(spark))
      .filter(col(ViewName) === lit(viewName) && col(State) =!= lit(RefreshTransactionState.Committed.value))
      .collect()
      .toSeq
      .map(decode)
  }

  private def decode(row: Row): RefreshTransaction =
    RefreshTransaction(
      refreshId = row.getAs[String](RefreshId),
      viewName = row.getAs[String](ViewName),
      state = RefreshTransactionState.fromString(row.getAs[String](State)),
      dataVersion = Option(row.getAs[java.lang.Long](DataVersion)).map(_.longValue())
    )

  private def requireState(
      spark: SparkSession,
      refreshId: String,
      allowed: Set[RefreshTransactionState]
  ): RefreshTransaction = {
    val transaction = lookup(spark, refreshId).getOrElse {
      throw new IllegalStateException(s"Refresh transaction '$refreshId' does not exist.")
    }
    if (!allowed.contains(transaction.state)) {
      throw new IllegalStateException(
        s"Refresh transaction '$refreshId' is ${transaction.state.value}; expected ${allowed.map(_.value).toSeq.sorted.mkString(" or ")}."
      )
    }
    transaction
  }
}
