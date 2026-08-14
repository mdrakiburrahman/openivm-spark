package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

sealed trait RefreshTransactionState { def value: String }
object RefreshTransactionState {
  case object Prepared      extends RefreshTransactionState { val value = "PREPARED"       }
  case object DataCommitted extends RefreshTransactionState { val value = "DATA_COMMITTED" }
  case object Committed     extends RefreshTransactionState { val value = "COMMITTED"      }
  case object Aborted       extends RefreshTransactionState { val value = "ABORTED"        }

  def fromString(value: String): RefreshTransactionState = value match {
    case Prepared.value      => Prepared
    case DataCommitted.value => DataCommitted
    case Committed.value     => Committed
    case Aborted.value       => Aborted
    case other               => throw new IllegalArgumentException(s"Unknown refresh transaction state '$other'.")
  }
}

final case class RefreshTransaction(
    refreshId: String,
    viewName: String,
    state: RefreshTransactionState,
    targetLocation: String,
    startDataVersion: Long,
    dataVersion: Option[Long],
    sourceVersions: Map[String, Long]
)

/** Recoverable refresh protocol for the Delta/CDF multi-driver mode.
  *
  * There is one durable row per view. PREPARED records the exact target
  * version to restore and source versions to consume. DATA_COMMITTED means all
  * target statements completed and recovery must roll the metadata forward.
  */
object RefreshTransactionCatalog extends DeltaRetrySupport {
  private val RefreshId        = "refresh_id"
  private val ViewName         = "view_name"
  private val State            = "state"
  private val TargetLocation   = "target_location"
  private val StartDataVersion = "start_data_version"
  private val DataVersion      = "data_version"
  private val SourceVersions   = "source_versions"
  private val UpdatedAt        = "updated_at"

  private val schema = StructType(
    Seq(
      StructField(RefreshId, StringType, nullable = false),
      StructField(ViewName, StringType, nullable = false),
      StructField(State, StringType, nullable = false),
      StructField(TargetLocation, StringType, nullable = false),
      StructField(StartDataVersion, LongType, nullable = false),
      StructField(DataVersion, LongType, nullable = true),
      StructField(SourceVersions, MapType(StringType, LongType, valueContainsNull = false), nullable = false),
      StructField(UpdatedAt, TimestampType, nullable = false)
    )
  )

  private def enabled(spark: SparkSession): Boolean = FeatureGate.deltaCatalogEnabled(spark)
  private def path(spark: SparkSession): String =
    FeatureGate.catalogPath(spark).stripSuffix("/") + "/refresh_transactions"

  def ensureTable(spark: SparkSession): Unit = {
    if (!enabled(spark)) return
    withDeltaRetry {
      DeltaTable.createIfNotExists(spark).location(path(spark)).addColumns(schema).partitionedBy(ViewName).execute()
    }
  }

  def prepare(
      spark: SparkSession,
      refreshId: String,
      viewName: String,
      targetLocation: String,
      startDataVersion: Long,
      sourceVersions: Map[String, Long]
  ): Unit = {
    if (!enabled(spark)) return
    withDeltaRetry {
      ensureTable(spark)
      val row = Row(
        refreshId,
        viewName,
        RefreshTransactionState.Prepared.value,
        targetLocation,
        startDataVersion,
        null,
        sourceVersions,
        new java.sql.Timestamp(System.currentTimeMillis())
      )
      val incoming = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row), 1), schema)
      DeltaTable
        .forPath(spark, path(spark))
        .as("target")
        .merge(incoming.as("incoming"), col(s"target.$ViewName") === col(s"incoming.$ViewName"))
        .whenMatched(
          col(s"target.$State").isin(RefreshTransactionState.Committed.value, RefreshTransactionState.Aborted.value)
        )
        .updateAll()
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
              (col(State) === lit(RefreshTransactionState.DataCommitted.value) && col(DataVersion) === lit(dataVersion))
          ),
          Map(
            State       -> lit(RefreshTransactionState.DataCommitted.value),
            DataVersion -> lit(dataVersion),
            UpdatedAt   -> current_timestamp()
          )
        )
    }
    val transaction = requireState(spark, refreshId, Set(RefreshTransactionState.DataCommitted))
    require(transaction.dataVersion.contains(dataVersion))
  }

  def commit(spark: SparkSession, refreshId: String): Unit =
    transition(spark, refreshId, RefreshTransactionState.DataCommitted, RefreshTransactionState.Committed)

  private def abort(spark: SparkSession, refreshId: String): Unit =
    transition(spark, refreshId, RefreshTransactionState.Prepared, RefreshTransactionState.Aborted)

  private def transition(
      spark: SparkSession,
      refreshId: String,
      from: RefreshTransactionState,
      to: RefreshTransactionState
  ): Unit = {
    if (!enabled(spark)) return
    withDeltaRetry {
      ensureTable(spark)
      DeltaTable
        .forPath(spark, path(spark))
        .update(
          col(RefreshId) === lit(refreshId) && col(State).isin(from.value, to.value),
          Map(State -> lit(to.value), UpdatedAt -> current_timestamp())
        )
    }
    requireState(spark, refreshId, Set(to))
  }

  /** Reconcile the previous owner's transaction while holding the view lease. */
  def recoverIncomplete(spark: SparkSession, viewName: String): Unit = {
    if (!enabled(spark)) return
    incompleteForView(spark, viewName).foreach {
      case transaction if transaction.state == RefreshTransactionState.Prepared =>
        val currentVersion = latestVersion(spark, transaction.targetLocation)
        if (currentVersion < transaction.startDataVersion) {
          throw new IllegalStateException(
            s"MV '${transaction.viewName}' is at Delta version $currentVersion, before recorded start " +
              s"version ${transaction.startDataVersion}."
          )
        }
        if (currentVersion > transaction.startDataVersion) {
          DeltaTable
            .forPath(spark, transaction.targetLocation)
            .restoreToVersion(transaction.startDataVersion)
            .collect()
        }
        abort(spark, transaction.refreshId)

      case transaction if transaction.state == RefreshTransactionState.DataCommitted =>
        val dataVersion = transaction.dataVersion.getOrElse {
          throw new IllegalStateException(s"DATA_COMMITTED refresh '${transaction.refreshId}' has no data version.")
        }
        val identifier = CatalystSqlParser.parseTableIdentifier(transaction.viewName)
        MvCatalog.advance(spark, identifier, dataVersion)
        CdfWatermarkCatalog.putAll(spark, transaction.viewName, transaction.sourceVersions)
        commit(spark, transaction.refreshId)

      case _ => ()
    }
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
      .filter(
        col(ViewName) === lit(viewName) &&
          col(State).isin(RefreshTransactionState.Prepared.value, RefreshTransactionState.DataCommitted.value)
      )
      .collect()
      .toSeq
      .map(decode)
  }

  private def latestVersion(spark: SparkSession, location: String): Long =
    DeltaTable.forPath(spark, location).history(1).collect().head.getAs[Long]("version")

  private def decode(row: Row): RefreshTransaction =
    RefreshTransaction(
      refreshId = row.getAs[String](RefreshId),
      viewName = row.getAs[String](ViewName),
      state = RefreshTransactionState.fromString(row.getAs[String](State)),
      targetLocation = row.getAs[String](TargetLocation),
      startDataVersion = row.getAs[Long](StartDataVersion),
      dataVersion = Option(row.getAs[java.lang.Long](DataVersion)).map(_.longValue()),
      sourceVersions = row.getJavaMap[String, Long](row.fieldIndex(SourceVersions)).asScala.toMap.map {
        case (source, version) => source -> version.longValue()
      }
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
        s"Refresh transaction '$refreshId' is ${transaction.state.value}; expected " +
          s"${allowed.map(_.value).toSeq.sorted.mkString(" or ")}."
      )
    }
    transaction
  }
}
