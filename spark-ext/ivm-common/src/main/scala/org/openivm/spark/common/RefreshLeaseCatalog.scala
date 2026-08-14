package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/** Cross-driver, per-view refresh lease stored in Delta.
  *
  * The owner periodically renews the lease. Every refresh-data statement also
  * calls [[assertActive]], so a driver that loses ownership stops before its
  * next write instead of continuing alongside the new owner.
  */
object RefreshLeaseCatalog extends DeltaRetrySupport {
  private val ViewName     = "view_name"
  private val OwnerId      = "owner_id"
  private val FencingToken = "fencing_token"
  private val ExpiresAt    = "expires_at"

  private val LeaseMillis = TimeUnit.MINUTES.toMillis(5)
  private val RenewMillis = TimeUnit.MINUTES.toMillis(1)

  private val schema = StructType(
    Seq(
      StructField(ViewName, StringType, nullable = false),
      StructField(OwnerId, StringType, nullable = true),
      StructField(FencingToken, LongType, nullable = false),
      StructField(ExpiresAt, TimestampType, nullable = false)
    )
  )

  private val activeLease = new ThreadLocal[Lease]()

  private def enabled(spark: SparkSession): Boolean = FeatureGate.deltaCatalogEnabled(spark)
  private def path(spark: SparkSession): String =
    FeatureGate.catalogPath(spark).stripSuffix("/") + "/refresh_leases"

  final class Lease private[RefreshLeaseCatalog] (
      spark: SparkSession,
      val viewName: String,
      val ownerId: String,
      val fencingToken: Long,
      renewer: ScheduledExecutorService,
      renewalFailure: AtomicReference[Throwable]
  ) extends AutoCloseable {

    def assertOwned(): Unit = {
      Option(renewalFailure.get()).foreach(throw _)
      RefreshLeaseCatalog.assertOwned(spark, viewName, ownerId, fencingToken)
    }

    override def close(): Unit = {
      renewer.shutdownNow()
      try RefreshLeaseCatalog.release(spark, viewName, ownerId, fencingToken)
      finally if (activeLease.get() eq this) activeLease.remove()
    }
  }

  def assertActive(): Unit = Option(activeLease.get()).foreach(_.assertOwned())

  def withLease[A](spark: SparkSession, viewName: String)(body: Lease => A): A = {
    if (!enabled(spark)) return body(null)
    val lease = acquire(spark, viewName)
    activeLease.set(lease)
    try body(lease)
    finally lease.close()
  }

  private def ensureTable(spark: SparkSession): Unit = withDeltaRetry {
    DeltaTable.createIfNotExists(spark).location(path(spark)).addColumns(schema).partitionedBy(ViewName).execute()
  }

  private def acquire(spark: SparkSession, viewName: String): Lease = {
    ensureTable(spark)
    val ownerId  = UUID.randomUUID().toString
    val expires  = new Timestamp(System.currentTimeMillis() + LeaseMillis)
    val incoming = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(viewName, ownerId, 1L, expires)), 1), schema)

    withDeltaRetry {
      DeltaTable
        .forPath(spark, path(spark))
        .as("target")
        .merge(incoming.as("incoming"), col(s"target.$ViewName") === col(s"incoming.$ViewName"))
        .whenMatched(
          col(s"target.$ExpiresAt") < current_timestamp() || col(s"target.$OwnerId") === col(s"incoming.$OwnerId")
        )
        .updateExpr(
          Map(
            OwnerId      -> s"incoming.$OwnerId",
            FencingToken -> s"target.$FencingToken + 1",
            ExpiresAt    -> s"incoming.$ExpiresAt"
          )
        )
        .whenNotMatched()
        .insertAll()
        .execute()
    }

    val row = current(spark, viewName).getOrElse {
      throw new IllegalStateException(s"Refresh lease for '$viewName' disappeared during acquisition.")
    }
    if (row._1 != ownerId) {
      throw new IllegalStateException(s"Materialized view '$viewName' is already being refreshed by another driver.")
    }

    val failure = new AtomicReference[Throwable]()
    val renewer = Executors.newSingleThreadScheduledExecutor { runnable =>
      val thread = new Thread(runnable, s"openivm-refresh-lease-${viewName.hashCode.abs}")
      thread.setDaemon(true)
      thread
    }
    val token = row._2
    renewer.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit =
          try renew(spark, viewName, ownerId, token)
          catch { case error: Throwable => failure.compareAndSet(null, error) }
      },
      RenewMillis,
      RenewMillis,
      TimeUnit.MILLISECONDS
    )
    new Lease(spark, viewName, ownerId, token, renewer, failure)
  }

  private def current(spark: SparkSession, viewName: String): Option[(String, Long, Timestamp)] = {
    ensureTable(spark)
    spark.read
      .format("delta")
      .load(path(spark))
      .filter(col(ViewName) === lit(viewName))
      .select(OwnerId, FencingToken, ExpiresAt)
      .take(1)
      .headOption
      .map(row => (row.getString(0), row.getLong(1), row.getTimestamp(2)))
  }

  private def assertOwned(spark: SparkSession, viewName: String, ownerId: String, token: Long): Unit = {
    val now = System.currentTimeMillis()
    val valid = current(spark, viewName).exists { case (owner, currentToken, expires) =>
      owner == ownerId && currentToken == token && expires.getTime > now
    }
    if (!valid) throw new IllegalStateException(s"Lost distributed refresh lease for '$viewName'.")
  }

  private def renew(spark: SparkSession, viewName: String, ownerId: String, token: Long): Unit = withDeltaRetry {
    val expires = new Timestamp(System.currentTimeMillis() + LeaseMillis)
    DeltaTable
      .forPath(spark, path(spark))
      .update(
        col(ViewName) === lit(viewName) && col(OwnerId) === lit(ownerId) && col(FencingToken) === lit(token),
        Map(ExpiresAt -> lit(expires))
      )
    assertOwned(spark, viewName, ownerId, token)
  }

  private def release(spark: SparkSession, viewName: String, ownerId: String, token: Long): Unit = withDeltaRetry {
    DeltaTable
      .forPath(spark, path(spark))
      .update(
        col(ViewName) === lit(viewName) && col(OwnerId) === lit(ownerId) && col(FencingToken) === lit(token),
        Map(OwnerId -> lit(null).cast(StringType), ExpiresAt -> current_timestamp())
      )
  }
}
