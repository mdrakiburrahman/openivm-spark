package org.openivm.spark.commands

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias, View}
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, Metadata, Protocol, RemoveFile}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.{col, count, lit, sum, when}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DecimalType, IntegerType, LongType, ShortType, StringType, StructField, TimestampType}
import org.openivm.spark.common.{BatchVerdict, CdfChangeBatch, DeltaTableVersion, MvCatalog, MvMetadata, MvProjectionSource}

import scala.util.control.NonFatal

private[commands] object PublicCdfNoWrite {
  import MvCommandHelper._

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  private def check(gate: String)(value: Boolean): Boolean = {
    if (!value) logger.debug(s"Public CDF cancellation declined: $gate")
    value
  }

  private val ChangeType    = "_change_type"
  private val Positive      = Seq("insert", "update_postimage")
  private val Negative      = Seq("delete", "update_preimage")
  private val EligibleTypes = Set[DataType](BooleanType, ByteType, ShortType, IntegerType, LongType)
  private val CdfColumns    = Set(ChangeType, "_commit_version", "_commit_timestamp")

  private def unalias(plan: LogicalPlan): LogicalPlan = plan match {
    case alias: SubqueryAlias => unalias(alias.child)
    case other                => other
  }

  private def identityAttribute(expr: Expression): Option[AttributeReference] = expr match {
    case attr: AttributeReference                           => Some(attr)
    case alias: Alias                                       => identityAttribute(alias.child)
    case cast: Cast if cast.dataType == cast.child.dataType => identityAttribute(cast.child)
    case _                                                  => None
  }

  /** Prove that the public relation exposes only same-name, same-type backing attributes.
    * A projection marker alone is not enough: a filter, join, cast or remapping would make
    * MvProjectionSource.project on backing CDF an invalid substitute for the live view.
    */
  private def backingRelation(plan: LogicalPlan): Option[LogicalRelation] = unalias(plan) match {
    case view: View if MvProjectionSource.isProjection(view) => backingRelation(view.child)
    case project: Project if project.projectList.forall { output =>
          identityAttribute(output).exists(attr => output.name.equalsIgnoreCase(attr.name))
        } =>
      backingRelation(project.child)
    case relation: LogicalRelation => Some(relation)
    case _                         => None
  }

  private def directConsumer(plan: LogicalPlan, source: LogicalPlan): Boolean = unalias(plan) match {
    case project: Project =>
      project.projectList.forall(output => identityAttribute(output).nonEmpty) &&
      unalias(project.child).sameResult(unalias(source))
    case _ => false
  }

  private def eligible(field: StructField, replacement: Boolean): Boolean =
    !CdfColumns.contains(field.name.toLowerCase(java.util.Locale.ROOT)) &&
      !field.metadata.contains("__CHAR_VARCHAR_TYPE_STRING") &&
      (EligibleTypes.contains(field.dataType) || (replacement &&
        Set[DataType](StringType, DecimalType(18, 2), TimestampType).contains(field.dataType)))

  // Deliberately admit only the single inferred-CDF commit emitted by the full refresh writer.
  // With no explicit CDC files or deletion vectors, Delta reads every data-changing add/remove.
  private def isPhysicalOverwrite(actions: Seq[Action], baseline: Long): Boolean = {
    val info = actions.collect { case commit: CommitInfo => commit }
    val adds = actions.collect { case add: AddFile => add.path }
    val removes = actions.collect { case remove: RemoveFile => remove.path }
    def parameter(info: CommitInfo, key: String, expected: String): Boolean =
      info.operationParameters.get(key).exists(value => value == expected || value == "\"" + expected + "\"")
    info.size == 1 && info.head.operation == "WRITE" && info.head.readVersion.contains(baseline) &&
    parameter(info.head, "mode", "Overwrite") &&
    parameter(info.head, "predicate", "true") &&
    adds.nonEmpty && removes.nonEmpty && (adds ++ removes).distinct.size == adds.size + removes.size &&
    actions.forall {
      case _: CommitInfo => true
      case add: AddFile => add.dataChange && add.deletionVector == null
      case remove: RemoveFile => remove.dataChange && remove.deletionVector == null
      case _ => false
    }
  }

  private def replacementCancels(public: DataFrame, keys: Seq[Column], end: Long): Boolean = {
    val sign = when(col("_commit_version") === lit(end),
      when(col(ChangeType) === "insert", lit(1L)).when(col(ChangeType) === "delete", lit(-1L)))
    val groups = public.groupBy(keys: _*).agg(
      sum(sign.cast(DecimalType(38, 0))).as("_net"),
      count(when(sign.isNull, lit(1))).as("_bad")
    )
    val result = groups.agg(count(lit(1)), count(when(col("_bad") =!= 0L || col("_net").isNull || col("_net") =!= 0L, lit(1)))).head()
    result.getLong(0) > 0L && result.getLong(1) == 0L
  }

  private def cancelled(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).take(32).exists {
      case _: InterruptedException | _: java.io.InterruptedIOException | _: java.util.concurrent.CancellationException => true
      case e: SparkThrowable =>
        SparkCommandCompat.errorCondition(e).exists(c => c == "SPARK_JOB_CANCELLED" || c == "OPERATION_CANCELED") ||
          (e.isInstanceOf[org.apache.spark.SparkException] &&
            Option(e.getMessage).exists(_.matches("(?s)Job [0-9]+ cancelled.*")))
      case _ => false
    }

  /** This optimization is a proof, never a fallback/error classifier. Reuse the managed-MV
    * refresh locks already held by the caller and bind the actual Delta read to the recorded path.
    */
  def provesCancellation(spark: SparkSession, consumer: MvMetadata, batch: CdfChangeBatch, verdict: BatchVerdict): Boolean =
    try {
      val replacement = verdict == BatchVerdict.Replace
      if (verdict != BatchVerdict.Mutating && !replacement) return false
      if (replacement && (batch.endVersionInclusive != batch.startVersionExclusive + 1L ||
          spark.conf.get("spark.sql.files.ignoreMissingFiles", "false").toBoolean ||
          spark.conf.get("spark.sql.files.ignoreCorruptFiles", "false").toBoolean)) return false
      val source = batch.baseTable
      val dep    = readMvDependencyMap(consumer.properties).toOption.collect { case Seq(only) => only }
      dep.exists { dependency =>
        val expected = (dependency.deltaLogDataPath, dependency.deltaTableMetadataId)
        val id       = spark.sessionState.sqlParser.parseTableIdentifier(dependency.key)
        MvCatalog.lookup(spark, id).exists { producer =>
          val logical = spark.table(source)
          val plan    = logical.queryExecution.analyzed
          val schema  = logical.schema
          val fingerprint =
            MvCatalog.schemaFingerprint(Map(source -> schema), Map(source -> MvCatalog.mvIdentity(producer)))
          def identitiesAndVersionsMatch: Boolean =
            mvBackingIdentity(spark, producer).contains(expected) &&
              deltaPhysicalIdentity(spark, source).contains(expected) &&
              MaterializedViewLifecycle.liveOwnership(spark, id, producer).owned &&
              MaterializedViewLifecycle.liveOwnership(spark, consumer.name, consumer).owned &&
              DeltaTableVersion.latest(spark, consumer.location) == consumer.lastVersion

          check("public schema types")(schema.nonEmpty && schema.forall(eligible(_, replacement))) &&
          check("persisted schema fingerprint")(fingerprint == consumer.sourceSchemaFingerprint) &&
          check("direct consumer")(directConsumer(spark.sql(consumer.querySql).queryExecution.analyzed, plan)) &&
          check("ownership and versions")(identitiesAndVersionsMatch) && backingRelation(plan).flatMap(DeltaTableVersion.deltaLogFromPlan).exists {
            log =>
              val start = batch.startVersionExclusive + 1L
              val end   = batch.endVersionInclusive
              def currentEnd: Boolean = {
                val snapshot = log.update()
                snapshot.version == end && snapshot.metadata.id == dependency.deltaTableMetadataId
              }
              def completeHistory: Boolean = {
                val commits = log.getChanges(start, failOnDataLoss = true).takeWhile(_._1 <= end).toVector
                commits.map(_._1) == (start to end) && (if (replacement) {
                  val baseline = log.getSnapshotAt(batch.startVersionExclusive)
                  val latest = log.update()
                  baseline.metadata == latest.metadata && baseline.metadata.id == dependency.deltaTableMetadataId &&
                  baseline.protocol == latest.protocol &&
                  // Guard against raw CHAR/VARCHAR metadata masked by Spark's public projection.
                  baseline.metadata.schema.forall(f => !f.metadata.contains("__CHAR_VARCHAR_TYPE_STRING")) &&
                  schema.forall(f => baseline.metadata.schema.fields.exists(b => b.name == f.name && b.dataType == f.dataType)) &&
                  isPhysicalOverwrite(commits.head._2, batch.startVersionExclusive)
                } else commits.forall { case (_, actions) =>
                  !actions.exists {
                    case _: Metadata | _: Protocol => true
                    case _                         => false
                  } && actions.collectFirst { case info: CommitInfo => info.operation }.contains("MERGE")
                })
              }
              log.dataPath.toUri.normalize().toString.stripSuffix("/") == dependency.deltaLogDataPath &&
              check("current end")(currentEnd) && check("complete supported history")(completeHistory) && {
                val raw = spark.read
                  .format("delta")
                  .option("readChangeFeed", "true")
                  .option("startingVersion", start)
                  .option("endingVersion", end)
                  .load(producer.location)
                val cdfFields = if (replacement) Seq(ChangeType, "_commit_version") else Seq(ChangeType)
                val public = MvProjectionSource.project(raw, schema, cdfFields)
                def quoted(value: String): String = s"`${value.replace("`", "``")}`"
                val columns                       = schema.fieldNames.toSeq.map(name => col(quoted(name)))
                val zero = if (replacement) {
                  val keys = schema.fields.toSeq.zip(columns).map { case (field, column) =>
                    if (field.dataType == StringType) column.cast(BinaryType).as(field.name) else column
                  }
                  val sc = spark.sparkContext
                  val property = "spark.openivm.publicCdfNoWrite.proof"
                  val prior = sc.getLocalProperty(property)
                  sc.setLocalProperty(property, "replacement")
                  try replacementCancels(public, keys.zipWithIndex.map { case (key, i) => key.as(s"_k$i") }, end)
                  finally sc.setLocalProperty(property, prior)
                } else {
                val adds                          = public.where(col(ChangeType).isin(Positive: _*)).select(columns: _*)
                val removes                       = public.where(col(ChangeType).isin(Negative: _*)).select(columns: _*)
                raw.where(col(ChangeType).isNull || !col(ChangeType).isin((Positive ++ Negative): _*)).isEmpty &&
                adds.exceptAll(removes).isEmpty && removes.exceptAll(adds).isEmpty
                }
                zero && currentEnd && identitiesAndVersionsMatch
              }
          }
        }
      }
    } catch {
      case NonFatal(error) if !cancelled(error) =>
        logger.debug("Public CDF cancellation unavailable", error)
        false
    }
}
