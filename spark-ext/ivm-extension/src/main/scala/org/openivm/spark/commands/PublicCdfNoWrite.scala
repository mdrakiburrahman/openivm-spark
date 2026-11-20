package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias, View}
import org.apache.spark.sql.delta.actions.{CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, IntegerType, LongType, ShortType}
import org.openivm.spark.common.{CdfChangeBatch, DeltaTableVersion, MvCatalog, MvMetadata, MvProjectionSource}

import scala.util.control.NonFatal

private[commands] object PublicCdfNoWrite {
  import MvCommandHelper._

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

  /** This optimization is a proof, never a fallback/error classifier. Reuse the managed-MV
    * refresh locks already held by the caller and bind the actual Delta read to the recorded path.
    */
  def provesCancellation(spark: SparkSession, consumer: MvMetadata, batch: CdfChangeBatch): Boolean =
    try {
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

          schema.nonEmpty && schema.forall { field =>
            !CdfColumns.contains(field.name.toLowerCase(java.util.Locale.ROOT)) &&
            EligibleTypes.contains(field.dataType)
          } && fingerprint == consumer.sourceSchemaFingerprint &&
          directConsumer(spark.sql(consumer.querySql).queryExecution.analyzed, plan) &&
          identitiesAndVersionsMatch && backingRelation(plan).flatMap(DeltaTableVersion.deltaLogFromPlan).exists {
            log =>
              val start = batch.startVersionExclusive + 1L
              val end   = batch.endVersionInclusive
              def currentEnd: Boolean = {
                val snapshot = log.update()
                snapshot.version == end && snapshot.metadata.id == dependency.deltaTableMetadataId
              }
              // Only complete MERGE history using an unchanged schema/protocol is admitted.
              // Do not infer safety from operation metrics, or read past the captured end.
              def mergeOnly: Boolean = {
                val commits = log.getChanges(start, failOnDataLoss = true).takeWhile(_._1 <= end).toVector
                commits.map(_._1) == (start to end) && commits.forall { case (_, actions) =>
                  !actions.exists {
                    case _: Metadata | _: Protocol => true
                    case _                         => false
                  } && actions.collectFirst { case info: CommitInfo => info.operation }.contains("MERGE")
                }
              }
              log.dataPath.toUri.normalize().toString.stripSuffix("/") == dependency.deltaLogDataPath &&
              currentEnd && mergeOnly && {
                val raw = spark.read
                  .format("delta")
                  .option("readChangeFeed", "true")
                  .option("startingVersion", start)
                  .option("endingVersion", end)
                  .load(producer.location)
                val public                        = MvProjectionSource.project(raw, schema, Seq(ChangeType))
                def quoted(value: String): String = s"`${value.replace("`", "``")}`"
                val columns                       = schema.fieldNames.toSeq.map(name => col(quoted(name)))
                val adds                          = public.where(col(ChangeType).isin(Positive: _*)).select(columns: _*)
                val removes                       = public.where(col(ChangeType).isin(Negative: _*)).select(columns: _*)
                raw.where(col(ChangeType).isNull || !col(ChangeType).isin((Positive ++ Negative): _*)).isEmpty &&
                adds.exceptAll(removes).isEmpty && removes.exceptAll(adds).isEmpty &&
                currentEnd && identitiesAndVersionsMatch
              }
          }
        }
      }
    } catch { case NonFatal(_) => false }
}
