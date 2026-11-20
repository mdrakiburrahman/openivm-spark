package org.openivm.spark.commands

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  Cast,
  Expression,
  LessThanOrEqual,
  Literal,
  RowNumber,
  WindowExpression
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  Distinct,
  Filter,
  LogicalPlan,
  Project,
  SubqueryAlias,
  View,
  Window
}
import org.apache.spark.sql.delta.actions.{AddCDCFile, CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.{col, lit, sum, when}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DecimalType, IntegerType, LongType, ShortType}
import org.openivm.spark.common.{
  CdfChangeBatch,
  DeltaTableVersion,
  MvCatalog,
  MvMetadata,
  MvProjectionSource,
  RefreshTypeCode
}

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

  private val MaxRankGroupVersions = 4L
  private val MaxRankGroupCdcBytes = BigInt(8 * 1024 * 1024)

  private def deterministicRankOrCount(plan: LogicalPlan, source: LogicalPlan, refreshType: Int): Boolean = {
    def sameSource(child: LogicalPlan): Boolean = unalias(child) match {
      // Spark 3.5 inserts an identity Project below Window during analysis.
      case project: Project if project.projectList.forall(_.isInstanceOf[AttributeReference]) &&
            project.projectList.map(_.exprId) == project.child.output.map(_.exprId) =>
        sameSource(project.child)
      case other => other.sameResult(unalias(source))
    }
    def rankOutput(expr: Expression): Boolean = expr match {
      case alias: Alias =>
        alias.child match {
          case window: WindowExpression => window.windowFunction.isInstanceOf[RowNumber]
          case _                        => false
        }
      case _ => false
    }
    def rankShape(node: LogicalPlan): Boolean = unalias(node) match {
      case project: Project => project.projectList.forall(identityAttribute(_).nonEmpty) && rankShape(project.child)
      case window: Window =>
        val keys = window.partitionSpec ++ window.orderSpec.map(_.child)
        // If a rank tie remains, every public input field is identical and the assigned
        // sequence of row numbers is observationally the same multiset.
        keys.forall(_.isInstanceOf[AttributeReference]) && window.orderSpec.nonEmpty &&
        keys.collect { case attr: AttributeReference => attr.exprId }.toSet == window.child.output
          .map(_.exprId)
          .toSet &&
        window.windowExpressions.size == 1 && window.windowExpressions.forall(rankOutput) && sameSource(window.child)
      case _ => false
    }
    def rankId(node: LogicalPlan): Option[org.apache.spark.sql.catalyst.expressions.ExprId] = unalias(node) match {
      case project: Project => rankId(project.child)
      case window: Window   => window.windowExpressions.find(rankOutput).map(_.exprId)
      case _                => None
    }
    def boundedRank(node: LogicalPlan): Boolean = unalias(node) match {
      case project: Project => project.projectList.forall(identityAttribute(_).nonEmpty) && boundedRank(project.child)
      case filter: Filter =>
        filter.condition match {
          case LessThanOrEqual(attr: AttributeReference, bound: Literal) =>
            bound.dataType == IntegerType && Option(bound.value).exists(v => v.asInstanceOf[Int] > 0) &&
            rankId(filter.child).contains(attr.exprId) && rankShape(filter.child)
          case _ => false
        }
      case _ => false
    }
    def countOutput(expr: Expression): Boolean = expr match {
      case alias: Alias => countOutput(alias.child)
      case aggregate: AggregateExpression =>
        aggregate.aggregateFunction match {
          case count: Count =>
            !aggregate.isDistinct && aggregate.filter.isEmpty && count.children.size == 1 && count.children.forall {
              case _: AttributeReference | _: Literal => true
              case _                                  => false
            }
          case _ => false
        }
      case _ => false
    }
    refreshType match {
      case RefreshTypeCode.WindowPartition => rankShape(plan) || boundedRank(plan)
      case RefreshTypeCode.AggregateGroup =>
        unalias(plan) match {
          case group: Aggregate =>
            val keys = group.groupingExpressions.collect { case a: AttributeReference => a.exprId }.toSet
            keys.nonEmpty && group.groupingExpressions.forall(_.isInstanceOf[AttributeReference]) &&
            group.aggregateExpressions.exists(countOutput) && group.aggregateExpressions.forall { output =>
              countOutput(output) || identityAttribute(output).exists(a => keys(a.exprId))
            } && sameSource(group.child)
          case _ => false
        }
      case _ => false
    }
  }

  private def signedBagIsEmpty(public: DataFrame, attributes: Seq[String]): Boolean = {
    def quoted(value: String): String = s"`${value.replace("`", "``")}`"
    val keyNames                      = attributes.indices.map(i => s"_ivm_proof_key_$i")
    val keys                          = attributes.zip(keyNames).map { case (name, key) => col(quoted(name)).as(key) }
    val change                        = col(ChangeType)
    val signed = public.select(
      (keys ++ Seq(
        when(change.isin(Positive: _*), lit(1))
          .when(change.isin(Negative: _*), lit(-1))
          .cast(DecimalType(38, 0))
          .as("_ivm_proof_signed"),
        when(change.isNull || !change.isin((Positive ++ Negative): _*), lit(1)).otherwise(lit(0)).as("_ivm_proof_bad")
      )): _*
    )
    // Grouping uses Spark's actual NULL and primitive-value equality; an invalid sign or
    // any failed/overflowed exact sum must decline, not disappear through three-valued logic.
    signed
      .groupBy(keyNames.map(col): _*)
      .agg(sum(col("_ivm_proof_signed")).as("_ivm_proof_net"), sum(col("_ivm_proof_bad")).as("_ivm_proof_invalid"))
      .where("_ivm_proof_net IS NULL OR _ivm_proof_net <> 0 OR _ivm_proof_invalid IS NULL OR _ivm_proof_invalid <> 0")
      .isEmpty
  }

  /** This optimization is a proof, never a fallback/error classifier. Reuse the managed-MV
    * refresh locks already held by the caller and bind the actual Delta read to the recorded path.
    */
  def provesCancellation(
      spark: SparkSession,
      consumer: MvMetadata,
      batch: CdfChangeBatch,
      rankGroup: Boolean
  ): Boolean =
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

          val producerEligible = !rankGroup ||
            unalias(spark.sessionState.sqlParser.parsePlan(producer.querySql)).isInstanceOf[Distinct]

          producerEligible && schema.nonEmpty && schema.forall { field =>
            !CdfColumns.contains(field.name.toLowerCase(java.util.Locale.ROOT)) &&
            EligibleTypes.contains(field.dataType)
          } && fingerprint == consumer.sourceSchemaFingerprint && {
            val definition = spark.sql(consumer.querySql).queryExecution.analyzed
            if (rankGroup) deterministicRankOrCount(definition, plan, consumer.refreshType)
            else directConsumer(definition, plan)
          } &&
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
                val budgetOk = !rankGroup || {
                  val sizes = commits.map { case (_, actions) =>
                    actions.collect { case file: AddCDCFile => file.size }
                  }
                  sizes.forall(files => files.nonEmpty && files.forall(_ > 0L)) &&
                  sizes.flatten.foldLeft(BigInt(0))(_ + _) <= MaxRankGroupCdcBytes
                }
                budgetOk && commits.map(_._1) == (start to end) && commits.forall { case (_, actions) =>
                  !actions.exists {
                    case _: Metadata | _: Protocol => true
                    case _                         => false
                  } && actions.collectFirst { case info: CommitInfo => info.operation }.contains("MERGE")
                }
              }
              log.dataPath.toUri.normalize().toString.stripSuffix("/") == dependency.deltaLogDataPath &&
              (!rankGroup || (end - batch.startVersionExclusive <= MaxRankGroupVersions)) && currentEnd && mergeOnly && {
                val raw = spark.read
                  .format("delta")
                  .option("readChangeFeed", "true")
                  .option("startingVersion", start)
                  .option("endingVersion", end)
                  .load(producer.location)
                val public = MvProjectionSource.project(raw, schema, Seq(ChangeType))
                val canceled =
                  if (rankGroup) signedBagIsEmpty(public, schema.fieldNames.toSeq)
                  else {
                    def quoted(value: String): String = s"`${value.replace("`", "``")}`"
                    val columns                       = schema.fieldNames.toSeq.map(name => col(quoted(name)))
                    val adds    = public.where(col(ChangeType).isin(Positive: _*)).select(columns: _*)
                    val removes = public.where(col(ChangeType).isin(Negative: _*)).select(columns: _*)
                    raw.where(col(ChangeType).isNull || !col(ChangeType).isin((Positive ++ Negative): _*)).isEmpty &&
                    adds.exceptAll(removes).isEmpty && removes.exceptAll(adds).isEmpty
                  }
                canceled && currentEnd && identitiesAndVersionsMatch
              }
          }
        }
      }
    } catch { case NonFatal(_) => false }
}
