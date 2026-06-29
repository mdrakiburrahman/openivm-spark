package org.openivm.spark.common

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}

sealed trait BatchVerdict
object BatchVerdict {
  case object Noop       extends BatchVerdict
  case object InsertOnly extends BatchVerdict
  case object Mutating   extends BatchVerdict
  case object Replace    extends BatchVerdict
}

sealed trait DeltaShape {
  def compileFactValue: String
}
object DeltaShape {
  case object InsertOnly extends DeltaShape {
    override val compileFactValue: String = "INSERT_ONLY"
  }
  case object Unchanged extends DeltaShape {
    override val compileFactValue: String = "UNCHANGED"
  }
  case object General extends DeltaShape {
    override val compileFactValue: String = "GENERAL"
  }
}

object DeltaCommitClassifier {

  import BatchVerdict._

  def latestVersion(spark: SparkSession, tableNameOrPath: String): Long =
    deltaLogFor(spark, tableNameOrPath).update().version

  def classify(spark: SparkSession, tableNameOrPath: String, lastConsumedVersion: Long): BatchVerdict = {
    val deltaLog = deltaLogFor(spark, tableNameOrPath)
    val latest   = deltaLog.update().version
    if (lastConsumedVersion >= latest) Noop
    else {
      val changes =
        deltaLog.getChanges(lastConsumedVersion + 1L, failOnDataLoss = false).filter(_._1 <= latest).toVector
      changes.map { case (_, actions) => classifyCommit(actions) }.foldLeft(Noop: BatchVerdict)(combine)
    }
  }

  def classifyShape(spark: SparkSession, tableNameOrPath: String, lastConsumedVersion: Long): DeltaShape =
    shapeOf(classify(spark, tableNameOrPath, lastConsumedVersion))

  def shapeOf(verdict: BatchVerdict): DeltaShape =
    verdict match {
      case Noop       => DeltaShape.Unchanged
      case InsertOnly => DeltaShape.InsertOnly
      case Mutating   => DeltaShape.General
      case Replace    => DeltaShape.General
    }

  private def deltaLogFor(spark: SparkSession, tableNameOrPath: String): DeltaLog =
    if (looksLikePath(tableNameOrPath)) DeltaLog.forTable(spark, new Path(tableNameOrPath))
    else DeltaLog.forTable(spark, CatalystSqlParser.parseTableIdentifier(tableNameOrPath))

  private def looksLikePath(tableNameOrPath: String): Boolean =
    tableNameOrPath.startsWith("/") || tableNameOrPath.startsWith("file:") || tableNameOrPath.contains("/")

  private def combine(left: BatchVerdict, right: BatchVerdict): BatchVerdict =
    if (rank(left) >= rank(right)) left else right

  private def rank(verdict: BatchVerdict): Int = verdict match {
    case Noop       => 0
    case InsertOnly => 1
    case Mutating   => 2
    case Replace    => 3
  }

  private[common] def classifyCommit(actions: Seq[Action]): BatchVerdict = {
    val commitInfo = actions.collectFirst { case info: CommitInfo => info }
    val operation  = commitInfo.map(_.operation).getOrElse("").trim.toUpperCase
    val parameters = commitInfo.map(_.operationParameters).getOrElse(Map.empty[String, String])

    if (isReplace(operation, parameters)) {
      // Replaces invalidate incremental semantics across the full target.
      Replace
    } else if (hasDataChangeRemove(actions) || hasDeletionVectorAdd(actions) || isMutatingOperation(operation)) {
      // Any row removal or deletion-vector rewrite must take the mutating path.
      Mutating
    } else if (isNoDataChangeOnly(actions, operation)) {
      // Metadata-only and dataChange=false maintenance commits leave rows unchanged.
      Noop
    } else if (isProvenAppend(actions, commitInfo, operation, parameters)) {
      // Append-only is emitted only for WRITE append / blind-append commits.
      InsertOnly
    } else {
      // Unknown data-changing operations are conservatively mutating.
      Mutating
    }
  }

  private def isReplace(operation: String, parameters: Map[String, String]): Boolean = {
    val mode = parameters.get("mode").orElse(parameters.get("Mode")).map(_.trim.toUpperCase)
    operation.contains("REPLACE") || operation == "TRUNCATE" || mode.exists(mode =>
      mode == "OVERWRITE" || mode == "REPLACE"
    )
  }

  private def isMutatingOperation(operation: String): Boolean =
    operation == "DELETE" || operation == "UPDATE" || operation == "MERGE"

  private def hasDataChangeRemove(actions: Seq[Action]): Boolean =
    actions.exists { case remove: RemoveFile => remove.dataChange; case _ => false }

  private def hasDeletionVectorAdd(actions: Seq[Action]): Boolean =
    actions.exists { case add: AddFile => add.dataChange && add.deletionVector != null; case _ => false }

  private def isNoDataChangeOnly(actions: Seq[Action], operation: String): Boolean =
    hasNoDataChange(actions) && (isKnownNoopOperation(operation) || hasNoFileActions(actions))

  private def hasNoDataChange(actions: Seq[Action]): Boolean =
    actions.forall {
      case add: AddFile       => !add.dataChange
      case remove: RemoveFile => !remove.dataChange
      case _                  => true
    }

  private def hasNoFileActions(actions: Seq[Action]): Boolean =
    actions.forall {
      case _: AddFile    => false
      case _: RemoveFile => false
      case _             => true
    }

  private def isKnownNoopOperation(operation: String): Boolean =
    operation == "OPTIMIZE" || operation == "VACUUM START" || operation == "VACUUM END" || operation.contains(
      "TBLPROPERTIES"
    )

  private def isProvenAppend(
      actions: Seq[Action],
      commitInfo: Option[CommitInfo],
      operation: String,
      parameters: Map[String, String]
  ): Boolean = {
    val hasDataChangeAdd = actions.exists { case add: AddFile => add.dataChange; case _ => false }
    val mode             = parameters.get("mode").orElse(parameters.get("Mode")).map(_.trim.toUpperCase)
    val writeAppend      = operation == "WRITE" && mode.forall(_ == "APPEND")
    val blindAppend      = commitInfo.exists(_.isBlindAppend.contains(true))

    hasDataChangeAdd && !hasDataChangeRemove(actions) && (blindAppend || writeAppend)
  }
}
