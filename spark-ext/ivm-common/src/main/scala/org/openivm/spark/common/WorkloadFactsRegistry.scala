package org.openivm.spark.common

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.StructType

final case class ForeignKeyRelation(
    childTable: String,
    childColumns: Seq[String],
    parentTable: String,
    parentColumns: Seq[String],
    rely: Boolean = true
)

final case class UniqueKey(table: String, columns: Seq[String], rely: Boolean = true)

final case class DeltaConstraint(table: String, name: String, expression: String)

final case class GeneratedColumn(table: String, column: String, expression: String)

final case class WorkloadConstraintFacts(
    fkRelations: Seq[ForeignKeyRelation] = Seq.empty,
    uniqueKeys: Seq[UniqueKey] = Seq.empty,
    deltaConstraints: Seq[DeltaConstraint] = Seq.empty,
    generatedColumns: Seq[GeneratedColumn] = Seq.empty
)

final class WorkloadFactsRegistry {
  import WorkloadFactsRegistry._

  def discover(
      spark: SparkSession,
      sourceTables: Seq[String],
      configuredFkRelations: Seq[ForeignKeyRelation] = Seq.empty,
      configuredUniqueKeys: Seq[UniqueKey] = Seq.empty
  ): WorkloadConstraintFacts = {
    val perTable = sourceTables.distinct.map(table => tableFacts(spark, table))
    WorkloadConstraintFacts(
      fkRelations =
        distinctFk(configuredFkRelations ++ sessionForeignKeys(spark, sourceTables) ++ perTable.flatMap(_.fkRelations)),
      uniqueKeys = distinctUnique(configuredUniqueKeys ++ perTable.flatMap(_.uniqueKeys)),
      deltaConstraints = perTable.flatMap(_.deltaConstraints).distinct,
      generatedColumns = perTable.flatMap(_.generatedColumns).distinct
    )
  }
}

object WorkloadFactsRegistry {
  private val ForeignKeyPrefix       = "spark.openivm.fk."
  private val ForeignKeyListKey      = "spark.openivm.fk"
  private val UniqueKeyKey           = "spark.openivm.unique_key"
  private val UniqueKeyPrefix        = "spark.openivm.unique_key."
  private val PrimaryKey             = "spark.openivm.pk"
  private val PrimaryKeyPrefix       = "spark.openivm.pk."
  private val DeltaConstraintPrefix  = "delta.constraints."
  private val GenerationMetadataKey  = "delta.generationExpression"
  private val IdentityStartPrefix    = "delta.identity.start"
  private val IdentityGeneratedValue = "IDENTITY"

  def forRefresh(): WorkloadFactsRegistry = new WorkloadFactsRegistry

  private[common] def tableFacts(spark: SparkSession, table: String): WorkloadConstraintFacts = {
    val catalogTable = resolveCatalogTable(spark, table)
    val properties   = tableProperties(spark, table, catalogTable)
    val schema       = tableSchema(spark, table)
    val generated    = schema.toSeq.flatMap(generatedColumn(table, _))
    val identityKeys = generated
      .filter(_.expression == IdentityGeneratedValue)
      .map(col => UniqueKey(table, Seq(col.column)))
    WorkloadConstraintFacts(
      fkRelations = parseForeignKeys(table, properties),
      uniqueKeys = parseUniqueKeys(table, properties) ++ identityKeys,
      deltaConstraints = parseDeltaConstraints(table, properties),
      generatedColumns = generated
    )
  }

  private[common] def parseForeignKeys(
      childTable: String,
      properties: Map[String, String]
  ): Seq[ForeignKeyRelation] = {
    val keyed = properties.toSeq.flatMap { case (rawKey, rawValue) =>
      val key = rawKey.trim
      if (key == ForeignKeyListKey) parseForeignKeyList(childTable, rawValue)
      else if (key.startsWith(ForeignKeyPrefix)) {
        val childSpec = key.stripPrefix(ForeignKeyPrefix)
        parseForeignKey(childTable, childSpec, rawValue)
      } else Seq.empty
    }
    distinctFk(keyed)
  }

  private[common] def parseUniqueKeys(table: String, properties: Map[String, String]): Seq[UniqueKey] =
    distinctUnique(
      properties.toSeq.flatMap { case (rawKey, rawValue) =>
        val key = rawKey.trim
        if (
          key == UniqueKeyKey || key == PrimaryKey || key.startsWith(UniqueKeyPrefix) || key
            .startsWith(PrimaryKeyPrefix)
        ) {
          splitColumns(rawValue).filter(_.nonEmpty).map(cols => UniqueKey(table, cols))
        } else Seq.empty
      }
    )

  private def parseForeignKeyList(childTable: String, rawValue: String): Seq[ForeignKeyRelation] =
    splitTopLevel(rawValue, ';').flatMap { part =>
      splitRelation(part).toSeq.flatMap { case (childSpec, parentSpec) =>
        parseForeignKey(childTable, childSpec, parentSpec)
      }
    }

  private def parseForeignKey(
      childTable: String,
      childSpec: String,
      parentSpec: String
  ): Seq[ForeignKeyRelation] = {
    val (child, parent) = splitRelation(parentSpec).getOrElse(childSpec -> parentSpec)
    for {
      childCols <- splitColumns(child)
      parentRef <- parseParentRef(parent)
      if childCols.size == parentRef._2.size
    } yield ForeignKeyRelation(childTable, childCols, parentRef._1, parentRef._2)
  }.toSeq

  private def splitRelation(raw: String): Option[(String, String)] = {
    val idx = Seq(raw.indexOf("->"), raw.indexOf("=")).filter(_ >= 0).sorted.headOption
    idx.map { i =>
      val sepWidth = if (raw.substring(i).startsWith("->")) 2 else 1
      raw.substring(0, i).trim -> raw.substring(i + sepWidth).trim
    }
  }

  private def parseParentRef(raw: String): Option[(String, Seq[String])] = {
    val trimmed = raw.trim
    val paren   = """^(.+)\((.+)\)$""".r
    trimmed match {
      case paren(table, cols) =>
        splitColumns(cols).map(table.trim -> _)
      case _ =>
        val lastDot = trimmed.lastIndexOf('.')
        if (lastDot <= 0 || lastDot == trimmed.length - 1) None
        else Some(trimmed.substring(0, lastDot).trim -> Seq(unquote(trimmed.substring(lastDot + 1).trim)))
    }
  }

  private def splitColumns(raw: String): Option[Seq[String]] = {
    val cleaned = raw.trim.stripPrefix("(").stripSuffix(")")
    val cols = splitTopLevel(cleaned, ',')
      .map(unquote)
      .filter(_.nonEmpty)
    if (cols.nonEmpty) Some(cols) else None
  }

  private def splitTopLevel(raw: String, delimiter: Char): Seq[String] =
    raw.split(delimiter).toSeq.map(_.trim).filter(_.nonEmpty)

  private def parseDeltaConstraints(table: String, properties: Map[String, String]): Seq[DeltaConstraint] =
    properties.toSeq
      .collect {
        case (key, value) if key.startsWith(DeltaConstraintPrefix) =>
          DeltaConstraint(table, key.stripPrefix(DeltaConstraintPrefix), value)
      }
      .sortBy(_.name)

  private[common] def generatedColumn(
      table: String,
      field: org.apache.spark.sql.types.StructField
  ): Option[GeneratedColumn] = {
    if (field.metadata.contains(GenerationMetadataKey)) {
      Some(GeneratedColumn(table, field.name, field.metadata.getString(GenerationMetadataKey)))
    } else {
      if (field.metadata.json.contains(IdentityStartPrefix))
        Some(GeneratedColumn(table, field.name, IdentityGeneratedValue))
      else None
    }
  }

  /**
   * Resolve the source table's `CatalogTable` exactly once.
   *
   * Every Hive metastore read runs inside Spark's globally synchronized Hive
   * client, so each redundant lookup is a serialized section shared by all
   * concurrent commands. `getTableMetadata` is the single call that both
   * [[catalogProperties]] and [[deltaProperties]] need, so it is issued once
   * and the result is threaded through.
   *
   * Returning `None` reproduces the previous per-helper `catch` fallbacks: a
   * `delta.`/path`` identifier fails `requireDbExists`, a temp view is not an
   * external-catalog table, and a missing table throws — in all three cases
   * the old code also degraded to the path-based/empty behaviour.
   */
  private def resolveCatalogTable(spark: SparkSession, table: String): Option[CatalogTable] =
    try Some(spark.sessionState.catalog.getTableMetadata(parseTableIdentifier(spark, table)))
    catch { case _: Throwable => None }

  private def tableProperties(
      spark: SparkSession,
      table: String,
      catalogTable: Option[CatalogTable]
  ): Map[String, String] =
    deltaProperties(spark, table, catalogTable) ++ catalogProperties(catalogTable)

  /**
   * `DeltaLog.forTable(spark, id)` resolves `id` through
   * `SessionCatalog.getTableMetadata` and then delegates to
   * `DeltaLog.forTable(spark, catalogTable)`, so passing the already-resolved
   * `CatalogTable` is the identical code path with the metastore round-trip
   * removed. The identifier form is kept for the unresolved case, where
   * `DeltaLog` still has to run its own `delta.`/path`` detection.
   */
  private def deltaProperties(
      spark: SparkSession,
      table: String,
      catalogTable: Option[CatalogTable]
  ): Map[String, String] =
    try {
      val log = catalogTable match {
        case Some(resolved) => DeltaLog.forTable(spark, resolved)
        case None           => DeltaLog.forTable(spark, parseTableIdentifier(spark, table))
      }
      log.update().metadata.configuration
    } catch {
      case _: Throwable =>
        try DeltaLog.forTable(spark, new Path(table)).update().metadata.configuration
        catch { case _: Throwable => Map.empty[String, String] }
    }

  private def catalogProperties(catalogTable: Option[CatalogTable]): Map[String, String] =
    catalogTable.fold(Map.empty[String, String])(_.properties)

  private def tableSchema(spark: SparkSession, table: String): StructType =
    try spark.table(table).schema
    catch { case _: Throwable => StructType(Nil) }

  private[common] def sessionForeignKeys(
      spark: SparkSession,
      sourceTables: Seq[String]
  ): Seq[ForeignKeyRelation] = {
    val sourceAliases = sourceTables.flatMap(table => Seq(table, shortName(table))).toSet
    val fromConf = spark.conf.getAll.toSeq.flatMap { case (rawKey, rawValue) =>
      val key = rawKey.trim
      if (key.startsWith(ForeignKeyPrefix)) {
        val childTable = key.stripPrefix(ForeignKeyPrefix)
        if (sourceAliases.contains(childTable)) {
          parseForeignKeyList(childTable, rawValue).filter(fk => sourceAliases.contains(fk.parentTable))
        } else Seq.empty
      } else Seq.empty
    }
    distinctFk(fromConf)
  }

  private def parseTableIdentifier(spark: SparkSession, table: String): TableIdentifier =
    spark.sessionState.sqlParser.parseTableIdentifier(table)

  private def shortName(table: String): String = table.split('.').lastOption.getOrElse(table)

  private def distinctFk(fks: Seq[ForeignKeyRelation]): Seq[ForeignKeyRelation] =
    fks
      .groupBy(fk => (fk.childTable, fk.childColumns, fk.parentTable, fk.parentColumns))
      .values
      .map(_.head)
      .toSeq
      .sortBy(fk => (fk.childTable, fk.childColumns.mkString(","), fk.parentTable, fk.parentColumns.mkString(",")))

  private def distinctUnique(keys: Seq[UniqueKey]): Seq[UniqueKey] =
    keys
      .groupBy(key => (key.table, key.columns))
      .values
      .map(_.head)
      .toSeq
      .sortBy(key => (key.table, key.columns.mkString(",")))

  private def unquote(raw: String): String =
    raw.trim.stripPrefix("`").stripSuffix("`").stripPrefix("\"").stripSuffix("\"")
}
