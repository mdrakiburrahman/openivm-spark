package org.openivm.spark.common

import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/** Builds the exact signed source delta between two immutable Delta snapshots. */
object SourceVersionDelta {

  private val log = LoggerFactory.getLogger(getClass)

  def validate(
      spark: SparkSession,
      batch: SourceVersionChangeBatch,
      expectedSchema: StructType
  ): StructType = {
    val latest = DeltaTableVersion.requireLatest(spark, batch.baseTable)
    if (batch.startVersionInclusive < 0L || batch.endVersionInclusive < 0L)
      throw new IllegalArgumentException(s"Delta source versions must be non-negative for '${batch.baseTable}'")
    if (batch.endVersionInclusive < batch.startVersionInclusive)
      throw new IllegalArgumentException(
        s"Delta source '${batch.baseTable}' cannot move backwards from " +
          s"${batch.startVersionInclusive} to ${batch.endVersionInclusive}"
      )
    if (batch.endVersionInclusive > latest)
      throw new IllegalArgumentException(
        s"Delta source '${batch.baseTable}' has latest version $latest, " +
          s"so requested version ${batch.endVersionInclusive} does not exist"
      )

    val oldSchema = schemaAt(spark, batch.baseTable, batch.startVersionInclusive)
    val newSchema = schemaAt(spark, batch.baseTable, batch.endVersionInclusive)
    if (oldSchema != newSchema)
      throw new IllegalStateException(
        s"Delta source '${batch.baseTable}' changes schema between versions " +
          s"${batch.startVersionInclusive} and ${batch.endVersionInclusive}; " +
          "source repinning requires identical endpoint schemas"
      )
    if (oldSchema != expectedSchema)
      throw new IllegalStateException(
        s"Delta source '${batch.baseTable}' snapshot schema does not match the materialized view's persisted schema"
      )
    oldSchema
  }

  def schemaAt(spark: SparkSession, source: String, version: Long): StructType =
    snapshot(spark, source, version).schema

  def registerSourceDeltaView(
      spark: SparkSession,
      batch: SourceVersionChangeBatch,
      sourceSchema: StructType
  ): String = {
    validate(spark, batch, sourceSchema)
    if (CdfChangePropagation.tableHasCdf(spark, batch.baseTable)) {
      try registerCdfView(spark, batch, sourceSchema)
      catch {
        case NonFatal(cdfError) if supportsBagDiff(sourceSchema) =>
          log.warn(
            s"[openivm-mv] bounded CDF read failed for '${batch.baseTable}' " +
              s"${batch.startVersionInclusive}->${batch.endVersionInclusive}; using exact EXCEPT ALL bag diff",
            cdfError
          )
          registerBagDiffView(spark, batch, sourceSchema)
        case NonFatal(cdfError) =>
          throw new IllegalStateException(
            s"Cannot advance '${batch.baseTable}' from version ${batch.startVersionInclusive} to " +
              s"${batch.endVersionInclusive}: bounded CDF is unavailable and the source schema contains MapType, " +
              "which Spark cannot compare with EXCEPT ALL for an exact bag diff",
            cdfError
          )
      }
    } else if (supportsBagDiff(sourceSchema)) {
      registerBagDiffView(spark, batch, sourceSchema)
    } else {
      throw new IllegalStateException(
        s"Cannot advance '${batch.baseTable}' from version ${batch.startVersionInclusive} to " +
          s"${batch.endVersionInclusive}: Delta CDF is disabled and the source schema contains MapType, " +
          "which Spark cannot compare with EXCEPT ALL for an exact bag diff"
      )
    }
  }

  def buildBagDiffSql(batch: SourceVersionChangeBatch, sourceSchema: StructType): String = {
    val source    = quoteMultipart(batch.baseTable)
    val viewName  = quoteIdent(StagingDeltaView.deltaViewName(batch.baseTable))
    val columns   = sourceSchema.fieldNames.map(quoteIdent)
    val colList   = columns.mkString(", ")
    val oldRows   = s"SELECT $colList FROM $source VERSION AS OF ${batch.startVersionInclusive}"
    val newRows   = s"SELECT $colList FROM $source VERSION AS OF ${batch.endVersionInclusive}"
    val retracted = s"$oldRows\nEXCEPT ALL\n$newRows"
    val added     = s"$newRows\nEXCEPT ALL\n$oldRows"
    s"""CREATE OR REPLACE TEMP VIEW $viewName AS
       |SELECT $colList,
       |       CURRENT_TIMESTAMP() AS `openivm_timestamp`,
       |       CAST(-1 AS INT) AS `openivm_multiplicity`
       |FROM ($retracted) `openivm_repin_old`
       |UNION ALL
       |SELECT $colList,
       |       CURRENT_TIMESTAMP() AS `openivm_timestamp`,
       |       CAST(1 AS INT) AS `openivm_multiplicity`
       |FROM ($added) `openivm_repin_new`""".stripMargin
  }

  def supportsBagDiff(schema: StructType): Boolean = {
    def containsMap(dataType: DataType): Boolean = dataType match {
      case _: MapType         => true
      case struct: StructType => struct.fields.exists(field => containsMap(field.dataType))
      case array: ArrayType   => containsMap(array.elementType)
      case _                  => false
    }
    !schema.fields.exists(field => containsMap(field.dataType))
  }

  private def registerCdfView(
      spark: SparkSession,
      batch: SourceVersionChangeBatch,
      sourceSchema: StructType
  ): String = {
    val from = batch.startVersionInclusive + 1L
    val raw = spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", from)
      .option("endingVersion", batch.endVersionInclusive)
      .table(batch.baseTable)
      .filter(col("_change_type").isin("insert", "delete", "update_preimage", "update_postimage"))

    val sourceColumns = sourceSchema.fieldNames.map(name => col(quoteIdent(name)))
    val outputColumns = sourceColumns ++ Seq(
      col("_commit_timestamp").cast(TimestampType).as("openivm_timestamp"),
      when(col("_change_type").isin("insert", "update_postimage"), lit(1))
        .otherwise(lit(-1))
        .cast(IntegerType)
        .as("openivm_multiplicity")
    )
    raw.select(outputColumns: _*).createOrReplaceTempView(StagingDeltaView.deltaViewName(batch.baseTable))
    s"/* bounded Delta CDF ${batch.baseTable} versions $from..${batch.endVersionInclusive} */"
  }

  private def registerBagDiffView(
      spark: SparkSession,
      batch: SourceVersionChangeBatch,
      sourceSchema: StructType
  ): String = {
    val sql = buildBagDiffSql(batch, sourceSchema)
    spark.sql(sql)
    sql
  }

  private def snapshot(spark: SparkSession, source: String, version: Long): DataFrame =
    spark.read.format("delta").option("versionAsOf", version).table(source)

  private def quoteMultipart(name: String): String =
    name.split("\\.").map(quoteIdent).mkString(".")

  private def quoteIdent(name: String): String =
    s"`${name.replace("`", "``")}`"
}
