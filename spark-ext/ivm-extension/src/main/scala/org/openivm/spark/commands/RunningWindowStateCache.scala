package org.openivm.spark.commands

import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.functions.{coalesce, col, greatest, lit, struct, to_json}
import org.apache.spark.sql.types.StructType
import org.openivm.spark.common.{DeltaTableVersion, MvMetadata, WindowStateCatalog}

/** Reuses the prototype's keyed JSON state store for the native suffix program.
  * Only tail-value seeds are cached; other state layouts keep native history
  * scans. Bootstrap is lazy, so CREATE adds no history scan.
  */
private[commands] object RunningWindowStateCache {
  private val Max     = "openivm_running_max"
  private val Count   = "openivm_running_count"
  private val Nulls   = "openivm_running_nulls"
  private val Headers = Set(Max, Count, Nulls)
  private val JsonOptions = Map(
    "ignoreNullFields"   -> "false",
    "timeZone"           -> "UTC",
    "timestampFormat"    -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
    "timestampNTZFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "mode"               -> "FAILFAST"
  )

  private def quoted(name: String): String = s"`${name.replace("`", "``")}`"
  private def json(columns: Seq[String]): Column =
    to_json(struct(columns.map(name => col(quoted(name))): _*), JsonOptions)
  private def fromJson(spark: SparkSession, payloads: Seq[String], schema: StructType): DataFrame =
    spark.read.schema(schema).options(JsonOptions).json(spark.createDataset(payloads)(Encoders.STRING))

  def load(spark: SparkSession, meta: MvMetadata, stateSql: String): Option[State] = {
    val stateName    = s"openivm_run_state_${meta.name.table}"
    val affectedName = s"openivm_run_affected_${meta.name.table}"
    val prefix       = s"CREATE OR REPLACE TEMPORARY VIEW $stateName AS"
    val snapshotRef  = s"delta.${quoted(meta.location)} VERSION AS OF ${meta.lastVersion}"
    if (!stateSql.startsWith(prefix) || !stateSql.contains(snapshotRef)) return None
    val body       = stateSql.substring(prefix.length).trim.stripSuffix(";")
    val historical = spark.sql(body)
    val affected   = spark.table(quoted(affectedName))
    val keys       = affected.columns.toSeq
    val schema     = historical.schema
    val seeds      = schema.fieldNames.toSet -- keys -- Headers
    // These native seeds select the final cumulative output, rather than
    // aggregating raw input. SUM/COUNT/AVG auxiliary layouts cannot be updated
    // by substituting suffix output rows and deliberately retain the scan.
    val supported = historical.queryExecution.analyzed
      .collectFirst { case aggregate: Aggregate =>
        aggregate.aggregateExpressions.filter(e => seeds.contains(e.name)).forall { expression =>
          val functions = expression.collect { case a: AggregateExpression => a.aggregateFunction.prettyName }
          functions.nonEmpty && functions.forall(f => f == "max" || f == "max_by")
        }
      }
      .contains(true)
    if (keys.isEmpty || seeds.isEmpty || !Headers.subsetOf(schema.fieldNames.toSet) || !supported) return None
    val snapshot = DeltaTableVersion.deltaLogOption(spark, meta.location).get.update()
    if (snapshot.version != meta.lastVersion) return None
    val identity      = Seq(snapshot.metadata.id, body.replace(snapshotRef, "MV"), schema.json).mkString("\n")
    val view          = MvCommandHelper.metaName(meta.name)
    val partitionKeys = affected.select(json(keys)).collect().map(_.getString(0)).toVector
    val cached        = WindowStateCatalog.getMany(spark, view, identity, meta.lastVersion, partitionKeys)
    if (cached.nonEmpty) {
      val restored = fromJson(spark, cached.values.toSeq, schema)
      val missing  = partitionKeys.filterNot(cached.contains)
      val state =
        if (missing.isEmpty) restored
        else {
          val missingName = s"openivm_run_missing_${meta.name.table}"
          fromJson(spark, missing, affected.schema).createOrReplaceTempView(missingName)
          // Capture the analyzed plan before dropping the temporary key relation.
          val uncached =
            try {
              val df = spark.sql(body.replace(affectedName, missingName))
              df.queryExecution.analyzed
              df
            } finally spark.catalog.dropTempView(missingName)
          restored.unionByName(uncached)
        }
      state.createOrReplaceTempView(stateName)
    }
    Some(new State(spark, meta, body, snapshotRef, stateName, view, identity, keys, partitionKeys, cached.size, schema))
  }

  final class State private[RunningWindowStateCache] (
      spark: SparkSession,
      meta: MvMetadata,
      body: String,
      snapshotRef: String,
      stateName: String,
      view: String,
      identity: String,
      keys: Seq[String],
      partitionKeys: Seq[String],
      val hits: Int,
      schema: StructType
  ) {
    val requested: Int = partitionKeys.size

    /** The result is already materialized before any target write. Aggregate
      * only that suffix, retaining native tail/null semantics; add historical
      * row counters so ROWS positions and future append bounds remain correct.
      */
    def prepareUpdate(): Update = {
      val suffix =
        spark.sql(body.replace(snapshotRef, quoted(s"openivm_run_result_${meta.name.table}"))).alias("suffix")
      val old                                        = spark.table(quoted(stateName)).alias("old")
      def field(alias: String, name: String): Column = col(s"$alias.${quoted(name)}")
      val joined =
        suffix.join(old, keys.map(k => field("suffix", k).eqNullSafe(field("old", k))).reduce(_ && _), "left")
      val updated = joined.select(schema.fields.toIndexedSeq.map { f =>
        val value = f.name match {
          case Count | Nulls => field("suffix", f.name) + coalesce(field("old", f.name), lit(0L))
          case Max           => greatest(field("suffix", f.name), field("old", f.name))
          case _             => field("suffix", f.name)
        }
        value.cast(f.dataType).as(f.name)
      }: _*)
      val entries = updated
        .select(json(keys), json(schema.fieldNames.toSeq))
        .collect()
        .map { row =>
          row.getString(0) -> row.getString(1)
        }
        .toMap
      new Update(spark, view, identity, meta.lastVersion, partitionKeys, entries)
    }
  }

  final class Update private[RunningWindowStateCache] (
      spark: SparkSession,
      view: String,
      identity: String,
      previousVersion: Long,
      affectedKeys: Seq[String],
      entries: Map[String, String]
  ) {
    def commit(version: Long): Unit = {
      // A suffix-only program makes exactly one target append. Extra commits
      // (including a concurrent external write) invalidate this generation.
      if (version == previousVersion + 1L)
        WindowStateCatalog.putAll(spark, view, identity, previousVersion, version, affectedKeys, entries)
    }
  }
}
