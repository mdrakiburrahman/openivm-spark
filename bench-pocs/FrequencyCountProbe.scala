package org.openivm.spark.common

import java.nio.file.Files
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry
import scala.jdk.CollectionConverters._

/** Paired grouped-preparation experiment; not SF100, dbt or the full rank query. */
object FrequencyCountProbe {
  private final case class Completion(
      summary: () => DataFrame,
      hits: Int,
      misses: Int,
      strategy: String,
      publish: (String, Array[Byte]) => Unit
  )
  private final case class Driver(
      bootstrap: () => Unit,
      prepare: (DataFrame, DataFrame, String) => Completion,
      close: () => Unit
  )
  private def exact(left: DataFrame, right: DataFrame): Unit = {
    val aligned = left.select(right.columns.map(col): _*)
    require(aligned.exceptAll(right).count() == 0L && right.exceptAll(aligned).count() == 0L,
      "grouped preparation changed the output bag")
  }
  def main(args: Array[String]): Unit = {
    require(args.isEmpty, "frequency-count probe takes no arguments")
    val root = Files.createTempDirectory("openivm-frequency-count-probe-")
    val spark = SparkSession.builder().master("local[4]").appName("FrequencyCountProbe")
      .config("spark.ui.enabled", "false").config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
      .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      def trades(start: Long, end: Long, groups: Int): DataFrame = spark.range(start, end, 1, 4).selectExpr(
        s"cast(id % $groups as bigint) as k", s"cast(id % $groups + $start as bigint) as customer",
        s"cast(id % $groups + $start as bigint) as security", s"cast(id % $groups + $start as bigint) as account",
        s"date_add(DATE '2020-01-01', cast(id % $groups + $start / 1000 as int)) as day",
        "cast(id % 100 as double) * 0.5 as amount")
      val brokers = spark.range(64).selectExpr("id as k", "id+100 as broker_id", "concat('broker',id) as first_name",
        "case when id % 7 = 0 then cast(null as string) else concat('name',id) end as last_name")
      trades(0, 1000000, 64).write.parquet(root.resolve("initial").toString)
      trades(1000000, 1010000, 39).write.parquet(root.resolve("add2").toString)
      trades(1010000, 1010100, 1).write.parquet(root.resolve("add3").toString)
      def read(name: String) = spark.read.parquet(root.resolve(name).toString)
      def joined(df: DataFrame) = df.join(brokers, Seq("k"))
      val keys = Vector("k", "broker_id", "first_name", "last_name")
      val channels = Vector("customer" -> "customers", "security" -> "securities", "account" -> "accounts", "day" -> "days")
      def ordinary(df: DataFrame) = df.groupBy(keys.map(col): _*)
        .agg(count(lit(1L)).as("n"), sum("amount").as("total"), avg("amount").as("mean"))
      def summary(df: DataFrame) = df.groupBy(keys.map(col): _*).agg(
        count(lit(1L)).as("n"), sum("amount").as("total"), avg("amount").as("mean"),
        countDistinct("customer").as("customers"), countDistinct("security").as("securities"),
        countDistinct("account").as("accounts"), countDistinct("day").as("days"))
      val base = joined(read("initial"))
      val current2 = joined(read("initial").unionByName(read("add2")))
      val current3 = joined(read("initial").unionByName(read("add2")).unionByName(read("add3")))
      val delta2 = joined(read("add2")).withColumn("openivm_multiplicity", lit(1L))
      val delta3 = joined(read("add3")).withColumn("openivm_multiplicity", lit(1L))
      val initialSummary = summary(base)
      def frozen(df: DataFrame): DataFrame = spark.createDataFrame(df.collect().toVector.asJava, df.schema)
      val expected2 = frozen(summary(current2))
      val expected3 = frozen(summary(current3))
      def driver(label: String, mode: String, round: Int): Driver = {
        val view = s"default.frequency_probe_${label}_${mode}_$round"
        if (label == "baseline") {
          val state = new ReferenceGroupedDistinctState(spark, view, "frequency-probe", base.schema,
            initialSummary.schema, keys, "n", channels)
          Driver(() => state.prepareBootstrap(initialSummary, "0").publish(Array[Byte](0)),
            (delta, current, epoch) => {
              val next = state.prepare(delta, current, epoch)(ordinary)
              Completion(() => next.summary, next.hits, next.misses, "expanded_frequencies", (e, c) => next.publish(e, c))
            }, () => state.close())
        } else {
          val state = new GroupedDistinctState(spark, view, "frequency-probe", base.schema,
            initialSummary.schema, keys, "n", channels)
          Driver(() => state.prepareBootstrap(initialSummary, "0").publish(Array[Byte](0)),
            (delta, current, epoch) => {
              val next = state.prepare(delta, current, epoch)(ordinary)
              Completion(() => next.summary, next.hits, next.misses, next.lookupStrategy, (e, c) => next.publish(e, c))
            }, () => state.close())
        }
      }
      val report = new ObjectMapper().createObjectNode()
      report.put("scope", "paired grouped preparation only; not SF100, dbt or the full rank query")
      report.put("reference_commit", "7383ad194edbe2003a26f6d44c278e9b2f34ca5b")
      report.put("spark_version", spark.version)
      report.put("initial_rows", 1000000)
      report.put("local_cores", 4)
      report.put("warmup_rounds_per_mode", 1)
      report.put("total_rounds_per_mode", 5)
      report.put("cached_non_ivm_counterpart", "unavailable")
      report.put("target_writes_and_remote_backup_included", false)
      val samples = report.putArray("samples")
      var checks = 0
      val modes = Vector(("fabric_like", 200, 26214400L), ("spark_like", 16, 104857600L))
      for (round <- 0 until 5) {
        val modeOrder = if (round % 2 == 0) modes else modes.reverse
        modeOrder.foreach { case (mode, partitions, threshold) =>
          spark.conf.set("spark.sql.shuffle.partitions", partitions)
          spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
          val order = if (round % 2 == 0) Vector("baseline", "candidate") else Vector("candidate", "baseline")
          order.foreach { label =>
            val state = driver(label, mode, round)
            try {
              val boot = System.nanoTime()
              state.bootstrap()
              val seed = samples.addObject()
              seed.put("mode", mode); seed.put("round", round); seed.put("batch", 1); seed.put("label", label)
              seed.put("bootstrap_seconds", (System.nanoTime() - boot).toDouble / 1e9)
              Vector((2, delta2, current2, "0", expected2), (3, delta3, current3, "1", expected3)).foreach {
                case (batch, delta, current, epoch, expected) =>
                  val job = s"frequency-$round-$mode-$label-$batch"
                  spark.sparkContext.setJobGroup(job, job)
                  val start = System.nanoTime()
                  val next = state.prepare(delta, current, epoch)
                  val seconds = (System.nanoTime() - start).toDouble / 1e9
                  val jobs = spark.sparkContext.statusTracker.getJobIdsForGroup(job).length
                  spark.sparkContext.clearJobGroup()
                  if (label == "candidate") require(next.strategy == "conditional_counts", "optimized path not used")
                  val actual = next.summary()
                  exact(actual, expected); checks += 2
                  val commit = System.nanoTime()
                  next.publish(if (batch == 2) "1" else "2", Array[Byte](batch.toByte))
                  val sample = samples.addObject()
                  sample.put("mode", mode); sample.put("round", round); sample.put("batch", batch); sample.put("label", label)
                  sample.put("preparation_seconds", seconds); sample.put("jobs", jobs)
                  sample.put("state_publish_seconds", (System.nanoTime() - commit).toDouble / 1e9)
                  sample.put("hits", next.hits); sample.put("misses", next.misses); sample.put("lookup", next.strategy)
              }
            } finally state.close()
          }
        }
      }
      report.put("exact_except_all_checks", checks)
      println("FREQUENCY_COUNT_PROBE_JSON=" + report.toString)
    } finally {
      OpenIvmRocksDBRegistry.closeAll()
      spark.stop(); SparkSession.clearActiveSession(); SparkSession.clearDefaultSession()
      val walk = Files.walk(root)
      try walk.iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists)
      finally walk.close()
    }
  }
}
