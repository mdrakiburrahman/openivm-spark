package org.openivm.spark.common

import java.nio.file.{Files, Path}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.types.{LongType, StructType}
import org.openivm.spark.common.rocksdb._
import org.openivm.spark.parser.GroupedWindowStagePlan
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.Try

/** Standalone component experiment, not a production refresh implementation. */
object SecurityMembershipProbe {
  private val Key = "sk_security_id"
  private val Weight = "__membership_weight"
  private val Prefix = RocksDBCodec.utf8("membership_probe:count:")
  private val EpochKey = RocksDBCodec.utf8("membership_probe:epoch")
  private val Cf = "default"
  private val Limit = 50000
  private val ByteLimit = 8L * 1024L * 1024L
  private final case class Prepared(before: Long, writes: Vector[(Long, Long)], positive: Vector[Long])

  private final class Membership(path: Path) {
    private val db = new OpenIvmRocksDB(path.toString, OpenIvmRocksDBConf.default, Seq(Cf))
    private def key(n: Long) = Prefix ++ RocksDBCodec.encodeLongBE(n)
    private def epoch: Option[Long] = db.get(Cf, EpochKey).map(RocksDBCodec.decodeLongBE)
    private def contributions(frame: DataFrame): Vector[(Long, Long)] = {
      require(frame.schema(Key).dataType == LongType, "POC supports Long keys only")
      val rows = frame.filter(col(Key).isNotNull).groupBy(col(Key))
        .agg(sum(col(Weight)).as(Weight)).filter(col(Weight) =!= 0L).limit(Limit + 1).collect().toVector
      require(rows.size <= Limit, "membership update cap")
      rows.map(row => row.getLong(0) -> row.getLong(1))
    }
    def bootstrap(frame: DataFrame): Vector[Long] = {
      val counts = contributions(frame.withColumn(Weight, lit(1L)))
      require(counts.forall(_._2 > 0L), "invalid membership seed")
      db.withBatch { batch =>
        require(epoch.isEmpty, "membership already initialized")
        counts.foreach { case (k, n) => OpenIvmRocksDBBatchOps.put(db, batch, Cf, key(k), RocksDBCodec.encodeLongBE(n)) }
        OpenIvmRocksDBBatchOps.put(db, batch, Cf, EpochKey, RocksDBCodec.encodeLongBE(0L))
      }
      counts.map(_._1)
    }
    def prepare(frame: DataFrame, before: Long): Prepared = {
      // Spark jobs stay outside the RocksDB session/writer lock.
      val changes = contributions(frame)
      db.withSession {
        require(epoch.contains(before), "membership source generation changed")
        val old = db.multiGet(Cf, changes.map(p => key(p._1)))
          .map(_.map(RocksDBCodec.decodeLongBE).getOrElse(0L))
        val writes = changes.zip(old).map { case ((k, delta), prior) =>
          val next = Math.addExact(prior, delta)
          require(next >= 0L, "negative membership count")
          k -> next
        }
        val all = db.boundedPrefixScan(Cf, Prefix, Limit, ByteLimit).map { case (k, value) =>
          RocksDBCodec.decodeLongBE(k.drop(Prefix.length)) -> RocksDBCodec.decodeLongBE(value)
        }.toMap ++ writes
        val positive = all.iterator.collect { case (k, n) if n > 0L => k }.toVector
        require(positive.size <= Limit, "membership output cap")
        Prepared(before, writes, positive)
      }
    }
    def publish(prepared: Prepared): Unit = {
      db.withBatch { batch =>
        require(epoch.contains(prepared.before), "stale membership preparation")
        prepared.writes.foreach { case (k, n) =>
          if (n == 0L) OpenIvmRocksDBBatchOps.delete(db, batch, Cf, key(k))
          else OpenIvmRocksDBBatchOps.put(db, batch, Cf, key(k), RocksDBCodec.encodeLongBE(n))
        }
        OpenIvmRocksDBBatchOps.put(db, batch, Cf, EpochKey, RocksDBCodec.encodeLongBE(Math.addExact(prepared.before, 1L)))
      }
      ()
    }
    def close(): Unit = db.close()
  }

  private def exact(spark: SparkSession, schema: StructType, expected: Array[Row], actual: Array[Row]): Unit = {
    val left = spark.createDataFrame(expected.toIndexedSeq.asJava, schema)
    val right = spark.createDataFrame(actual.toIndexedSeq.asJava, schema)
    require(left.exceptAll(right).count() == 0L && right.exceptAll(left).count() == 0L,
      "membership changed complete output bag")
  }
  private def members(spark: SparkSession, keys: Vector[Long]): DataFrame =
    spark.createDataset(keys)(Encoders.scalaLong).toDF(Key)

  private def gate(spark: SparkSession, path: Path): Unit = {
    val schema = new StructType().add(Key, LongType, true).add(Weight, LongType, false)
    def frame(rows: Seq[Row]) = spark.createDataFrame(rows.asJava, schema)
    val state = new Membership(path)
    try {
      val seed = state.bootstrap(frame(Seq(Row(1L, 1L), Row(1L, 1L), Row(null, 1L))))
      require(seed == Vector(1L), "duplicate/null membership seed")
      val one = state.prepare(frame(Seq(Row(1L, -1L), Row(2L, 1L))), 0L)
      require(one.positive.toSet == Set(1L, 2L), "one duplicate must still match")
      state.publish(one)
      require(Try(state.publish(one)).isFailure, "replayed preparation accepted")
      val last = state.prepare(frame(Seq(Row(1L, -1L), Row(null, 1L))), 1L)
      require(last.positive == Vector(2L), "last-match removal")
      state.publish(last)
      require(Try(state.prepare(frame(Seq(Row(2L, -2L))), 2L)).isFailure, "negative count accepted")
    } finally state.close()
    val reopened = new Membership(path)
    try {
      require(reopened.prepare(frame(Seq.empty), 2L).positive == Vector(2L), "state lost across reopen")
    } finally reopened.close()
  }

  private final case class Fixture(sources: Map[String, DataFrame], summary: DataFrame, delta: DataFrame)
  private val Trade = "default.gw_stats_trade"
  private val Security = "default.gw_stats_security"
  private val Watches = "default.gw_stats_watches"

  def main(args: Array[String]): Unit = {
    require(args.isEmpty, "membership probe takes no arguments")
    val root = Files.createTempDirectory("openivm-membership-spark-probe-")
    val spark = SparkSession.builder().master("local[4]").appName("SecurityMembershipProbe")
      .config("spark.ui.enabled", "false").config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
      .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val opened = mutable.ArrayBuffer.empty[Membership]
    try {
      gate(spark, root.resolve("gate"))
      val resource = Source.fromInputStream(getClass.getResourceAsStream("/queries/trade-volume-stats-window.sql"), "UTF-8")
      val query = try resource.mkString finally resource.close()
      val contract = GroupedWindowStagePlan.extract(spark, query, Vector(Trade, Security, Watches))
      val security = spark.range(1000).selectExpr("concat('S',id) AS symbol", "id AS sk_security_id", "id%100 AS sk_company_id")
      val fixtures = Vector(1000000L, 1010000L, 1010100L).zipWithIndex.map { case (end, batch) =>
        val trades = spark.range(0L, end, 1L, 8).selectExpr(
          "id%1000 AS sk_broker_id", "id%5000 AS sk_customer_id", "cast(floor(id/100)%1000 AS bigint) AS sk_security_id",
          "id%10000 AS sk_account_id", "cast(date_add(DATE '2024-01-01',cast(id%7 AS int)) AS timestamp) AS create_timestamp",
          "id%50+1 AS quantity", "cast(id%70 AS double)*0.25 AS trade_price",
          "cast(id%5 AS double)*0.125 AS commission", "cast(id%7 AS double)*0.25 AS fee")
        var right = spark.range(0L, 1000000L, 1L, 8).selectExpr("id%100 AS sk_security_id")
        (1 to batch).foreach { b => right = right.union(spark.range(2).select(lit(100L + b).as(Key))) }
        val tradePath = root.resolve(s"trade-$batch").toString
        val rightPath = root.resolve(s"watches-$batch").toString
        trades.write.parquet(tradePath); right.write.parquet(rightPath)
        val sources = Map(Trade -> spark.read.parquet(tradePath), Security -> security, Watches -> spark.read.parquet(rightPath))
        val grouped = contract.summary(sources)
        // Match the existing summary bridge equally in both alternatives.
        val payload = grouped.toJSON.collect().toVector
        val summary = spark.read.schema(grouped.schema).json(spark.createDataset(payload)(Encoders.STRING))
        val delta = spark.range(2).select(lit(100L + batch).as(Key), lit(1L).as(Weight))
        Fixture(sources, summary, delta)
      }
      val mapper = new ObjectMapper()
      val report = mapper.createObjectNode()
      report.put("scope", "Spark captured post-query plus membership-state component; not SF100 or production refresh")
      report.put("common_group_summary_excluded", true)
      report.put("target_writes_and_remote_backup_included", false)
      report.put("previous_commit_and_cached_non_ivm_comparison", "unavailable")
      report.put("spark_version", spark.version)
      report.put("rocksdb_metrics_enabled", org.openivm.spark.telemetry.metrics.OpenIvmMetrics.enabled)
      report.put("local_cores", 4)
      report.put("host_available_processors", Runtime.getRuntime.availableProcessors())
      report.put("host_load_average_start", java.lang.management.ManagementFactory.getOperatingSystemMXBean.getSystemLoadAverage)
      report.put("rounds", 4); report.put("warmup_rounds", 1)
      val samples = report.putArray("samples")
      var checks = 0
      for ((mode, partitions, threshold) <- Vector(("fabric-like", 200, 26214400L), ("spark-like", 16, 104857600L))) {
        spark.conf.set("spark.sql.shuffle.partitions", partitions)
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
        // Full-query shape and state lifecycle must pass before this mode is timed.
        val gateState = new Membership(root.resolve(s"$mode-full-query-gate")); opened += gateState
        fixtures.zipWithIndex.foreach { case (f, batch) =>
          val keys = if (batch == 0) gateState.bootstrap(f.sources(Watches))
            else { val p = gateState.prepare(f.delta, batch - 1L); gateState.publish(p); p.positive }
          val baseline = contract.post(f.sources, f.summary)
          val candidate = contract.post(f.sources.updated(Watches, members(spark, keys)), f.summary)
          exact(spark, baseline.schema, baseline.collect(), candidate.collect()); checks += 2
        }
        gateState.close()
        for (round <- 0 until 4) {
          var state: Membership = null
          fixtures.zipWithIndex.foreach { case (f, batch) =>
            var baselineRows: Array[Row] = null
            var candidateRows: Array[Row] = null
            val order = if (round % 2 == 0) Vector("baseline", "candidate") else Vector("candidate", "baseline")
            order.foreach { label =>
              var executed: DataFrame = null
              val started = System.nanoTime()
              if (label == "baseline") {
                executed = contract.post(f.sources, f.summary)
                baselineRows = executed.collect()
              }
              else {
                val prepared: Either[Vector[Long], Prepared] = if (batch == 0) {
                  state = new Membership(root.resolve(s"$mode-round-$round")); opened += state
                  Left(state.bootstrap(f.sources(Watches)))
                } else Right(state.prepare(f.delta, batch - 1L))
                val keys = prepared.fold(identity, _.positive)
                executed = contract.post(f.sources.updated(Watches, members(spark, keys)), f.summary)
                candidateRows = executed.collect()
                prepared.foreach(state.publish)
                if (batch == 2) state.close()
              }
              val seconds = (System.nanoTime() - started).toDouble / 1e9
              val sample = samples.addObject()
              sample.put("mode", mode); sample.put("round", round); sample.put("batch", batch + 1)
              sample.put("label", label); sample.put("seconds", seconds)
              if (round == 0 && batch == 1) sample.put("executed_plan", executed.queryExecution.executedPlan.toString)
            }
            exact(spark, contract.post(f.sources, f.summary).schema, baselineRows, candidateRows); checks += 2
          }
        }
      }
      report.put("exact_except_all_checks", checks)
      report.put("host_load_average_end", java.lang.management.ManagementFactory.getOperatingSystemMXBean.getSystemLoadAverage)
      // One structured benchmark record; no diagnostic test logging.
      println("MEMBERSHIP_POC_JSON=" + mapper.writeValueAsString(report))
    } finally {
      opened.foreach(_.close())
      spark.stop(); SparkSession.clearActiveSession(); SparkSession.clearDefaultSession()
      val walk = Files.walk(root)
      try walk.iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists)
      finally walk.close()
    }
  }
}
