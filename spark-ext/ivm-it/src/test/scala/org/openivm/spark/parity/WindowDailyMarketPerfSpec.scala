package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, RefreshSqlLogCatalog}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}
import org.apache.spark.sql.functions.{col, lit, when}

class WindowDailyMarketPerfSpec extends IvmParitySpecBase("window-daily-market-perf") with CdfMode {

  private val strategy           = sys.props.getOrElse("openivm.perf.strategy", "replace")
  private val partitionCount     = sys.props.getOrElse("openivm.perf.partitions", "2048").toInt
  private val rowsPerPartition   = sys.props.getOrElse("openivm.perf.rowsPerPartition", "8192").toInt
  private val affectedPartitions = sys.props.getOrElse("openivm.perf.affectedPartitions", "32").toInt
  private val mutation           = sys.props.getOrElse("openivm.perf.mutation", "delete")

  override protected def extraSparkConf: Map[String, String] =
    Map(
      "spark.master"                                -> "local[16]",
      "spark.sql.shuffle.partitions"                -> "16",
      FeatureGate.QueryLogEnabledKey                -> "true",
      FeatureGate.WindowSinglePassReplaceEnabledKey -> (strategy == "replace").toString,
      FeatureGate.WindowCascadeMergeEnabledKey      -> "true",
      FeatureGate.WindowSnapshotCacheEnabledKey     -> "false"
    )

  describe("TPC-DI daily-market refresh performance") {
    it("benchmarks one isolated refresh shape and verifies bag equality") {
      val strategySlug = strategy.replace('-', '_')
      val source       = s"dm_perf_${strategySlug}_source"
      val target       = s"dm_perf_${strategySlug}_target"
      sql(
        s"CREATE TABLE $source(" +
          "dm_date DATE, dm_s_symb STRING, dm_close DECIMAL(18,4), " +
          "dm_high DECIMAL(18,4), dm_low DECIMAL(18,4), dm_vol BIGINT) USING DELTA"
      )
      sql(
        s"""INSERT INTO $source
           |SELECT DATE_ADD(DATE '2000-01-01', CAST(id % $rowsPerPartition AS INT)),
           |       CONCAT('S', LPAD(CAST(CAST(id / $rowsPerPartition AS BIGINT) AS STRING), 6, '0')),
           |       CAST(10 + (id % 17) AS DECIMAL(18,4)),
           |       CAST(20 + (id % 19) AS DECIMAL(18,4)),
           |       CAST(5 + (id % 13) AS DECIMAL(18,4)),
           |       CAST(100 + id AS BIGINT)
           |FROM RANGE(${partitionCount.toLong * rowsPerPartition})""".stripMargin
      ).collect()

      val viewSql =
        "WITH cumulative AS (" +
          "SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, " +
          "MIN(dm_low) OVER w AS fifty_two_week_low, " +
          "MAX(dm_high) OVER w AS fifty_two_week_high " +
          s"FROM $source " +
          "WINDOW w AS (PARTITION BY dm_s_symb ORDER BY dm_date)" +
          "), flagged AS (" +
          "SELECT *, CASE WHEN dm_low = fifty_two_week_low THEN dm_date END AS low_date_flag, " +
          "CASE WHEN dm_high = fifty_two_week_high THEN dm_date END AS high_date_flag FROM cumulative" +
          ") SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, " +
          "fifty_two_week_low, fifty_two_week_high, " +
          "MAX(low_date_flag) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS fifty_two_week_low_date, " +
          "MAX(high_date_flag) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS fifty_two_week_high_date " +
          "FROM flagged"

      if (Set("full", "direct-replace", "target-cdf").contains(strategy)) {
        val properties =
          if (strategy == "target-cdf") " TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')" else ""
        sql(s"CREATE TABLE $target USING DELTA$properties AS $viewSql").collect()
      } else sql(s"CREATE MATERIALIZED VIEW $target AS $viewSql").collect()

      val affected = s"CAST(SUBSTRING(dm_s_symb, 2) AS INT) < $affectedPartitions"
      val middle   = s"dm_date = DATE_ADD(DATE '2000-01-01', ${rowsPerPartition / 2})"
      mutation match {
        case "delete" => sql(s"DELETE FROM $source WHERE $affected AND $middle").collect()
        case "update-close" =>
          sql(s"UPDATE $source SET dm_close = dm_close + 1 WHERE $affected AND $middle").collect()
        case "update-low" =>
          sql(s"UPDATE $source SET dm_low = -1 WHERE $affected AND $middle").collect()
        case other => fail(s"unsupported mutation: $other")
      }

      val symbols          = (0 until affectedPartitions).map(id => f"'S$id%06d'").mkString(", ")
      val replacePredicate = s"dm_s_symb IN ($symbols)"
      if (strategy == "target-cdf") {
        sql(s"CREATE TABLE ${target}_old_affected USING DELTA AS SELECT * FROM $target WHERE $replacePredicate")
          .collect()
      }

      RefreshSqlLogCatalog.removeAll(spark)
      val started = System.nanoTime()
      strategy match {
        case "full" => sql(s"CREATE OR REPLACE TABLE $target USING DELTA AS $viewSql").collect()
        case "direct-replace" | "target-cdf" =>
          val oldVersion = sql(s"DESCRIBE HISTORY $target LIMIT 1").head().getAs[Long]("version")
          sql(
            s"INSERT INTO $target REPLACE WHERE $replacePredicate " +
              s"SELECT * FROM ($viewSql) openivm_recompute WHERE $replacePredicate"
          ).collect()
          if (strategy == "target-cdf") {
            val newVersion = sql(s"DESCRIBE HISTORY $target LIMIT 1").head().getAs[Long]("version")
            val cdf = spark.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", oldVersion + 1)
              .option("endingVersion", newVersion)
              .table(target)
            val signed = cdf
              .select(
                col("dm_date"),
                col("dm_s_symb"),
                col("dm_close"),
                col("dm_high"),
                col("dm_low"),
                col("dm_vol"),
                col("fifty_two_week_low"),
                col("fifty_two_week_high"),
                col("fifty_two_week_low_date"),
                col("fifty_two_week_high_date"),
                when(col("_change_type").isin("delete", "update_preimage"), lit(-1))
                  .otherwise(lit(1))
                  .as("openivm_multiplicity")
              )
            signed.write.format("delta").mode("overwrite").saveAsTable(s"${target}_cascade")
          }
        case _ => refreshMv(target)
      }
      val elapsedMs = (System.nanoTime() - started) / 1000000L

      assertMvCorrect(target, viewSql)
      if (strategy == "target-cdf") {
        val userColumns = spark.table(target).columns.toSeq
        val actual      = spark.table(s"${target}_cascade")
        val oldSigned = spark
          .table(s"${target}_old_affected")
          .select(userColumns.map(col) :+ lit(-1).as("openivm_multiplicity"): _*)
        val newSigned = spark
          .table(target)
          .where(replacePredicate)
          .select(userColumns.map(col) :+ lit(1).as("openivm_multiplicity"): _*)
        val expected = oldSigned.unionAll(newSigned)
        actual.exceptAll(expected).count() shouldBe 0L
        expected.exceptAll(actual).count() shouldBe 0L
      }
      val statements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
      if (strategy == "replace") statements.exists(_.contains("REPLACE WHERE")) shouldBe true
      if (strategy == "fallback") {
        statements.exists(_.contains("REPLACE WHERE")) shouldBe false
        statements.exists(statement =>
          statement.contains("MERGE INTO") && statement.contains("WHEN MATCHED THEN DELETE")
        ) shouldBe true
      }
      println(
        s"DAILY_MARKET_PERF strategy=$strategy mutation=$mutation partitions=$partitionCount " +
          s"affected_partitions=$affectedPartitions rows_per_partition=$rowsPerPartition elapsed_ms=$elapsedMs"
      )
    }
  }
}
