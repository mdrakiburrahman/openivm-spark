package org.openivm.spark.parity

import org.openivm.spark.common.{
  CdfWatermarkCatalog,
  ChangeWatermark,
  FeatureGate,
  GroupedDistinctState,
  MvCatalog,
  RefreshSqlLogCatalog,
  RefreshTypeCode
}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class GlobalWindowSinglePassCdfSpec       extends GlobalWindowSinglePassCdfBase(backing = false)
class BackedGlobalWindowSinglePassCdfSpec extends GlobalWindowSinglePassCdfBase(backing = true)

class CachedBackedGlobalWindowSinglePassCdfSpec extends GlobalWindowSinglePassCdfBase(backing = true, cache = true)

abstract class GlobalWindowSinglePassCdfBase(backing: Boolean, cache: Boolean = false)
    extends IvmParitySpecBase(s"global-window-single-pass-$backing-$cache")
    with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
    FeatureGate.QueryLogEnabledKey                -> "true",
    FeatureGate.WindowSnapshotCacheEnabledKey     -> cache.toString,
    FeatureGate.GroupedWindowStateEnabledKey      -> "true",
    "spark.openivm.catalogPreservesColumnCase"    -> "false"
  )

  it("falls back incrementally when grouped state exceeds capacity without publishing prepared cache state") {
    sql("CREATE TABLE gw_capacity_source(k BIGINT,v BIGINT) USING DELTA")
    sql("INSERT INTO gw_capacity_source VALUES(0,1)")
    val rank = if (backing) "Rank" else "rank"
    val query = "WITH grouped AS (SELECT k,COUNT(*) AS n,COUNT(DISTINCT v) AS d,SUM(v) AS s " +
      "FROM gw_capacity_source GROUP BY k) " +
      s"SELECT k,n,d,s,RANK() OVER(ORDER BY n DESC) AS $rank FROM grouped"
    val view = "gw_capacity_rank"
    sql(s"CREATE MATERIALIZED VIEW $view AS $query")
    val id   = spark.sessionState.sqlParser.parseTableIdentifier(view)
    val meta = MvCatalog.lookup(spark, id).get
    meta.refreshType shouldBe RefreshTypeCode.WindowPartition
    meta.properties.contains("_ivm_grouped_window_input_v1") shouldBe true
    GroupedDistinctState.checkpoint(spark, view).nonEmpty shouldBe true

    def usedGroupedState: Boolean = sql("SHOW OPENIVM QUERY LOG")
      .collect()
      .map(_.getString(9))
      .exists(_.contains("openivm_grouped_result_"))
    val consumer = "SELECT SUM(n) AS n,SUM(d) AS d FROM gw_capacity_rank"
    sql(s"CREATE MATERIALIZED VIEW gw_capacity_consumer AS $consumer")
    sql("INSERT INTO gw_capacity_source VALUES(0,2)")
    RefreshSqlLogCatalog.removeAll(spark)
    refreshMv(view)
    usedGroupedState shouldBe true
    assertMvCorrect(view, query)
    refreshMv("gw_capacity_consumer")

    // Derived rows and this versioned checkpoint are published in one RocksDB batch.
    def cacheCheckpoint: Option[Vector[Byte]] = GroupedDistinctState.checkpoint(spark, view).map(_.toVector)
    val initialCheckpoint = cacheCheckpoint
    initialCheckpoint.nonEmpty shouldBe true
    // Cross the real summary limit with narrow rows, without injecting a fake rejection.
    sql(s"INSERT INTO gw_capacity_source SELECT id,id FROM range(1,${GroupedDistinctState.MaxGroups + 1L})")

    Vector(
      Vector.empty[String],
      Vector("DELETE FROM gw_capacity_source WHERE k>2", "INSERT INTO gw_capacity_source VALUES(0,5),(0,5)")
    ).foreach { mutations =>
      mutations.foreach(sql)
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv(view)
      assertMvCorrect(view, query)
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.WindowPartition
      usedGroupedState shouldBe false
      cacheCheckpoint shouldBe initialCheckpoint
      refreshMv("gw_capacity_consumer")
      assertMvCorrect("gw_capacity_consumer", consumer)
    }
    val version = mvDataVersion(view)
    refreshMv(view)
    mvDataVersion(view) shouldBe version
  }

  it("preserves the managed catalog target when creating grouped-window state") {
    val key   = "spark.openivm.managedTablesRoot"
    val prior = spark.conf.getOption(key)
    spark.conf.set(key, s"$warehouseDir/Tables")
    try {
      sql("CREATE TABLE gw_managed_input(k BIGINT,v BIGINT) USING DELTA")
      sql("INSERT INTO gw_managed_input VALUES(1,7),(1,7),(2,8),(3,NULL)")
      val rank = if (backing) "Rank" else "rank"
      val query = "WITH grouped AS (SELECT k,COUNT(*) AS n,COUNT(DISTINCT v) AS d," +
        s"SUM(v) AS s FROM gw_managed_input GROUP BY k) SELECT k,n,d,s,DENSE_RANK() OVER(ORDER BY n DESC) AS $rank FROM grouped"
      RefreshSqlLogCatalog.removeAll(spark)
      sql(s"CREATE MATERIALIZED VIEW default.gw_managed_rank AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("default.gw_managed_rank")
      MvCatalog.lookup(spark, id).get.properties.contains("_ivm_grouped_window_input_v1") shouldBe true
      val writes = sql("SHOW OPENIVM QUERY LOG")
        .collect()
        .map(_.getString(9))
        .filter(_.contains("openivm_grouped_initial_"))
      writes.length shouldBe 1
      writes.head should startWith("CREATE TABLE `default`.")
      assertMvCorrect("gw_managed_rank", query)
      sql("INSERT INTO gw_managed_input VALUES(1,9),(4,10)")
      sql("DELETE FROM gw_managed_input WHERE k=2")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("default.gw_managed_rank")
      assertMvCorrect("gw_managed_rank", query)
      sql("SHOW OPENIVM QUERY LOG")
        .collect()
        .map(_.getString(9))
        .exists(_.contains("openivm_grouped_result_")) shouldBe true
    } finally {
      prior match {
        case Some(value) => spark.conf.set(key, value)
        case None        => spark.conf.unset(key)
      }
    }
  }

  it("maintains the complete broker query from grouped state through mixed source and cash changes") {
    sql(
      "CREATE TABLE gw_group_trade(sk_broker_id BIGINT,sk_customer_id BIGINT,sk_security_id BIGINT," +
        "sk_account_id BIGINT,create_timestamp TIMESTAMP,quantity BIGINT,trade_price DOUBLE,commission DOUBLE,fee DOUBLE) USING DELTA"
    )
    sql(
      "CREATE TABLE gw_group_broker(sk_broker_id BIGINT,broker_id BIGINT,first_name STRING,last_name STRING) USING DELTA"
    )
    sql("CREATE TABLE gw_group_cash(sk_account_id BIGINT) USING DELTA")
    sql("INSERT INTO gw_group_broker VALUES(1,11,'A','One'),(2,22,'B','Two'),(3,33,NULL,'Cash')")
    sql(
      "INSERT INTO gw_group_trade VALUES " +
        "(1,100,1,1000,TIMESTAMP '2024-01-01',10,2.5,0.5,0.25),(1,100,1,1000,TIMESTAMP '2024-01-01',10,2.5,0.5,0.25)," +
        "(1,NULL,2,1001,TIMESTAMP '2024-01-02',20,1.25,NULL,0.5),(2,200,NULL,2000,TIMESTAMP '2024-01-01',20,3,1,0.5)," +
        "(2,201,3,2001,NULL,20,3,1,NULL),(3,300,3,3000,TIMESTAMP '2024-01-03',50,10,1,0.25)"
    )
    sql("INSERT INTO gw_group_cash VALUES(3000)")
    val resource =
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("/queries/broker-performance-window.sql"), "UTF-8")
    val query =
      try resource.mkString
      finally resource.close()
    sql(s"CREATE MATERIALIZED VIEW gw_group_rank AS $query")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("gw_group_rank")
    MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.WindowPartition
    withClue("initial grouped admission: ") {
      MvCatalog.lookup(spark, id).get.properties.contains("_ivm_grouped_window_input_v1") shouldBe true
    }
    withClue(s"initial grouped cache at target version ${mvDataVersion("gw_group_rank")}: ") {
      org.openivm.spark.common.GroupedDistinctState.checkpoint(spark, "gw_group_rank").nonEmpty shouldBe true
    }
    assertMvCorrect("gw_group_rank", query)
    val consumer = "SELECT sk_broker_id, total_notional, rank_by_notional, rank_by_customers FROM gw_group_rank"
    sql(s"CREATE MATERIALIZED VIEW gw_group_consumer AS $consumer")
    val steps = Vector(
      Vector("INSERT INTO gw_group_trade VALUES(1,101,2,1001,TIMESTAMP '2024-01-02',5,2.5,0.5,0.25)"),
      Vector(
        "INSERT INTO gw_group_trade VALUES(2,202,4,2002,TIMESTAMP '2024-01-03',5,2,0.5,0.25)",
        "UPDATE gw_group_broker SET last_name='BothChanged' WHERE sk_broker_id=2",
        "DELETE FROM gw_group_trade WHERE sk_customer_id=100"
      ),
      Vector("INSERT INTO gw_group_cash VALUES(1001)"),
      Vector("UPDATE gw_group_trade SET commission=commission+0.0625 WHERE sk_broker_id=2"),
      Vector("DELETE FROM gw_group_trade")
    )
    steps.zipWithIndex.foreach { case (mutations, index) =>
      mutations.foreach(sql)
      val before = mvDataVersion("gw_group_rank")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("gw_group_rank")
      assertMvCorrect("gw_group_rank", query)
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.WindowPartition
      withClue(s"grouped command path batch$index: ") {
        val statements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9))
        statements.exists(_.contains("openivm_grouped_result_")) shouldBe true
        statements.count(_.contains("REPLACE WHERE true")) shouldBe 1
        mvDataVersion("gw_group_rank") shouldBe before + 1L
      }
      refreshMv("gw_group_consumer")
      assertMvCorrect("gw_group_consumer", consumer)
    }
    // Construct the persisted image of an interruption after grouped state/
    // target publication but before parent metadata and CDF watermarks advance.
    // This is an application retry check, not a machine-crash simulation.
    val priorMeta = MvCatalog.lookup(spark, id).get
    val priorWatermarks = priorMeta.sourceTables.map { source =>
      val initial = priorMeta.changeWatermarks(source).asInstanceOf[ChangeWatermark.DeltaVersion].version
      source -> CdfWatermarkCatalog.get(spark, "gw_group_rank", source).getOrElse(initial)
    }.toMap
    sql("INSERT INTO gw_group_trade VALUES(2,203,5,2003,TIMESTAMP '2024-01-04',5,2,0.5,0.25)")
    refreshMv("gw_group_rank")
    assertMvCorrect("gw_group_rank", query)
    val checkpoint = org.openivm.spark.common.GroupedDistinctState.checkpoint(spark, "gw_group_rank").map(_.toVector)
    MvCatalog.upsert(spark, priorMeta)
    CdfWatermarkCatalog.putAll(spark, "gw_group_rank", priorWatermarks)
    RefreshSqlLogCatalog.removeAll(spark)
    refreshMv("gw_group_rank")
    assertMvCorrect("gw_group_rank", query)
    sql("SHOW OPENIVM QUERY LOG")
      .collect()
      .map(_.getString(9))
      .exists(_.contains("openivm_grouped_result_")) shouldBe false
    org.openivm.spark.common.GroupedDistinctState.checkpoint(spark, "gw_group_rank").map(_.toVector) shouldBe checkpoint
    refreshMv("gw_group_consumer")
    assertMvCorrect("gw_group_consumer", consumer)
    val emptyVersion = mvDataVersion("gw_group_rank")
    refreshMv("gw_group_rank")
    mvDataVersion("gw_group_rank") shouldBe emptyVersion
  }

  it("maintains the complete security statistics query with RANK, HAVING and STDDEV") {
    sql(
      "CREATE TABLE gw_stats_trade(sk_security_id BIGINT,sk_broker_id BIGINT,sk_account_id BIGINT," +
        "create_timestamp TIMESTAMP,quantity BIGINT,trade_price DOUBLE,fee DOUBLE,commission DOUBLE) USING DELTA"
    )
    sql("CREATE TABLE gw_stats_security(symbol STRING,sk_security_id BIGINT,sk_company_id BIGINT) USING DELTA")
    sql("CREATE TABLE gw_stats_watches(sk_security_id BIGINT) USING DELTA")
    sql("INSERT INTO gw_stats_security VALUES('ABC',1,11),('XYZ',2,22),('ONE',3,33)")
    sql(
      "INSERT INTO gw_stats_trade VALUES " +
        "(1,1,10,TIMESTAMP '2024-01-01',10,2.5,0.25,0.5),(1,1,10,TIMESTAMP '2024-01-01',10,2.5,0.25,0.5)," +
        "(1,2,11,TIMESTAMP '2024-01-02',20,3.0,NULL,0.25),(2,1,20,TIMESTAMP '2024-01-01',30,4.0,0.5,0.25)," +
        "(2,1,NULL,NULL,5,4.0,0.5,NULL),(3,3,30,TIMESTAMP '2024-01-03',1,5.0,0.25,0.125)"
    )
    val resource =
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("/queries/trade-volume-stats-window.sql"), "UTF-8")
    val query =
      try resource.mkString
      finally resource.close()
    sql(s"CREATE MATERIALIZED VIEW gw_stats_rank AS $query")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("gw_stats_rank")
    MvCatalog.lookup(spark, id).get.properties.contains("_ivm_grouped_window_input_v1") shouldBe true
    assertMvCorrect("gw_stats_rank", query)
    val mutations = Vector(
      Vector("INSERT INTO gw_stats_trade VALUES(3,4,31,TIMESTAMP '2024-01-04',1,4.0,0.25,0.125)"),
      Vector(
        "UPDATE gw_stats_trade SET trade_price=trade_price+0.125,fee=fee+0.0625 WHERE sk_security_id=1",
        "UPDATE gw_stats_security SET symbol='RENAMED',sk_company_id=111 WHERE sk_security_id=1",
        "DELETE FROM gw_stats_trade WHERE sk_security_id=2 AND sk_account_id IS NULL"
      ),
      Vector("INSERT INTO gw_stats_watches VALUES(1)"),
      Vector("DELETE FROM gw_stats_trade")
    )
    mutations.foreach { step =>
      step.foreach(sql)
      val before = mvDataVersion("gw_stats_rank")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("gw_stats_rank")
      assertMvCorrect("gw_stats_rank", query)
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.WindowPartition
      sql("SHOW OPENIVM QUERY LOG")
        .collect()
        .map(_.getString(9))
        .exists(_.contains("openivm_grouped_result_")) shouldBe true
      mvDataVersion("gw_stats_rank") shouldBe before + 1L
    }
    val before = mvDataVersion("gw_stats_rank")
    refreshMv("gw_stats_rank")
    mvDataVersion("gw_stats_rank") shouldBe before
  }

  it("preserves timestamp microseconds through grouped-window commands") {
    sql("CREATE TABLE gw_timestamp(k BIGINT,v BIGINT,t TIMESTAMP) USING DELTA")
    sql(
      "INSERT INTO gw_timestamp VALUES(1,7,TIMESTAMP '2024-01-01 01:02:03.123456')," +
        "(2,8,TIMESTAMP '2024-01-01 01:02:03.654321')"
    )
    val rank = if (backing) "Rank" else "rank"
    val query = s"WITH grouped AS (SELECT k,COUNT(*) AS n,COUNT(DISTINCT v) AS d," +
      s"MIN(t) AS lo,MAX(t) AS hi FROM gw_timestamp GROUP BY k) " +
      s"SELECT k,n,d,lo,hi,DENSE_RANK() OVER(ORDER BY n DESC) AS $rank FROM grouped"
    sql(s"CREATE MATERIALIZED VIEW gw_timestamp_rank AS $query")
    assertMvCorrect("gw_timestamp_rank", query)
    val changes = Vector(
      Vector(
        "UPDATE gw_timestamp SET t=TIMESTAMP '2024-01-01 01:02:03.111111' WHERE k=1",
        "INSERT INTO gw_timestamp VALUES(1,9,TIMESTAMP '2024-01-01 01:02:03.999999')",
        "DELETE FROM gw_timestamp WHERE k=2"
      ),
      Vector("UPDATE gw_timestamp SET t=TIMESTAMP '2024-01-01 01:02:03.222222'")
    )
    changes.foreach { step =>
      step.foreach(sql)
      val before = mvDataVersion("gw_timestamp_rank")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("gw_timestamp_rank")
      assertMvCorrect("gw_timestamp_rank", query)
      sql("SHOW OPENIVM QUERY LOG")
        .collect()
        .map(_.getString(9))
        .exists(_.contains("openivm_grouped_result_")) shouldBe true
      mvDataVersion("gw_timestamp_rank") shouldBe before + 1L
    }
  }

  it("keeps the existing incremental grouped-rank path when grouped state is disabled") {
    val key   = FeatureGate.GroupedWindowStateEnabledKey
    val prior = spark.conf.getOption(key)
    spark.conf.set(key, "false")
    try {
      sql("CREATE TABLE gw_group_off(id BIGINT,g STRING,amount DOUBLE) USING DELTA")
      sql("INSERT INTO gw_group_off VALUES(1,'a',10),(1,'a',10),(2,'b',20)")
      val query = """WITH grouped AS (SELECT g,COUNT(*) AS n,COUNT(DISTINCT id) AS d,SUM(amount) AS s
      |FROM gw_group_off GROUP BY g) SELECT g,n,d,s,DENSE_RANK() OVER(ORDER BY s DESC) AS r FROM grouped""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW gw_group_off_rank AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("gw_group_off_rank")
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.WindowPartition
      sql("INSERT INTO gw_group_off VALUES(3,'a',15)")
      sql("UPDATE gw_group_off SET amount=5 WHERE g='b'")
      sql("DELETE FROM gw_group_off WHERE id=1")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("gw_group_off_rank")
      assertMvCorrect("gw_group_off_rank", query)
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.WindowPartition
      sql("SHOW OPENIVM QUERY LOG")
        .collect()
        .map(_.getString(9))
        .exists(_.contains("openivm_grouped_result_")) shouldBe false
    } finally
      prior match {
        case Some(value) => spark.conf.set(key, value)
        case None        => spark.conf.unset(key)
      }
  }

  it("replaces global ranks once, without a cascade table, and preserves CDF consumers") {
    sql("CREATE TABLE gw_sales(id INT, region STRING, amount INT) USING DELTA")
    sql("INSERT INTO gw_sales VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 15)")
    val rankAlias = if (backing) "Rank" else "rank"
    val query = s"SELECT region, total, DENSE_RANK() OVER (ORDER BY total DESC) AS $rankAlias " +
      "FROM (SELECT region, SUM(amount) AS total FROM gw_sales GROUP BY region) t"
    sql(s"CREATE MATERIALIZED VIEW gw_rank AS $query")

    val id   = spark.sessionState.sqlParser.parseTableIdentifier("gw_rank")
    val meta = MvCatalog.lookup(spark, id).get
    meta.refreshType shouldBe RefreshTypeCode.WindowPartition
    meta.usesBackingDataTable shouldBe backing

    def checkRefresh(): Unit = {
      val before = mvDataVersion("gw_rank")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("gw_rank")
      assertMvCorrect("gw_rank", query)
      val statements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
      withClue(statements.mkString("\n")) {
        mvDataVersion("gw_rank") shouldBe before + 1
        statements.count(_.contains("REPLACE WHERE true")) shouldBe 1
      }
      statements.count(_.trim == "CACHE TABLE `openivm_new_gw_rank`") shouldBe (if (cache) 1 else 0)
      statements.mkString("\n") should not include "/_ivm/view_deltas/"
      statements.exists(_.trim.toUpperCase.startsWith("DELETE FROM")) shouldBe false
    }

    sql("INSERT INTO gw_sales VALUES (4, 'west', 30), (5, 'north', 45)")
    checkRefresh()

    // A consumer created after the terminal optimization starts from the new
    // snapshot, then observes mixed mutations and rank changes through CDF.
    val downstream = "SELECT region, total, rank FROM gw_rank"
    sql(s"CREATE MATERIALIZED VIEW gw_consumer AS $downstream")
    sql("UPDATE gw_sales SET amount = 100 WHERE id = 1")
    sql("DELETE FROM gw_sales WHERE id = 3")
    sql("INSERT INTO gw_sales VALUES (6, 'south', 100)")
    checkRefresh()
    refreshMv("gw_consumer")
    assertMvCorrect("gw_consumer", downstream)

    sql("DELETE FROM gw_sales")
    checkRefresh()
    refreshMv("gw_consumer")
    assertMvCorrect("gw_consumer", downstream)
    spark.table("gw_rank").count() shouldBe 0L
    val emptyVersion = mvDataVersion("gw_rank")
    refreshMv("gw_rank")
    mvDataVersion("gw_rank") shouldBe emptyVersion
  }

  it("retains the native delete/insert and cascade program when disabled") {
    restartSpark(extraSparkConf + (FeatureGate.WindowSinglePassReplaceEnabledKey -> "false"))
    sql("CREATE TABLE gw_control_sales(id INT, region STRING, amount INT) USING DELTA")
    sql("INSERT INTO gw_control_sales VALUES (1, 'east', 10), (2, 'west', 20)")
    val rankAlias = if (backing) "Rank" else "rank"
    val query = s"SELECT region, total, DENSE_RANK() OVER (ORDER BY total DESC) AS $rankAlias " +
      "FROM (SELECT region, SUM(amount) AS total FROM gw_control_sales GROUP BY region) t"
    sql(s"CREATE MATERIALIZED VIEW gw_control AS $query")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("gw_control")
    MvCatalog.lookup(spark, id).get.usesBackingDataTable shouldBe backing
    sql("INSERT INTO gw_control_sales VALUES (3, 'east', 30)")
    val before = mvDataVersion("gw_control")
    RefreshSqlLogCatalog.removeAll(spark)
    refreshMv("gw_control")
    assertMvCorrect("gw_control", query)
    mvDataVersion("gw_control") shouldBe before + 2
    val statements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).mkString("\n")
    statements should include("/_ivm/view_deltas/")
    statements should not include "REPLACE WHERE true"
  }

}
