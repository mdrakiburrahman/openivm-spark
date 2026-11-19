package org.openivm.spark.parity

import org.apache.spark.sql.Row
import org.openivm.spark.common.{
  BatchVerdict,
  CdfWatermarkCatalog,
  ChangeWatermark,
  DeltaCommitClassifier,
  FeatureGate,
  MvCatalog,
  RefreshProfileCatalog,
  RefreshSqlLogAsyncFlusher,
  RefreshSqlLogCatalog,
  RefreshTypeCode
}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParitySpecBase}

class WindowReplacementCdfSpec extends IvmParitySpecBase("window-replacement-cdf") with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
    FeatureGate.QueryLogEnabledKey                -> "true",
    FeatureGate.ProfileRefreshKey                 -> "true",
    FeatureGate.GroupedWindowStateEnabledKey      -> "false"
  )

  private def refreshType(view: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(view)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"Missing MV metadata for $view")).refreshType
  }

  private def queryLog(view: String) = {
    RefreshSqlLogAsyncFlusher.awaitQuiescence(30000L) shouldBe true
    RefreshSqlLogCatalog.scanAll(spark).filter(row => row.mode == "refresh" && row.viewName.split('.').last == view)
  }

  it("routes a global window SQL rewrite with grouped state disabled to a downstream replacement full writer") {
    val upstream   = "wrcdf_rank"
    val downstream = "wrcdf_consumer"
    val rankQuery = "SELECT region, total, DENSE_RANK() OVER (ORDER BY total DESC) AS region_rank " +
      "FROM (SELECT region, SUM(amount) AS total FROM wrcdf_sales GROUP BY region) t"
    val consumerQuery = s"SELECT region, total, region_rank FROM $upstream"

    sql("CREATE TABLE wrcdf_sales(id INT, region STRING, amount INT) USING DELTA")
    sql("INSERT INTO wrcdf_sales VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 15)")
    sql(s"CREATE MATERIALIZED VIEW $upstream AS $rankQuery")
    sql(s"CREATE MATERIALIZED VIEW $downstream AS $consumerQuery")
    refreshType(upstream) shouldBe RefreshTypeCode.WindowPartition
    refreshType(downstream) shouldBe RefreshTypeCode.SimpleProjection
    val upstreamBefore   = mvDataVersion(upstream)
    val downstreamBefore = mvDataVersion(downstream)

    sql("INSERT INTO wrcdf_sales VALUES (4, 'west', 35), (5, 'north', 30)")
    refreshMv(upstream)
    val upstreamLog = queryLog(upstream)
    val replaces    = upstreamLog.filter(_.stmtKind == "replace_where_overwrite")
    replaces.size shouldBe 1
    replaces.head.sqlText should startWith("INSERT INTO")
    replaces.head.sqlText should include("REPLACE WHERE true")
    upstreamLog.exists(_.category == "full_refresh_stmt") shouldBe false

    val path       = mvDataLocation(upstream)
    val commit     = sql(s"DESCRIBE HISTORY delta.`$path` LIMIT 1").head()
    val version    = commit.getAs[Long]("version")
    val operation  = commit.getAs[String]("operation")
    val parameters = commit.getAs[Map[String, String]]("operationParameters")
    val verdict    = DeltaCommitClassifier.classify(spark, path, upstreamBefore)
    version shouldBe upstreamBefore + 1L
    operation shouldBe "WRITE"
    parameters.get("mode") shouldBe Some("Overwrite")
    Seq("predicate", "replaceWhere", "partitionPredicate").flatMap(parameters.get).foreach { predicate =>
      Set("", "[]", "true") should contain(predicate)
    }
    verdict shouldBe BatchVerdict.Replace

    refreshMv(downstream)
    val downstreamLog = queryLog(downstream)
    val writes        = downstreamLog.filter(_.category == "full_refresh_stmt")
    writes.map(_.stmtKind) shouldBe Seq("replace_where_writer")
    writes.head.sqlText should startWith(
      "DataFrameWriter.format(\"delta\").mode(\"overwrite\").option(\"replaceWhere\", \"true\")"
    )
    val reasons = RefreshProfileCatalog.scanAll(spark).collect {
      case row if row.refreshId == writes.head.refreshId && row.stepName == "full_refresh_decision" => row.detail
    }
    reasons shouldBe Seq("reason_code=replacement_or_unavailable_classifier;state_lag=false;cdf_rebaseline=false")
    mvDataVersion(downstream) shouldBe downstreamBefore + 1L
    refreshType(downstream) shouldBe RefreshTypeCode.SimpleProjection

    assertMvCorrect(upstream, rankQuery)
    assertMvCorrect(downstream, rankQuery)
  }

  it("replaces a duplicate and null-containing Delta row bag with one ON false MERGE commit") {
    val target = "wrcdf_merge_target"
    sql(
      s"CREATE TABLE $target (id INT, label STRING) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"
    )
    sql(s"INSERT INTO $target VALUES (1, 'old'), (1, 'old'), (2, NULL), (NULL, NULL)")
    val before = DeltaCommitClassifier.latestVersion(spark, target)

    sql(
      s"MERGE INTO $target AS t USING (SELECT * FROM VALUES (3, 'new'), (3, 'new'), (2, NULL), " +
        "(NULL, 'new') AS incoming(id, label)) AS s ON false " +
        "WHEN NOT MATCHED THEN INSERT * WHEN NOT MATCHED BY SOURCE THEN DELETE"
    ).collect()
    val commit     = sql(s"DESCRIBE HISTORY $target LIMIT 1").head()
    val version    = commit.getAs[Long]("version")
    val operation  = commit.getAs[String]("operation")
    val parameters = commit.getAs[Map[String, String]]("operationParameters")
    version shouldBe before + 1L
    operation shouldBe "MERGE"
    parameters.get("predicate") shouldBe Some("[\"false\"]")
    DeltaCommitClassifier.classify(spark, target, before) shouldBe BatchVerdict.Mutating

    val cdf = sql(s"SELECT _change_type, id, label FROM table_changes('$target', $version, $version)")
    val expectedCdf = Seq(
      Row("delete", 1, "old"),
      Row("delete", 1, "old"),
      Row("delete", 2, null),
      Row("delete", null, null),
      Row("insert", 3, "new"),
      Row("insert", 3, "new"),
      Row("insert", 2, null),
      Row("insert", null, "new")
    )
    cdf.collect().toSeq should contain theSameElementsAs expectedCdf
    sql(s"SELECT id, label FROM $target").collect().toSeq should contain theSameElementsAs
      Seq(Row(3, "new"), Row(3, "new"), Row(2, null), Row(null, "new"))
  }

  it("uses an atomic global-window merge to supply a downstream CDF refresh") {
    spark.conf.set(FeatureGate.GlobalWindowCdfMergeEnabledKey, "true")
    try {
      val upstream   = "wrcdf_opt_rank"
      val downstream = "wrcdf_opt_consumer"
      val rankQuery = "SELECT region, total, DENSE_RANK() OVER (ORDER BY total DESC) AS region_rank " +
        "FROM (SELECT region, SUM(amount) AS total FROM wrcdf_opt_sales GROUP BY region) t"
      sql("CREATE TABLE wrcdf_opt_sales(id INT, region STRING, amount INT) USING DELTA")
      sql("INSERT INTO wrcdf_opt_sales VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 15)")
      sql(s"CREATE MATERIALIZED VIEW $upstream AS $rankQuery")
      sql(s"CREATE MATERIALIZED VIEW $downstream AS SELECT region, total, region_rank FROM $upstream")
      refreshType(upstream) shouldBe RefreshTypeCode.WindowPartition
      refreshType(downstream) shouldBe RefreshTypeCode.SimpleProjection

      def refreshAndCheck(): Unit = {
        val before             = mvDataVersion(upstream)
        val previousDownstream = mvDataVersion(downstream)
        RefreshSqlLogCatalog.removeAll(spark)
        refreshMv(upstream)
        val upstreamLog = queryLog(upstream)
        val merges = upstreamLog.filter(row =>
          row.category == "rewritten_stmt" && row.sqlText.contains("WHEN NOT MATCHED BY SOURCE THEN DELETE")
        )
        merges.size shouldBe 1
        merges.head.stmtKind shouldBe "global_window_cdf_merge"
        val profile = RefreshProfileCatalog
          .scanAll(spark)
          .find(row =>
            row.refreshId == merges.head.refreshId &&
              row.stepName == "execute_refresh_sql_attempt" &&
              row.detail.contains(s"query_log_order=${merges.head.stmtOrder};")
          )
          .getOrElse(fail("missing full-domain merge profile"))
        profile.detail should include("origin_class=executor_helper")
        profile.detail should include("credit_reason=full_domain_rewrite")
        profile.detail should include("credit_ns=0")
        upstreamLog.exists(_.category == "full_refresh_stmt") shouldBe false
        mvDataVersion(upstream) shouldBe before + 1L
        val path = mvDataLocation(upstream)
        sql(s"DESCRIBE HISTORY delta.`$path` LIMIT 1").head().getAs[String]("operation") shouldBe "MERGE"
        DeltaCommitClassifier.classify(spark, path, before) shouldBe BatchVerdict.Mutating
        assertMvCorrect(upstream, rankQuery)
        refreshMv(downstream)
        queryLog(downstream).exists(_.category == "full_refresh_stmt") shouldBe false
        mvDataVersion(downstream) shouldBe previousDownstream + 1L
        assertMvCorrect(downstream, rankQuery)
      }

      sql("INSERT INTO wrcdf_opt_sales VALUES (4, 'west', 35), (5, 'north', 30)")
      refreshAndCheck()
      sql("DELETE FROM wrcdf_opt_sales")
      refreshAndCheck()

      val upstreamVersion   = mvDataVersion(upstream)
      val downstreamVersion = mvDataVersion(downstream)
      refreshMv(upstream)
      refreshMv(downstream)
      mvDataVersion(upstream) shouldBe upstreamVersion
      mvDataVersion(downstream) shouldBe downstreamVersion

      sql("INSERT OVERWRITE TABLE wrcdf_opt_sales SELECT 9, 'reset', 99")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv(upstream)
      queryLog(upstream).exists(_.category == "full_refresh_stmt") shouldBe true
      refreshMv(downstream)
      queryLog(downstream).exists(_.category == "full_refresh_stmt") shouldBe true
      assertMvCorrect(upstream, rankQuery)
      assertMvCorrect(downstream, rankQuery)
    } finally spark.conf.unset(FeatureGate.GlobalWindowCdfMergeEnabledKey)
  }

  it("uses the merge for grouped-window state and recovers a stale published watermark") {
    spark.conf.set(FeatureGate.GlobalWindowCdfMergeEnabledKey, "true")
    spark.conf.set(FeatureGate.GroupedWindowStateEnabledKey, "true")
    try {
      val upstream   = "wrcdf_group_rank"
      val downstream = "wrcdf_group_consumer"
      val query = "WITH grouped AS (SELECT k, COUNT(*) AS n, COUNT(DISTINCT v) AS d, SUM(v) AS s " +
        "FROM wrcdf_group_src GROUP BY k) " +
        "SELECT k, n, d, s, RANK() OVER (ORDER BY n DESC) AS r FROM grouped"
      sql("CREATE TABLE wrcdf_group_src(k BIGINT, v BIGINT) USING DELTA")
      sql("INSERT INTO wrcdf_group_src VALUES (1, 2), (1, 2), (2, 3)")
      sql(s"CREATE MATERIALIZED VIEW $upstream AS $query")
      sql(s"CREATE MATERIALIZED VIEW $downstream AS SELECT k, n, d, s, r FROM $upstream")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(upstream)
      MvCatalog.lookup(spark, id).get.properties.contains("_ivm_grouped_window_input_v1") shouldBe true
      val metaBefore = MvCatalog.lookup(spark, id).get
      val watermarksBefore = metaBefore.sourceTables.map { source =>
        val initial = metaBefore.changeWatermarks(source).asInstanceOf[ChangeWatermark.DeltaVersion].version
        source -> CdfWatermarkCatalog.get(spark, upstream, source).getOrElse(initial)
      }.toMap
      sql("INSERT INTO wrcdf_group_src VALUES (1, 4), (3, 5)")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv(upstream)
      val log = queryLog(upstream)
      log.count(_.stmtKind == "global_window_cdf_merge") shouldBe 1
      log.exists(_.sqlText.contains("openivm_grouped_result_")) shouldBe true
      assertMvCorrect(upstream, query)
      refreshMv(downstream)
      queryLog(downstream).exists(_.category == "full_refresh_stmt") shouldBe false
      assertMvCorrect(downstream, query)

      MvCatalog.upsert(spark, metaBefore)
      CdfWatermarkCatalog.putAll(spark, upstream, watermarksBefore)
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv(upstream)
      val recovered = queryLog(upstream)
      recovered.exists(_.category == "full_refresh_stmt") shouldBe true
      val reasons = RefreshProfileCatalog.scanAll(spark).filter(_.stepName == "full_refresh_decision")
      reasons.exists(_.detail.contains("reason_code=state_lag")) shouldBe true
      assertMvCorrect(upstream, query)
      refreshMv(downstream)
      assertMvCorrect(downstream, query)
    } finally {
      spark.conf.unset(FeatureGate.GlobalWindowCdfMergeEnabledKey)
      spark.conf.set(FeatureGate.GroupedWindowStateEnabledKey, "false")
    }
  }

  it("keeps the normal write for a terminal window or a target without CDF") {
    spark.conf.set(FeatureGate.GlobalWindowCdfMergeEnabledKey, "true")
    try {
      Seq(false, true).foreach { consumer =>
        val suffix = if (consumer) "nocdf" else "terminal"
        val source = s"wrcdf_${suffix}_src"
        val view   = s"wrcdf_${suffix}_mv"
        val query  = s"SELECT id, DENSE_RANK() OVER (ORDER BY id) AS r FROM $source"
        sql(s"CREATE TABLE $source(id INT) USING DELTA")
        sql(s"INSERT INTO $source VALUES (1), (2)")
        sql(s"CREATE MATERIALIZED VIEW $view AS $query")
        if (consumer) {
          sql(s"CREATE MATERIALIZED VIEW wrcdf_nocdf_consumer AS SELECT id, r FROM $view")
          sql(s"ALTER TABLE $view SET TBLPROPERTIES ('delta.enableChangeDataFeed'='false')")
        }
        sql(s"INSERT INTO $source VALUES (3)")
        RefreshSqlLogCatalog.removeAll(spark)
        refreshMv(view)
        val log = queryLog(view)
        log.exists(_.stmtKind == "global_window_cdf_merge") shouldBe false
        log.exists(row =>
          row.stmtKind == "replace_where_overwrite" && row.sqlText.contains("REPLACE WHERE true")
        ) shouldBe true
        assertMvCorrect(view, query)
      }
    } finally spark.conf.unset(FeatureGate.GlobalWindowCdfMergeEnabledKey)
  }
}

class WindowReplacementInterceptSpec extends IvmParitySpecBase("window-replacement-intercept") with InterceptMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
    FeatureGate.QueryLogEnabledKey                -> "true"
  )

  Seq(false, true).foreach { enabled =>
    it(s"preserves intercept-mode signed propagation when global CDF merge is enabled=$enabled") {
      spark.conf.set(FeatureGate.GlobalWindowCdfMergeEnabledKey, enabled.toString)
      try {
        val suffix     = if (enabled) "on" else "off"
        val source     = s"wrint_${suffix}_src"
        val upstream   = s"wrint_${suffix}_mv"
        val downstream = s"wrint_${suffix}_consumer"
        val query      = s"SELECT id, DENSE_RANK() OVER (ORDER BY id) AS r FROM $source"
        sql(s"CREATE TABLE $source(id INT) USING DELTA")
        sql(s"INSERT INTO $source VALUES (1), (2)")
        sql(s"CREATE MATERIALIZED VIEW $upstream AS $query")
        sql(s"CREATE MATERIALIZED VIEW $downstream AS SELECT id, r FROM $upstream")
        sql(s"INSERT INTO $source VALUES (3)")
        RefreshSqlLogCatalog.removeAll(spark)
        refreshMv(upstream)
        RefreshSqlLogAsyncFlusher.awaitQuiescence(30000L) shouldBe true
        RefreshSqlLogCatalog.scanAll(spark).exists(_.stmtKind == "global_window_cdf_merge") shouldBe false
        refreshMv(downstream)
        assertMvCorrect(upstream, query)
        assertMvCorrect(downstream, query)
      } finally spark.conf.unset(FeatureGate.GlobalWindowCdfMergeEnabledKey)
    }
  }
}
