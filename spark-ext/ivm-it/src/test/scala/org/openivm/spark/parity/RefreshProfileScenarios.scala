package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.Row
import org.openivm.spark.common.{
  ChangeFeedMode,
  FeatureGate,
  RefreshProfileCatalog,
  RefreshSqlLogAsyncFlusher,
  RefreshSqlLogCatalog,
  RewriteLineage
}
import org.openivm.spark.common.rocksdb.OpenIvmStateSync
import org.openivm.spark.telemetry.OpenIvmExecutionSpan
import java.util.concurrent.{CountDownLatch, TimeUnit}

/** End-to-end coverage of `RefreshProfile` instrumentation in
  * `MaterializedViewCommands` + the `SHOW OPENIVM REFRESH PROFILE` SQL
  * statement. All table/MV names are prefixed `profile_` to avoid Delta
  * warehouse path collisions when this spec runs in parallel with other
  * parity specs.
  */
abstract class RefreshProfileScenarios extends IvmParitySpecBase("refresh-profile") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(
      "spark.openivm.profile.refresh"      -> "true",
      FeatureGate.QueryLogEnabledKey       -> "true",
      FeatureGate.StateSyncAsyncEnabledKey -> "false"
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    RefreshProfileCatalog.ensureTables(spark)
  }

  protected def clearProfile(): Unit = RefreshProfileCatalog.removeAll(spark)

  protected def showProfile(): Seq[Row] =
    sql("SHOW OPENIVM REFRESH PROFILE").collect().toSeq

  protected def stepsByRefresh(rows: Seq[Row]): Map[String, Seq[String]] =
    rows.groupBy(_.getString(0)).map { case (rid, rs) =>
      rid -> rs.sortBy(_.getInt(3)).map(_.getString(4))
    }

  protected def detailsForStep(rows: Seq[Row], stepName: String): Seq[String] =
    rows.filter(_.getString(4) == stepName).map(_.getString(6))

  describe("RefreshProfile — CREATE path") {
    it("emits create_compile_classification, create_mv_initial_load, create_mv_total with _create_mv_ refresh_id") {
      clearProfile()
      sql("CREATE TABLE profile_t1 (k INT, v INT) USING DELTA")
      sql("INSERT INTO profile_t1 VALUES (1, 10), (2, 20)")
      sql("CREATE MATERIALIZED VIEW profile_mv1 AS SELECT k, SUM(v) AS s FROM profile_t1 GROUP BY k")

      val rows  = showProfile()
      val steps = stepsByRefresh(rows)

      steps.keys.exists(_.contains("_create_mv_")) shouldBe true
      val createRid = steps.keys.find(_.contains("_create_mv_")).get
      val names     = steps(createRid).toSet
      names should contain("create_mv_system_tables")
      names should contain("create_compile_classification")
      names should contain("create_mv_initial_load")
      names should contain("create_mv_publish_metadata")
      names should contain("create_mv_total")
      names should contain("query_span")
      val span = detailsForStep(rows, "query_span").head
      span should include("start_epoch_ms=")
      span should include("end_epoch_ms=")
      span should include("outcome=")
      clearProfile()
      sql("CREATE MATERIALIZED VIEW IF NOT EXISTS profile_mv1 AS SELECT k, SUM(v) AS s FROM profile_t1 GROUP BY k")
      val details = detailsForStep(showProfile(), "create_guard_discovery")
      details should have size 1
      details.head should include("start_monotonic_ns=")
    }
  }

  describe("RefreshProfile — REFRESH no deltas") {
    it("emits acquire_locks + total_refresh with no execute_refresh_sql_stmt rows") {
      clearProfile()
      sql("CREATE TABLE profile_t2 (k INT, v INT) USING DELTA")
      sql("INSERT INTO profile_t2 VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW profile_mv2 AS SELECT k, SUM(v) AS s FROM profile_t2 GROUP BY k")
      clearProfile() // drop CREATE rows so we only assert on REFRESH

      sql("REFRESH MATERIALIZED VIEW profile_mv2").collect()

      val rows  = showProfile()
      val steps = stepsByRefresh(rows)
      steps.keys.exists(rid => !rid.contains("_create_mv_")) shouldBe true
      val refreshRid = steps.keys.find(rid => !rid.contains("_create_mv_")).get
      val names      = steps(refreshRid).toSet
      names should contain("acquire_locks")
      names should contain("total_refresh")
      names should contain("query_span")
      names should not contain "execute_refresh_sql_stmt"
      val skips = detailsForStep(rows, "execute_refresh_sql_attempt")
      skips should have size 1
      skips.head should include("status=skipped")
      skips.head should include("duration_ns=0")
      skips.head should include("origin_class=unknown")
      skips.head should include("credit_ns=0")
      skips.head should include("reason=no_pending_deltas")
      names should contain("profile_persist")
      names should contain("state_backup")
      names should contain("after_query_span")
      names should contain("command_span")
    }
  }

  describe("RefreshProfile — REFRESH with deltas") {
    it("records enqueue time rather than remote upload time in asynchronous mode") {
      sql("CREATE TABLE profile_async_src (id INT) USING DELTA")
      sql("CREATE MATERIALIZED VIEW profile_async_mv AS SELECT id FROM profile_async_src")
      sql("INSERT INTO profile_async_src VALUES (1)")
      clearProfile()
      val entered  = new CountDownLatch(1)
      val release  = new CountDownLatch(1)
      val finished = new CountDownLatch(1)
      spark.conf.set(FeatureGate.StateSyncAsyncEnabledKey, "true")
      OpenIvmStateSync.setStateSyncUriHookForTesting(_ => Some("file:/profile-async-backup"))
      OpenIvmStateSync.setBackupPassHookForTesting { (_, _, _) =>
        entered.countDown()
        try release.await(60, TimeUnit.SECONDS) shouldBe true
        finally finished.countDown()
      }
      try {
        sql("REFRESH MATERIALIZED VIEW profile_async_mv").collect()
        entered.await(30, TimeUnit.SECONDS) shouldBe true
        sql("SELECT id FROM profile_async_mv").collect().toSeq shouldBe Seq(Row(1))
        val rows = showProfile()
        rows.count(_.getString(4) == "state_backup_enqueue") shouldBe 1
        rows.count(_.getString(4) == "state_backup") shouldBe 0
        detailsForStep(rows, "command_span").head should include("outcome=incremental_executed")
        OpenIvmExecutionSpan.hasCurrentSpan shouldBe false
      } finally {
        release.countDown()
        try finished.await(60, TimeUnit.SECONDS) shouldBe true
        finally {
          spark.conf.set(FeatureGate.StateSyncAsyncEnabledKey, "false")
          OpenIvmStateSync.setBackupPassHookForTesting(null)
          OpenIvmStateSync.setStateSyncUriHookForTesting(null)
        }
      }
    }

    it("emits statement and RocksDB contention telemetry") {
      clearProfile()
      sql("CREATE TABLE profile_t3 (k INT, v INT) USING DELTA")
      sql("INSERT INTO profile_t3 VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW profile_mv3 AS SELECT k, SUM(v) AS s FROM profile_t3 GROUP BY k")
      sql("INSERT INTO profile_t3 VALUES (2, 20), (1, 30)")
      clearProfile()

      var backupEntered = false
      OpenIvmStateSync.setStateSyncUriHookForTesting(_ => Some("file:/profile-backup"))
      OpenIvmStateSync.setBackupPassHookForTesting { (_, _, _) =>
        OpenIvmExecutionSpan.hasCurrentSpan shouldBe true
        backupEntered = true
      }
      try sql("REFRESH MATERIALIZED VIEW profile_mv3").collect()
      finally {
        OpenIvmStateSync.setBackupPassHookForTesting(null)
        OpenIvmStateSync.setStateSyncUriHookForTesting(null)
      }
      backupEntered shouldBe true
      OpenIvmExecutionSpan.hasCurrentSpan shouldBe false

      val rows    = showProfile()
      val query   = rows.find(_.getString(4) == "query_span").get
      val backup  = rows.find(_.getString(4) == "state_backup").get
      val after   = rows.find(_.getString(4) == "after_query_span").get
      val command = rows.find(_.getString(4) == "command_span").get
      command.getLong(5) should be >= query.getLong(5)
      after.getLong(5) should be >= backup.getLong(5)
      rows.count(_.getString(4) == "query_span") shouldBe 1
      rows.count(_.getString(4) == "command_span") shouldBe 1
      val commonFinalization = Seq(
        "gc_prepare",
        "sql_log_handoff",
        "state_lag_check",
        "finalize_consume_frozen_sources",
        "finalize_data_version",
        "finalize_catalog_advance",
        "finalize_mark_consumed"
      )
      val interceptOnly = Seq("finalize_dependency_inventory", "finalize_prune_consumed")
      if (changeFeedMode == ChangeFeedMode.Cdf) {
        interceptOnly.foreach(step => detailsForStep(rows, step) shouldBe empty)
      }
      (commonFinalization ++ (if (changeFeedMode == ChangeFeedMode.Cdf) Seq.empty else interceptOnly)).foreach { step =>
        val details = detailsForStep(rows, step)
        details should have size 1
        details.head should include("clock_domain_id=")
        details.head should include("start_monotonic_ns=")
        details.head should include("duration_ns=")
      }
      val stmtDetails = detailsForStep(rows, "execute_refresh_sql_stmt")
      stmtDetails should not be empty
      stmtDetails.exists(_.contains("stmt_kind=")) shouldBe true
      stmtDetails.exists(_.contains("bytes=")) shouldBe true

      val attempts = detailsForStep(rows, "execute_refresh_sql_attempt")
      attempts should not be empty
      attempts.exists(_.contains("work_class=incremental_sql")) shouldBe true
      attempts.foreach { detail =>
        detail should include("clock_domain_id=")
        detail should include("start_epoch_ms=")
        detail should include("start_monotonic_ns=")
        detail should include("duration_ns=")
        detail should include("work_class=incremental_")
        if (detail.contains("status=skipped")) {
          detail should include("duration_ns=0")
          detail should include("credit_ns=0")
        } else {
          detail should include("status=success")
          detail should include("action_id=")
          detail should include("credit_ns=0")
          detail should include("query_log_order=")
          detail should include("attempt=0")
        }
      }
      val dispatchDetails = detailsForStep(rows, "generate_refresh_sql.dispatch")
      dispatchDetails should not be empty
      dispatchDetails.head should include("refresh_type=")

      val rocksdbDetails = detailsForStep(rows, "rocksdb_operation")
      rocksdbDetails should not be empty
      rocksdbDetails.exists(_.contains("db_scope=mv")) shouldBe true
      rocksdbDetails.exists(_.contains("db_scope=index")) shouldBe false
      rocksdbDetails.exists(_.contains("operation=get")) shouldBe true
      rocksdbDetails.foreach { detail =>
        detail should include("operation_count=")
        detail should include("jvm_lock_wait_ns=")
        detail should include("max_jvm_lock_wait_ns=")
        detail should include("external_lock_wait_ns=")
        detail should include("native_open_ns=")
        detail should include("native_close_ns=")
        detail should include("body_ns=")
        detail should not include spark.conf.get("spark.sql.warehouse.dir")
      }
    }

    it("links ordinary dispatched statements to their exact captured origin without credit") {
      sql("CREATE TABLE profile_lineage_src (k INT, v INT) USING DELTA")
      sql("INSERT INTO profile_lineage_src VALUES (1, 10), (1, 20), (2, 30)")
      sql("CREATE MATERIALIZED VIEW profile_lineage_mv AS SELECT k, SUM(v) AS s FROM profile_lineage_src GROUP BY k")
      sql("DELETE FROM profile_lineage_src WHERE v = 10")
      clearProfile()
      sql("REFRESH MATERIALIZED VIEW profile_lineage_mv").collect()
      assertMvCorrect("profile_lineage_mv", "SELECT k, SUM(v) AS s FROM profile_lineage_src GROUP BY k")
      val rows      = showProfile()
      val refreshId = rows.find(_.getString(4) == "query_span").get.getString(0)
      RefreshSqlLogAsyncFlusher.awaitQuiescence(30000) shouldBe true
      val logs = RefreshSqlLogCatalog.scanAll(spark).filter(_.refreshId == refreshId)
      val attempts = detailsForStep(rows, "execute_refresh_sql_attempt")
        .filter(_.contains("compiler_program_digest="))
      attempts should not be empty
      attempts.foreach { detail =>
        val fields = detail
          .split(";")
          .iterator
          .map(_.split("=", 2))
          .collect { case Array(key, value) =>
            key -> value
          }
          .toMap
        val log = logs
          .find(r =>
            r.category == "rewritten_stmt" && r.stmtOrder == fields("query_log_order").toInt &&
              r.attemptIdx == fields("attempt").toInt
          )
          .get
        fields("executed_digest") shouldBe RewriteLineage.sha256Utf8(log.sqlText)
        fields("compiler_program_digest") should fullyMatch regex "[0-9a-f]{64}"
        fields("source_digest") should fullyMatch regex "[0-9a-f]{64}"
        fields("compiler_program_digest_kind") shouldBe "native_program_utf8"
        fields("source_ordinal_basis") shouldBe "zero_based_compiler_program_split"
        fields("source_digest_basis") shouldBe "split_statement_utf8_without_delimiter"
        fields("origin_class") shouldBe "unknown"
        fields("credit_ns") shouldBe "0"
      }
    }

    it("separates incremental cleanup attempts from their retry wrapper") {
      sql("CREATE TABLE profile_cleanup_src (k INT, v INT) USING DELTA")
      sql("INSERT INTO profile_cleanup_src VALUES (1, 10), (2, 20)")
      sql("CREATE MATERIALIZED VIEW profile_cleanup_mv AS SELECT k, COUNT(*) AS n FROM profile_cleanup_src GROUP BY k")
      sql("DELETE FROM profile_cleanup_src WHERE k = 1")
      clearProfile()
      sql("REFRESH MATERIALIZED VIEW profile_cleanup_mv").collect()
      assertMvCorrect("profile_cleanup_mv", "SELECT k, COUNT(*) AS n FROM profile_cleanup_src GROUP BY k")
      val rows = showProfile()
      val attempts = detailsForStep(rows, "execute_refresh_sql_attempt")
        .filter(_.contains("work_class=incremental_cleanup"))
      attempts should have size 1
      attempts.head should include("status=success")
      attempts.head should include("attempt=0")
      attempts.head should include("origin_class=executor_helper")
      attempts.head should include("credit_reason=cleanup")
      attempts.head should include("duration_ns=")
      val wrappers = detailsForStep(rows, "execute_refresh_cleanup_retry")
      wrappers should have size 1
      wrappers.head should include("work_class=incremental_cleanup_retry")
      wrappers.head should include("duration_ns=")
    }

    it("records full writers separately from incremental SQL") {
      sql("CREATE TABLE profile_full_src (k INT) USING DELTA")
      sql("INSERT INTO profile_full_src VALUES (1), (2)")
      val query = "SELECT k FROM profile_full_src INTERSECT ALL SELECT k FROM profile_full_src"
      sql(s"CREATE MATERIALIZED VIEW profile_full_mv AS $query")
      sql("INSERT INTO profile_full_src VALUES (3)")
      clearProfile()
      sql("REFRESH MATERIALIZED VIEW profile_full_mv").collect()
      assertMvCorrect("profile_full_mv", query)
      val rows     = showProfile()
      val attempts = detailsForStep(rows, "execute_full_refresh_attempt")
      attempts should have size 1
      attempts.head should include("work_class=full_refresh_writer")
      attempts.head should include("status=success")
      attempts.head should include("origin_class=executor_helper")
      attempts.head should include("credit_ns=0")
      attempts.head should include("attempt=0")
      attempts.head should include("duration_ns=")
      detailsForStep(rows, "execute_full_refresh_retry") should have size 1
      detailsForStep(rows, "execute_refresh_sql_attempt") shouldBe empty
    }
  }

  describe("RefreshProfile — durable backup failure") {
    it("retains the query profile and records the failed command without leaking its active span") {
      sql("CREATE TABLE profile_backup_src(k INT) USING DELTA")
      sql("CREATE MATERIALIZED VIEW profile_backup_mv AS SELECT k FROM profile_backup_src")
      clearProfile()
      OpenIvmStateSync.setStateSyncUriHookForTesting(_ => Some("file:/profile-failed-backup"))
      OpenIvmStateSync.setBackupPassHookForTesting { (_, _, _) =>
        OpenIvmExecutionSpan.hasCurrentSpan shouldBe true
        throw new java.io.IOException("injected profile backup failure")
      }
      try {
        val failure = intercept[Exception] {
          sql("REFRESH MATERIALIZED VIEW profile_backup_mv").collect()
        }
        Iterator
          .iterate[Throwable](failure)(_.getCause)
          .takeWhile(_ != null)
          .exists(_.getMessage == "injected profile backup failure") shouldBe true
      } finally {
        OpenIvmStateSync.setBackupPassHookForTesting(null)
        OpenIvmStateSync.setStateSyncUriHookForTesting(null)
      }
      OpenIvmExecutionSpan.hasCurrentSpan shouldBe false
      val rows = showProfile()
      rows.count(_.getString(4) == "query_span") shouldBe 1
      rows.count(_.getString(4) == "state_backup") shouldBe 1
      detailsForStep(rows, "command_span").head should include("outcome=refresh_failed")
    }
  }

  describe("RefreshProfile — ordering invariant") {
    it("scanAll returns rows ordered by (profile_timestamp, refresh_id, step_order)") {
      clearProfile()
      sql("CREATE TABLE profile_t4 (k INT, v INT) USING DELTA")
      sql("INSERT INTO profile_t4 VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW profile_mv4 AS SELECT k, SUM(v) AS s FROM profile_t4 GROUP BY k")
      sql("INSERT INTO profile_t4 VALUES (2, 20)")
      sql("REFRESH MATERIALIZED VIEW profile_mv4").collect()
      sql("INSERT INTO profile_t4 VALUES (3, 30)")
      sql("REFRESH MATERIALIZED VIEW profile_mv4").collect()

      val rows = showProfile()
      rows.size should be > 0

      // Verify rows are ordered by (profile_timestamp, refresh_id, step_order)
      val triples = rows.map(r => (r.getTimestamp(2).getTime, r.getString(0), r.getInt(3)))
      val sorted  = triples.sortBy(t => (t._1, t._2, t._3))
      triples shouldBe sorted

      // Within each refresh_id, step_order is monotonically increasing from 0
      val byRid = rows.groupBy(_.getString(0))
      byRid.foreach { case (_, rs) =>
        val orders = rs.map(_.getInt(3)).sorted
        orders.head shouldBe 0
        orders.zip(orders.tail).forall { case (a, b) => b == a + 1 } shouldBe true
      }
    }
  }

  describe("RefreshProfile — scalar cascade") {
    it("separately times version pinning and snapshot-delta materialization") {
      clearProfile()
      sql("CREATE TABLE profile_scalar_src (v INT) USING DELTA")
      sql("INSERT INTO profile_scalar_src VALUES (10), (20)")
      sql(
        "CREATE MATERIALIZED VIEW profile_scalar_up AS " +
          "SELECT SUM(v) AS total FROM profile_scalar_src"
      )
      sql(
        "CREATE MATERIALIZED VIEW profile_scalar_down AS " +
          "SELECT total FROM profile_scalar_up"
      )
      sql("INSERT INTO profile_scalar_src VALUES (30)")
      clearProfile()

      sql("REFRESH MATERIALIZED VIEW profile_scalar_up").collect()

      val rows  = showProfile()
      val names = rows.map(_.getString(4)).toSet
      if (changeFeedMode == ChangeFeedMode.Intercept) {
        names should contain("pin_simple_aggregate_version")
        names should contain("simple_aggregate_cascade_snapshot")
        detailsForStep(rows, "pin_simple_aggregate_version").head should include("source=delta_history")
        detailsForStep(rows, "simple_aggregate_cascade_snapshot").head should include("old_version=")
      } else {
        // CDF reads the pre/post images from Delta's change feed and does not
        // materialize the interception-only signed snapshot.
        names should not contain "pin_simple_aggregate_version"
        names should not contain "simple_aggregate_cascade_snapshot"
      }
    }
  }

  describe("RefreshProfile — profile gate OFF") {
    it("SHOW OPENIVM REFRESH PROFILE returns 0 rows after CREATE/REFRESH when gate is off") {
      restartSpark(Map("spark.openivm.profile.refresh" -> "false"))
      RefreshProfileCatalog.ensureTables(spark)
      clearProfile()

      sql("CREATE TABLE profile_t5 (k INT, v INT) USING DELTA")
      sql("INSERT INTO profile_t5 VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW profile_mv5 AS SELECT k, SUM(v) AS s FROM profile_t5 GROUP BY k")
      sql("INSERT INTO profile_t5 VALUES (2, 20)")
      sql("REFRESH MATERIALIZED VIEW profile_mv5").collect()

      val rows = showProfile()
      rows shouldBe empty
    }
  }
}
