package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.Row
import org.openivm.spark.common.RefreshProfileCatalog

/** End-to-end coverage of `RefreshProfile` instrumentation in
  * `MaterializedViewCommands` + the `SHOW OPENIVM REFRESH PROFILE` SQL
  * statement. All table/MV names are prefixed `profile_` to avoid Delta
  * warehouse path collisions when this spec runs in parallel with other
  * parity specs.
  */
abstract class RefreshProfileScenarios extends IvmParitySpecBase("refresh-profile") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map("spark.openivm.profile.refresh" -> "true")

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
      names should contain("create_view_index")
      names should contain("create_mv_publish_metadata")
      names should contain("create_mv_total")
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
      names should not contain "execute_refresh_sql_stmt"
    }
  }

  describe("RefreshProfile — REFRESH with deltas") {
    it("emits statement and RocksDB contention telemetry") {
      clearProfile()
      sql("CREATE TABLE profile_t3 (k INT, v INT) USING DELTA")
      sql("INSERT INTO profile_t3 VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW profile_mv3 AS SELECT k, SUM(v) AS s FROM profile_t3 GROUP BY k")
      sql("INSERT INTO profile_t3 VALUES (2, 20), (1, 30)")
      clearProfile()

      sql("REFRESH MATERIALIZED VIEW profile_mv3").collect()

      val rows        = showProfile()
      val stmtDetails = detailsForStep(rows, "execute_refresh_sql_stmt")
      stmtDetails should not be empty
      stmtDetails.exists(_.contains("stmt_kind=")) shouldBe true
      stmtDetails.exists(_.contains("bytes=")) shouldBe true

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
