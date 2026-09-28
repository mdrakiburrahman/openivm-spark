package org.openivm.spark.parity

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.AnalysisException
import org.openivm.spark.common.{FeatureGate, QueryLogExport, RefreshSqlLogAsyncFlusher}
import org.openivm.spark.parity.base.{IvmParityMode, IvmParitySpecBase}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import java.util.concurrent.{Callable, CyclicBarrier, Executors, TimeUnit}
import scala.jdk.CollectionConverters._
import scala.collection.mutable

abstract class QueryLogExportScenarios
    extends IvmParitySpecBase("query-log-export")
    with BeforeAndAfterEach
    with Eventually {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.QueryLogEnabledKey -> "true", FeatureGate.ProfileRefreshKey -> "false")
  override protected def sparkMaster: String = "local[2]"

  private val mapper   = new ObjectMapper()
  private val requests = mutable.ArrayBuffer.empty[String]

  private def capture(request: String, statement: String, beforeAction: () => Unit = () => ()): Unit = {
    val context  = spark.sparkContext
    val previous = context.getLocalProperty(QueryLogExport.RequestIdProperty)
    context.setLocalProperty(QueryLogExport.RequestIdProperty, request)
    try {
      QueryLogExport.begin(spark, request)
      requests.synchronized { requests += request }
      var succeeded = false
      try {
        beforeAction()
        sql(statement).collect()
        succeeded = true
      } finally QueryLogExport.end(spark, request, succeeded)
    } finally context.setLocalProperty(QueryLogExport.RequestIdProperty, previous)
  }

  private def snapshot(request: String): JsonNode =
    mapper.readTree(QueryLogExport.snapshotJson(spark.newSession(), request, 100000, 67108864))

  private def terminal(request: String, status: String = "complete"): JsonNode = {
    eventually(timeout(Span(60, Seconds))) {
      val result = snapshot(request)
      result.path("status").asText() shouldBe status
      result.path("pending_flushes").asInt() shouldBe 0
    }
    snapshot(request)
  }

  private def records(result: JsonNode): Vector[JsonNode] =
    result.path("records").elements().asScala.toVector

  override def afterEach(): Unit =
    try {
      requests.foreach { request =>
        eventually(timeout(Span(60, Seconds))) { QueryLogExport.release(spark, request) }
      }
      requests.clear()
    } finally super.afterEach()

  override def afterAll(): Unit =
    try RefreshSqlLogAsyncFlusher.awaitQuiescence(30000L) shouldBe true
    finally super.afterAll()

  describe("scoped SQL export through real CREATE/REFRESH lifecycles") {
    it("exports complete CREATE and REFRESH SQL, including late cleanup, with profiling disabled") {
      FeatureGate.profileRefreshEnabled(spark) shouldBe false
      sql("CREATE TABLE qexport_source (k INT, v INT) USING DELTA")
      sql("INSERT INTO qexport_source VALUES (1, 10), (2, 20)")
      val body = "SELECT k, SUM(v) AS total, COUNT(*) AS cnt FROM qexport_source GROUP BY k"
      capture("create", s"CREATE MATERIALIZED VIEW qexport_mv AS $body")
      val created = terminal("create")
      created.path("capture_complete").asBoolean() shouldBe true
      created.path("invocations").get(0).path("mode").asText() shouldBe "create"
      created.path("invocations").get(0).path("outcome").asText() shouldBe "create_executed"
      records(created).find(_.path("category").asText() == "original_query").get.path("sql_text").asText() shouldBe body
      records(created).map(_.path("category").asText()) should contain allOf (
        "initial_load_ctas", "catalog_registration"
      )
      sql("INSERT INTO qexport_source VALUES (1, 5), (3, 30)")
      capture("refresh", "REFRESH MATERIALIZED VIEW qexport_mv")
      val refreshed = terminal("refresh")
      refreshed.path("capture_complete").asBoolean() shouldBe true
      refreshed.path("record_count").asInt() shouldBe records(refreshed).size
      refreshed.path("invocations").get(0).path("outcome").asText() shouldBe "incremental_executed"
      records(refreshed).map(_.path("category").asText()) should contain allOf (
        "original_query", "register_source_delta", "rewritten_stmt", "drop_cleanup"
      )
      records(refreshed).filter(_.path("category").asText() == "register_source_delta").foreach { row =>
        row.path("representation_kind").asText() shouldBe "diagnostic"
      }
      records(refreshed).map(_.path("refresh_id").asText()).distinct should have size 1
      records(refreshed).head.path("refresh_id").asText() should not be
        created.path("invocations").get(0).path("refresh_id").asText()
      assertMvCorrect("qexport_mv", body)
    }

    it("reports a zero-row native no-op explicitly instead of inferring it from an empty catalog") {
      sql("CREATE TABLE qexport_noop_source (k INT, v INT) USING DELTA")
      sql("INSERT INTO qexport_noop_source VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW qexport_noop_mv AS SELECT k, SUM(v) AS total FROM qexport_noop_source GROUP BY k")
      capture("native-no-op", "REFRESH MATERIALIZED VIEW qexport_noop_mv")
      val result = terminal("native-no-op")
      result.path("sql_succeeded").asBoolean() shouldBe true
      result.path("capture_complete").asBoolean() shouldBe true
      result.path("record_count").asInt() shouldBe 0
      result.path("invocations").get(0).path("outcome").asText() shouldBe "no_pending_deltas"
      result.path("invocations").get(0).path("completed").asBoolean() shouldBe true
      records(result) shouldBe empty
      assertMvCorrect("qexport_noop_mv", "SELECT k, SUM(v) AS total FROM qexport_noop_source GROUP BY k")
    }

    it("isolates concurrently created and refreshed MVs without making SQL workers wait for exports") {
      (1 to 2).foreach { index =>
        sql(s"CREATE TABLE qexport_parallel_source_$index (k INT, v INT) USING DELTA")
        sql(s"INSERT INTO qexport_parallel_source_$index VALUES (1, $index)")
      }
      val pool = Executors.newFixedThreadPool(2)
      try {
        Seq("create", "refresh").foreach { phase =>
          if (phase == "refresh")
            (1 to 2).foreach(index => sql(s"INSERT INTO qexport_parallel_source_$index VALUES (2, ${index + 10})"))
          val barrier = new CyclicBarrier(2)
          val futures = (1 to 2).map { index =>
            pool.submit(new Callable[Unit] {
              override def call(): Unit = {
                val statement =
                  if (phase == "create")
                    s"CREATE MATERIALIZED VIEW qexport_parallel_mv_$index AS " +
                      s"SELECT k, SUM(v) AS total FROM qexport_parallel_source_$index GROUP BY k"
                  else s"REFRESH MATERIALIZED VIEW qexport_parallel_mv_$index"
                capture(s"parallel-$phase-$index", statement, () => { barrier.await(30, TimeUnit.SECONDS); () })
              }
            })
          }
          futures.foreach(_.get(180, TimeUnit.SECONDS))
          (1 to 2).foreach { index =>
            val result = terminal(s"parallel-$phase-$index")
            result.path("invocations").size() shouldBe 1
            records(result) should not be empty
            records(result).foreach { row =>
              row.path("view_name").asText() should endWith(s"qexport_parallel_mv_$index")
              row.path("mode").asText() shouldBe phase
            }
            assertMvCorrect(
              s"qexport_parallel_mv_$index",
              s"SELECT k, SUM(v) AS total FROM qexport_parallel_source_$index GROUP BY k"
            )
          }
        }
      } finally pool.shutdownNow()
    }

    it("preserves SQL analysis and parse errors while distinguishing native failure from missing lifecycle") {
      sql("CREATE TABLE qexport_error_source (k INT) USING DELTA")
      val error = intercept[AnalysisException] {
        capture(
          "native-failure",
          "CREATE MATERIALIZED VIEW qexport_error_mv AS SELECT absent FROM qexport_error_source"
        )
      }
      error.getMessage should include("absent")
      val nativeFailure = terminal("native-failure", "failed")
      nativeFailure.path("sql_succeeded").asBoolean() shouldBe false
      nativeFailure.path("capture_complete").asBoolean() shouldBe true
      nativeFailure.path("failure").path("code").asText() shouldBe "SQL_FAILED"
      nativeFailure.path("invocations").get(0).path("outcome").asText() shouldBe "create_failed"
      records(nativeFailure).map(_.path("category").asText()) should contain("original_query")
      intercept[Exception] { capture("parse-failure", "CREATE MATERIALIZED VIEW") }
      val parseFailure = terminal("parse-failure", "failed")
      parseFailure.path("capture_complete").asBoolean() shouldBe false
      parseFailure.path("failure").path("code").asText() shouldBe "NO_LIFECYCLE"
      parseFailure.path("invocations").size() shouldBe 0
    }

    it("leaves CREATE/REFRESH behavior unchanged when the logger gate is disabled but rejects capture acceptance") {
      RefreshSqlLogAsyncFlusher.awaitQuiescence(30000L) shouldBe true
      restartSpark(Map(FeatureGate.QueryLogEnabledKey -> "false", FeatureGate.ProfileRefreshKey -> "false"))
      sql("CREATE TABLE qexport_disabled_source (k INT, v INT) USING DELTA")
      sql("INSERT INTO qexport_disabled_source VALUES (1, 10)")
      capture(
        "disabled-create",
        "CREATE MATERIALIZED VIEW qexport_disabled_mv AS SELECT k, SUM(v) AS total FROM qexport_disabled_source GROUP BY k"
      )
      sql("INSERT INTO qexport_disabled_source VALUES (2, 20)")
      capture("disabled-refresh", "REFRESH MATERIALIZED VIEW qexport_disabled_mv")
      Seq("disabled-create", "disabled-refresh").foreach { request =>
        val result = terminal(request, "failed")
        result.path("sql_succeeded").asBoolean() shouldBe true
        result.path("capture_complete").asBoolean() shouldBe false
        result.path("failure").path("code").asText() shouldBe "QUERY_LOG_DISABLED"
        records(result) shouldBe empty
      }
      assertMvCorrect("qexport_disabled_mv", "SELECT k, SUM(v) AS total FROM qexport_disabled_source GROUP BY k")
    }
  }
}
