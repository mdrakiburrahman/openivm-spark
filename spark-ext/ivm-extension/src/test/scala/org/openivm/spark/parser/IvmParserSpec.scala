package org.openivm.spark.parser

import java.io.File
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, TimestampType}
import org.openivm.spark.commands.AnnotateOpenIvmMvQueryHashCommand
import org.openivm.spark.commands.BeginOpenIvmInsightsRequestCommand
import org.openivm.spark.commands.CreateMaterializedViewCommand
import org.openivm.spark.commands.DropMaterializedViewCommand
import org.openivm.spark.commands.EndOpenIvmInsightsRequestCommand
import org.openivm.spark.commands.ExplainCreateMaterializedViewCommand
import org.openivm.spark.commands.AdvanceMaterializedViewSourceVersionsCommand
import org.openivm.spark.commands.RefreshMaterializedViewCommand
import org.openivm.spark.commands.ReleaseOpenIvmInsightsRequestCommand
import org.openivm.spark.commands.ShowMaterializedViewRefreshSqlCommand
import org.openivm.spark.commands.ShowOpenIvmInsightsCommand
import org.openivm.spark.commands.ShowRefreshProfileCommand
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit / integration tests for [[IvmParser]].
 *
 * The SparkSession is built once for the suite (session-scoped) with
 * `spark.sql.extensions` pointing at [[org.openivm.spark.OpenIvmSparkExtensions]]
 * so that the full injection chain is exercised.
 */
class IvmParserSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("IvmParserSpec")
      .config("spark.sql.extensions", "org.openivm.spark.OpenIvmSparkExtensions")
      .config("spark.openivm.enabled", "true")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.warehouse.dir",
        new File(
          s"target/test-warehouse-ivm-parser-${UUID.randomUUID().toString.take(8)}"
        ).getCanonicalPath
      )
      .getOrCreate()
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
    } finally {
      super.afterAll()
    }

  describe("version-pinned grouped-window plan binding") {
    it("preserves quoted bindings, case-sensitive identities and source checks") {
      val setting = "spark.sql.caseSensitive"
      val prior   = spark.conf.get(setting)
      val source  = "`stage.db`.`Input.Rows`"
      val query = s"""WITH grouped AS (SELECT id AS g,COUNT(*) AS n,COUNT(DISTINCT id) AS d,SUM(id) AS s
        |FROM $source GROUP BY id) SELECT g,n,d,s,DENSE_RANK() OVER(ORDER BY s DESC) AS r FROM grouped""".stripMargin
      try {
        Seq(false, true).foreach { caseSensitive =>
          spark.conf.set(setting, caseSensitive.toString)
          val contract = GroupedWindowStagePlan.extract(spark, query, Vector(source))
          val frame    = spark.range(1L, 3L).toDF("id")
          val frames   = Map(source -> frame)
          contract.post(frames, contract.summary(frames)).count() shouldBe 2L
          contract.input(frames).count() shouldBe 2L
          val lowerCase = Map("`stage.db`.`input.rows`" -> frame)
          if (caseSensitive) {
            intercept[IllegalArgumentException] {
              contract.summary(lowerCase)
            }.getMessage should include("missing or unexpected stage source binding")
          } else {
            contract.summary(lowerCase).count() shouldBe 2L
            intercept[IllegalArgumentException] {
              contract.summary(frames ++ lowerCase)
            }.getMessage should include("ambiguous stage source binding")
          }
          intercept[IllegalArgumentException] {
            contract.input(Map.empty)
          }.getMessage should include("missing or unexpected stage source binding")
        }
      } finally spark.conf.set(setting, prior)
    }

    it("binds explicit SQL aliases to canonical command sources without live catalog lookup") {
      val query = """WITH grouped AS (SELECT id AS g,COUNT(*) AS n,COUNT(DISTINCT id) AS d,SUM(id) AS s
        |FROM stage_input GROUP BY id) SELECT g,n,d,s,DENSE_RANK() OVER(ORDER BY s DESC) AS r FROM grouped""".stripMargin
      val contract = GroupedWindowStagePlan.extract(
        spark,
        query,
        Vector("default.stage_input"),
        Map("stage_input" -> "default.stage_input")
      )
      val frames = Map("default.stage_input" -> spark.range(1L, 3L).toDF("id"))
      val rows = contract
        .post(frames, contract.summary(frames))
        .collect()
        .map(r => (r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getInt(4)))
        .toSet
      rows shouldBe Set((1L, 1L, 1L, 1L, 2), (2L, 1L, 1L, 2L, 1))
      intercept[IllegalArgumentException] {
        GroupedWindowStagePlan.extract(
          spark,
          query,
          Vector("default.stage_input"),
          Map("stage_input" -> "other.stage_input")
        )
      }.getMessage should include("alias must name a declared source")
    }
    it("binds qualified sources, ordered CTEs, nested scope and correlated subqueries explicitly") {
      val bound = spark.range(1L, 3L).toDF("id")
      def run(query: String, inputs: Map[String, org.apache.spark.sql.DataFrame]) =
        GroupedWindowStagePlan.bindReadPlan(spark, spark.sessionState.sqlParser.parsePlan(query), inputs)
      run("SELECT db.source.id FROM db.source", Map("db.source" -> bound))
        .collect()
        .map(_.getLong(0))
        .toSet shouldBe Set(1L, 2L)
      val query = """WITH first AS (SELECT id FROM pinned),
        |second AS (WITH first AS (SELECT id+10 AS id FROM pinned) SELECT id FROM first)
        |SELECT a.id, b.id FROM first a JOIN second b ON b.id=a.id+10
        |WHERE EXISTS (SELECT 1 FROM pinned p WHERE p.id=a.id)""".stripMargin
      val setting = "spark.sql.legacy.ctePrecedencePolicy"
      val prior   = spark.sessionState.conf.getAllConfs.get(setting)
      try {
        spark.conf.set(setting, "CORRECTED")
        run(query, Map("pinned" -> bound)).collect().map(r => r.getLong(0) -> r.getLong(1)).toSet shouldBe
          Set(1L -> 11L, 2L -> 12L)
        spark.conf.set(setting, "LEGACY")
        run(query, Map("pinned" -> bound)).count() shouldBe 0L
        spark.conf.set(setting, "EXCEPTION")
        intercept[org.apache.spark.sql.AnalysisException] {
          run(query, Map("pinned" -> bound)).collect()
        }.getMessage should include("AMBIGUOUS_ALIAS_IN_NESTED_CTE")
      } finally
        prior match {
          case Some(value) => spark.conf.set(setting, value)
          case None        => spark.conf.unset(setting)
        }
    }

    it("refuses live-catalog fallback in missing CTE definitions and nested subqueries") {
      val bound = spark.range(1L, 3L).toDF("id")
      bound.createOrReplaceTempView("unbound_stage_source")
      try {
        val queries = Seq(
          "SELECT * FROM unbound_stage_source",
          "WITH first AS (SELECT * FROM unbound_stage_source) SELECT * FROM first",
          "SELECT * FROM pinned p WHERE EXISTS (SELECT 1 FROM unbound_stage_source u WHERE u.id=p.id)",
          "WITH later AS (SELECT * FROM next), next AS (SELECT * FROM pinned) SELECT * FROM later",
          "WITH unbound_stage_source AS (SELECT * FROM unbound_stage_source) SELECT * FROM unbound_stage_source",
          "SELECT * FROM other.pinned"
        )
        queries.foreach { query =>
          intercept[IllegalArgumentException] {
            GroupedWindowStagePlan.bindReadPlan(
              spark,
              spark.sessionState.sqlParser.parsePlan(query),
              Map("pinned" -> bound)
            )
          }.getMessage should include("unbound stage relation")
        }
        intercept[IllegalArgumentException] {
          GroupedWindowStagePlan.bindReadPlan(
            spark,
            spark.sessionState.sqlParser.parsePlan("SELECT * FROM pinned"),
            Map("pinned" -> bound, "PINNED" -> bound)
          )
        }.getMessage should include("ambiguous stage source binding")
      } finally spark.catalog.dropTempView("unbound_stage_source")
    }

    it("extracts only the signed native input query and normalizes compiler timestamp gates") {
      val sql = """UPDATE openivm_views SET refresh_in_progress=true;
        |WITH input_rows AS (
        | SELECT id, openivm_multiplicity FROM openivm_delta_input
        | WHERE (openivm_timestamp>=CAST('2026-01-01 00:00:00' AS TIMESTAMP)))
        |INSERT INTO openivm_delta_stage(id,openivm_multiplicity)
        |SELECT id,openivm_multiplicity FROM input_rows;""".stripMargin
      val input        = spark.sql("SELECT CAST(7 AS BIGINT) AS id, CAST(-2 AS BIGINT) AS openivm_multiplicity")
      val first        = GroupedWindowStagePlan.inputDelta(spark, sql, "stage")
      val changedClock = GroupedWindowStagePlan.inputDelta(spark, sql.replace("2026-01-01", "2026-02-02"), "stage")
      changedClock.identity shouldBe first.identity
      val rows = first(Map("openivm_delta_input" -> input)).collect()
      rows.length shouldBe 1
      rows.head.getLong(0) shouldBe 7L
      rows.head.getLong(1) shouldBe -2L
      intercept[IllegalArgumentException] {
        GroupedWindowStagePlan.inputDelta(spark, sql + sql, "stage")
      }.getMessage should include("one native signed-input contribution")
      intercept[IllegalArgumentException] {
        first(Map("other.openivm_delta_input" -> input))
      }.getMessage should include("unbound stage relation")
    }
  }

  // ---------------------------------------------------------------------------
  // Test 1 — basic CREATE MATERIALIZED VIEW
  // ---------------------------------------------------------------------------
  describe("CREATE MATERIALIZED VIEW") {
    it("parses a simple AS SELECT 1 to CreateMaterializedViewCommand") {
      val plan = spark.sessionState.sqlParser.parsePlan(
        "CREATE MATERIALIZED VIEW v AS SELECT 1"
      )
      plan shouldBe a[CreateMaterializedViewCommand]
      val cmd = plan.asInstanceOf[CreateMaterializedViewCommand]
      cmd.name shouldBe TableIdentifier("v")
      cmd.ifNotExists shouldBe false
      cmd.provider shouldBe None
      cmd.properties shouldBe Map.empty
      // inner query plan is a Project wrapping a range / one-row relation
      cmd.query shouldBe a[Project]
    }

    // -------------------------------------------------------------------------
    // Test 2 — full-form with IF NOT EXISTS, three-part name, USING, TBLPROPERTIES
    // -------------------------------------------------------------------------
    it("parses IF NOT EXISTS / USING / TBLPROPERTIES / three-part name") {
      val sql =
        "CREATE MATERIALIZED VIEW IF NOT EXISTS db.s.v " +
          "USING DELTA TBLPROPERTIES('a'='b') AS SELECT id FROM t WHERE id > 0"
      val plan = spark.sessionState.sqlParser.parsePlan(sql)
      plan shouldBe a[CreateMaterializedViewCommand]
      val cmd = plan.asInstanceOf[CreateMaterializedViewCommand]
      cmd.name shouldBe TableIdentifier("v", Some("s"), Some("db"))
      cmd.ifNotExists shouldBe true
      cmd.provider shouldBe Some("DELTA")
      cmd.properties shouldBe Map("a" -> "b")
      // query body has a filter (WHERE id > 0) under the top-level Project
      cmd.query shouldBe a[Project]
      cmd.query.asInstanceOf[Project].child shouldBe a[Filter]
    }

    // -------------------------------------------------------------------------
    // Test 9 — originalQueryText is preserved verbatim
    // -------------------------------------------------------------------------
    it("captures originalQueryText verbatim from the AS clause") {
      val queryBody = "SELECT 42 AS answer"
      val sql       = s"CREATE MATERIALIZED VIEW v AS $queryBody"
      val plan      = spark.sessionState.sqlParser.parsePlan(sql)
      plan shouldBe a[CreateMaterializedViewCommand]
      val cmd = plan.asInstanceOf[CreateMaterializedViewCommand]
      cmd.originalQueryText shouldBe queryBody
    }

    // -------------------------------------------------------------------------
    // Test 10 — CLUSTER BY (#24)
    // -------------------------------------------------------------------------
    it("parses a CLUSTER BY clause into clusterColumns (declaration order)") {
      val sql =
        "CREATE MATERIALIZED VIEW v CLUSTER BY (region, day) AS SELECT region, day FROM t"
      val plan = spark.sessionState.sqlParser.parsePlan(sql)
      plan shouldBe a[CreateMaterializedViewCommand]
      val cmd = plan.asInstanceOf[CreateMaterializedViewCommand]
      cmd.clusterColumns shouldBe Seq("region", "day")
      cmd.originalQueryText shouldBe "SELECT region, day FROM t"
    }

    it("defaults clusterColumns to empty when no CLUSTER BY clause is present") {
      val plan = spark.sessionState.sqlParser.parsePlan("CREATE MATERIALIZED VIEW v AS SELECT 1")
      plan.asInstanceOf[CreateMaterializedViewCommand].clusterColumns shouldBe empty
    }
  }

  // ---------------------------------------------------------------------------
  // Test 11 — EXPLAIN CREATE MATERIALIZED VIEW (#4)
  // ---------------------------------------------------------------------------
  describe("EXPLAIN CREATE MATERIALIZED VIEW") {
    it("parses to ExplainCreateMaterializedViewCommand with the inner query text") {
      val plan = spark.sessionState.sqlParser.parsePlan(
        "EXPLAIN CREATE MATERIALIZED VIEW v AS SELECT id FROM t"
      )
      plan shouldBe a[ExplainCreateMaterializedViewCommand]
      val cmd = plan.asInstanceOf[ExplainCreateMaterializedViewCommand]
      cmd.name shouldBe TableIdentifier("v")
      cmd.queryText shouldBe "SELECT id FROM t"
      cmd.clusterColumns shouldBe empty
      cmd.output.map(_.name) shouldBe Seq("explain")
    }

    it("carries CLUSTER BY columns through the EXPLAIN wrapper") {
      val plan = spark.sessionState.sqlParser.parsePlan(
        "EXPLAIN CREATE MATERIALIZED VIEW db.v CLUSTER BY (k) AS SELECT k FROM t"
      )
      val cmd = plan.asInstanceOf[ExplainCreateMaterializedViewCommand]
      cmd.name shouldBe TableIdentifier("v", Some("db"))
      cmd.clusterColumns shouldBe Seq("k")
    }

    it("does not intercept a bare EXPLAIN <query> (delegates to Spark)") {
      val plan = spark.sessionState.sqlParser.parsePlan("EXPLAIN SELECT 1")
      plan should not be a[ExplainCreateMaterializedViewCommand]
    }
  }

  // ---------------------------------------------------------------------------
  // Test 12 — SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW (#25)
  // ---------------------------------------------------------------------------
  describe("SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW") {
    it("parses to ShowMaterializedViewRefreshSqlCommand with the inner query text") {
      val plan = spark.sessionState.sqlParser.parsePlan(
        "SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW v AS SELECT id FROM t"
      )
      plan shouldBe a[ShowMaterializedViewRefreshSqlCommand]
      val cmd = plan.asInstanceOf[ShowMaterializedViewRefreshSqlCommand]
      cmd.name shouldBe TableIdentifier("v")
      cmd.queryText shouldBe "SELECT id FROM t"
      cmd.output.map(_.name) shouldBe Seq("refresh_sql")
    }

    it("does not collide with SHOW OPENIVM REFRESH PROFILE routing") {
      val plan = spark.sessionState.sqlParser.parsePlan("SHOW OPENIVM REFRESH PROFILE")
      plan shouldBe a[ShowRefreshProfileCommand]
    }
  }

  // ---------------------------------------------------------------------------
  // Test 13 — OPTIMIZE is NOT intercepted (falls through to base Spark/Delta)
  // ---------------------------------------------------------------------------
  describe("OPTIMIZE passthrough") {
    it("does not route OPTIMIZE to an IVM node") {
      // base Spark on the unit-test classpath (no Delta command extension) rejects
      // OPTIMIZE, which proves IvmParser delegated rather than intercepting it.
      an[ParseException] should be thrownBy {
        spark.sessionState.sqlParser.parsePlan("OPTIMIZE v")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3 — REFRESH MATERIALIZED VIEW
  // ---------------------------------------------------------------------------
  describe("REFRESH MATERIALIZED VIEW") {
    it("parses to RefreshMaterializedViewCommand") {
      val plan = spark.sessionState.sqlParser.parsePlan("REFRESH MATERIALIZED VIEW v")
      plan shouldBe a[RefreshMaterializedViewCommand]
      val cmd = plan.asInstanceOf[RefreshMaterializedViewCommand]
      cmd.name shouldBe TableIdentifier("v")
    }
  }

  describe("ALTER MATERIALIZED VIEW ADVANCE SOURCE VERSIONS") {
    it("parses an exact multi-source immutable version map") {
      val plan = spark.sessionState.sqlParser.parsePlan(
        "ALTER MATERIALIZED VIEW db.mv ADVANCE SOURCE VERSIONS (db.a = 5, `db`.`b` = 12)"
      )
      plan shouldBe a[AdvanceMaterializedViewSourceVersionsCommand]
      val cmd = plan.asInstanceOf[AdvanceMaterializedViewSourceVersionsCommand]
      cmd.name shouldBe TableIdentifier("mv", Some("db"))
      cmd.versionsBySource shouldBe Map("db.a" -> 5L, "db.b" -> 12L)
    }

    it("rejects duplicate source keys before they can collapse into a Map") {
      an[ParseException] should be thrownBy {
        spark.sessionState.sqlParser.parsePlan(
          "ALTER MATERIALIZED VIEW mv ADVANCE SOURCE VERSIONS (db.a = 5, DB.A = 6)"
        )
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 4 — DROP MATERIALIZED VIEW IF EXISTS
  // ---------------------------------------------------------------------------
  describe("DROP MATERIALIZED VIEW") {
    it("parses IF EXISTS flag correctly") {
      val plan = spark.sessionState.sqlParser.parsePlan("DROP MATERIALIZED VIEW IF EXISTS v")
      plan shouldBe a[DropMaterializedViewCommand]
      val cmd = plan.asInstanceOf[DropMaterializedViewCommand]
      cmd.name shouldBe TableIdentifier("v")
      cmd.ifExists shouldBe true
    }

    it("parses without IF EXISTS") {
      val plan = spark.sessionState.sqlParser.parsePlan("DROP MATERIALIZED VIEW v")
      plan shouldBe a[DropMaterializedViewCommand]
      plan.asInstanceOf[DropMaterializedViewCommand].ifExists shouldBe false
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5 — SHOW OPENIVM REFRESH PROFILE
  // ---------------------------------------------------------------------------
  describe("SHOW OPENIVM REFRESH PROFILE") {
    it("parses to ShowRefreshProfileCommand with the DuckDB-compatible schema") {
      val plan = spark.sessionState.sqlParser.parsePlan("SHOW OPENIVM REFRESH PROFILE")
      plan shouldBe a[ShowRefreshProfileCommand]
      plan.output.map(attr => (attr.name, attr.dataType, attr.nullable)) shouldBe Seq(
        ("refresh_id", StringType, false),
        ("view_name", StringType, false),
        ("profile_timestamp", TimestampType, false),
        ("step_order", IntegerType, false),
        ("step_name", StringType, false),
        ("duration_ms", LongType, false),
        ("detail", StringType, false)
      )
    }

    describe("request-scoped OPENIVM INSIGHTS") {
      it("parses BEGIN, SHOW, END, and RELEASE with existing string-literal conventions") {
        val begin = spark.sessionState.sqlParser
          .parsePlan(
            "oPeNiVm InSiGhTs BeGiN ReQuEsT 'request''one' RUN 'run-1' NODE 'model.pkg.node'"
          )
          .asInstanceOf[BeginOpenIvmInsightsRequestCommand]
        begin.requestId shouldBe "request'one"
        begin.runId shouldBe "run-1"
        begin.dbtNodeId shouldBe "model.pkg.node"
        begin.materialization shouldBe None
        begin.targetRelation shouldBe None

        val show = spark.sessionState.sqlParser
          .parsePlan("SHOW OPENIVM INSIGHTS FOR REQUEST 'request''one' AFTER 7 LIMIT 25")
          .asInstanceOf[ShowOpenIvmInsightsCommand]
        show.requestId shouldBe "request'one"
        show.afterSequence shouldBe 7L
        show.maxEvents shouldBe 25

        val succeeded = spark.sessionState.sqlParser
          .parsePlan("OPENIVM INSIGHTS END REQUEST 'request''one' STATUS SUCCEEDED")
          .asInstanceOf[EndOpenIvmInsightsRequestCommand]
        succeeded.requestId shouldBe "request'one"
        succeeded.succeeded shouldBe true
        succeeded.errorClass shouldBe None
        succeeded.errorCode shouldBe None

        val failed = spark.sessionState.sqlParser
          .parsePlan("OPENIVM INSIGHTS END REQUEST 'request''one' STATUS FAILED")
          .asInstanceOf[EndOpenIvmInsightsRequestCommand]
        failed.succeeded shouldBe false
        failed.errorClass shouldBe None
        failed.errorCode shouldBe None

        val release = spark.sessionState.sqlParser
          .parsePlan("OPENIVM INSIGHTS RELEASE REQUEST 'request''one'")
          .asInstanceOf[ReleaseOpenIvmInsightsRequestCommand]
        release.requestId shouldBe "request'one"
      }

      it("parses optional model context, safe failure identity, and the typed MV annotation") {
        val begin = spark.sessionState.sqlParser
          .parsePlan(
            """OPENIVM INSIGHTS BEGIN REQUEST 'context'
              |RUN 'run-context'
              |NODE 'model.pkg.context'
              |MATERIALIZATION 'materialized_view'
              |TARGET 'analytics.sales_summary'""".stripMargin
          )
          .asInstanceOf[BeginOpenIvmInsightsRequestCommand]
        begin.materialization shouldBe Some("materialized_view")
        begin.targetRelation shouldBe Some("analytics.sales_summary")

        val failed = spark.sessionState.sqlParser
          .parsePlan(
            """OPENIVM INSIGHTS END REQUEST 'context' STATUS FAILED
              |ERROR_CLASS 'org.apache.spark.SparkException'
              |ERROR_CODE 'SPARK_JOB_CANCELLED'""".stripMargin
          )
          .asInstanceOf[EndOpenIvmInsightsRequestCommand]
        failed.errorClass shouldBe Some("org.apache.spark.SparkException")
        failed.errorCode shouldBe Some("SPARK_JOB_CANCELLED")

        val annotation = spark.sessionState.sqlParser
          .parsePlan(
            """OPENIVM INSIGHTS ANNOTATE MATERIALIZED VIEW QUERY HASH
              |TARGET 'analytics.sales_summary'
              |OLD '0123456789abcdef0123456789abcdef'
              |NEW 'fedcba9876543210fedcba9876543210'
              |POLICY 'rebuild'
              |DECISION 'rebuild'
              |REASON 'query_hash_changed'""".stripMargin
          )
          .asInstanceOf[AnnotateOpenIvmMvQueryHashCommand]
        annotation.targetRelation shouldBe "analytics.sales_summary"
        annotation.oldQueryHash shouldBe "0123456789abcdef0123456789abcdef"
        annotation.newQueryHash shouldBe "fedcba9876543210fedcba9876543210"
        annotation.policy shouldBe "rebuild"
        annotation.decision shouldBe "rebuild"
        annotation.reason shouldBe "query_hash_changed"
      }

      it("rejects partial model/failure context and failure identity on success") {
        Seq(
          """OPENIVM INSIGHTS BEGIN REQUEST 'bad' RUN 'run' NODE 'node'
            |MATERIALIZATION 'materialized_view'""".stripMargin,
          """OPENIVM INSIGHTS END REQUEST 'bad' STATUS FAILED
            |ERROR_CLASS 'org.example.Failure'""".stripMargin,
          """OPENIVM INSIGHTS END REQUEST 'bad' STATUS SUCCEEDED
            |ERROR_CLASS 'org.example.Failure' ERROR_CODE 'FAILED'""".stripMargin
        ).foreach { statement =>
          an[ParseException] should be thrownBy spark.sessionState.sqlParser.parsePlan(statement)
        }
      }

      it("rejects arbitrary fields appended to the typed MV annotation") {
        an[ParseException] should be thrownBy spark.sessionState.sqlParser.parsePlan(
          """OPENIVM INSIGHTS ANNOTATE MATERIALIZED VIEW QUERY HASH
            |TARGET 'analytics.sales_summary'
            |OLD '0123456789abcdef0123456789abcdef'
            |NEW 'fedcba9876543210fedcba9876543210'
            |POLICY 'rebuild'
            |DECISION 'rebuild'
            |REASON 'query_hash_changed'
            |SQL 'DROP TABLE protected_relation'""".stripMargin
        )
      }

      it("exposes the exact ordered SHOW schema") {
        val plan = spark.sessionState.sqlParser
          .parsePlan("SHOW OPENIVM INSIGHTS FOR REQUEST 'schema' AFTER 0 LIMIT 1")
          .asInstanceOf[ShowOpenIvmInsightsCommand]
        plan.output.map(attribute => (attribute.name, attribute.dataType, attribute.nullable)) shouldBe Seq(
          ("record_type", StringType, false),
          ("request_id", StringType, false),
          ("run_id", StringType, true),
          ("dbt_node_id", StringType, true),
          ("capture_status", StringType, false),
          ("next_sequence", LongType, false),
          ("has_more", BooleanType, false),
          ("sequence", LongType, true),
          ("event_timestamp", TimestampType, true),
          ("operation_id", StringType, true),
          ("materialized_view", StringType, true),
          ("operation", StringType, true),
          ("stage", StringType, true),
          ("event_type", StringType, true),
          ("level", StringType, true),
          ("code", StringType, true),
          ("message", StringType, true),
          ("status", StringType, true),
          ("duration_ms", LongType, true),
          ("parent_sequence", LongType, true),
          ("details_json", StringType, true),
          ("terminal", BooleanType, true)
        )
      }

      it("rejects negative cursors and non-positive or oversized limits") {
        Seq(
          "SHOW OPENIVM INSIGHTS FOR REQUEST 'bad' AFTER -1 LIMIT 1",
          "SHOW OPENIVM INSIGHTS FOR REQUEST 'bad' AFTER 0 LIMIT 0",
          "SHOW OPENIVM INSIGHTS FOR REQUEST 'bad' AFTER 0 LIMIT 2147483648"
        ).foreach { statement =>
          an[ParseException] should be thrownBy spark.sessionState.sqlParser.parsePlan(statement)
        }
      }
    }

    it("rejects optional clauses") {
      an[ParseException] should be thrownBy {
        spark.sessionState.sqlParser.parsePlan("SHOW OPENIVM REFRESH PROFILE WHERE step_order > 0")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 6 — passthrough for ordinary SQL
  // ---------------------------------------------------------------------------
  describe("Passthrough") {
    it("delegates SELECT 1 to Spark's own parser unchanged") {
      // If this throws, the delegation is broken.
      val result = spark.sql("SELECT 1 AS x").collect()
      result.head.getInt(0) shouldBe 1
    }

    it("delegates SELECT on parsePlan") {
      val plan = spark.sessionState.sqlParser.parsePlan("SELECT 1")
      plan shouldBe a[Project]
    }
  }

  // ---------------------------------------------------------------------------
  // Test 6 — malformed CREATE MATERIALIZED VIEW raises ParseException
  // ---------------------------------------------------------------------------
  describe("Error handling") {
    it("raises ParseException for missing AS") {
      an[ParseException] should be thrownBy {
        spark.sessionState.sqlParser.parsePlan("CREATE MATERIALIZED VIEW v SELECT 1")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 7 — case-insensitive keyword recognition
  // ---------------------------------------------------------------------------
  describe("Case insensitivity") {
    it("recognises cReAtE mAtErIaLiZeD vIeW") {
      val plan = spark.sessionState.sqlParser.parsePlan(
        "cReAtE mAtErIaLiZeD vIeW v AS SELECT 1"
      )
      plan shouldBe a[CreateMaterializedViewCommand]
    }
  }

  // ---------------------------------------------------------------------------
  // Test 8 — leading whitespace / comments before the keyword
  // ---------------------------------------------------------------------------
  describe("Leading comments") {
    it("handles a single-line comment before CREATE MATERIALIZED VIEW") {
      val sql  = " -- hi\n CREATE MATERIALIZED VIEW v AS SELECT 1"
      val plan = spark.sessionState.sqlParser.parsePlan(sql)
      plan shouldBe a[CreateMaterializedViewCommand]
      plan.asInstanceOf[CreateMaterializedViewCommand].name shouldBe TableIdentifier("v")
    }

    it("handles block comments before CREATE MATERIALIZED VIEW") {
      val sql  = "/* block\n comment */ CREATE MATERIALIZED VIEW v AS SELECT 1"
      val plan = spark.sessionState.sqlParser.parsePlan(sql)
      plan shouldBe a[CreateMaterializedViewCommand]
    }
  }
}
