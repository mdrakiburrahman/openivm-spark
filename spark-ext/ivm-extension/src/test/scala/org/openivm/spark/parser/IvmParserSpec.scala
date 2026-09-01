package org.openivm.spark.parser

import java.io.File
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, TimestampType}
import org.openivm.spark.commands.CreateMaterializedViewCommand
import org.openivm.spark.commands.DropMaterializedViewCommand
import org.openivm.spark.commands.ExplainCreateMaterializedViewCommand
import org.openivm.spark.commands.AdvanceMaterializedViewSourceVersionsCommand
import org.openivm.spark.commands.RefreshMaterializedViewCommand
import org.openivm.spark.commands.ShowMaterializedViewRefreshSqlCommand
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
