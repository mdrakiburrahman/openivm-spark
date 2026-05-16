package org.openivm.spark.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.openivm.spark.commands.CreateMaterializedViewCommand
import org.openivm.spark.commands.DropMaterializedViewCommand
import org.openivm.spark.commands.RefreshMaterializedViewCommand
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
        System.getProperty("java.io.tmpdir") + "/ivm-parser-spec-warehouse"
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
  // Test 5 — passthrough for ordinary SQL
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
