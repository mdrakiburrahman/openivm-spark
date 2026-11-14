package org.openivm.spark.parity

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator
import org.openivm.spark.common.{FeatureGate, MvCatalog, MvMetadata, RefreshTypeCode}
import org.openivm.spark.compiler.OpenIvmCompileException
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class SparkExpressionIntegrationScenarios extends IvmParitySpecBase("expression-bridge") {
  self: IvmParityMode =>
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.CompileClassificationCacheEnabledKey -> "true",
    FeatureGate.ProfileRefreshKey                    -> "true"
  )

  describe("Spark expression bridge") {
    it("retains a real native compile-failure receipt through create and refresh") {
      assume(sys.env.get("OPENIVM_MARKER_INTEGRATION_TEST").contains("1"))
      sql("CREATE TABLE expr_bad_source (id INT) USING DELTA")
      sql("INSERT INTO expr_bad_source VALUES (1)")
      val query = "SELECT id, reflect('java.lang.Math', 'abs', id) AS v FROM expr_bad_source"
      intercept[OpenIvmCompileException] {
        sql(s"EXPLAIN CREATE MATERIALIZED VIEW expr_bad_explain AS $query").collect()
      }
      val logger        = "org.openivm.spark.commands.CreateMaterializedViewCommand"
      val previousLevel = LogManager.getLogger(logger).getLevel
      Configurator.setLevel(logger, Level.FATAL)
      try sql(s"CREATE MATERIALIZED VIEW expr_bad_mv AS $query")
      finally Configurator.setLevel(logger, previousLevel)
      val identifier = spark.sessionState.sqlParser.parseTableIdentifier("expr_bad_mv")
      def checkFailureReceipt(): Unit = {
        val meta = MvCatalog.lookup(spark, identifier).get
        meta.refreshType shouldBe RefreshTypeCode.FullRefresh
        meta.properties.get(MvMetadata.CompileRefreshTypeKey) shouldBe Some("COMPILE_FAILED")
        meta.properties.get(MvMetadata.RefreshReasonKey) shouldBe Some("compile_failed")
      }
      checkFailureReceipt()
      sql("INSERT INTO expr_bad_source VALUES (-2)")
      refreshMv("expr_bad_mv")
      checkFailureReceipt()
      assertMvCorrect("expr_bad_mv", query)
    }

    it("preserves incremental results for mixed changes") {
      assume(sys.env.get("OPENIVM_MARKER_INTEGRATION_TEST").contains("1"))
      spark.conf.set("spark.sql.legacy.sizeOfNull", "true")
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      Seq(false, true).foreach { ansi =>
        spark.conf.set("spark.sql.ansi.enabled", ansi.toString)
        val source = s"expr_cdf_source_$ansi"
        val mv     = s"expr_cdf_mv_$ansi"
        val query = s"""SELECT id, txt RLIKE '(?i)ä' AS re, size(split(txt, '/')) AS n,
          |element_at(split(txt, '/'), -1) AS leaf,
          |nullif(regexp_replace(regexp_replace(lower(trim(cast(txt AS STRING))), '/+', '/'), '/$$', ''), '') AS canonical,
          |regexp_extract(trim(cast(txt AS STRING)), '[A-Z](/.*)', 1) AS capture,
          |sha2(coalesce(txt, 'Unknown'), 256) AS scalar_hash,
          |sha2(to_json(struct(id, t, deleted, num), map('ignoreNullFields', 'false', 'timeZone', 'UTC')), 256) AS struct_hash,
          |sha2(to_json(named_struct('Odd Name', txt, 'nested', named_struct('Mixed', id)),
          |map('ignoreNullFields', 'false', 'timeZone', 'UTC')), 256) AS digest
          |FROM $source WHERE txt IS NULL OR CAST(txt AS STRING) RLIKE '(?i)ä' OR txt LIKE '%/%'
          |OR lower(element_at(split(txt, '/'), -1)) IN ('alpha', 'beta')""".stripMargin
        sql(s"CREATE TABLE $source (id INT, txt STRING, t TIMESTAMP, num BIGINT, deleted BOOLEAN) USING DELTA")
        sql(s"""INSERT INTO $source VALUES
          |(1, NULL, TIMESTAMP '2024-01-01 01:02:03', 1, false),
          |(2, 'Ä', TIMESTAMP '2024-02-01 01:02:03', 2, true),
          |(3, 'a/b', NULL, NULL, NULL)""".stripMargin)
        val explained =
          sql(s"EXPLAIN CREATE MATERIALIZED VIEW expr_explain_$ansi AS $query").collect().head.getString(0)
        explained should include(""""compile_refresh_type_name":"SIMPLE_PROJECTION"""")
        val dryProgram =
          sql(s"SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW expr_show_$ansi AS $query").collect().head.getString(0)
        dryProgram.toLowerCase should include("regexp_replace")
        dryProgram should not include "__openivm_spark_expression"
        sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
        val identifier = spark.sessionState.sqlParser.parseTableIdentifier(mv)
        MvCatalog.lookup(spark, identifier).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
        assertMvCorrect(mv, query)
        sql(s"DELETE FROM $source WHERE id = 1")
        sql(s"UPDATE $source SET txt = 'x/y' WHERE id = 2")
        sql(s"INSERT INTO $source VALUES (4, 'ä', NULL, 4, false)")
        refreshMv(mv)
        assertMvCorrect(mv, query)
        sql(s"DELETE FROM $source WHERE id = 4")
        sql(s"UPDATE $source SET txt = 'z/q' WHERE id = 3")
        sql(s"INSERT INTO $source VALUES (5, 'ä', NULL, 5, false)")
        refreshMv(mv)
        assertMvCorrect(mv, query)
        val detail = sql("SHOW OPENIVM REFRESH PROFILE")
          .where(s"view_name IN ('$mv','default.$mv') AND step_name = 'generate_refresh_sql.compile'")
          .select("detail")
          .collect()
          .last
          .getString(0)
        detail should include("compile_cache_hit=true")
        sql(s"""INSERT OVERWRITE TABLE $source
          |SELECT 8, 'x/y', TIMESTAMP '2024-03-01 01:02:03', 8L, false""".stripMargin)
        refreshMv(mv)
        assertMvCorrect(mv, query)
      }
    }
  }
}

class SparkExpressionCdfIntegrationSpec       extends SparkExpressionIntegrationScenarios with CdfMode
class SparkExpressionInterceptIntegrationSpec extends SparkExpressionIntegrationScenarios with InterceptMode
