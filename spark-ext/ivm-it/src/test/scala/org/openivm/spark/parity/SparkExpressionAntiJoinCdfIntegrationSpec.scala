package org.openivm.spark.parity

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator
import org.openivm.spark.common.{MvCatalog, MvMetadata, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class SparkExpressionAntiJoinIntegrationScenarios extends IvmParitySpecBase("expression-anti-join") {
  self: IvmParityMode =>

  describe("Spark expression bridge anti-join") {
    ignore("incrementally maintains correlated anti-join expressions") { /* TODO: native emits no real delta without the bridge too. */
      assume(sys.env.get("OPENIVM_MARKER_INTEGRATION_TEST").contains("1"))
      sql("CREATE TABLE expr_anti_source (id INT, txt STRING) USING DELTA")
      sql("CREATE TABLE expr_anti_blocked (txt STRING) USING DELTA")
      sql("INSERT INTO expr_anti_source VALUES (1, 'A'), (2, 'B'), (3, NULL)")
      sql("INSERT INTO expr_anti_blocked VALUES ('a')")
      val query = """SELECT t.id, t.txt FROM expr_anti_source t
        |WHERE NOT EXISTS (SELECT 1 FROM expr_anti_blocked u WHERE u.txt = lower(t.txt))""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW expr_anti_mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("expr_anti_mv")
      MvCatalog.lookup(spark, id).get.refreshType should not be RefreshTypeCode.FullRefresh
      assertMvCorrect("expr_anti_mv", query)
      sql("INSERT INTO expr_anti_blocked VALUES ('b')")
      sql("INSERT INTO expr_anti_source VALUES (4, 'C')")
      refreshMv("expr_anti_mv")
      assertMvCorrect("expr_anti_mv", query)
    }

    it("classifies a correlated anti-join with no real delta as an intentional full refresh") {
      assume(sys.env.get("OPENIVM_MARKER_INTEGRATION_TEST").contains("1"))
      sql("CREATE TABLE expr_anti_full_source (id INT, txt STRING) USING DELTA")
      sql("CREATE TABLE expr_anti_full_blocked (txt STRING) USING DELTA")
      sql("INSERT INTO expr_anti_full_source VALUES (1, 'A'), (2, 'B'), (3, NULL)")
      sql("INSERT INTO expr_anti_full_blocked VALUES ('a')")
      val query = """SELECT t.id, t.txt FROM expr_anti_full_source t
        |WHERE NOT EXISTS (SELECT 1 FROM expr_anti_full_blocked u WHERE u.txt = lower(t.txt))""".stripMargin
      val logger        = "org.openivm.spark.commands.CreateMaterializedViewCommand"
      val previousLevel = LogManager.getLogger(logger).getLevel
      Configurator.setLevel(logger, Level.FATAL)
      try sql(s"CREATE MATERIALIZED VIEW expr_anti_full_mv AS $query")
      finally Configurator.setLevel(logger, previousLevel)
      val id   = spark.sessionState.sqlParser.parseTableIdentifier("expr_anti_full_mv")
      val meta = MvCatalog.lookup(spark, id).get
      meta.refreshType shouldBe RefreshTypeCode.FullRefresh
      meta.properties.get(MvMetadata.CompileRefreshTypeKey) shouldBe Some("SEMI_ANTI_RECOMPUTE")
      meta.properties.get(MvMetadata.RefreshReasonKey) shouldBe Some("no_real_delta")
      assertMvCorrect("expr_anti_full_mv", query)
      val downstreamQuery    = "SELECT id, txt FROM expr_anti_full_mv WHERE id > 1"
      val downstreamExpected = s"SELECT id, txt FROM ($query) filtered WHERE id > 1"
      sql(s"CREATE MATERIALIZED VIEW expr_anti_full_downstream AS $downstreamQuery")
      assertMvCorrect("expr_anti_full_downstream", downstreamExpected)
      sql("INSERT INTO expr_anti_full_blocked VALUES ('b')")
      sql("INSERT INTO expr_anti_full_source VALUES (4, 'C')")
      refreshMv("expr_anti_full_mv")
      refreshMv("expr_anti_full_downstream")
      assertMvCorrect("expr_anti_full_mv", query)
      assertMvCorrect("expr_anti_full_downstream", downstreamExpected)
    }
  }
}

class SparkExpressionAntiJoinCdfIntegrationSpec extends SparkExpressionAntiJoinIntegrationScenarios with CdfMode
class SparkExpressionAntiJoinInterceptIntegrationSpec extends SparkExpressionAntiJoinIntegrationScenarios with InterceptMode
