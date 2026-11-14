package org.openivm.spark.parity

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator
import org.openivm.spark.common.{FeatureGate, MvCatalog, MvMetadata, RefreshTypeCode}
import org.openivm.spark.compiler.{OpenIvmCompileException, SparkExpressionPrepass}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class SparkExpressionIntegrationScenarios extends IvmParitySpecBase("expression-bridge") {
  self: IvmParityMode =>
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.CompileClassificationCacheEnabledKey -> "true",
    FeatureGate.ProfileRefreshKey                    -> "true"
  )

  describe("Spark expression bridge") {
    it("incrementally maintains max_by with struct values and ordering keys") {
      sql("CREATE TABLE expr_struct_source (grp STRING, id INT, name STRING, ts TIMESTAMP) USING DELTA")
      sql("""INSERT INTO expr_struct_source VALUES
        |('a', 1, 'dated', TIMESTAMP '2024-01-01 00:00:00'), ('a', 2, 'null-key', NULL),
        |('b', 3, NULL, TIMESTAMP '2024-02-01 00:00:00')""".stripMargin)
      val query = """SELECT grp, max_by(struct(id AS f1, name AS f2, ts AS f3),
        |struct(ts AS f1)) AS picked FROM expr_struct_source GROUP BY grp""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW expr_struct_mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("expr_struct_mv")
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.AggregateGroup
      assertMvCorrect("expr_struct_mv", query)
      sql("INSERT INTO expr_struct_source VALUES ('a', 4, 'later', TIMESTAMP '2024-03-01 00:00:00')")
      refreshMv("expr_struct_mv")
      assertMvCorrect("expr_struct_mv", query)
      sql("DELETE FROM expr_struct_source WHERE id = 4")
      sql("UPDATE expr_struct_source SET ts = NULL WHERE id = 3")
      sql("INSERT INTO expr_struct_source VALUES ('c', 5, NULL, NULL)")
      refreshMv("expr_struct_mv")
      assertMvCorrect("expr_struct_mv", query)
      sql("DELETE FROM expr_struct_source WHERE id = 1")
      refreshMv("expr_struct_mv")
      assertMvCorrect("expr_struct_mv", query)
    }

    it("incrementally maintains struct aggregate fields containing Spark expressions") {
      sql("CREATE TABLE expr_struct_marker_source (grp STRING, id INT, name STRING, ts TIMESTAMP) USING DELTA")
      sql("""INSERT INTO expr_struct_marker_source VALUES
        |('a', 1, 'FIRST', TIMESTAMP '2024-01-01 00:00:00'),
        |('a', 2, 'SECOND', TIMESTAMP '2024-02-01 00:00:00')""".stripMargin)
      val query = """SELECT grp, max_by(struct(id AS f1, lower(name) AS f2),
        |struct(ts AS f1)) AS picked FROM expr_struct_marker_source GROUP BY grp""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW expr_struct_marker_mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("expr_struct_marker_mv")
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.AggregateGroup
      assertMvCorrect("expr_struct_marker_mv", query)
      sql("UPDATE expr_struct_marker_source SET name = 'UPDATED' WHERE id = 2")
      sql("INSERT INTO expr_struct_marker_source VALUES ('b', 3, 'THIRD', NULL)")
      refreshMv("expr_struct_marker_mv")
      assertMvCorrect("expr_struct_marker_mv", query)
    }

    it("preserves qualified epoch conversion and conditional errors") {
      sql("CREATE TABLE expr_epoch_source (id INT, ts TIMESTAMP, txt STRING, invalid BOOLEAN) USING DELTA")
      sql("INSERT INTO expr_epoch_source VALUES (1, TIMESTAMP '2024-01-01 01:02:03', 'A', false)")
      val query = """SELECT s.id, unix_timestamp(s.ts) AS epoch, unix_micros(s.ts) AS micros,
                    |CASE WHEN s.invalid THEN CAST(raise_error('invalid') AS STRING) ELSE s.txt END AS text_value,
                    |CASE WHEN s.invalid THEN CAST(raise_error('invalid') AS TIMESTAMP) ELSE s.ts END AS time_value
                    |FROM expr_epoch_source s""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW expr_epoch_mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("expr_epoch_mv")
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
      assertMvCorrect("expr_epoch_mv", query)
      sql("UPDATE expr_epoch_source SET txt = 'B' WHERE id = 1")
      sql("INSERT INTO expr_epoch_source VALUES (2, TIMESTAMP '2024-02-01 01:02:03.123456', 'C', false)")
      refreshMv("expr_epoch_mv")
      assertMvCorrect("expr_epoch_mv", query)
      sql("INSERT INTO expr_epoch_source VALUES (3, TIMESTAMP '2024-03-01 01:02:03', 'D', true)")
      val previousLevel = LogManager.getRootLogger.getLevel
      Configurator.setRootLevel(Level.FATAL)
      try {
        intercept[Exception] {
          refreshMv("expr_epoch_mv")
        }
      } finally Configurator.setRootLevel(previousLevel)
    }

    it("preserves pinned-source identity while incrementally refreshing a live join") {
      sql("CREATE TABLE expr_pin_dim (id INT, label STRING) USING DELTA")
      sql("INSERT INTO expr_pin_dim VALUES (1, 'FIRST'), (2, 'SECOND')")
      val pinned = sql("DESCRIBE HISTORY expr_pin_dim").selectExpr("max(version)").head().getLong(0)
      sql("UPDATE expr_pin_dim SET label = 'CHANGED' WHERE id = 1")
      sql("INSERT INTO expr_pin_dim VALUES (3, 'NEW')")
      sql("CREATE TABLE expr_pin_fact (id INT, dim_id INT) USING DELTA")
      sql("INSERT INTO expr_pin_fact VALUES (10, 1)")
      val query = s"""SELECT f.id, lower(d.label) AS label
        |FROM expr_pin_dim VERSION AS OF $pinned d JOIN expr_pin_fact f ON d.id = f.dim_id""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW expr_pin_mv AS $query")
      val id   = spark.sessionState.sqlParser.parseTableIdentifier("expr_pin_mv")
      val meta = MvCatalog.lookup(spark, id).get
      meta.properties.get(MvMetadata.CompileRefreshTypeKey) should not be Some("COMPILE_FAILED")
      meta.refreshType should not be RefreshTypeCode.FullRefresh
      assertMvCorrect("expr_pin_mv", query)
      sql("INSERT INTO expr_pin_fact VALUES (11, 2), (12, 3)")
      refreshMv("expr_pin_mv")
      assertMvCorrect("expr_pin_mv", query)
    }

    ignore("maintains native map lookup") { /* TODO: Spark cannot GROUP BY MAP in the native delta program. */
      sql("CREATE TABLE expr_baseline_source (id INT, txt STRING, m MAP<STRING, INT>) USING DELTA")
      sql("INSERT INTO expr_baseline_source VALUES (1, 'ABC', map('a', 1)), (2, NULL, map('a', 2))")
      val query = "SELECT id, element_at(m, 'a') AS n FROM expr_baseline_source"
      sql(s"CREATE MATERIALIZED VIEW expr_baseline_mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("expr_baseline_mv")
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
      assertMvCorrect("expr_baseline_mv", query)
      sql("UPDATE expr_baseline_source SET m = map('a', 3) WHERE id = 1")
      sql("INSERT INTO expr_baseline_source VALUES (3, 'XYZ', map('a', 4))")
      refreshMv("expr_baseline_mv")
      assertMvCorrect("expr_baseline_mv", query)
    }

    it("preserves an eliminated constant branch") {
      sql("CREATE TABLE expr_branch_source (id INT, txt STRING) USING DELTA")
      sql("INSERT INTO expr_branch_source VALUES (1, 'ABC'), (2, NULL)")
      val query = "SELECT id, CASE WHEN false THEN lower(txt) ELSE 'x' END AS v FROM expr_branch_source"
      sql(s"CREATE MATERIALIZED VIEW expr_branch_mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("expr_branch_mv")
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
      assertMvCorrect("expr_branch_mv", query)
      sql("UPDATE expr_branch_source SET txt = 'DEF' WHERE id = 1")
      sql("INSERT INTO expr_branch_source VALUES (3, 'XYZ')")
      refreshMv("expr_branch_mv")
      assertMvCorrect("expr_branch_mv", query)
    }

    it("fails closed for unsupported marker expressions") {
      sql("CREATE TABLE expr_unsupported_source (id INT) USING DELTA")
      val query = "SELECT id, sha2(CAST(rand() AS STRING), 256) AS h FROM expr_unsupported_source"
      intercept[IllegalArgumentException] {
        sql(s"EXPLAIN CREATE MATERIALIZED VIEW expr_unsupported_explain AS $query").collect()
      }
      intercept[IllegalArgumentException] {
        sql(s"CREATE MATERIALIZED VIEW expr_unsupported_mv AS $query")
      }
      val id = spark.sessionState.sqlParser.parseTableIdentifier("expr_unsupported_mv")
      MvCatalog.lookup(spark, id) shouldBe None
    }

    it("recompiles a legacy MV when it requires rebaseline") {
      sql("CREATE TABLE expr_legacy_source (id INT, txt STRING) USING DELTA")
      sql("INSERT INTO expr_legacy_source VALUES (1, 'A')")
      sql("CREATE MATERIALIZED VIEW expr_legacy_mv AS SELECT id, lower(txt) AS v FROM expr_legacy_source")
      val id         = spark.sessionState.sqlParser.parseTableIdentifier("expr_legacy_mv")
      val properties = MvCatalog.lookup(spark, id).get.properties - SparkExpressionPrepass.RestoredInitialLoadProperty
      MvCatalog.updateProperties(spark, id, properties)
      sql("INSERT OVERWRITE TABLE expr_legacy_source SELECT 2, 'B'")
      refreshMv("expr_legacy_mv")
      assertMvCorrect("expr_legacy_mv", "SELECT id, lower(txt) AS v FROM expr_legacy_source")
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
      sql("INSERT INTO expr_legacy_source VALUES (3, 'C')")
      refreshMv("expr_legacy_mv")
      assertMvCorrect("expr_legacy_mv", "SELECT id, lower(txt) AS v FROM expr_legacy_source")
    }

    it("preserves aggregate bookkeeping when rebaselining a legacy MV") {
      sql("CREATE TABLE expr_legacy_agg_source (id INT, txt STRING) USING DELTA")
      sql("INSERT INTO expr_legacy_agg_source VALUES (1, 'A'), (2, 'A')")
      val query = "SELECT lower(txt) AS v, SUM(id) AS total FROM expr_legacy_agg_source GROUP BY lower(txt)"
      sql(s"CREATE MATERIALIZED VIEW expr_legacy_agg_mv AS $query")
      val id     = spark.sessionState.sqlParser.parseTableIdentifier("expr_legacy_agg_mv")
      val before = MvCatalog.lookup(spark, id).get
      before.refreshType shouldBe RefreshTypeCode.AggregateGroup
      MvCatalog.updateProperties(spark, id, before.properties - SparkExpressionPrepass.RestoredInitialLoadProperty)
      sql("INSERT OVERWRITE TABLE expr_legacy_agg_source SELECT 4, 'B' UNION ALL SELECT 5, 'B'")
      refreshMv("expr_legacy_agg_mv")
      assertMvCorrect("expr_legacy_agg_mv", query)
      val meta = MvCatalog.lookup(spark, id).get
      meta.refreshType shouldBe RefreshTypeCode.AggregateGroup
      val actual = spark.read.format("delta").load(meta.location).select("v", "total", "openivm_count_star")
      val expected = sql("""SELECT lower(txt) AS v, SUM(id) AS total, COUNT(*) AS openivm_count_star
                           |FROM expr_legacy_agg_source GROUP BY lower(txt)""".stripMargin)
      actual.exceptAll(expected).count() shouldBe 0L
      expected.exceptAll(actual).count() shouldBe 0L
      sql("INSERT INTO expr_legacy_agg_source VALUES (6, 'B')")
      refreshMv("expr_legacy_agg_mv")
      assertMvCorrect("expr_legacy_agg_mv", query)
    }

    it("keeps pinned sources frozen when rebaselining a legacy MV") {
      sql("CREATE TABLE expr_legacy_dim (id INT, label STRING) USING DELTA")
      sql("INSERT INTO expr_legacy_dim VALUES (1, 'OLD')")
      val version = sql("DESCRIBE HISTORY expr_legacy_dim").selectExpr("max(version)").head().getLong(0)
      sql("UPDATE expr_legacy_dim SET label = 'NEW' WHERE id = 1")
      sql("CREATE TABLE expr_legacy_fact (id INT, amount INT) USING DELTA")
      sql("INSERT INTO expr_legacy_fact VALUES (1, 10)")
      val query = s"""SELECT lower(d.label) AS label, f.amount FROM expr_legacy_dim VERSION AS OF $version d
                     |JOIN expr_legacy_fact f ON f.id = d.id""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW expr_legacy_pin_mv AS $query")
      val id   = spark.sessionState.sqlParser.parseTableIdentifier("expr_legacy_pin_mv")
      val meta = MvCatalog.lookup(spark, id).get
      meta.refreshType should not be RefreshTypeCode.FullRefresh
      MvCatalog.updateProperties(spark, id, meta.properties - SparkExpressionPrepass.RestoredInitialLoadProperty)
      sql("INSERT OVERWRITE TABLE expr_legacy_fact SELECT 1, 20")
      refreshMv("expr_legacy_pin_mv")
      assertMvCorrect("expr_legacy_pin_mv", query)
      sql("INSERT INTO expr_legacy_fact VALUES (1, 30)")
      refreshMv("expr_legacy_pin_mv")
      assertMvCorrect("expr_legacy_pin_mv", query)
    }

    it("rebaselines against advanced pinned sources") {
      sql("CREATE TABLE expr_advance_dim (id INT, label STRING) USING DELTA")
      sql("INSERT INTO expr_advance_dim VALUES (1, 'OLD')")
      val oldVersion = sql("DESCRIBE HISTORY expr_advance_dim").selectExpr("max(version)").head().getLong(0)
      sql("CREATE TABLE expr_advance_fact (id INT, amount INT) USING DELTA")
      sql("INSERT INTO expr_advance_fact VALUES (1, 10)")
      def query(version: Long): String =
        s"""SELECT lower(d.label) AS label, f.amount FROM expr_advance_dim VERSION AS OF $version d
           |JOIN expr_advance_fact f ON f.id = d.id""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW expr_advance_mv AS ${query(oldVersion)}")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("expr_advance_mv")
      MvCatalog.lookup(spark, id).get.refreshType should not be RefreshTypeCode.FullRefresh
      sql("UPDATE expr_advance_dim SET label = 'NEW' WHERE id = 1")
      val newVersion = sql("DESCRIBE HISTORY expr_advance_dim").selectExpr("max(version)").head().getLong(0)
      sql(s"ALTER MATERIALIZED VIEW expr_advance_mv ADVANCE SOURCE VERSIONS (expr_advance_dim = $newVersion)")
      assertMvCorrect("expr_advance_mv", query(newVersion))
      sql("INSERT OVERWRITE TABLE expr_advance_fact SELECT 1, 20")
      refreshMv("expr_advance_mv")
      assertMvCorrect("expr_advance_mv", query(newVersion))
    }

    it("retains a real native compile-failure receipt through create and refresh") {
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
        if (!ansi) {
          val meta = MvCatalog.lookup(spark, identifier).get
          val tier = detail.split(";").find(_.startsWith("compile_cache_tier=")).get.stripPrefix("compile_cache_tier=")
          val cached      = MvMetadata.cachedCompiledSql(meta.properties, meta.sourceSchemaFingerprint, tier).get
          val token       = "__openivm_expr_([0-9a-f]{64})_[0-9]+__".r.findFirstMatchIn(cached).get.group(1)
          val legacyTier  = tier.takeWhile(_ != ':') + ":" + token
          val fingerprint = meta.sourceSchemaFingerprint
          val currentKeys = Seq(
            MvMetadata.compileCacheSqlKey(fingerprint, tier),
            MvMetadata.compileCacheInitialLoadSqlKey(fingerprint, tier),
            MvMetadata.compileCacheRefreshTypeKey(fingerprint, tier),
            MvMetadata.compileCacheRefreshTypeNameKey(fingerprint, tier)
          )
          val stale = MvMetadata.compiledProperties(
            fingerprint,
            legacyTier,
            "SELECT stale_legacy_cache",
            "",
            RefreshTypeCode.SimpleProjection,
            "SIMPLE_PROJECTION"
          )
          MvCatalog.updateProperties(spark, identifier, (meta.properties -- currentKeys) ++ stale)
          sql(s"DELETE FROM $source WHERE id = 5")
          sql(s"UPDATE $source SET txt = 'x/y' WHERE id = 3")
          sql(s"INSERT INTO $source VALUES (6, 'a/b', NULL, 6, false)")
          refreshMv(mv)
          assertMvCorrect(mv, query)
          val migratedDetail = sql("SHOW OPENIVM REFRESH PROFILE")
            .where(s"view_name IN ('$mv','default.$mv') AND step_name = 'generate_refresh_sql.compile'")
            .select("detail")
            .collect()
            .last
            .getString(0)
          migratedDetail should include("compile_cache_hit=false")
          migratedDetail should include(s"compile_cache_tier=$tier")
        }
        sql(s"""INSERT OVERWRITE TABLE $source
          |SELECT 8, 'x/y', TIMESTAMP '2024-03-01 01:02:03', 8L, false""".stripMargin)
        refreshMv(mv)
        assertMvCorrect(mv, query)
        MvCatalog.lookup(spark, identifier).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
        sql(s"INSERT INTO $source VALUES (9, 'a/b', NULL, 9, false)")
        refreshMv(mv)
        assertMvCorrect(mv, query)
      }
    }
  }
}

class SparkExpressionCdfIntegrationSpec       extends SparkExpressionIntegrationScenarios with CdfMode
class SparkExpressionInterceptIntegrationSpec extends SparkExpressionIntegrationScenarios with InterceptMode
