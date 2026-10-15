package org.openivm.spark.parity

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{FeatureGate, MvCatalog}
import org.openivm.spark.parity.base.IvmParitySpecBase

abstract class CompileCacheScenarios extends IvmParitySpecBase("compile-cache") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.CompileClassificationCacheEnabledKey -> "true",
      FeatureGate.WindowRunningIncrementalEnabledKey   -> "true",
      FeatureGate.ProfileRefreshKey                    -> "true",
      FeatureGate.QueryLogEnabledKey                   -> "true"
    )

  private def cacheSqlKeys(metaProps: Map[String, String], fingerprint: String): Seq[String] = {
    val prefix = s"_ivm_compile_cache:$fingerprint:"
    metaProps.keys.filter(k => k.startsWith(prefix) && k.endsWith(":sql")).toSeq
  }

  describe("compile-classification cache") {
    it("reuses cached SQL on a cache hit while preserving correctness after restore") {
      sql("CREATE TABLE IF NOT EXISTS cc_sales(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO cc_sales VALUES ('us', 10)")
      sql(
        "CREATE MATERIALIZED VIEW cc_mv AS " +
          "SELECT region, amount FROM cc_sales WHERE amount > 0"
      )

      sql("INSERT INTO cc_sales VALUES ('eu', 20)")
      refreshMv("cc_mv")
      assertMvCorrect("cc_mv", "SELECT region, amount FROM cc_sales WHERE amount > 0")

      val metaBefore = MvCatalog.lookup(spark, TableIdentifier("cc_mv")).get
      val keys       = cacheSqlKeys(metaBefore.properties, metaBefore.sourceSchemaFingerprint)
      keys should not be empty

      val poisonedProps = metaBefore.properties.map {
        case (key, value) if keys.contains(key) =>
          key -> value.replace("openivm_delta_cc_sales", "openivm_delta_cc_missing_cache_sentinel")
        case other => other
      }
      MvCatalog.updateProperties(spark, metaBefore.name, poisonedProps)

      sql("INSERT INTO cc_sales VALUES ('apac', 30)")
      val ex = intercept[Exception] {
        refreshMv("cc_mv")
      }
      ex.getMessage.toLowerCase should include("cc_missing_cache_sentinel")

      MvCatalog.updateProperties(spark, metaBefore.name, metaBefore.properties)
      refreshMv("cc_mv")
      assertMvCorrect("cc_mv", "SELECT region, amount FROM cc_sales WHERE amount > 0")

      // A persisted program from an obsolete compiler must never execute,
      // even when its query/schema/facts key otherwise matches this refresh.
      val stale     = MvCatalog.lookup(spark, metaBefore.name).get
      val staleKeys = cacheSqlKeys(stale.properties, stale.sourceSchemaFingerprint)
      val obsoleteProps = stale.properties.map {
        case (key, value) if staleKeys.contains(key) =>
          key -> value.replace("openivm_delta_cc_sales", "openivm_delta_cc_obsolete_compiler")
        case ("_ivm_compile_cache_generation", _) => "_ivm_compile_cache_generation" -> "obsolete"
        case other                                => other
      }
      MvCatalog.updateProperties(spark, stale.name, obsoleteProps)
      sql("INSERT INTO cc_sales VALUES ('latam', 40)")
      refreshMv("cc_mv")
      assertMvCorrect("cc_mv", "SELECT region, amount FROM cc_sales WHERE amount > 0")
      val restored = MvCatalog.lookup(spark, stale.name).get.properties
      restored("_ivm_compile_cache_generation") shouldBe stale.properties("_ivm_compile_cache_generation")
      restored.values.exists(_.contains("cc_obsolete_compiler")) shouldBe false
    }

    it("reuses a running-window program with fresh scratch paths and recompiles mixed changes") {
      sql("CREATE TABLE cc_window_source(id INT, k STRING, t INT, v INT) USING DELTA")
      sql("INSERT INTO cc_window_source VALUES (1,'A',1,10),(2,'A',2,8),(3,'B',1,20)")
      val query = "SELECT id,k,t,v,MIN(v) OVER (PARTITION BY k ORDER BY t) AS lo FROM cc_window_source"
      sql(s"CREATE MATERIALIZED VIEW cc_window AS $query")
      val childQuery = "SELECT k,SUM(lo) AS total,COUNT(*) AS n FROM cc_window GROUP BY k"
      sql(s"CREATE MATERIALIZED VIEW cc_window_child AS $childQuery")

      def refreshAndCheck(): Unit = {
        refreshMv("cc_window")
        refreshMv("cc_window_child")
        assertMvCorrect("cc_window", query)
        assertMvCorrect("cc_window_child", childQuery)
      }
      def latestCompileDetail: String =
        sql("SHOW OPENIVM REFRESH PROFILE")
          .where("view_name IN ('cc_window','default.cc_window') AND step_name = 'generate_refresh_sql.compile'")
          .select("detail")
          .collect()
          .last
          .getString(0)

      sql("INSERT INTO cc_window_source VALUES (4,'A',3,6),(5,'B',2,18)")
      refreshAndCheck()
      latestCompileDetail should include("compile_cache_hit=false")
      sql("INSERT INTO cc_window_source VALUES (6,'A',4,5),(7,'B',3,17)")
      refreshAndCheck()
      latestCompileDetail should include("compile_cache_hit=true")

      val paths = sql("SHOW OPENIVM QUERY LOG")
        .where("view_name IN ('cc_window','default.cc_window') AND category = 'rewritten_stmt'")
        .select("sql_text")
        .collect()
        .flatMap(row => "(?i)delta\\.`([^`]*view_deltas[^`]+)`".r.findAllMatchIn(row.getString(0)).map(_.group(1)))
        .toSet
      paths should have size 2

      sql("DELETE FROM cc_window_source WHERE id = 6")
      sql("INSERT INTO cc_window_source VALUES (8,'A',1,3)")
      refreshAndCheck()
      latestCompileDetail should include("compile_cache_hit=false")
    }
  }
}
