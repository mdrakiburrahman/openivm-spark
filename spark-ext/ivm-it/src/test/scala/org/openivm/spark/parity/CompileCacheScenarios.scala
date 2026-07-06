package org.openivm.spark.parity

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{FeatureGate, MvCatalog}
import org.openivm.spark.parity.base.IvmParitySpecBase

abstract class CompileCacheScenarios extends IvmParitySpecBase("compile-cache") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.CompileClassificationCacheEnabledKey -> "true")

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
    }
  }
}
