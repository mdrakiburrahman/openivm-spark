package org.openivm.spark.it

import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Smoke test for the openivm-spark extension.
 *
 * Verifies that a SparkSession with the openivm-spark extension installed
 * boots successfully:
 *  - the extension class loads (no `ClassNotFoundException`)
 *  - the master feature gate defaults to off (zero behaviour change)
 *  - turning the feature gate on does not break SparkSession init
 *  - the openivm DuckDB extension is present at the expected path
 *
 * This suite is the canonical "the jar still loads" check.
 */
class ExtensionLoadingSpec extends AnyFunSpec with Matchers {

  describe("OpenIvmSparkExtensions") {

    it("loads cleanly with the feature gate OFF (default)") {
      val spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("openivm-spark-smoke-disabled")
        .config("spark.sql.extensions", "org.openivm.spark.OpenIvmSparkExtensions")
        .config("spark.ui.enabled", false)
        .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/openivm-warehouse")
        .getOrCreate()

      try {
        // A trivial query proves the SQL engine still works.
        spark.sql("SELECT 1 AS x").collect().head.getInt(0) shouldBe 1
      } finally {
        spark.stop()
      }
    }

    it("loads cleanly with the feature gate ON") {
      val spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("openivm-spark-smoke-enabled")
        .config("spark.sql.extensions", "org.openivm.spark.OpenIvmSparkExtensions")
        .config("spark.openivm.enabled", true)
        .config("spark.ui.enabled", false)
        .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/openivm-warehouse")
        .getOrCreate()

      try {
        spark.sql("SELECT 2 AS x").collect().head.getInt(0) shouldBe 2
      } finally {
        spark.stop()
      }
    }
  }

  describe("OpenIVM DuckDB extension artifact") {

    it("is present at the expected runtime path") {
      val expected = sys.env.getOrElse("OPENIVM_EXTENSION_PATH", "/opt/openivm/openivm.duckdb_extension")
      val f        = new java.io.File(expected)
      withClue(s"expected the openivm.duckdb_extension at $expected (set OPENIVM_EXTENSION_PATH to override): ") {
        f.exists() shouldBe true
      }
    }
  }
}
