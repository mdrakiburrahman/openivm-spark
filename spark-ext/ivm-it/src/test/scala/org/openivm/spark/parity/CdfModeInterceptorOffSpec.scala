package org.openivm.spark.parity

import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

/** Proves that under `spark.openivm.changeFeed.mode = cdf` the
  * `IvmDmlInterceptorRule` is NOT in the resolution chain (so other writers
  * — non-Spark engines, dbt sessions without our extension, structured
  * streaming jobs — can mutate the base Delta tables freely without going
  * through openivm-spark's staging mechanism).
  *
  * Companion sibling lives implicitly: there is no `*Spec` (intercept) form
  * because the assertion is mode-specific.
  */
class CdfModeInterceptorOffSpec extends IvmParitySpecBase("cdf-mode-interceptor-off") with CdfMode {

  describe("CDF mode wiring") {

    it("does not install IvmDmlInterceptorRule in the analyzer's resolution chain") {
      val rules      = spark.sessionState.analyzer.extendedResolutionRules
      val classNames = rules.map(_.getClass.getName)

      withClue(s"resolution rules: ${classNames.mkString(", ")}") {
        classNames.exists(_.contains("IvmDmlInterceptorRule")) shouldBe false
      }
    }

    it("does not install IvmStrategy in the planner's extra strategies") {
      val strategies = spark.sessionState.planner.extraPlanningStrategies
      val classNames = strategies.map(_.getClass.getName)

      withClue(s"planner strategies: ${classNames.mkString(", ")}") {
        classNames.exists(_.contains("IvmStrategy")) shouldBe false
      }
    }

    it("an INSERT into a tracked Delta source does NOT create a staging artifact") {
      sql("CREATE TABLE cdf_off_src(id INT, v INT) USING DELTA")
      sql("INSERT INTO cdf_off_src VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW cdf_off_mv AS SELECT id, v FROM cdf_off_src")

      sql("INSERT INTO cdf_off_src VALUES (2, 20)")

      val staged = org.openivm.spark.common.StagingCatalog
        .collectFor(spark, "cdf_off_mv", Seq("cdf_off_src"))
      withClue(s"unexpected staged rows under CDF mode: ${staged.map(_.opType).mkString(",")}") {
        staged shouldBe empty
      }

      sql("REFRESH MATERIALIZED VIEW cdf_off_mv")
      assertMvCorrect("cdf_off_mv", "SELECT id, v FROM cdf_off_src")
    }
  }
}
