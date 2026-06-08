package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.commands.{
  CreateMaterializedViewCommand,
  DropMaterializedViewCommand,
  RefreshMaterializedViewCommand
}
import org.openivm.spark.parser.IvmParser

/** Slice of `ParserSpec` covering direct `IvmParser.parsePlan` dispatch:
  * CREATE / REFRESH / DROP MATERIALIZED VIEW each route to the correct
  * command, while non-MV SQL falls through to Spark's wrapped parser
  * unchanged.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
abstract class ParserPassthroughScenarios extends IvmParitySpecBase("parser-passthrough") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ──────────────────────────────────────────────────────────────────────────
  // (15) Direct IvmParser.parsePlan exercises the injected parser without
  //     going through Spark's session SQL machinery. We instantiate the
  //     parser exactly as OpenIvmSparkExtensions does.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(15) IvmParser directly: parsePlan dispatch") {

    it("dispatches CREATE MATERIALIZED VIEW to CreateMaterializedViewCommand") {
      val parser = new IvmParser(spark, spark.sessionState.sqlParser)
      val plan   = parser.parsePlan("CREATE MATERIALIZED VIEW v15a AS SELECT 1 AS x")
      plan shouldBe a[CreateMaterializedViewCommand]
    }

    it("dispatches REFRESH MATERIALIZED VIEW to RefreshMaterializedViewCommand") {
      val parser = new IvmParser(spark, spark.sessionState.sqlParser)
      val plan   = parser.parsePlan("REFRESH MATERIALIZED VIEW v15b")
      plan shouldBe a[RefreshMaterializedViewCommand]
    }

    it("dispatches DROP MATERIALIZED VIEW to DropMaterializedViewCommand") {
      val parser = new IvmParser(spark, spark.sessionState.sqlParser)
      val plan   = parser.parsePlan("DROP MATERIALIZED VIEW v15c")
      plan shouldBe a[DropMaterializedViewCommand]
    }

    it("delegates non-MV SQL (SELECT 1) to the wrapped Spark parser unchanged") {
      val parser = new IvmParser(spark, spark.sessionState.sqlParser)
      // Should not throw — and should resolve to a Spark logical plan,
      // not to one of our MV commands.
      val plan = parser.parsePlan("SELECT 1 AS x")
      plan shouldNot be(a[CreateMaterializedViewCommand])
      plan shouldNot be(a[RefreshMaterializedViewCommand])
      plan shouldNot be(a[DropMaterializedViewCommand])
    }
  }
}
