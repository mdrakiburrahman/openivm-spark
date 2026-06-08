package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** Depth-2 cascade regression for a GROUP_RECOMPUTE upstream after openivm
  * `4471f4e929fd3b21ac55ea0c47249d4716853c98` started emitting
  * `openivm_delta_<view>` from recompute paths whenever
  * `force_view_delta_cascade=true` is set in the CompileFacts payload (which
  * openivm-spark always sets).
  */
abstract class ChainedGroupRecomputeCascadeScenarios extends IvmParitySpecBase("chained-group-recompute-cascade") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  protected def mvRefreshTypeName(name: String): String = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshTypeName
  }

  describe("GROUP_RECOMPUTE upstream → SIMPLE_PROJECTION downstream") {
    it("keeps the downstream incremental across a no-op recompute plus two refresh batches") {
      sql("CREATE TABLE IF NOT EXISTS cgrc_src(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO cgrc_src VALUES " +
          "(1,'east',10), (2,'east',10), (3,'east',20), (4,'west',30)"
      )

      val upstreamSql =
        "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM cgrc_src GROUP BY region"
      val downstreamSql =
        s"SELECT region, total_distinct FROM ($upstreamSql) cgrc_expected"

      sql(s"CREATE MATERIALIZED VIEW cgrc_totals AS $upstreamSql")
      sql("CREATE MATERIALIZED VIEW cgrc_totals_proj AS SELECT region, total_distinct FROM cgrc_totals")

      mvRefreshType("cgrc_totals") shouldBe RefreshTypeCode.GroupRecompute
      refreshChain("cgrc_totals", "cgrc_totals_proj")
      mvRefreshTypeName("cgrc_totals_proj") should not equal "FULL_REFRESH"
      assertMvCorrect("cgrc_totals_proj", downstreamSql)

      // No-op recompute: old rows == new rows for the affected region.
      sql("UPDATE cgrc_src SET amount = 20 WHERE id = 3")
      refreshChain("cgrc_totals", "cgrc_totals_proj")
      assertMvCorrect("cgrc_totals_proj", downstreamSql)

      // Batch 1: INSERT a new distinct value into the existing 'east' group.
      sql("INSERT INTO cgrc_src VALUES (5,'east',40)")
      refreshChain("cgrc_totals", "cgrc_totals_proj")
      assertMvCorrect("cgrc_totals_proj", downstreamSql)

      // Batch 2: UPDATE + DELETE in the same group across the next refresh.
      sql("UPDATE cgrc_src SET amount = 25 WHERE id = 3")
      sql("DELETE FROM cgrc_src WHERE id = 5")
      refreshChain("cgrc_totals", "cgrc_totals_proj")
      assertMvCorrect("cgrc_totals_proj", downstreamSql)
    }
  }
}
