package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** Depth-2 cascade regression for a WINDOW_PARTITION upstream after openivm
  * `4471f4e929fd3b21ac55ea0c47249d4716853c98` started emitting
  * `openivm_delta_<view>` from recompute paths whenever
  * `force_view_delta_cascade=true` is set in the CompileFacts payload (which
  * openivm-spark always sets).
  */
abstract class ChainedWindowPartitionCascadeScenarios extends IvmParitySpecBase("chained-window-partition-cascade") {
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

  describe("WINDOW_PARTITION upstream → SIMPLE_PROJECTION downstream") {
    it("keeps the downstream incremental across a no-op recompute plus two refresh batches") {
      sql("CREATE TABLE IF NOT EXISTS cwpc_src(id INT, dept STRING, salary INT) USING DELTA")
      sql(
        "INSERT INTO cwpc_src VALUES " +
          "(1,'eng',100), (2,'eng',200), (3,'eng',300), " +
          "(4,'sales',50), (5,'sales',75)"
      )

      val upstreamSql =
        "SELECT id, dept, salary, " +
          "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary, id) AS rn FROM cwpc_src"
      val downstreamSql =
        s"SELECT id, dept, salary, rn FROM ($upstreamSql) cwpc_expected"

      sql(s"CREATE MATERIALIZED VIEW cwpc_ranked AS $upstreamSql")
      sql("CREATE MATERIALIZED VIEW cwpc_ranked_proj AS SELECT id, dept, salary, rn FROM cwpc_ranked")

      mvRefreshType("cwpc_ranked") shouldBe RefreshTypeCode.WindowPartition
      refreshChain("cwpc_ranked", "cwpc_ranked_proj")
      mvRefreshTypeName("cwpc_ranked_proj") should not equal "FULL_REFRESH"
      assertMvCorrect("cwpc_ranked_proj", downstreamSql)

      // No-op recompute: Spark still stages an UPDATE, but old rows == new rows.
      sql("UPDATE cwpc_src SET salary = 200 WHERE id = 2")
      refreshChain("cwpc_ranked", "cwpc_ranked_proj")
      assertMvCorrect("cwpc_ranked_proj", downstreamSql)

      // Batch 1: INSERT into the already-existing 'eng' partition.
      sql("INSERT INTO cwpc_src VALUES (6,'eng',150)")
      refreshChain("cwpc_ranked", "cwpc_ranked_proj")
      assertMvCorrect("cwpc_ranked_proj", downstreamSql)

      // Batch 2: UPDATE + DELETE in the same partition across the next refresh.
      sql("UPDATE cwpc_src SET salary = 250 WHERE id = 3")
      sql("DELETE FROM cwpc_src WHERE id = 6")
      refreshChain("cwpc_ranked", "cwpc_ranked_proj")
      assertMvCorrect("cwpc_ranked_proj", downstreamSql)
    }
  }
}
