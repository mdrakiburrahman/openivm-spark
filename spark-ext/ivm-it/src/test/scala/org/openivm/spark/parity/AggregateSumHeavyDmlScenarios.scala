package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy carve-out of `AggregateSumSpec.scala`'s aggsum_sales 9-cycle DML walk
  * (~5m).  Lives in its own forked JVM so the rest of the parity suite is not
  * blocked by this monster test.
  *
  * Table / MV names are prefixed `aggsumheavy_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
abstract class AggregateSumHeavyDmlScenarios extends IvmParitySpecBase("aggregate-sum-heavy-dml") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  describe("basic aggregate — aggsumheavy_sales(region, SUM(amount), COUNT(amount))") {
    it("INSERT/DELETE/batched/no-op operations all keep the MV in sync") {
      sql("CREATE TABLE aggsumheavy_sales (id INT, region STRING, amount INT) USING DELTA")
      sql("INSERT INTO aggsumheavy_sales VALUES (1, 'east', 100), (2, 'west', 200), (3, 'east', 150)")
      val viewBody =
        "SELECT region, SUM(amount) AS total, COUNT(amount) AS cnt FROM aggsumheavy_sales GROUP BY region"
      sql(s"CREATE MATERIALIZED VIEW aggsumheavy_sales_summary AS $viewBody")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Insert into existing group
      sql("INSERT INTO aggsumheavy_sales VALUES (4, 'east', 50)")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Insert into new group
      sql("INSERT INTO aggsumheavy_sales VALUES (5, 'north', 300)")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Delete from existing group
      sql("DELETE FROM aggsumheavy_sales WHERE id = 1")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Multiple inserts in same group
      sql("INSERT INTO aggsumheavy_sales VALUES (6, 'west', 75), (7, 'west', 25)")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // No-op refresh
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Batch insert into multiple groups at once
      sql("INSERT INTO aggsumheavy_sales VALUES (10, 'south', 100), (11, 'south', 200), (12, 'north', 50)")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Mixed insert + delete in same cycle
      sql("INSERT INTO aggsumheavy_sales VALUES (13, 'west', 100)")
      sql("DELETE FROM aggsumheavy_sales WHERE id = 6")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)

      // Delete ALL rows from a group
      sql("DELETE FROM aggsumheavy_sales WHERE region = 'east'")
      refreshMv("aggsumheavy_sales_summary")
      assertMvCorrect("aggsumheavy_sales_summary", viewBody)
    }
  }
}
