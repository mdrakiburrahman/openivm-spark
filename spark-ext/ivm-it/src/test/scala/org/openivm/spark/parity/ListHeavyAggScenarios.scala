package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** Heavy-test isolation spin-off of [[ListSpec]] section (1).
  *
  * Extracted into its own spec class so that `Test/testGrouping` runs it in a
  * dedicated forked JVM (per `Settings.parallelForkSettings`), shrinking the
  * crowded host spec's wall-clock.
  *
  * Table / MV names inside this spec are prefixed `lha_*` / `mv_lha_*` so two
  * parallel JVMs (this one and the host `ListSpec`) cannot collide on Delta
  * paths.
  */
abstract class ListHeavyAggScenarios extends IvmParitySpecBase("list-heavy-agg") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  describe("(1) array_agg(val) over ARRAY<FLOAT> per group across INSERT/DELETE/mixed") {
    it("LIST aggregate maintained correctly across all DML; classifier = GROUP_RECOMPUTE-ish") {
      sql(
        "CREATE TABLE IF NOT EXISTS lha_items(id INT, grp INT, val ARRAY<FLOAT>) USING DELTA"
      )
      sql(
        "INSERT INTO lha_items VALUES " +
          "(1, 1, array(CAST(10.0 AS FLOAT), CAST(10.0 AS FLOAT))), " +
          "(2, 1, array(CAST(20.0 AS FLOAT), CAST(20.0 AS FLOAT))), " +
          "(3, 2, array(CAST(30.0 AS FLOAT), CAST(30.0 AS FLOAT)))"
      )

      val viewBody =
        "SELECT grp, array_sort(array_agg(val)) AS total, COUNT(*) AS n " +
          "FROM lha_items GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW mv_lha_list AS $viewBody")

      val rt = mvRefreshType("mv_lha_list")
      withClue(s"observed refreshType=$rt: ") {
        Seq(
          RefreshTypeCode.GroupRecompute,
          RefreshTypeCode.AggregateGroup,
          RefreshTypeCode.FullRefresh
        ) should contain(rt)
      }

      // Initial state — verified via bidirectional EXCEPT ALL with JSON-serialized arrays
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))

      // 1. Insert into existing group (grp=2)
      sql("INSERT INTO lha_items VALUES (4, 2, array(CAST(40.0 AS FLOAT), CAST(40.0 AS FLOAT)))")
      refreshMv("mv_lha_list")
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))

      // 2. Insert new group (grp=3)
      sql("INSERT INTO lha_items VALUES (5, 3, array(CAST(50.0 AS FLOAT), CAST(50.0 AS FLOAT)))")
      refreshMv("mv_lha_list")
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))

      // 3. Delete from existing group (id=1 in grp=1)
      sql("DELETE FROM lha_items WHERE id = 1")
      refreshMv("mv_lha_list")
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))

      // 4. Mandatory stress: mixed INSERT + DELETE in same batch
      sql("INSERT INTO lha_items VALUES (6, 1, array(CAST(100.0 AS FLOAT), CAST(100.0 AS FLOAT)))")
      sql("DELETE FROM lha_items WHERE id = 2")
      refreshMv("mv_lha_list")
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))

      // 5. No-op refresh
      refreshMv("mv_lha_list")
      assertMvCorrect("mv_lha_list", viewBody, arrayCols = Set("total"))
    }
  }
}
