package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** Inner-join slice of the [[JoinsSpec]] coverage. See JoinsOuterSpec /
  * JoinsDmlSpec for the remaining cases. Each split owns its SparkSession,
  * warehouse directory and table-name prefix so it can run in its own forked JVM.
  */
abstract class JoinsInnerScenarios extends IvmParitySpecBase("joins-inner") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ============================================================================
  // INNER JOIN
  // ============================================================================

  describe("(1) 2-way INNER JOIN projection: SELECT * FROM a JOIN b ON a.k = b.k") {
    it("incrementally maintains the join projection across INSERTs on both sides") {
      sql("CREATE TABLE IF NOT EXISTS j_inner_proj_a(k INT, v_a STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_inner_proj_b(k INT, v_b INT) USING DELTA")
      sql("INSERT INTO j_inner_proj_a VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
      sql("INSERT INTO j_inner_proj_b VALUES (1, 10), (1, 20), (2, 30)")

      sql(
        "CREATE MATERIALIZED VIEW mv_j_inner_proj AS " +
          "SELECT a.k, a.v_a, b.v_b FROM j_inner_proj_a a JOIN j_inner_proj_b b ON a.k = b.k"
      )

      val rt = mvRefreshType("mv_j_inner_proj")
      Seq(RefreshTypeCode.SimpleProjection, RefreshTypeCode.FullRefresh) should contain(rt)

      // Verify the initial snapshot is correct
      assertMvCorrect(
        "mv_j_inner_proj",
        "SELECT a.k, a.v_a, b.v_b FROM j_inner_proj_a a JOIN j_inner_proj_b b ON a.k = b.k"
      )

      // INSERT on both sides
      sql("INSERT INTO j_inner_proj_a VALUES (4, 'Dave')")
      sql("INSERT INTO j_inner_proj_b VALUES (3, 99), (4, 7)")
      refreshMv("mv_j_inner_proj")
      assertMvCorrect(
        "mv_j_inner_proj",
        "SELECT a.k, a.v_a, b.v_b FROM j_inner_proj_a a JOIN j_inner_proj_b b ON a.k = b.k"
      )
    }
  }

  describe("(2) 2-way INNER JOIN aggregate: SELECT a.k, SUM(b.v) FROM a JOIN b ON a.k=b.k GROUP BY a.k") {
    it("incrementally maintains a joined aggregate via affected-keys recompute") {
      sql("CREATE TABLE IF NOT EXISTS j_inner_agg_a(k INT, label STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_inner_agg_b(k INT, v INT) USING DELTA")
      sql("INSERT INTO j_inner_agg_a VALUES (1, 'A'), (2, 'B'), (3, 'C')")
      sql("INSERT INTO j_inner_agg_b VALUES (1, 10), (1, 20), (2, 30)")

      sql(
        "CREATE MATERIALIZED VIEW mv_j_inner_agg AS " +
          "SELECT a.k, SUM(b.v) AS total " +
          "FROM j_inner_agg_a a JOIN j_inner_agg_b b ON a.k = b.k GROUP BY a.k"
      )

      val rt = mvRefreshType("mv_j_inner_agg")
      Seq(RefreshTypeCode.AggregateGroup, RefreshTypeCode.GroupRecompute) should contain(rt)

      // INSERT on both sides at once
      sql("INSERT INTO j_inner_agg_a VALUES (4, 'D')")
      sql("INSERT INTO j_inner_agg_b VALUES (2, 1), (4, 100), (4, 5)")
      refreshMv("mv_j_inner_agg")
      assertMvCorrect(
        "mv_j_inner_agg",
        "SELECT a.k, SUM(b.v) AS total " +
          "FROM j_inner_agg_a a JOIN j_inner_agg_b b ON a.k = b.k GROUP BY a.k"
      )
    }
  }

  describe("(3) 3-way INNER JOIN aggregate: a JOIN b ON a.k=b.k JOIN c ON b.j=c.j (7 Möbius terms)") {
    it("incrementally maintains the 3-way joined aggregate (exercises 7 Möbius terms)") {
      sql("CREATE TABLE IF NOT EXISTS j_three_a(k INT, label STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_three_b(k INT, j INT, v INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_three_c(j INT, descr STRING) USING DELTA")

      sql("INSERT INTO j_three_a VALUES (1, 'A1'), (2, 'A2'), (3, 'A3')")
      sql("INSERT INTO j_three_b VALUES (1, 10, 100), (1, 20, 200), (2, 10, 50)")
      sql("INSERT INTO j_three_c VALUES (10, 'X'), (20, 'Y')")

      sql(
        "CREATE MATERIALIZED VIEW mv_j_three AS " +
          "SELECT a.k, c.descr, SUM(b.v) AS total " +
          "FROM j_three_a a " +
          "JOIN j_three_b b ON a.k = b.k " +
          "JOIN j_three_c c ON b.j = c.j " +
          "GROUP BY a.k, c.descr"
      )

      val rt = mvRefreshType("mv_j_three")
      Seq(RefreshTypeCode.AggregateGroup, RefreshTypeCode.GroupRecompute) should contain(rt)

      // Exercise all three sides — Möbius will mix 7 (+/-) terms
      sql("INSERT INTO j_three_a VALUES (4, 'A4')")
      sql("INSERT INTO j_three_b VALUES (3, 20, 7), (4, 10, 11)")
      sql("INSERT INTO j_three_c VALUES (30, 'Z')")
      refreshMv("mv_j_three")
      assertMvCorrect(
        "mv_j_three",
        "SELECT a.k, c.descr, SUM(b.v) AS total " +
          "FROM j_three_a a " +
          "JOIN j_three_b b ON a.k = b.k " +
          "JOIN j_three_c c ON b.j = c.j " +
          "GROUP BY a.k, c.descr"
      )

      // Add c-rows after the fact: previously unmatched fact rows now match
      sql("INSERT INTO j_three_c VALUES (10, 'X2')") // a new c row duplicates the join key 10
      refreshMv("mv_j_three")
      assertMvCorrect(
        "mv_j_three",
        "SELECT a.k, c.descr, SUM(b.v) AS total " +
          "FROM j_three_a a " +
          "JOIN j_three_b b ON a.k = b.k " +
          "JOIN j_three_c c ON b.j = c.j " +
          "GROUP BY a.k, c.descr"
      )
    }
  }

  describe("(4) regular N-term projection join with conflicting mixed DML") {
    it("stays bag-equal when old-state arms read pre-refresh Delta snapshots") {
      sql("CREATE TABLE IF NOT EXISTS j_nterm_a(id INT, k INT, label STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_nterm_b(k INT, category STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_nterm_c(category STRING, score INT) USING DELTA")
      sql("INSERT INTO j_nterm_a VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')")
      sql("INSERT INTO j_nterm_b VALUES (10, 'x'), (20, 'y'), (30, 'z')")
      sql("INSERT INTO j_nterm_c VALUES ('x', 1), ('y', 2), ('z', 3)")

      val query =
        "SELECT a.id, a.label, b.category, c.score " +
          "FROM j_nterm_a a JOIN j_nterm_b b ON a.k = b.k " +
          "JOIN j_nterm_c c ON b.category = c.category"
      sql(s"CREATE MATERIALIZED VIEW mv_j_nterm_snapshot AS $query")
      mvRefreshType("mv_j_nterm_snapshot") shouldBe RefreshTypeCode.SimpleProjection

      // Batch conflicting operations across every source before one refresh.
      sql("INSERT INTO j_nterm_a VALUES (4, 40, 'temporary'), (5, 50, 'e')")
      sql("UPDATE j_nterm_a SET label = 'bb', k = 30 WHERE id = 2")
      sql("DELETE FROM j_nterm_a WHERE id IN (1, 4)")
      sql("INSERT INTO j_nterm_b VALUES (40, 'w'), (50, 'q')")
      sql("UPDATE j_nterm_b SET category = 'zz' WHERE k = 30")
      sql("DELETE FROM j_nterm_b WHERE k = 10")
      sql("INSERT INTO j_nterm_c VALUES ('q', 5), ('w', 4), ('zz', 33)")
      sql("UPDATE j_nterm_c SET score = 22 WHERE category = 'y'")
      sql("DELETE FROM j_nterm_c WHERE category IN ('x', 'w')")

      refreshMv("mv_j_nterm_snapshot")
      assertMvCorrect("mv_j_nterm_snapshot", query)
    }
  }
}
