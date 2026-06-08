package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** LEFT / RIGHT / FULL OUTER join slice of the [[JoinsSpec]] coverage.
  * See JoinsInnerSpec / JoinsDmlSpec for the remaining cases.
  */
abstract class JoinsOuterScenarios extends IvmParitySpecBase("joins-outer") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ============================================================================
  // LEFT / RIGHT JOIN
  // ============================================================================

  describe("(4) 2-way LEFT JOIN projection: nullable right side") {
    it("MV preserves NULL right-side rows for unmatched left rows") {
      sql("CREATE TABLE IF NOT EXISTS j_left_proj_c(id INT, cname STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_left_proj_o(cid INT, product STRING, amount INT) USING DELTA")
      sql("INSERT INTO j_left_proj_c VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      sql("INSERT INTO j_left_proj_o VALUES (1, 'Widget', 100), (1, 'Gadget', 200)")

      sql(
        "CREATE MATERIALIZED VIEW mv_j_left_proj AS " +
          "SELECT c.cname, o.product, o.amount " +
          "FROM j_left_proj_c c LEFT JOIN j_left_proj_o o ON c.id = o.cid"
      )

      val rt = mvRefreshType("mv_j_left_proj")
      // Either incremental projection (2) or full-refresh fallback (3).
      Seq(RefreshTypeCode.SimpleProjection, RefreshTypeCode.FullRefresh) should contain(rt)

      assertMvCorrect(
        "mv_j_left_proj",
        "SELECT c.cname, o.product, o.amount " +
          "FROM j_left_proj_c c LEFT JOIN j_left_proj_o o ON c.id = o.cid"
      )

      // Right-side insert — Bob's NULL row should be replaced with a matched row
      sql("INSERT INTO j_left_proj_o VALUES (2, 'Bolt', 50)")
      refreshMv("mv_j_left_proj")
      assertMvCorrect(
        "mv_j_left_proj",
        "SELECT c.cname, o.product, o.amount " +
          "FROM j_left_proj_c c LEFT JOIN j_left_proj_o o ON c.id = o.cid"
      )

      // Left-side insert — new customer with no orders, NULL-extended row appears
      sql("INSERT INTO j_left_proj_c VALUES (4, 'Dave')")
      refreshMv("mv_j_left_proj")
      assertMvCorrect(
        "mv_j_left_proj",
        "SELECT c.cname, o.product, o.amount " +
          "FROM j_left_proj_c c LEFT JOIN j_left_proj_o o ON c.id = o.cid"
      )
    }
  }

  describe("(5) 2-way LEFT JOIN aggregate: COUNT preserves left rows with NULL right") {
    it("COUNT(o.amount) on the right is 0 for unmatched-left rows; refreshes incrementally") {
      sql("CREATE TABLE IF NOT EXISTS j_left_agg_c(id INT, cname STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_left_agg_o(cid INT, amount INT) USING DELTA")
      sql("INSERT INTO j_left_agg_c VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      sql("INSERT INTO j_left_agg_o VALUES (1, 100), (1, 200)")

      sql(
        "CREATE MATERIALIZED VIEW mv_j_left_agg AS " +
          "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_left_agg_c c LEFT JOIN j_left_agg_o o ON c.id = o.cid " +
          "GROUP BY c.cname"
      )

      val rt = mvRefreshType("mv_j_left_agg")
      Seq(RefreshTypeCode.AggregateGroup, RefreshTypeCode.GroupRecompute) should contain(rt)

      assertMvCorrect(
        "mv_j_left_agg",
        "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_left_agg_c c LEFT JOIN j_left_agg_o o ON c.id = o.cid " +
          "GROUP BY c.cname"
      )

      // Right-side insert — Bob's count goes from 0 to 1
      sql("INSERT INTO j_left_agg_o VALUES (2, 50)")
      refreshMv("mv_j_left_agg")
      assertMvCorrect(
        "mv_j_left_agg",
        "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_left_agg_c c LEFT JOIN j_left_agg_o o ON c.id = o.cid " +
          "GROUP BY c.cname"
      )

      // Left-side insert — new customer with no orders (COUNT = 0)
      sql("INSERT INTO j_left_agg_c VALUES (4, 'Dave')")
      refreshMv("mv_j_left_agg")
      assertMvCorrect(
        "mv_j_left_agg",
        "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_left_agg_c c LEFT JOIN j_left_agg_o o ON c.id = o.cid " +
          "GROUP BY c.cname"
      )
    }
  }

  describe("(6) 2-way RIGHT JOIN projection: nullable left side") {
    it("preserves NULL left-side rows for unmatched right rows") {
      sql("CREATE TABLE IF NOT EXISTS j_right_proj_l(id INT, lname STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_right_proj_r(lid INT, rval INT) USING DELTA")
      sql("INSERT INTO j_right_proj_l VALUES (1, 'Alice'), (2, 'Bob')")
      sql("INSERT INTO j_right_proj_r VALUES (1, 10), (1, 20), (3, 99)")

      sql(
        "CREATE MATERIALIZED VIEW mv_j_right_proj AS " +
          "SELECT l.lname, r.rval " +
          "FROM j_right_proj_l l RIGHT JOIN j_right_proj_r r ON l.id = r.lid"
      )

      // Accept either incremental SimpleProjection (2) or FullRefresh (3).
      val rt = mvRefreshType("mv_j_right_proj")
      Seq(RefreshTypeCode.SimpleProjection, RefreshTypeCode.FullRefresh) should contain(rt)

      assertMvCorrect(
        "mv_j_right_proj",
        "SELECT l.lname, r.rval " +
          "FROM j_right_proj_l l RIGHT JOIN j_right_proj_r r ON l.id = r.lid"
      )

      // Insert on the right with a matching key (replaces unmatched-right semantics)
      sql("INSERT INTO j_right_proj_r VALUES (2, 30)")
      // Insert on the left with no matching right (no MV change expected — right join)
      sql("INSERT INTO j_right_proj_l VALUES (4, 'Dave')")
      // Insert on the right with no matching left (unmatched-right NULL row appears)
      sql("INSERT INTO j_right_proj_r VALUES (5, 77)")
      refreshMv("mv_j_right_proj")

      assertMvCorrect(
        "mv_j_right_proj",
        "SELECT l.lname, r.rval " +
          "FROM j_right_proj_l l RIGHT JOIN j_right_proj_r r ON l.id = r.lid"
      )
    }
  }

  // ============================================================================
  // FULL OUTER JOIN
  // ============================================================================

  describe("(7) 2-way FULL OUTER JOIN projection") {
    it("preserves unmatched rows from both sides") {
      sql("CREATE TABLE IF NOT EXISTS j_fo_proj_e(id INT, ename STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_fo_proj_p(id INT, emp_id INT, title STRING) USING DELTA")
      sql("INSERT INTO j_fo_proj_e VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      sql(
        "INSERT INTO j_fo_proj_p VALUES (10, 1, 'Alpha'), (20, 1, 'Beta'), (30, 4, 'Gamma')"
      )

      sql(
        "CREATE MATERIALIZED VIEW mv_j_fo_proj AS " +
          "SELECT e.ename, p.title " +
          "FROM j_fo_proj_e e FULL OUTER JOIN j_fo_proj_p p ON e.id = p.emp_id"
      )

      // Acceptable: SimpleProjection (2) or FullRefresh (3).
      val rt = mvRefreshType("mv_j_fo_proj")
      Seq(RefreshTypeCode.SimpleProjection, RefreshTypeCode.FullRefresh) should contain(rt)

      assertMvCorrect(
        "mv_j_fo_proj",
        "SELECT e.ename, p.title " +
          "FROM j_fo_proj_e e FULL OUTER JOIN j_fo_proj_p p ON e.id = p.emp_id"
      )

      // Insert on right: Bob (unmatched-left) → matched
      sql("INSERT INTO j_fo_proj_p VALUES (40, 2, 'Delta')")
      // Insert on right with no match (new unmatched-right row)
      sql("INSERT INTO j_fo_proj_p VALUES (50, 99, 'Epsilon')")
      // Insert on left with no match (new unmatched-left row)
      sql("INSERT INTO j_fo_proj_e VALUES (5, 'Eve')")
      refreshMv("mv_j_fo_proj")

      assertMvCorrect(
        "mv_j_fo_proj",
        "SELECT e.ename, p.title " +
          "FROM j_fo_proj_e e FULL OUTER JOIN j_fo_proj_p p ON e.id = p.emp_id"
      )
    }
  }

  describe("(8) 2-way FULL OUTER JOIN aggregate (Zhang & Larson MERGE)") {
    it("aggregate over FULL OUTER refreshes incrementally") {
      sql("CREATE TABLE IF NOT EXISTS j_fo_agg_e(id INT, ename STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS j_fo_agg_p(id INT, emp_id INT, score INT) USING DELTA")
      sql("INSERT INTO j_fo_agg_e VALUES (1, 'Alice'), (2, 'Bob')")
      sql("INSERT INTO j_fo_agg_p VALUES (10, 1, 100), (20, 1, 50), (30, 4, 200)")

      sql(
        "CREATE MATERIALIZED VIEW mv_j_fo_agg AS " +
          "SELECT COALESCE(e.ename, '<orphan>') AS who, " +
          "       SUM(p.score) AS total, COUNT(p.score) AS n " +
          "FROM j_fo_agg_e e FULL OUTER JOIN j_fo_agg_p p ON e.id = p.emp_id " +
          "GROUP BY COALESCE(e.ename, '<orphan>')"
      )

      val rt = mvRefreshType("mv_j_fo_agg")
      Seq(RefreshTypeCode.AggregateGroup, RefreshTypeCode.GroupRecompute) should contain(rt)

      assertMvCorrect(
        "mv_j_fo_agg",
        "SELECT COALESCE(e.ename, '<orphan>') AS who, " +
          "       SUM(p.score) AS total, COUNT(p.score) AS n " +
          "FROM j_fo_agg_e e FULL OUTER JOIN j_fo_agg_p p ON e.id = p.emp_id " +
          "GROUP BY COALESCE(e.ename, '<orphan>')"
      )

      // Insert on right: Bob (unmatched-left) → matched; new orphan
      sql("INSERT INTO j_fo_agg_p VALUES (40, 2, 75), (50, 99, 1)")
      // Insert on left: a new employee with no projects (NULL right)
      sql("INSERT INTO j_fo_agg_e VALUES (3, 'Carol')")
      refreshMv("mv_j_fo_agg")

      assertMvCorrect(
        "mv_j_fo_agg",
        "SELECT COALESCE(e.ename, '<orphan>') AS who, " +
          "       SUM(p.score) AS total, COUNT(p.score) AS n " +
          "FROM j_fo_agg_e e FULL OUTER JOIN j_fo_agg_p p ON e.id = p.emp_id " +
          "GROUP BY COALESCE(e.ename, '<orphan>')"
      )
    }
  }
}
