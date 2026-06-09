package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Split from the original parity spec.  Scope:
  * Depth-2 fan-out, upstream cascade, empty-base and group-delete scenarios from `pipeline.test`.
  *
  * Includes sections: (G), (I), (L), (M).
  */
abstract class PipelineCascadeScenarios extends IvmParitySpecBase("pipeline-cascade") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Issue refreshes in dependency order: every name in `mvs` is refreshed
    * after the ones before it. */
  protected def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  // ──────────────────────────────────────────────────────────────────────────
  // (G) Fan-out: one MV feeds two independent two-level children
  //     pipeline.test L288–L365
  //     sales → region_totals → top_region
  //     sales → region_totals → overall
  // ──────────────────────────────────────────────────────────────────────────

  describe("(G) Fan-out: region_totals feeds top_region and overall (both depth-2)") {

    it("first batch: refresh root then both children — both downstreams converge") {
      sql("CREATE TABLE IF NOT EXISTS plc_sales(region STRING, amount INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW plc_region_totals AS " +
          "SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region"
      )
      sql(
        "CREATE MATERIALIZED VIEW plc_top_region AS " +
          "SELECT region, total FROM plc_region_totals"
      )
      sql(
        "CREATE MATERIALIZED VIEW plc_overall AS " +
          "SELECT SUM(total) AS grand FROM plc_region_totals"
      )
      sql("INSERT INTO plc_sales VALUES ('US',100),('EU',200),('US',300)")

      // Refresh root, then one child, then the other.
      refreshChain("plc_region_totals", "plc_top_region", "plc_overall")

      assertMvCorrect("plc_region_totals", "SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region")
      assertMvCorrect(
        "plc_top_region",
        "SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region"
      )
      assertMvCorrect(
        "plc_overall",
        "SELECT SUM(total) AS grand FROM (SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region) t"
      )
    }

    it("second batch: a new region propagates to both children") {
      sql("INSERT INTO plc_sales VALUES ('JP',1000)")
      refreshChain("plc_region_totals", "plc_top_region", "plc_overall")

      assertMvCorrect("plc_region_totals", "SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region")
      assertMvCorrect(
        "plc_top_region",
        "SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region"
      )
      assertMvCorrect(
        "plc_overall",
        "SELECT SUM(total) AS grand FROM (SELECT region, SUM(amount) AS total FROM plc_sales GROUP BY region) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (I) Upstream cascade equivalent — manual upstream-first refresh
  //     pipeline.test L430–L477: up_base → up_l1 → up_l2.
  //     openivm uses `SET openivm_cascade_refresh = 'upstream'`; here we
  //     refresh in dependency order explicitly.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(I) Manual upstream-first order: up_base → up_l1 → up_l2") {

    it("first batch: upstream refreshed before leaf yields correct chained state") {
      sql("CREATE TABLE IF NOT EXISTS plc_up_base(x INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW plc_up_l1 AS SELECT SUM(x) AS total FROM plc_up_base"
      )
      sql(
        "CREATE MATERIALIZED VIEW plc_up_l2 AS SELECT total * 2 AS doubled FROM plc_up_l1"
      )
      sql("INSERT INTO plc_up_base VALUES (10),(20)")
      refreshChain("plc_up_l1", "plc_up_l2")
      assertMvCorrect("plc_up_l1", "SELECT SUM(x) AS total FROM plc_up_base")
      assertMvCorrect(
        "plc_up_l2",
        "SELECT total * 2 AS doubled FROM (SELECT SUM(x) AS total FROM plc_up_base) t"
      )
    }

    it("second batch: additional row propagates to both MVs") {
      sql("INSERT INTO plc_up_base VALUES (5)")
      refreshChain("plc_up_l1", "plc_up_l2")
      assertMvCorrect("plc_up_l1", "SELECT SUM(x) AS total FROM plc_up_base")
      assertMvCorrect(
        "plc_up_l2",
        "SELECT total * 2 AS doubled FROM (SELECT SUM(x) AS total FROM plc_up_base) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (L) Full chain through 2 levels where base becomes empty then repopulates
  //     pipeline.test L541–L690 (`pipe_empty` block).
  // ──────────────────────────────────────────────────────────────────────────

  describe("(L) Two-level chain that goes empty and back: pipe_empty → pipe_e1 → pipe_e2") {

    it("initial populated state: both MVs match the recomputed view body") {
      sql("CREATE TABLE IF NOT EXISTS plc_pipe_empty(user_id STRING, value INT) USING DELTA")
      sql("INSERT INTO plc_pipe_empty VALUES ('a',100),('b',200)")
      sql(
        "CREATE MATERIALIZED VIEW plc_pipe_e1 AS " +
          "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id"
      )
      sql(
        "CREATE MATERIALIZED VIEW plc_pipe_e2 AS " +
          "SELECT SUM(total) AS grand FROM plc_pipe_e1"
      )
      refreshChain("plc_pipe_e1", "plc_pipe_e2")
      assertMvCorrect(
        "plc_pipe_e1",
        "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id"
      )
      assertMvCorrect(
        "plc_pipe_e2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id) t"
      )
    }

    it("DELETE everything: intermediate MV empties and the leaf collapses to NULL grand") {
      sql("DELETE FROM plc_pipe_empty")
      refreshChain("plc_pipe_e1", "plc_pipe_e2")
      assertMvCorrect(
        "plc_pipe_e1",
        "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id"
      )
      assertMvCorrect(
        "plc_pipe_e2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id) t"
      )
    }

    it("repopulate: new group propagates back through both MVs") {
      sql("INSERT INTO plc_pipe_empty VALUES ('x',50)")
      refreshChain("plc_pipe_e1", "plc_pipe_e2")
      assertMvCorrect(
        "plc_pipe_e1",
        "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id"
      )
      assertMvCorrect(
        "plc_pipe_e2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT user_id, SUM(value) AS total FROM plc_pipe_empty GROUP BY user_id) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (M) Two-level chain with deletes that wipe a group at the source
  //     pipeline.test L693–L842 (`pipe_up` block — depth-2 portion only).
  //     Refreshed in upstream order; verifies grand sums for INSERT, repeated
  //     INSERT, and DELETE-by-group sequences.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(M) Two-level chain: pipe_up → pipe_up1 → pipe_up2 (upstream-first manual order)") {

    it("first batch: INSERT then refresh in order produces correct chained state") {
      sql("CREATE TABLE IF NOT EXISTS plc_pipe_up(k STRING, v INT) USING DELTA")
      sql("INSERT INTO plc_pipe_up VALUES ('a',10)")
      sql(
        "CREATE MATERIALIZED VIEW plc_pipe_up1 AS " +
          "SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k"
      )
      sql(
        "CREATE MATERIALIZED VIEW plc_pipe_up2 AS " +
          "SELECT SUM(total) AS grand FROM plc_pipe_up1"
      )
      refreshChain("plc_pipe_up1", "plc_pipe_up2")
      assertMvCorrect("plc_pipe_up1", "SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k")
      assertMvCorrect(
        "plc_pipe_up2",
        "SELECT SUM(total) AS grand FROM (SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k) t"
      )
    }

    it("second batch: repeated INSERTs across multiple groups propagate") {
      sql("INSERT INTO plc_pipe_up VALUES ('a',5),('b',20)")
      refreshChain("plc_pipe_up1", "plc_pipe_up2")
      assertMvCorrect("plc_pipe_up1", "SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k")
      assertMvCorrect(
        "plc_pipe_up2",
        "SELECT SUM(total) AS grand FROM (SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k) t"
      )
    }

    it("DELETE entire group 'a': group disappears at every level") {
      sql("DELETE FROM plc_pipe_up WHERE k = 'a'")
      refreshChain("plc_pipe_up1", "plc_pipe_up2")
      assertMvCorrect("plc_pipe_up1", "SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k")
      assertMvCorrect(
        "plc_pipe_up2",
        "SELECT SUM(total) AS grand FROM (SELECT k, SUM(v) AS total FROM plc_pipe_up GROUP BY k) t"
      )
    }
  }
}
