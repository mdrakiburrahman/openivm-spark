package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy carve-out of `ChainedDmlSpec.scala` §(E) — the chm_events fan-out
  * depth-2 chain (events → events_by_user → user_count and
  * events → events_by_type → type_count) convergence walk (~3m40).  Lives in
  * its own forked JVM so the rest of the parity suite is not blocked by this
  * monster test.
  *
  * Note: the two `it` blocks inside describe(E) share state (the second
  * exercises DELETE on the same base table / MVs created by the first), so
  * they are extracted together as one cohesive scenario.  Table / MV names
  * are prefixed `chmheavy_` to guarantee no Delta-path collision with the
  * host spec or any other parallel forked JVM.
  */
abstract class ChainedDmlHeavyConvergenceScenarios extends IvmParitySpecBase("chained-dml-heavy-convergence") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Issue refreshes in dependency order: every name in `mvs` is refreshed
    * after the ones before it. */
  protected def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  // ──────────────────────────────────────────────────────────────────────────
  // (E) Fan-out: a single base table feeds two independent depth-2 chains
  //     (chained.test L326–L450).
  //     events → events_by_user → user_count
  //     events → events_by_type → type_count
  // ──────────────────────────────────────────────────────────────────────────

  describe("(E) Fan-out depth-2 chains share the same base table") {

    it("first batch: both chains converge with the source after refresh") {
      sql("CREATE TABLE IF NOT EXISTS chmheavy_events(user_id STRING, event_type STRING, value INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW chmheavy_events_by_user AS " +
          "SELECT user_id, SUM(value) AS total_value FROM chmheavy_events GROUP BY user_id"
      )
      sql(
        "CREATE MATERIALIZED VIEW chmheavy_events_by_type AS " +
          "SELECT event_type, SUM(value) AS total_value FROM chmheavy_events GROUP BY event_type"
      )
      sql(
        "CREATE MATERIALIZED VIEW chmheavy_event_user_count AS " +
          "SELECT COUNT(*) AS num_users FROM chmheavy_events_by_user"
      )
      sql(
        "CREATE MATERIALIZED VIEW chmheavy_event_type_count AS " +
          "SELECT COUNT(*) AS num_types FROM chmheavy_events_by_type"
      )
      sql(
        "INSERT INTO chmheavy_events VALUES " +
          "('u1','click',10),('u2','click',20),('u1','purchase',100),('u3','click',5)"
      )
      refreshChain(
        "chmheavy_events_by_user",
        "chmheavy_events_by_type",
        "chmheavy_event_user_count",
        "chmheavy_event_type_count"
      )
      assertMvCorrect(
        "chmheavy_events_by_user",
        "SELECT user_id, SUM(value) AS total_value FROM chmheavy_events GROUP BY user_id"
      )
      assertMvCorrect(
        "chmheavy_events_by_type",
        "SELECT event_type, SUM(value) AS total_value FROM chmheavy_events GROUP BY event_type"
      )
      assertMvCorrect(
        "chmheavy_event_user_count",
        "SELECT COUNT(*) AS num_users FROM (" +
          "SELECT user_id, SUM(value) AS total_value FROM chmheavy_events GROUP BY user_id) t"
      )
      assertMvCorrect(
        "chmheavy_event_type_count",
        "SELECT COUNT(*) AS num_types FROM (" +
          "SELECT event_type, SUM(value) AS total_value FROM chmheavy_events GROUP BY event_type) t"
      )
    }

    it("second batch with DELETE propagates through both chains") {
      sql("INSERT INTO chmheavy_events VALUES ('u4','purchase',50),('u1','click',15)")
      sql("DELETE FROM chmheavy_events WHERE user_id = 'u3'")
      refreshChain(
        "chmheavy_events_by_user",
        "chmheavy_events_by_type",
        "chmheavy_event_user_count",
        "chmheavy_event_type_count"
      )
      assertMvCorrect(
        "chmheavy_events_by_user",
        "SELECT user_id, SUM(value) AS total_value FROM chmheavy_events GROUP BY user_id"
      )
      assertMvCorrect(
        "chmheavy_events_by_type",
        "SELECT event_type, SUM(value) AS total_value FROM chmheavy_events GROUP BY event_type"
      )
      assertMvCorrect(
        "chmheavy_event_user_count",
        "SELECT COUNT(*) AS num_users FROM (" +
          "SELECT user_id, SUM(value) AS total_value FROM chmheavy_events GROUP BY user_id) t"
      )
      assertMvCorrect(
        "chmheavy_event_type_count",
        "SELECT COUNT(*) AS num_types FROM (" +
          "SELECT event_type, SUM(value) AS total_value FROM chmheavy_events GROUP BY event_type) t"
      )
    }
  }
}
