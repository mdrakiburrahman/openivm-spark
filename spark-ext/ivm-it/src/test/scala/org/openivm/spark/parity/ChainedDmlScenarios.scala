package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/** Split from the original parity spec.  Scope:
  * Depth-2 chains exercised by fan-out, empty-intermediate, and UPDATE-driven DML mutations from `chained.test`.
  *
  * Includes sections: (E), (H), (I).
  */
abstract class ChainedDmlScenarios extends IvmParitySpecBase("chained-dml") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  private final class BufferingAppender(name: String)
      extends AbstractAppender(
        name,
        null,
        PatternLayout.createDefaultLayout(),
        false,
        Property.EMPTY_ARRAY
      ) {
    protected val buffer = ArrayBuffer.empty[String]

    override def append(event: LogEvent): Unit =
      buffer.synchronized {
        buffer += event.getMessage.getFormattedMessage
      }

    def messages: Seq[String] = buffer.synchronized(buffer.toVector)
  }

  protected def withLogCapture[A](body: BufferingAppender => A): A = {
    val appender = new BufferingAppender(s"chm-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try body(appender)
    finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  protected def noPendingFor(logs: Seq[String], viewName: String): Boolean =
    logs.exists(msg => msg.contains(s"view='`$viewName`'") && msg.contains("outcome='no_pending_deltas'"))

  /** Issue refreshes in dependency order: every name in `mvs` is refreshed
    * after the ones before it. */
  protected def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  // ──────────────────────────────────────────────────────────────────────────
  // (E) Fan-out: a single base table feeds two independent depth-2 chains
  //     (chained.test L326–L450).
  //     events → events_by_user → user_count
  //     events → events_by_type → type_count
  //
  //     Extracted to [[ChainedDmlHeavyConvergenceSpec]] (~3m40 wall, plus the
  //     coupled second-batch DELETE round which shares the same base table /
  //     MV chain so it had to travel with the heavy `it`) so it runs in its
  //     own forked JVM and does not bottleneck the rest of this spec.
  // ──────────────────────────────────────────────────────────────────────────

  // ──────────────────────────────────────────────────────────────────────────
  // (G) Downstream refresh must see an upstream MV_VIEW_DELTA trigger even
  //     when the original base table has no direct pending rows for it.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(G) Downstream MV refresh sees upstream cascade deltas") {
    it("does not no-op short-circuit after the upstream MV refresh emits a view-delta") {
      sql("CREATE TABLE IF NOT EXISTS chm_rsc_base(id INT, val INT) USING DELTA")
      sql("INSERT INTO chm_rsc_base VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW chm_rsc_l1 AS SELECT id, val FROM chm_rsc_base")
      sql("CREATE MATERIALIZED VIEW chm_rsc_l2 AS SELECT id, val FROM chm_rsc_l1")

      sql("INSERT INTO chm_rsc_base VALUES (2, 20)")
      refreshChain("chm_rsc_l1")
      withLogCapture { appender =>
        refreshChain("chm_rsc_l2")
        noPendingFor(appender.messages, "chm_rsc_l2") shouldBe false
      }

      assertMvCorrect("chm_rsc_l1", "SELECT id, val FROM chm_rsc_base")
      assertMvCorrect("chm_rsc_l2", "SELECT id, val FROM chm_rsc_base")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (H) Intermediate MV becomes completely empty mid-test
  //     (chained.test L743–L899): chm_empty → chm_l1 → chm_l2.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(H) Two-level chain with an empty intermediate state") {

    it("initial populated state: both MVs match the recomputed view body") {
      sql("CREATE TABLE IF NOT EXISTS chm_empty(grp STRING, val INT) USING DELTA")
      sql("INSERT INTO chm_empty VALUES ('a',10),('b',20)")
      sql(
        "CREATE MATERIALIZED VIEW chm_empty_l1 AS " +
          "SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp"
      )
      sql(
        "CREATE MATERIALIZED VIEW chm_empty_l2 AS " +
          "SELECT SUM(total) AS grand FROM chm_empty_l1"
      )
      refreshChain("chm_empty_l1", "chm_empty_l2")
      assertMvCorrect("chm_empty_l1", "SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp")
      assertMvCorrect(
        "chm_empty_l2",
        "SELECT SUM(total) AS grand FROM (SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp) t"
      )
    }

    it("DELETE ALL: intermediate MV empties and the leaf collapses to a NULL grand-total row") {
      sql("DELETE FROM chm_empty")
      refreshChain("chm_empty_l1", "chm_empty_l2")
      assertMvCorrect("chm_empty_l1", "SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp")
      assertMvCorrect(
        "chm_empty_l2",
        "SELECT SUM(total) AS grand FROM (SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp) t"
      )
    }

    it("repopulation: new group bubbles back up through the chain") {
      sql("INSERT INTO chm_empty VALUES ('x',100)")
      refreshChain("chm_empty_l1", "chm_empty_l2")
      assertMvCorrect("chm_empty_l1", "SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp")
      assertMvCorrect(
        "chm_empty_l2",
        "SELECT SUM(total) AS grand FROM (SELECT grp, SUM(val) AS total FROM chm_empty GROUP BY grp) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (I) Two-level chain exercised by UPDATEs that move rows between groups
  //     (chained.test L902–L1057): chm_update → ch4_l1 → ch4_l2.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(I) Two-level chain with UPDATEs that move data between groups") {

    it("initial INSERTs propagate through both MVs") {
      sql("CREATE TABLE IF NOT EXISTS chm_update(grp STRING, val INT) USING DELTA")
      sql("INSERT INTO chm_update VALUES ('a',10),('a',20),('b',30)")
      sql(
        "CREATE MATERIALIZED VIEW chm_update_l1 AS " +
          "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM chm_update GROUP BY grp"
      )
      sql(
        "CREATE MATERIALIZED VIEW chm_update_l2 AS " +
          "SELECT SUM(total) AS grand FROM chm_update_l1"
      )
      refreshChain("chm_update_l1", "chm_update_l2")
      assertMvCorrect(
        "chm_update_l1",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM chm_update GROUP BY grp"
      )
      assertMvCorrect(
        "chm_update_l2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT grp, SUM(val) AS total FROM chm_update GROUP BY grp) t"
      )
    }

    it("UPDATE moves a single row from 'a' to 'b' — propagates through both MVs") {
      sql("UPDATE chm_update SET grp = 'b' WHERE val = 20")
      refreshChain("chm_update_l1", "chm_update_l2")
      assertMvCorrect(
        "chm_update_l1",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM chm_update GROUP BY grp"
      )
      assertMvCorrect(
        "chm_update_l2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT grp, SUM(val) AS total FROM chm_update GROUP BY grp) t"
      )
    }

    it("UPDATE moves every 'b' row to 'c' — 'b' disappears, 'c' appears") {
      sql("UPDATE chm_update SET grp = 'c' WHERE grp = 'b'")
      refreshChain("chm_update_l1", "chm_update_l2")
      assertMvCorrect(
        "chm_update_l1",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM chm_update GROUP BY grp"
      )
      assertMvCorrect(
        "chm_update_l2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT grp, SUM(val) AS total FROM chm_update GROUP BY grp) t"
      )
    }
  }
}
