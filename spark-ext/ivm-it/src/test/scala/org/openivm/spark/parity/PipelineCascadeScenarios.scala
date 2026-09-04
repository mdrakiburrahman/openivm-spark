package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{ChangeFeedMode, MvCatalog, RefreshTypeCode, StagingCatalog, StagingDelta}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/** Split from the original parity spec.  Scope:
  * Depth-2 fan-out, upstream cascade, empty-base and group-delete scenarios from `pipeline.test`.
  *
  * Includes sections: (G), (I), (L), (M).
  */
abstract class PipelineCascadeScenarios extends IvmParitySpecBase("pipeline-cascade") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(
      "spark.openivm.fuseScratch.enabled"              -> "true",
      "spark.openivm.fuseScratch.cascadeCache.enabled" -> "true"
    )

  /** Issue refreshes in dependency order: every name in `mvs` is refreshed
    * after the ones before it. */
  protected def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => sql(s"REFRESH MATERIALIZED VIEW $m").collect())

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

  private def withLogCapture[A](body: BufferingAppender => A): A = {
    val appender = new BufferingAppender(s"pipeline-cascade-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try body(appender)
    finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

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
      val upstreamMeta = MvCatalog.lookup(spark, TableIdentifier("plc_up_l1")).get
      val leafMeta     = MvCatalog.lookup(spark, TableIdentifier("plc_up_l2")).get
      upstreamMeta.refreshType shouldBe RefreshTypeCode.SimpleAggregate
      upstreamMeta.emitsCascadeViewDelta shouldBe true
      leafMeta.refreshType shouldBe RefreshTypeCode.SimpleProjection
      sql("INSERT INTO plc_up_base VALUES (10),(20)")
      refreshChain("plc_up_l1")
      if (changeFeedMode == ChangeFeedMode.Intercept) {
        val leafName = leafMeta.name.database.fold(leafMeta.name.table)(db => s"$db.${leafMeta.name.table}")
        val cascade = StagingCatalog
          .collectFor(spark, leafName, leafMeta.sourceTables, leafMeta.sourceWatermarks)
          .filter(_.opType == StagingDelta.OpTypes.MvViewDelta)
          .lastOption
          .getOrElse(fail("missing SIMPLE_AGGREGATE cascade staging row"))
        val signedTotals = spark.read
          .format("delta")
          .load(cascade.stagingPath)
          .select("total", "openivm_multiplicity")
          .collect()
          .map { row =>
            Option(row.getAs[java.lang.Long]("total")).map(_.longValue()) ->
              row.getAs[Int]("openivm_multiplicity")
          }
          .toSet
        signedTotals shouldBe Set(None -> -1, Some(30L) -> 1)
      }
      refreshChain("plc_up_l2")
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

    it("retracts the old scalar and propagates NULL when the base becomes empty") {
      sql("DELETE FROM plc_up_base")
      refreshChain("plc_up_l1", "plc_up_l2")
      assertMvCorrect("plc_up_l1", "SELECT SUM(x) AS total FROM plc_up_base")
      assertMvCorrect(
        "plc_up_l2",
        "SELECT total * 2 AS doubled FROM (SELECT SUM(x) AS total FROM plc_up_base) t"
      )
    }

    it("retracts NULL and propagates the new scalar when the base is repopulated") {
      sql("INSERT INTO plc_up_base VALUES (7),(8)")
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

  describe("(N) SIMPLE_PROJECTION cascade reuses the fused scratch cache") {

    it("refreshes the upstream through the fused path and feeds the downstream from the cached delta") {
      sql("CREATE TABLE IF NOT EXISTS plc_fuse_src(id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO plc_fuse_src VALUES (1, 'Alice', 30), (2, 'Bob', 22)")
      sql(
        "CREATE MATERIALIZED VIEW plc_fuse_up AS " +
          "SELECT id, name, age FROM plc_fuse_src WHERE age >= 18"
      )
      sql(
        "CREATE MATERIALIZED VIEW plc_fuse_down AS " +
          "SELECT id, name FROM plc_fuse_up WHERE age >= 25"
      )
      sql("INSERT INTO plc_fuse_src VALUES (3, 'Carol', 40)")

      val lines = withLogCapture { appender =>
        sql("REFRESH MATERIALIZED VIEW plc_fuse_up").collect()
        appender.messages.filter(m => m.startsWith("[openivm-perf] ") && m.contains("view='`plc_fuse_up`'"))
      }

      withClue("captured [openivm-perf] lines:\n" + lines.mkString("\n") + "\n") {
        lines.exists { line =>
          line.contains("phase='stmt'") &&
          line.contains("stmt_kind='view_delta_ctas'") &&
          line.contains("fused='true'")
        } shouldBe true
      }

      sql("REFRESH MATERIALIZED VIEW plc_fuse_down").collect()
      assertMvCorrect("plc_fuse_up", "SELECT id, name, age FROM plc_fuse_src WHERE age >= 18")
      assertMvCorrect("plc_fuse_down", "SELECT id, name FROM plc_fuse_src WHERE age >= 25")
    }
  }

  describe("(O) multi-column SIMPLE_AGGREGATE cascade") {

    it("propagates every old and new scalar column to a downstream projection") {
      sql("CREATE TABLE IF NOT EXISTS plc_scalar_multi_src(x INT) USING DELTA")
      sql("INSERT INTO plc_scalar_multi_src VALUES (10),(20),(NULL)")
      sql(
        "CREATE MATERIALIZED VIEW plc_scalar_multi_up AS " +
          "SELECT SUM(x) AS total, COUNT(*) AS row_count, COUNT(x) AS value_count FROM plc_scalar_multi_src"
      )
      sql(
        "CREATE MATERIALIZED VIEW plc_scalar_multi_down AS " +
          "SELECT total, row_count, value_count FROM plc_scalar_multi_up"
      )

      sql("DELETE FROM plc_scalar_multi_src WHERE x = 10 OR x IS NULL")
      sql("INSERT INTO plc_scalar_multi_src VALUES (5),(NULL)")
      refreshChain("plc_scalar_multi_up", "plc_scalar_multi_down")

      assertMvCorrect(
        "plc_scalar_multi_up",
        "SELECT SUM(x) AS total, COUNT(*) AS row_count, COUNT(x) AS value_count FROM plc_scalar_multi_src"
      )
      assertMvCorrect(
        "plc_scalar_multi_down",
        "SELECT SUM(x) AS total, COUNT(*) AS row_count, COUNT(x) AS value_count FROM plc_scalar_multi_src"
      )
    }
  }
}
