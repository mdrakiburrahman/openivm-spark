package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, MvMetadata, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

/** Regression coverage for materialized views whose sources are pinned to a
  * Delta snapshot (`FROM t VERSION AS OF <v>`).
  *
  * Spark accepts and executes the pin, but the OpenIVM compile bridge used to
  * forward that Spark/Delta syntax verbatim to DuckDB, which rejects it
  * (`Parser Error: syntax error at or near "as"`). The view was then recorded
  * as `compile_refresh_type=COMPILE_FAILED` /
  * `effective_refresh_type=FULL_REFRESH`, so EVERY refresh — including refreshes
  * with an empty delta — re-executed the whole view body.
  *
  * The pin is a storage concern with no meaning for the row-less DuckDB tables
  * the bridge registers, so it is split out of the compile-bridge copy of the
  * body only, and re-applied to every source read Spark executes. That makes a
  * pinned source a FROZEN relation: post-pin DML is consumed but never applied.
  */
abstract class TimeTravelPinnedSourceScenarios extends IvmParitySpecBase("time-travel-pin") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvMeta(name: String): MvMetadata = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not in catalog"))
  }

  protected def latestVersion(table: String): Long =
    spark.sql(s"DESCRIBE HISTORY $table").selectExpr("max(version)").head().getLong(0)

  protected def assertNotCompileFailed(mv: String): Unit = {
    val meta = mvMeta(mv)
    withClue(s"$mv compile refresh type: ") {
      meta.properties.getOrElse(MvMetadata.CompileRefreshTypeKey, "") should not be "COMPILE_FAILED"
    }
    withClue(s"$mv refresh reason: ") {
      meta.properties.getOrElse(MvMetadata.RefreshReasonKey, "") should not be "compile_failed"
    }
    withClue(s"$mv effective refresh type (${meta.refreshTypeName}): ") {
      meta.refreshType should not be RefreshTypeCode.FullRefresh
    }
  }

  /** `<source>`, `<mv>`, pinned version. The source holds `(1,'a',10)` and
    * `(2,'b',20)` at the pinned version plus a post-pin row `(3,'a',5)` that
    * must never reach the view.
    */
  protected def pinnedAggregateFixture(suffix: String): (String, String, Long) = {
    val src = s"ttp_src_$suffix"
    val mv  = s"ttp_mv_$suffix"
    sql(s"CREATE TABLE IF NOT EXISTS $src(id INT, grp STRING, val INT) USING DELTA")
    sql(s"INSERT INTO $src VALUES (1, 'a', 10), (2, 'b', 20)")
    val pinned = latestVersion(src)
    sql(s"INSERT INTO $src VALUES (3, 'a', 5)")
    sql(
      s"CREATE MATERIALIZED VIEW $mv AS SELECT grp, SUM(val) AS total, COUNT(*) AS cnt " +
        s"FROM $src VERSION AS OF $pinned GROUP BY grp"
    )
    (src, mv, pinned)
  }

  protected def pinnedExpectation(src: String, pinned: Long): String =
    s"SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM $src VERSION AS OF $pinned GROUP BY grp"

  // ── §1: A pinned source must not demote the view to FULL_REFRESH ──────────

  describe("(TTP-1) CREATE on a version-pinned source") {
    it("classifies the view incrementally instead of recording COMPILE_FAILED") {
      val (_, mv, _) = pinnedAggregateFixture("cls")
      assertNotCompileFailed(mv)
      mvMeta(mv).refreshTypeName shouldBe "AGGREGATE_GROUP"
    }

    it("stores the user's view body with the pin intact") {
      val (_, mv, pinned) = pinnedAggregateFixture("sql")
      mvMeta(mv).querySql should include(s"VERSION AS OF $pinned")
    }

    it("loads the pinned snapshot, not the live table") {
      val (src, mv, pinned) = pinnedAggregateFixture("load")
      assertMvCorrect(mv, pinnedExpectation(src, pinned))
      spark.table(mv).selectExpr("SUM(cnt)").head().getLong(0) shouldBe 2L
      spark.sql(s"SELECT COUNT(*) FROM $src").head().getLong(0) shouldBe 3L
    }
  }

  // ── §2: A pinned source is frozen across refreshes ────────────────────────

  describe("(TTP-2) REFRESH of a version-pinned source") {
    it("does not apply DML committed after the pinned version") {
      val (src, mv, pinned) = pinnedAggregateFixture("frozen")
      sql(s"INSERT INTO $src VALUES (4, 'b', 400), (5, 'c', 500)")
      refreshMv(mv)
      assertMvCorrect(mv, pinnedExpectation(src, pinned))
      assertNotCompileFailed(mv)
    }

    it("does not rewrite the view when only frozen deltas are pending") {
      val (src, mv, _) = pinnedAggregateFixture("noop")
      val before       = mvMeta(mv).lastVersion
      sql(s"INSERT INTO $src VALUES (6, 'a', 600)")
      refreshMv(mv)
      mvMeta(mv).lastVersion shouldBe before
    }

    it("stays a no-op when nothing changed at all") {
      val (src, mv, pinned) = pinnedAggregateFixture("empty")
      val before            = mvMeta(mv).lastVersion
      refreshMv(mv)
      refreshMv(mv)
      mvMeta(mv).lastVersion shouldBe before
      assertMvCorrect(mv, pinnedExpectation(src, pinned))
    }

    it("consumes frozen deltas so repeated refreshes stay no-ops") {
      val (src, mv, pinned) = pinnedAggregateFixture("consume")
      sql(s"INSERT INTO $src VALUES (7, 'a', 700)")
      refreshMv(mv)
      val after = mvMeta(mv).lastVersion
      sql(s"DELETE FROM $src WHERE id = 7")
      refreshMv(mv)
      mvMeta(mv).lastVersion shouldBe after
      assertMvCorrect(mv, pinnedExpectation(src, pinned))
    }
  }

  // ── §3: Mixed pinned + live sources ───────────────────────────────────────

  describe("(TTP-3) A join of a pinned dimension and a live fact") {
    it("keeps the dimension frozen while incrementally maintaining the fact") {
      sql("CREATE TABLE IF NOT EXISTS ttp_dim(dim_id INT, region STRING) USING DELTA")
      sql("INSERT INTO ttp_dim VALUES (1, 'east'), (2, 'west')")
      val pinned = latestVersion("ttp_dim")
      sql("INSERT INTO ttp_dim VALUES (3, 'north')")

      sql("CREATE TABLE IF NOT EXISTS ttp_fact(fact_id INT, dim_id INT, amount INT) USING DELTA")
      sql("INSERT INTO ttp_fact VALUES (1, 1, 100), (2, 2, 200)")

      sql(
        s"CREATE MATERIALIZED VIEW ttp_mv_join AS SELECT d.region, f.amount " +
          s"FROM ttp_dim VERSION AS OF $pinned d JOIN ttp_fact f ON f.dim_id = d.dim_id"
      )
      assertNotCompileFailed("ttp_mv_join")

      val expected =
        s"SELECT d.region, f.amount FROM ttp_dim VERSION AS OF $pinned d JOIN ttp_fact f ON f.dim_id = d.dim_id"
      assertMvCorrect("ttp_mv_join", expected)

      // Fact rows joining the pinned dimension must flow in; a fact row that
      // only matches a post-pin dimension row must not.
      sql("INSERT INTO ttp_fact VALUES (3, 1, 50), (4, 3, 999)")
      refreshMv("ttp_mv_join")
      assertMvCorrect("ttp_mv_join", expected)
      spark.table("ttp_mv_join").where("amount = 999").count() shouldBe 0L
    }
  }

  // ── §4: Unpinned views keep their existing behaviour ──────────────────────

  describe("(TTP-4) An unpinned view is unaffected") {
    it("still refreshes incrementally and tracks live rows") {
      sql("CREATE TABLE IF NOT EXISTS ttp_live_src(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO ttp_live_src VALUES (1, 'a', 10), (2, 'b', 20)")
      sql(
        "CREATE MATERIALIZED VIEW ttp_mv_live AS SELECT grp, SUM(val) AS total, COUNT(*) AS cnt " +
          "FROM ttp_live_src GROUP BY grp"
      )
      assertNotCompileFailed("ttp_mv_live")
      mvMeta("ttp_mv_live").querySql.toUpperCase should not include "VERSION AS OF"

      sql("INSERT INTO ttp_live_src VALUES (3, 'a', 5)")
      refreshMv("ttp_mv_live")
      assertMvCorrect(
        "ttp_mv_live",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM ttp_live_src GROUP BY grp"
      )
    }
  }
}
