package org.openivm.spark.common

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Tests for [[SparkMergeAssembler]] and all four assembler implementations.
  *
  * No live SparkSession is required — assemblers emit SQL strings, so all assertions
  * are pure string-pattern checks.
  *
  * Coverage map (per task spec §"Tests (mandatory)"):
  *   1.  type 0  → MergeAssembler
  *   2.  type 1  → MergeAssembler (scalar / no-key form)
  *   3.  type 2  → MergeAssembler (rowid / signed form)
  *   4.  type 3  → FullRefreshAssembler
  *   5.  type 4  → MergeAssembler + post-pass DELETE
  *   6.  type 5  → AffectedGroupsAssembler
  *   7.  type 6  → AffectedGroupsAssembler
  *   8.  type 8  → MergeAssembler (count-monoid)
  *   9.  type 9  → AuxStateAssembler
  *   10. type 7  → UnsupportedOperationException ("never emitted by classifier")
  *   11. type 42 → UnsupportedOperationException naming the int
  *   12. each handled type: mvName backtick-quoted, relevant ON/VALUES, deltaSql substring
  *   13. multi-statement order: type 4 → exactly 2 statements (MERGE then DELETE)
  *   14. SQL injection: mvName with semicolon is backtick-escaped
  */
class SparkMergeAssemblerSpec extends AnyFunSpec with Matchers {

  // ── shared test fixtures ──────────────────────────────────────────────────

  private val stdDeltaSql   = "WITH refresh_cte AS (SELECT k, SUM(v) AS delta_v FROM __delta GROUP BY k)"
  private val stdMvName     = "mydb.myschema.myview"
  private val stdMvNameQtd  = "`mydb`.`myschema`.`myview`"
  private val stdMvLocation = "dbfs:/delta/myview"

  private def mkInput(
      refreshType: Int,
      refreshTypeName: String,
      deltaSql: String = stdDeltaSql,
      mvName: String = stdMvName,
      groupKeys: Seq[String] = Seq("k"),
      rowIdColumn: Option[String] = None,
      havingPredicate: Option[String] = None,
      partitionColumn: Option[String] = None,
      auxTable: Option[String] = None
  ): AssemblyInput =
    AssemblyInput(
      refreshType = refreshType,
      refreshTypeName = refreshTypeName,
      deltaSql = deltaSql,
      mvName = mvName,
      mvLocation = stdMvLocation,
      groupKeys = groupKeys,
      rowIdColumn = rowIdColumn,
      havingPredicate = havingPredicate,
      partitionColumn = partitionColumn,
      auxTable = auxTable
    )

  // ── dispatch tests (tests 1–9) ────────────────────────────────────────────

  describe("SparkMergeAssembler.assemble — dispatch") {

    it("(1) type 0 AGGREGATE_GROUP dispatches to MergeAssembler") {
      val result = SparkMergeAssembler.assemble(mkInput(0, "AGGREGATE_GROUP"))
      MergeAssembler.supports(0) shouldBe true
      result.statements should have size 1
    }

    it("(2) type 1 SIMPLE_AGGREGATE dispatches to MergeAssembler (scalar form)") {
      val result = SparkMergeAssembler.assemble(mkInput(1, "SIMPLE_AGGREGATE", groupKeys = Nil))
      MergeAssembler.supports(1) shouldBe true
      result.statements should have size 1
      // Scalar MV has no group keys → ON TRUE
      result.statements.head should include("TRUE")
    }

    it("(3) type 2 SIMPLE_PROJECTION dispatches to MergeAssembler (rowid / signed form)") {
      val deltaSql = "WITH refresh_cte AS (SELECT _ivm_rowid, a, b, _ivm_sign FROM __delta)"
      val result = SparkMergeAssembler.assemble(
        mkInput(2, "SIMPLE_PROJECTION", deltaSql = deltaSql, groupKeys = Nil, rowIdColumn = Some("_ivm_rowid"))
      )
      MergeAssembler.supports(2) shouldBe true
      result.statements should have size 1
      val sql = result.statements.head
      sql should include("_ivm_rowid")
      sql should include("`_ivm_sign`")
      sql should include("DELETE")
    }

    it("(4) type 3 FULL_REFRESH dispatches to FullRefreshAssembler") {
      val viewBody = "SELECT a, b FROM base_table WHERE active = true"
      val result   = SparkMergeAssembler.assemble(mkInput(3, "FULL_REFRESH", deltaSql = viewBody))
      FullRefreshAssembler.supports(3) shouldBe true
      result.statements should have size 1
      result.statements.head should include("INSERT OVERWRITE TABLE")
    }

    it("(5) type 4 AGGREGATE_HAVING dispatches to MergeAssembler with a post-pass DELETE") {
      val result = SparkMergeAssembler.assemble(
        mkInput(4, "AGGREGATE_HAVING", havingPredicate = Some("SUM(v) > 10"))
      )
      MergeAssembler.supports(4) shouldBe true
      // Exactly 2 statements: MERGE then DELETE (test 13)
      result.statements should have size 2
      result.statements.head should include("MERGE INTO")
      result.statements(1) should include("DELETE FROM")
      result.statements(1) should include("SUM(v) > 10")
    }

    it("(6) type 5 WINDOW_PARTITION dispatches to AffectedGroupsAssembler") {
      val result = SparkMergeAssembler.assemble(
        mkInput(5, "WINDOW_PARTITION", groupKeys = Nil, partitionColumn = Some("part_col"))
      )
      AffectedGroupsAssembler.supports(5) shouldBe true
      result.statements should have size 3
      result.statements.head should include("CREATE OR REPLACE TEMP VIEW affected_keys_")
      result.statements(1) should include("DELETE FROM")
      result.statements(2) should include("INSERT INTO")
    }

    it("(7) type 6 GROUP_RECOMPUTE dispatches to AffectedGroupsAssembler") {
      val result = SparkMergeAssembler.assemble(mkInput(6, "GROUP_RECOMPUTE"))
      AffectedGroupsAssembler.supports(6) shouldBe true
      result.statements should have size 3
      result.statements.head should include("affected_keys_")
    }

    it("(8) type 8 DISTINCT_INCREMENTAL dispatches to MergeAssembler (count-monoid)") {
      val result = SparkMergeAssembler.assemble(mkInput(8, "DISTINCT_INCREMENTAL"))
      MergeAssembler.supports(8) shouldBe true
      result.statements should have size 1
      result.statements.head should include("MERGE INTO")
    }

    it("(9) type 9 SEMI_ANTI_RECOMPUTE dispatches to AuxStateAssembler") {
      val result = SparkMergeAssembler.assemble(
        mkInput(9, "SEMI_ANTI_RECOMPUTE", auxTable = Some("mydb.myschema.myview_aux"))
      )
      AuxStateAssembler.supports(9) shouldBe true
      result.statements should not be empty
    }

    it("(10) type 7 TopK throws UnsupportedOperationException mentioning 'never emitted'") {
      val ex = intercept[UnsupportedOperationException] {
        SparkMergeAssembler.assemble(mkInput(7, "TopK"))
      }
      ex.getMessage should include("never emitted by the classifier")
    }

    it("(11) unknown type 42 throws UnsupportedOperationException naming the int") {
      val ex = intercept[UnsupportedOperationException] {
        SparkMergeAssembler.assemble(mkInput(42, "UNKNOWN"))
      }
      ex.getMessage should include("42")
    }
  }

  // ── test 12: mvName backtick-quoted + ON/VALUES clause + deltaSql substring ──

  describe("SparkMergeAssembler.assemble — SQL content (test 12)") {

    it("type 0: output contains backtick-quoted mvName, <=> ON clause, and deltaSql") {
      val result = SparkMergeAssembler.assemble(mkInput(0, "AGGREGATE_GROUP"))
      val sql    = result.statements.head
      sql should include(stdMvNameQtd)
      sql should include("<=>")
      sql should include(stdDeltaSql)
    }

    it("type 1: output contains backtick-quoted mvName, TRUE ON clause, and deltaSql") {
      val result = SparkMergeAssembler.assemble(mkInput(1, "SIMPLE_AGGREGATE", groupKeys = Nil))
      val sql    = result.statements.head
      sql should include(stdMvNameQtd)
      sql should include("TRUE")
      sql should include(stdDeltaSql)
    }

    it("type 2: output contains backtick-quoted mvName, rowId ON clause, and deltaSql") {
      val deltaSql = "WITH refresh_cte AS (SELECT _ivm_rowid, x, _ivm_sign FROM __delta)"
      val result = SparkMergeAssembler.assemble(
        mkInput(2, "SIMPLE_PROJECTION", deltaSql = deltaSql, groupKeys = Nil, rowIdColumn = Some("_ivm_rowid"))
      )
      val sql = result.statements.head
      sql should include(stdMvNameQtd)
      sql should include("`_ivm_rowid`")
      sql should include("`_ivm_sign`")
      sql should include(deltaSql)
    }

    it("type 3: output contains backtick-quoted mvName, INSERT OVERWRITE, and deltaSql") {
      val viewBody = "SELECT x, y FROM src WHERE active = true"
      val result   = SparkMergeAssembler.assemble(mkInput(3, "FULL_REFRESH", deltaSql = viewBody))
      val sql      = result.statements.head
      sql should include(stdMvNameQtd)
      sql should include("INSERT OVERWRITE TABLE")
      sql should include(viewBody)
    }

    it("type 4: MERGE statement contains mvName and deltaSql; DELETE statement contains mvName and predicate") {
      val result = SparkMergeAssembler.assemble(
        mkInput(4, "AGGREGATE_HAVING", havingPredicate = Some("COUNT(*) >= 5"))
      )
      result.statements.head should include(stdMvNameQtd)
      result.statements.head should include(stdDeltaSql)
      result.statements.head should include("<=>")
      result.statements(1) should include(stdMvNameQtd)
      result.statements(1) should include("COUNT(*) >= 5")
    }

    it("type 5: CREATE VIEW, DELETE, INSERT all reference deltaSql and backtick-quoted mvName") {
      val result = SparkMergeAssembler.assemble(
        mkInput(5, "WINDOW_PARTITION", groupKeys = Nil, partitionColumn = Some("part_col"))
      )
      result.statements.head should include(stdDeltaSql)
      result.statements(1) should include(stdMvNameQtd)
      result.statements(2) should include(stdMvNameQtd)
      result.statements(2) should include(stdDeltaSql)
    }

    it("type 6: output contains deltaSql and backtick-quoted mvName") {
      val result = SparkMergeAssembler.assemble(mkInput(6, "GROUP_RECOMPUTE"))
      result.statements.head should include(stdDeltaSql)
      result.statements(1) should include(stdMvNameQtd)
      result.statements(2) should include(stdMvNameQtd)
      result.statements(2) should include(stdDeltaSql)
    }

    it("type 8: output contains backtick-quoted mvName, <=> ON clause, and deltaSql") {
      val result = SparkMergeAssembler.assemble(mkInput(8, "DISTINCT_INCREMENTAL"))
      val sql    = result.statements.head
      sql should include(stdMvNameQtd)
      sql should include("<=>")
      sql should include(stdDeltaSql)
    }

    it("type 9: some statement contains backtick-quoted mvName; some statement contains deltaSql") {
      val result = SparkMergeAssembler.assemble(
        mkInput(9, "SEMI_ANTI_RECOMPUTE", auxTable = Some("mydb.myschema.myview_aux"))
      )
      result.statements.exists(_.contains(stdMvNameQtd)) shouldBe true
      result.statements.exists(_.contains(stdDeltaSql)) shouldBe true
    }
  }

  // ── test 13: multi-statement order ───────────────────────────────────────

  describe("AssembledRefresh — statement ordering") {

    it("(13) type 4 returns exactly 2 statements: MERGE first, DELETE second") {
      val result = SparkMergeAssembler.assemble(
        mkInput(4, "AGGREGATE_HAVING", havingPredicate = Some("v > 0"))
      )
      result.statements should have size 2
      result.statements.head should startWith regex "(?s).*MERGE INTO.*".r
      result.statements(1) should startWith("DELETE FROM")
    }

    it("asSingleStatement joins statements with semicolons and ends with a semicolon") {
      val r = AssembledRefresh(Seq("SELECT 1", "SELECT 2"))
      r.asSingleStatement shouldBe "SELECT 1;\nSELECT 2;"
    }
  }

  // ── test 14: SQL injection safety ────────────────────────────────────────

  describe("SQL injection safety (test 14)") {

    it("an mvName containing ';' is backtick-escaped in the output") {
      val injected = "x; DROP TABLE y --"
      val result   = SparkMergeAssembler.assemble(mkInput(3, "FULL_REFRESH", deltaSql = "SELECT 1", mvName = injected))
      val sql      = result.statements.head
      // The escaped form must appear
      sql should include("`x; DROP TABLE y --`")
    }

    it("an mvName containing backtick characters is double-backtick-escaped") {
      val tricky = "my`table"
      val result = SparkMergeAssembler.assemble(mkInput(3, "FULL_REFRESH", deltaSql = "SELECT 1", mvName = tricky))
      result.statements.head should include("`my``table`")
    }

    it("a dot-qualified mvName is quoted per segment, not as a whole string") {
      val result = SparkMergeAssembler.assemble(mkInput(0, "AGGREGATE_GROUP"))
      // Each segment must be individually quoted
      result.statements.head should include("`mydb`")
      result.statements.head should include("`myschema`")
      result.statements.head should include("`myview`")
    }
  }

  // ── assembler kind constants ──────────────────────────────────────────────

  describe("Assembler.kind") {
    it("MergeAssembler.kind is 'merge'") { MergeAssembler.kind shouldBe "merge" }
    it("AffectedGroupsAssembler.kind is 'affected-groups'") { AffectedGroupsAssembler.kind shouldBe "affected-groups" }
    it("AuxStateAssembler.kind is 'aux-state'") { AuxStateAssembler.kind shouldBe "aux-state" }
    it("FullRefreshAssembler.kind is 'full-refresh'") { FullRefreshAssembler.kind shouldBe "full-refresh" }
  }

  // ── RefreshTypeCode constants (smoke) ─────────────────────────────────────

  describe("RefreshTypeCode") {
    it("has the correct integer values per openivm_constants.hpp") {
      RefreshTypeCode.AggregateGroup shouldBe 0
      RefreshTypeCode.SimpleAggregate shouldBe 1
      RefreshTypeCode.SimpleProjection shouldBe 2
      RefreshTypeCode.FullRefresh shouldBe 3
      RefreshTypeCode.AggregateHaving shouldBe 4
      RefreshTypeCode.WindowPartition shouldBe 5
      RefreshTypeCode.GroupRecompute shouldBe 6
      RefreshTypeCode.TopK shouldBe 7
      RefreshTypeCode.DistinctIncremental shouldBe 8
      RefreshTypeCode.SemiAntiRecompute shouldBe 9
      RefreshTypeCode.CurrentDiffRecompute shouldBe 10
    }
  }

  // ── projection MERGE: sign-based WHEN clauses ─────────────────────────────

  describe("MergeAssembler — SIMPLE_PROJECTION sign clauses") {

    it("rowid-keyed MERGE has DELETE clause for sign=-1 and INSERT clause for sign=+1") {
      val deltaSql = "WITH refresh_cte AS (SELECT _ivm_rowid, c1, _ivm_sign FROM __delta)"
      val result = MergeAssembler.assemble(
        mkInput(2, "SIMPLE_PROJECTION", deltaSql = deltaSql, groupKeys = Nil)
      )
      val sql = result.statements.head
      sql should include("WHEN MATCHED AND d.`_ivm_sign` = -1 THEN DELETE")
      sql should include("WHEN MATCHED AND d.`_ivm_sign` = 1 THEN UPDATE SET *")
      sql should include("WHEN NOT MATCHED AND d.`_ivm_sign` = 1 THEN INSERT *")
    }

    it("uses the rowIdColumn from AssemblyInput when provided") {
      val result = MergeAssembler.assemble(
        mkInput(2, "SIMPLE_PROJECTION", groupKeys = Nil, rowIdColumn = Some("my_row_id"))
      )
      result.statements.head should include("`my_row_id`")
    }

    it("defaults rowIdColumn to _ivm_rowid when not provided") {
      val result = MergeAssembler.assemble(
        mkInput(2, "SIMPLE_PROJECTION", groupKeys = Nil, rowIdColumn = None)
      )
      result.statements.head should include("`_ivm_rowid`")
    }
  }

  // ── type 4 havingPredicate fallback ──────────────────────────────────────

  describe("MergeAssembler — AGGREGATE_HAVING predicate handling") {

    it("falls back to WHERE NOT (TRUE) when havingPredicate is None") {
      val result = MergeAssembler.assemble(mkInput(4, "AGGREGATE_HAVING", havingPredicate = None))
      result.statements(1) shouldBe s"DELETE FROM $stdMvNameQtd WHERE NOT (TRUE)"
    }
  }

  // ── AffectedGroupsAssembler key selection ─────────────────────────────────

  describe("AffectedGroupsAssembler — key selection") {

    it("uses partitionColumn as the key for WINDOW_PARTITION when groupKeys is empty") {
      val result = AffectedGroupsAssembler.assemble(
        mkInput(5, "WINDOW_PARTITION", groupKeys = Nil, partitionColumn = Some("region"))
      )
      result.statements.head should include("`region`")
      result.statements(1) should include("`region`")
    }

    it("uses groupKeys for GROUP_RECOMPUTE, ignoring partitionColumn") {
      val result = AffectedGroupsAssembler.assemble(
        mkInput(6, "GROUP_RECOMPUTE", groupKeys = Seq("dept", "yr"), partitionColumn = Some("ignored"))
      )
      result.statements.head should include("`dept`")
      result.statements.head should include("`yr`")
    }

    it("temp view names increment across calls, keeping them unique") {
      val r1    = AffectedGroupsAssembler.assemble(mkInput(6, "GROUP_RECOMPUTE"))
      val r2    = AffectedGroupsAssembler.assemble(mkInput(6, "GROUP_RECOMPUTE"))
      val name1 = r1.statements.head
      val name2 = r2.statements.head
      // Both start with the same prefix but end with different counters
      name1 should include("affected_keys_")
      name2 should include("affected_keys_")
      name1 should not equal name2
    }
  }
}
