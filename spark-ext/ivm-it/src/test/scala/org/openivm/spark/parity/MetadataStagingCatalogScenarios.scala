package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{MvCatalog, StagingCatalog}

import java.io.File

/** Split from the original parity spec.  Scope:
  * Staging-catalog mechanics: DML produces staging rows, REFRESH consumes them, shared base tables retain rows until every MV has consumed, DROP-one-of-two MVs is safe, and `viewsForSource` indexes the catalog by source table.
  *
  * Includes sections: (2), (3), (4), (6), (9).
  */
abstract class MetadataStagingCatalogScenarios extends IvmParitySpecBase("metadata-staging-catalog") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Look up an MV by short name. */
  protected def lookup(name: String) =
    MvCatalog.lookup(spark, TableIdentifier(name))

  /** Spark may normalize warehouse paths to `file:/…` URIs. Compare and
    * `new File(...)` constructions need the URI scheme stripped so they match
    * the raw path on disk.
    */
  protected def stripFileScheme(s: String): String =
    if (s.startsWith("file:")) s.stripPrefix("file:") else s

  /** A `java.io.File` rooted at a (possibly `file:` URI) path. */
  protected def fileOf(path: String): File = new File(stripFileScheme(path))

  /** Returns the qualified (db.table) name of `tableName` as seen by the DML
    * interceptor — i.e. the same identifier shape used in
    * `MvMetadata.sourceTables`.
    */
  protected def qualName(tableName: String): String = {
    import org.apache.spark.sql.execution.datasources.LogicalRelation
    val analyzed = sql(s"SELECT * FROM $tableName").queryExecution.analyzed
    analyzed
      .collectFirst {
        case r: LogicalRelation if r.catalogTable.isDefined =>
          val id = r.catalogTable.get.identifier
          id.database.fold(id.table)(db => s"$db.${id.table}")
      }
      .getOrElse(tableName)
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (2) DML on a base table writes a staging-catalog row
  // ──────────────────────────────────────────────────────────────────────────
  describe("(2) StagingCatalog state after INSERT on a tracked base table") {

    itIntercept("INSERT INTO a base table writes one StagingDelta row with op_type=INSERT") {
      sql("CREATE TABLE IF NOT EXISTS t3_stg2(id INT, val INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW mv3_stg2 AS SELECT id, val FROM t3_stg2"
      )

      // Use a fresh, non-consumed view tag so collectFor returns ALL rows for
      // this base.
      val qn     = qualName("t3_stg2")
      val before = StagingCatalog.collectFor(spark, "__probe_m2_a__", Seq(qn))
      before shouldBe empty

      sql("INSERT INTO t3_stg2 VALUES (1, 100), (2, 200)")
      val after = StagingCatalog.collectFor(spark, "__probe_m2_b__", Seq(qn))
      after should not be empty
      after.head.opType shouldBe "INSERT"
      after.head.baseTable shouldBe qn
      after.head.stagingPath should not be empty
    }

    itIntercept("DELETE writes a DELETE staging row containing the deleted rows") {
      val qn = qualName("t3_stg2")
      sql("DELETE FROM t3_stg2 WHERE id = 1")
      val delete = StagingCatalog
        .collectFor(spark, "__probe_m2_c__", Seq(qn))
        .filter(_.opType == "DELETE")
      delete should not be empty
      val deletedRows = spark.read.format("delta").load(delete.head.stagingPath).collect()
      deletedRows.map(_.getAs[Int]("id")).toSet should contain(1)
    }

    itIntercept("UPDATE writes both UPDATE_BEFORE and UPDATE_AFTER staging rows") {
      val qn = qualName("t3_stg2")
      sql("UPDATE t3_stg2 SET val = 999 WHERE id = 2")
      val staging = StagingCatalog.collectFor(spark, "__probe_m2_d__", Seq(qn))
      staging.exists(_.opType == "UPDATE_BEFORE") shouldBe true
      staging.exists(_.opType == "UPDATE_AFTER") shouldBe true
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (3) After REFRESH the consumed staging rows for that MV are marked
  //     (consumed_by ⊇ {mvName}) and pruned when no other MV depends on the
  //     source table.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(3) REFRESH consumes pending staging deltas and prunes them") {

    itIntercept("after REFRESH for the only MV, fully-consumed staging rows are pruned") {
      sql("CREATE TABLE IF NOT EXISTS t4_stg3(id INT, val INT) USING DELTA")
      sql("INSERT INTO t4_stg3 VALUES (1, 1)")
      sql("CREATE MATERIALIZED VIEW mv4_stg3 AS SELECT id, val FROM t4_stg3")
      sql("INSERT INTO t4_stg3 VALUES (2, 2), (3, 3)")
      val qn = qualName("t4_stg3")
      // Before refresh: at least one staging row.
      StagingCatalog.collectFor(spark, "__probe_m3_a__", Seq(qn)) should not be empty

      refreshMv("mv4_stg3")
      // After refresh: every staging row's consumed_by ⊇ {mv4_stg3}; since mv4_stg3
      // is the only MV for t4_stg3, those rows must be pruned by pruneFullyConsumed.
      StagingCatalog.collectFor(spark, "__probe_m3_b__", Seq(qn)) shouldBe empty
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (4) Shared base table — delta retention until all dependent MVs consume
  // ──────────────────────────────────────────────────────────────────────────
  describe("(4) Shared base table — staging retention across multiple MVs") {

    itIntercept("when two MVs share a base, INSERT staging is retained until BOTH refresh") {
      sql("CREATE TABLE IF NOT EXISTS t5_stg4(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO t5_stg4 VALUES (1,'a',10),(2,'b',20)")
      sql(
        "CREATE MATERIALIZED VIEW mv5a_stg4 AS SELECT grp, SUM(val) AS s FROM t5_stg4 GROUP BY grp"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv5b_stg4 AS SELECT grp, COUNT(val) AS c FROM t5_stg4 GROUP BY grp"
      )
      val qn = qualName("t5_stg4")

      // Pre-baseline: pump in a single new row.
      sql("INSERT INTO t5_stg4 VALUES (3,'a',5)")
      val pendingBeforeAnyRefresh =
        StagingCatalog.collectFor(spark, "__probe_m4_a__", Seq(qn))
      pendingBeforeAnyRefresh should not be empty

      // Refresh only mv5a_stg4 — the staging row must remain because mv5b_stg4
      // has not yet consumed it.
      refreshMv("mv5a_stg4")
      val pendingAfterFirstRefresh =
        StagingCatalog.collectFor(spark, "__probe_m4_b__", Seq(qn))
      pendingAfterFirstRefresh should not be empty

      // Refresh mv5b_stg4 — now both MVs have consumed, so the staging row is
      // eligible for pruning.
      refreshMv("mv5b_stg4")
      val pendingAfterBothRefresh =
        StagingCatalog.collectFor(spark, "__probe_m4_c__", Seq(qn))
      pendingAfterBothRefresh shouldBe empty

      // Sanity: both MVs reflect the new row.
      val mv5a = sql("SELECT grp, s FROM mv5a_stg4 ORDER BY grp").collect()
      mv5a.find(_.getAs[String]("grp") == "a").get.getAs[Long]("s") shouldBe 15L
      val mv5b = sql("SELECT grp, c FROM mv5b_stg4 ORDER BY grp").collect()
      mv5b.find(_.getAs[String]("grp") == "a").get.getAs[Long]("c") shouldBe 2L
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (6) DROP one MV that shares a base — the other MV's catalog row,
  //     delta tracking, and refresh path remain intact.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(6) Drop one of two shared-source MVs — surviving MV unaffected") {

    itIntercept("dropping mv5a_stg4 leaves mv5b_stg4 fully operational, including REFRESH") {
      // mv5a_stg4 and mv5b_stg4 were created in test (4).
      lookup("mv5a_stg4").isDefined shouldBe true
      lookup("mv5b_stg4").isDefined shouldBe true

      sql("DROP MATERIALIZED VIEW mv5a_stg4")
      lookup("mv5a_stg4") shouldBe None
      lookup("mv5b_stg4") should not be None

      // Subsequent DML + refresh on the survivor must still work end-to-end.
      sql("INSERT INTO t5_stg4 VALUES (4, 'a', 7)")
      refreshMv("mv5b_stg4")
      val rows = sql("SELECT grp, c FROM mv5b_stg4 ORDER BY grp").collect()
      rows.find(_.getAs[String]("grp") == "a").get.getAs[Long]("c") shouldBe 3L
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (9) MvCatalog.viewsForSource finds MVs that depend on a given base table.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(9) MvCatalog.viewsForSource indexes by source_tables") {

    itIntercept("returns every MV whose source_tables contains the queried table") {
      sql("CREATE TABLE IF NOT EXISTS t9_stg9(id INT, val INT) USING DELTA")
      sql("CREATE MATERIALIZED VIEW mv9a_stg9 AS SELECT id, val FROM t9_stg9")
      sql("CREATE MATERIALIZED VIEW mv9b_stg9 AS SELECT SUM(val) AS total FROM t9_stg9")
      val qn  = qualName("t9_stg9")
      val mvs = MvCatalog.viewsForSource(spark, qn).map(_.name.table).toSet
      mvs should contain allOf ("mv9a_stg9", "mv9b_stg9")
    }

    itIntercept("returns empty for a base table no MV depends on") {
      sql("CREATE TABLE IF NOT EXISTS untracked_stg9(id INT) USING DELTA")
      val qn  = qualName("untracked_stg9")
      val mvs = MvCatalog.viewsForSource(spark, qn)
      mvs shouldBe empty
    }
  }
}
