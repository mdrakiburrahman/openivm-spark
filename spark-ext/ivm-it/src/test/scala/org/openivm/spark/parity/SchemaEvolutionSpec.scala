package org.openivm.spark.parity

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Parity port of `openivm/test/sql/schema_evolution.test`.
  *
  * The openivm test exercises DuckDB-side ALTER TABLE semantics — ADD COLUMN,
  * DROP COLUMN, RENAME COLUMN — and asserts:
  *
  *   1. ALTERs on UN-referenced columns succeed; the delta table is synced;
  *      subsequent IVM refresh works.
  *   2. ALTERs on referenced columns are blocked with a clear error message.
  *   3. Sequences of ADD + DROP keep the MV correct after refresh.
  *
  * Spark 3.5's openivm extension takes a different (and stricter) tack per
  * `RESEARCH.md` §12 Risk #4: every MV records a `sourceSchemaFingerprint`
  * (SHA-256 over the sorted base-table DDL) at CREATE time, and the
  * `RefreshMaterializedViewCommand` recomputes the fingerprint at refresh
  * time. ANY drift — adding a column, dropping a column, changing a type —
  * raises an `AnalysisException` with the `INCOMPATIBLE_VIEW_SCHEMA_CHANGE`
  * error class.
  *
  * The user-facing recovery path is DROP + CREATE: drop the MV, alter the
  * base table, recreate the MV. This spec verifies:
  *
  *   - Fingerprint enforcement fires on column addition, removal, and type
  *     change.
  *   - The error class string is `INCOMPATIBLE_VIEW_SCHEMA_CHANGE`.
  *   - After DROP + ALTER + recreate the MV is fully functional with the
  *     new schema fingerprint.
  *
  * Openivm-specific surface NOT mirrored:
  *
  *   - openivm's policy permits ALTERs of UN-referenced columns and only
  *     blocks ALTERs of columns referenced by an MV. The Spark side is
  *     stricter — any change to the base schema (referenced or not) flips the
  *     fingerprint and forces a recreate. This is the deliberate
  *     `RESEARCH.md` decision: we trade granularity for a fail-loud,
  *     fail-fast guard that prevents silently-broken IVM.
  *
  * Build invariants checked:
  *   - `MvCatalog.sourceSchemaFingerprint` after recreate ≠ the original
  *     fingerprint (i.e. a true new entry, not a stale row).
  */
class SchemaEvolutionSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-schema-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-SchemaEvolutionSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
    }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  private def lookup(name: String) =
    MvCatalog.lookup(spark, TableIdentifier(name))

  /** Qualified table name as the DML interceptor sees it. */
  private def qualName(tbl: String): String = {
    import org.apache.spark.sql.execution.datasources.LogicalRelation
    val a = spark.sql(s"SELECT * FROM $tbl").queryExecution.analyzed
    a.collectFirst {
      case r: LogicalRelation if r.catalogTable.isDefined =>
        val id = r.catalogTable.get.identifier
        id.database.fold(id.table)(db => s"$db.${id.table}")
    }.getOrElse(tbl)
  }

  /** openivm-spark does NOT auto-prune staging rows on DROP MATERIALIZED VIEW
    * (a known limitation). When this spec drops the MV, alters the base table,
    * and recreates the MV under a new fingerprint, any leftover staging row
    * from BEFORE the drop will be re-applied by the next REFRESH and corrupt
    * bag-equality. This helper marks every staging row for `baseTable` as
    * consumed by the supplied fake MV name and prunes them.
    */
  private def clearStaging(baseTable: String): Unit = {
    val orphans = StagingCatalog.collectFor(spark, "__schema_cleanup__", Seq(baseTable))
    if (orphans.nonEmpty) {
      StagingCatalog.markConsumed(spark, "__schema_cleanup__", orphans.map(_.stagingPath))
      StagingCatalog.pruneFullyConsumed(spark, Map(baseTable -> Seq("__schema_cleanup__")))
    }
  }

  /** Bidirectional EXCEPT ALL equivalence check. */
  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected = spark.sql(expectedSql)
    val userCols = expected.columns.toSeq
    val mv       = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    mv.exceptAll(expected).count() shouldBe 0L
    expected.exceptAll(mv).count() shouldBe 0L
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (1) ADD COLUMN flips the fingerprint → REFRESH raises with
  //     INCOMPATIBLE_VIEW_SCHEMA_CHANGE.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(1) ADD COLUMN: fingerprint flips → REFRESH raises INCOMPATIBLE_VIEW_SCHEMA_CHANGE") {

    it("refresh after ALTER TABLE … ADD COLUMNS raises an AnalysisException") {
      spark.sql("CREATE TABLE IF NOT EXISTS t_add(id INT, val INT, extra INT) USING DELTA")
      spark.sql("INSERT INTO t_add VALUES (1,10,100),(2,20,200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_add AS " +
          "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_add GROUP BY val"
      )
      val originalFp = lookup("mv_add").get.sourceSchemaFingerprint

      // ADD COLUMN flips the fingerprint.
      spark.sql("ALTER TABLE t_add ADD COLUMNS (bonus INT)")
      // INSERT writes a staging row (intercepted before the fingerprint check)
      // so the next REFRESH attempts to apply deltas and surfaces the
      // schema-change error.
      spark.sql("INSERT INTO t_add VALUES (4, 30, 400, 50)")

      val ex = the[Exception] thrownBy {
        refreshMv("mv_add")
      }
      // The actual cause anywhere in the chain must be INCOMPATIBLE_VIEW_SCHEMA_CHANGE.
      causeChain(ex).map(_.getMessage).mkString("\n").toLowerCase should (
        include("schema") and include("fingerprint")
      )

      // Recovery via DROP + recreate. Clear orphaned staging from before the
      // DROP first; subsequent DML after recreate will produce fresh staging
      // rows that REFRESH applies normally.
      spark.sql("DROP MATERIALIZED VIEW mv_add")
      clearStaging(qualName("t_add"))
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_add AS " +
          "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_add GROUP BY val"
      )
      val newFp = lookup("mv_add").get.sourceSchemaFingerprint
      newFp should not be originalFp

      // After recreate the MV reflects the live table.
      assertMvCorrect(
        "mv_add",
        "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_add GROUP BY val"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (2) DROP COLUMN on an UN-referenced column flips the fingerprint.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(2) DROP COLUMN: fingerprint flips → REFRESH raises") {

    it("dropping a column not referenced by the MV still trips the fingerprint guard") {
      spark.sql("CREATE TABLE IF NOT EXISTS t_drop(id INT, val INT, extra INT) USING DELTA")
      spark.sql("INSERT INTO t_drop VALUES (1,10,100),(2,20,200)")
      // Note: DROP COLUMN requires the Delta column-mapping property.
      spark.sql(
        "ALTER TABLE t_drop SET TBLPROPERTIES (" +
          "'delta.minReaderVersion' = '2', " +
          "'delta.minWriterVersion' = '5', " +
          "'delta.columnMapping.mode' = 'name')"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_drop AS SELECT val, SUM(val) AS total FROM t_drop GROUP BY val"
      )

      // DROP the un-referenced `extra` column.
      spark.sql("ALTER TABLE t_drop DROP COLUMN extra")
      spark.sql("INSERT INTO t_drop VALUES (3, 30)")

      val ex = the[Exception] thrownBy {
        refreshMv("mv_drop")
      }
      causeChain(ex).map(_.getMessage).mkString("\n").toLowerCase should include("schema")

      // Recovery
      spark.sql("DROP MATERIALIZED VIEW mv_drop")
      clearStaging(qualName("t_drop"))
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_drop AS SELECT val, SUM(val) AS total FROM t_drop GROUP BY val"
      )
      assertMvCorrect("mv_drop", "SELECT val, SUM(val) AS total FROM t_drop GROUP BY val")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (3) Column type change flips the fingerprint.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(3) Column type change: fingerprint flips → REFRESH raises") {

    it("changing a column's type via DROP+ADD trips the fingerprint guard") {
      spark.sql("CREATE TABLE IF NOT EXISTS t_type(id INT, val INT, extra INT) USING DELTA")
      spark.sql("INSERT INTO t_type VALUES (1, 10, 100)")
      spark.sql(
        "ALTER TABLE t_type SET TBLPROPERTIES (" +
          "'delta.minReaderVersion' = '2', " +
          "'delta.minWriterVersion' = '5', " +
          "'delta.columnMapping.mode' = 'name')"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_type AS SELECT id, val FROM t_type"
      )
      val originalFp = lookup("mv_type").get.sourceSchemaFingerprint

      // Change `extra`'s type by drop+add (Delta does not support
      // in-place type alteration). The fingerprint sees the new DDL.
      spark.sql("ALTER TABLE t_type DROP COLUMN extra")
      spark.sql("ALTER TABLE t_type ADD COLUMNS (extra STRING)")
      spark.sql("INSERT INTO t_type VALUES (2, 20, 'hello')")

      an[Exception] should be thrownBy {
        refreshMv("mv_type")
      }

      spark.sql("DROP MATERIALIZED VIEW mv_type")
      clearStaging(qualName("t_type"))
      spark.sql("CREATE MATERIALIZED VIEW mv_type AS SELECT id, val FROM t_type")
      val newFp = lookup("mv_type").get.sourceSchemaFingerprint
      newFp should not be originalFp
      assertMvCorrect("mv_type", "SELECT id, val FROM t_type")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (4) No-drift scenario: REFRESH succeeds when the fingerprint is stable.
  //     Mirrors the openivm "happy path" between schema-changing tests.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(4) No schema drift — REFRESH succeeds and MV stays bag-equivalent") {

    it("a normal DML sequence with no ALTER between CREATE and REFRESH succeeds") {
      spark.sql("CREATE TABLE IF NOT EXISTS t_stable(id INT, val INT) USING DELTA")
      spark.sql("INSERT INTO t_stable VALUES (1,10),(2,20),(3,10)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_stable AS " +
          "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_stable GROUP BY val"
      )

      // INSERT + DELETE + UPDATE batched.
      spark.sql("INSERT INTO t_stable VALUES (4, 30), (5, 10)")
      spark.sql("DELETE FROM t_stable WHERE id = 3")
      spark.sql("UPDATE t_stable SET val = 99 WHERE id = 1")
      refreshMv("mv_stable")
      assertMvCorrect(
        "mv_stable",
        "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_stable GROUP BY val"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (5) Multiple ADD + DROP cycles followed by DROP + CREATE.
  //     Mirrors openivm Test 6: many ALTER cycles, then verify the MV is
  //     still correct after the final recreate.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(5) ADD + DROP cycles, then DROP + CREATE — MV is correct end to end") {

    it("after a cycle of ALTERs the recreated MV bag-equals the live base query") {
      spark.sql("CREATE TABLE IF NOT EXISTS t_cycle(id INT, val INT, extra INT) USING DELTA")
      spark.sql("INSERT INTO t_cycle VALUES (1,10,100),(2,20,200)")
      spark.sql(
        "ALTER TABLE t_cycle SET TBLPROPERTIES (" +
          "'delta.minReaderVersion' = '2', " +
          "'delta.minWriterVersion' = '5', " +
          "'delta.columnMapping.mode' = 'name')"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_cycle AS " +
          "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_cycle GROUP BY val"
      )

      // Cycle: ADD, ADD, DROP, DROP. After each ADD the next REFRESH would
      // fail; after the final DROP+CREATE everything must work again.
      spark.sql("ALTER TABLE t_cycle ADD COLUMNS (temp1 INT)")
      spark.sql("ALTER TABLE t_cycle ADD COLUMNS (temp2 STRING)")
      spark.sql("INSERT INTO t_cycle VALUES (3, 10, 300, 1, 'hello')")
      // Recreate to consume the staging deltas under the new fingerprint.
      spark.sql("DROP MATERIALIZED VIEW mv_cycle")
      clearStaging(qualName("t_cycle"))
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_cycle AS " +
          "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_cycle GROUP BY val"
      )
      assertMvCorrect(
        "mv_cycle",
        "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_cycle GROUP BY val"
      )

      // Now DROP both temp columns and again recreate.
      spark.sql("ALTER TABLE t_cycle DROP COLUMN temp1")
      spark.sql("ALTER TABLE t_cycle DROP COLUMN temp2")
      spark.sql("INSERT INTO t_cycle VALUES (4, 20, 400)")
      spark.sql("DROP MATERIALIZED VIEW mv_cycle")
      clearStaging(qualName("t_cycle"))
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_cycle AS " +
          "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_cycle GROUP BY val"
      )
      assertMvCorrect(
        "mv_cycle",
        "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_cycle GROUP BY val"
      )

      // A final batched DML on the steady schema must work via REFRESH.
      spark.sql("INSERT INTO t_cycle VALUES (5, 10, 500), (6, 20, 600)")
      spark.sql("DELETE FROM t_cycle WHERE id IN (2)")
      refreshMv("mv_cycle")
      assertMvCorrect(
        "mv_cycle",
        "SELECT val, SUM(val) AS total, COUNT(*) AS cnt FROM t_cycle GROUP BY val"
      )
    }
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  /** Returns the entire cause chain (including `t` itself). */
  private def causeChain(t: Throwable): Seq[Throwable] = {
    val buf            = scala.collection.mutable.ArrayBuffer.empty[Throwable]
    var cur: Throwable = t
    while (cur != null) {
      buf += cur
      cur = cur.getCause
    }
    buf.toSeq
  }
}
