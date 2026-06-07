package org.openivm.spark.parity

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Split from the original parity spec.  Scope:
  * Staging-catalog mechanics: DML produces staging rows, REFRESH consumes them, shared base tables retain rows until every MV has consumed, DROP-one-of-two MVs is safe, and `viewsForSource` indexes the catalog by source table.
  *
  * Includes sections: (2), (3), (4), (6), (9).
  */
class MetadataStagingCatalogSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-meta-stg-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-MetadataStagingCatalogSpec")
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

  /** Look up an MV by short name. */
  private def lookup(name: String) =
    MvCatalog.lookup(spark, TableIdentifier(name))

  /** Spark may normalize warehouse paths to `file:/…` URIs. Compare and
    * `new File(...)` constructions need the URI scheme stripped so they match
    * the raw path on disk.
    */
  private def stripFileScheme(s: String): String =
    if (s.startsWith("file:")) s.stripPrefix("file:") else s

  /** A `java.io.File` rooted at a (possibly `file:` URI) path. */
  private def fileOf(path: String): File = new File(stripFileScheme(path))

  /** Returns the qualified (db.table) name of `tableName` as seen by the DML
    * interceptor — i.e. the same identifier shape used in
    * `MvMetadata.sourceTables`.
    */
  private def qualName(tableName: String): String = {
    import org.apache.spark.sql.execution.datasources.LogicalRelation
    val analyzed = spark.sql(s"SELECT * FROM $tableName").queryExecution.analyzed
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

    it("INSERT INTO a base table writes one StagingDelta row with op_type=INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS t3_stg2(id INT, val INT) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv3_stg2 AS SELECT id, val FROM t3_stg2"
      )

      // Use a fresh, non-consumed view tag so collectFor returns ALL rows for
      // this base.
      val qn     = qualName("t3_stg2")
      val before = StagingCatalog.collectFor(spark, "__probe_m2_a__", Seq(qn))
      before shouldBe empty

      spark.sql("INSERT INTO t3_stg2 VALUES (1, 100), (2, 200)")
      val after = StagingCatalog.collectFor(spark, "__probe_m2_b__", Seq(qn))
      after should not be empty
      after.head.opType shouldBe "INSERT"
      after.head.baseTable shouldBe qn
      after.head.stagingPath should not be empty
    }

    it("DELETE writes a DELETE staging row containing the deleted rows") {
      val qn = qualName("t3_stg2")
      spark.sql("DELETE FROM t3_stg2 WHERE id = 1")
      val delete = StagingCatalog
        .collectFor(spark, "__probe_m2_c__", Seq(qn))
        .filter(_.opType == "DELETE")
      delete should not be empty
      val deletedRows = spark.read.format("delta").load(delete.head.stagingPath).collect()
      deletedRows.map(_.getAs[Int]("id")).toSet should contain(1)
    }

    it("UPDATE writes both UPDATE_BEFORE and UPDATE_AFTER staging rows") {
      val qn = qualName("t3_stg2")
      spark.sql("UPDATE t3_stg2 SET val = 999 WHERE id = 2")
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

    it("after REFRESH for the only MV, fully-consumed staging rows are pruned") {
      spark.sql("CREATE TABLE IF NOT EXISTS t4_stg3(id INT, val INT) USING DELTA")
      spark.sql("INSERT INTO t4_stg3 VALUES (1, 1)")
      spark.sql("CREATE MATERIALIZED VIEW mv4_stg3 AS SELECT id, val FROM t4_stg3")
      spark.sql("INSERT INTO t4_stg3 VALUES (2, 2), (3, 3)")
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

    it("when two MVs share a base, INSERT staging is retained until BOTH refresh") {
      spark.sql("CREATE TABLE IF NOT EXISTS t5_stg4(id INT, grp STRING, val INT) USING DELTA")
      spark.sql("INSERT INTO t5_stg4 VALUES (1,'a',10),(2,'b',20)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv5a_stg4 AS SELECT grp, SUM(val) AS s FROM t5_stg4 GROUP BY grp"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv5b_stg4 AS SELECT grp, COUNT(val) AS c FROM t5_stg4 GROUP BY grp"
      )
      val qn = qualName("t5_stg4")

      // Pre-baseline: pump in a single new row.
      spark.sql("INSERT INTO t5_stg4 VALUES (3,'a',5)")
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
      val mv5a = spark.sql("SELECT grp, s FROM mv5a_stg4 ORDER BY grp").collect()
      mv5a.find(_.getAs[String]("grp") == "a").get.getAs[Long]("s") shouldBe 15L
      val mv5b = spark.sql("SELECT grp, c FROM mv5b_stg4 ORDER BY grp").collect()
      mv5b.find(_.getAs[String]("grp") == "a").get.getAs[Long]("c") shouldBe 2L
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (6) DROP one MV that shares a base — the other MV's catalog row,
  //     delta tracking, and refresh path remain intact.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(6) Drop one of two shared-source MVs — surviving MV unaffected") {

    it("dropping mv5a_stg4 leaves mv5b_stg4 fully operational, including REFRESH") {
      // mv5a_stg4 and mv5b_stg4 were created in test (4).
      lookup("mv5a_stg4").isDefined shouldBe true
      lookup("mv5b_stg4").isDefined shouldBe true

      spark.sql("DROP MATERIALIZED VIEW mv5a_stg4")
      lookup("mv5a_stg4") shouldBe None
      lookup("mv5b_stg4") should not be None

      // Subsequent DML + refresh on the survivor must still work end-to-end.
      spark.sql("INSERT INTO t5_stg4 VALUES (4, 'a', 7)")
      refreshMv("mv5b_stg4")
      val rows = spark.sql("SELECT grp, c FROM mv5b_stg4 ORDER BY grp").collect()
      rows.find(_.getAs[String]("grp") == "a").get.getAs[Long]("c") shouldBe 3L
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (9) MvCatalog.viewsForSource finds MVs that depend on a given base table.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(9) MvCatalog.viewsForSource indexes by source_tables") {

    it("returns every MV whose source_tables contains the queried table") {
      spark.sql("CREATE TABLE IF NOT EXISTS t9_stg9(id INT, val INT) USING DELTA")
      spark.sql("CREATE MATERIALIZED VIEW mv9a_stg9 AS SELECT id, val FROM t9_stg9")
      spark.sql("CREATE MATERIALIZED VIEW mv9b_stg9 AS SELECT SUM(val) AS total FROM t9_stg9")
      val qn  = qualName("t9_stg9")
      val mvs = MvCatalog.viewsForSource(spark, qn).map(_.name.table).toSet
      mvs should contain allOf ("mv9a_stg9", "mv9b_stg9")
    }

    it("returns empty for a base table no MV depends on") {
      spark.sql("CREATE TABLE IF NOT EXISTS untracked_stg9(id INT) USING DELTA")
      val qn  = qualName("untracked_stg9")
      val mvs = MvCatalog.viewsForSource(spark, qn)
      mvs shouldBe empty
    }
  }
}
