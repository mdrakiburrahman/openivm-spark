package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

import java.io.File

/** Split from the original parity spec.  Scope:
  * MV-catalog CRUD: CREATE populates `mv_metadata`, DROP cleans up, and the catalog table can be re-created with the same name.
  *
  * Includes sections: (1), (5), (7).
  */
abstract class MetadataMvCatalogScenarios extends IvmParitySpecBase("metadata-mv-catalog") {
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
  // (1) MV catalog populated correctly after CREATE
  // ──────────────────────────────────────────────────────────────────────────
  describe("(1) MV catalog state after CREATE MATERIALIZED VIEW") {

    it("populates mv_metadata with name, refreshType, sourceTables, schema fingerprint") {
      sql("CREATE TABLE IF NOT EXISTS t1_mvc1(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO t1_mvc1 VALUES (1,'a',10), (2,'b',20)")
      sql(
        "CREATE MATERIALIZED VIEW mv1_mvc1 AS SELECT grp, sum(val) AS total FROM t1_mvc1 GROUP BY grp"
      )

      val meta = lookup("mv1_mvc1").getOrElse(fail("mv1_mvc1 not in MvCatalog"))
      meta.name.table shouldBe "mv1_mvc1"
      meta.refreshType shouldBe RefreshTypeCode.AggregateGroup
      meta.refreshTypeName shouldBe "AGGREGATE_GROUP"
      meta.sourceTables should contain(qualName("t1_mvc1"))
      meta.sourceSchemaFingerprint should not be empty
      stripFileScheme(meta.location) should startWith(warehouseDir)
      meta.querySql.toLowerCase should include("sum(val)")
      meta.querySql.toLowerCase should include("group by grp")
    }

    it("two MVs over different sources yield two distinct rows in MvCatalog") {
      sql("CREATE TABLE IF NOT EXISTS t2_mvc1b(id INT, name STRING) USING DELTA")
      sql("INSERT INTO t2_mvc1b VALUES (1,'alice'),(2,'bob')")
      sql("CREATE MATERIALIZED VIEW mv2_mvc1b AS SELECT id, name FROM t2_mvc1b")

      MvCatalog.list(spark).map(_.name.table) should contain allOf ("mv1_mvc1", "mv2_mvc1b")

      val m1 = lookup("mv1_mvc1").get
      val m2 = lookup("mv2_mvc1b").get
      m1.refreshType shouldBe RefreshTypeCode.AggregateGroup
      m2.refreshType shouldBe RefreshTypeCode.SimpleProjection
    }

    it("the MV's Delta storage exists at the recorded location") {
      val meta   = lookup("mv1_mvc1").get
      val dir    = fileOf(meta.location)
      val deltaD = new File(stripFileScheme(meta.location), "_delta_log")
      dir.exists() shouldBe true
      deltaD.exists() shouldBe true
    }

    it("the MV is queryable through Spark's catalog (registered table)") {
      // `mv1_mvc1` was created earlier; SELECTing through Spark must succeed.
      val rows = sql("SELECT grp, total FROM mv1_mvc1 ORDER BY grp").collect()
      rows should have length 2
      rows.map(_.getAs[String]("grp")).toSet shouldBe Set("a", "b")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (5) DROP MATERIALIZED VIEW cleans up catalog row, Delta files,
  //     and Spark catalog table.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(5) DROP MATERIALIZED VIEW cleanup") {

    it("removes the MvCatalog row, deletes the Delta storage, drops the catalog table") {
      sql("CREATE TABLE IF NOT EXISTS t6_mvc5(id INT, val INT) USING DELTA")
      sql("INSERT INTO t6_mvc5 VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW mv6_mvc5 AS SELECT id, val FROM t6_mvc5")

      val meta = lookup("mv6_mvc5").get
      val dir  = fileOf(meta.location)
      dir.exists() shouldBe true

      sql("DROP MATERIALIZED VIEW mv6_mvc5")

      lookup("mv6_mvc5") shouldBe None
      dir.exists() shouldBe false
      // Spark catalog should no longer know about mv6_mvc5.
      spark.catalog.tableExists("mv6_mvc5") shouldBe false
    }

    it("DROP without IF EXISTS on a missing MV raises an AnalysisException") {
      an[AnalysisException] should be thrownBy {
        sql("DROP MATERIALIZED VIEW mv_does_not_exist_mvc5")
      }
    }

    it("DROP IF EXISTS on a missing MV is a no-op") {
      noException should be thrownBy {
        sql("DROP MATERIALIZED VIEW IF EXISTS mv_does_not_exist_mvc5")
      }
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (7) Re-create with the same name after DROP succeeds.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(7) Re-create an MV with the same name after DROP") {

    it("a CREATE after DROP succeeds and registers a fresh catalog row") {
      sql("CREATE TABLE IF NOT EXISTS t7_mvc7(id INT, val INT) USING DELTA")
      sql("INSERT INTO t7_mvc7 VALUES (1, 100)")
      sql("CREATE MATERIALIZED VIEW mv7_mvc7 AS SELECT id, val FROM t7_mvc7")
      lookup("mv7_mvc7").isDefined shouldBe true

      sql("DROP MATERIALIZED VIEW mv7_mvc7")
      lookup("mv7_mvc7") shouldBe None

      // Recreate with a different body (aggregate). Should fully succeed.
      sql(
        "CREATE MATERIALIZED VIEW mv7_mvc7 AS SELECT count(*) AS cnt FROM t7_mvc7"
      )
      val meta = lookup("mv7_mvc7").get
      meta.refreshType shouldBe RefreshTypeCode.SimpleAggregate
      meta.querySql.toLowerCase should include("count(*)")
      sql("SELECT cnt FROM mv7_mvc7").collect().head.getAs[Long](0) shouldBe 1L
    }
  }
}
