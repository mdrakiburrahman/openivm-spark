package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.MvCatalog

import java.io.File

/** Split from the original parity spec.  Scope:
  * MvMetadata schema-level concerns: the SHA-256 source-schema fingerprint and the `lastVersion` advancing across CREATE / REFRESH.
  *
  * Includes sections: (8), (10).
  */
abstract class MetadataSchemaScenarios extends IvmParitySpecBase("metadata-schema") {
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
  // (8) Fingerprint stability and recomputation
  // ──────────────────────────────────────────────────────────────────────────
  describe("(8) Schema fingerprint") {

    it("schemaFingerprint is deterministic and sensitive to schema content") {
      import org.apache.spark.sql.types._
      val schemaA: StructType =
        StructType(
          Array(
            StructField("a", IntegerType, nullable = true),
            StructField("b", StringType, nullable = true)
          )
        )
      val schemaB: StructType =
        StructType(
          Array(
            StructField("a", IntegerType, nullable = true),
            StructField("b", IntegerType, nullable = true) // <- b changed type
          )
        )

      val fp1 = MvCatalog.schemaFingerprint(Map("t" -> schemaA))
      val fp2 = MvCatalog.schemaFingerprint(Map("t" -> schemaA))
      val fp3 = MvCatalog.schemaFingerprint(Map("t" -> schemaB))
      fp1 shouldBe fp2
      fp1 should not be fp3
      // SHA-256 hex digest length.
      fp1.length shouldBe 64
    }

    it("two sources hashed in arbitrary order produce the same fingerprint") {
      import org.apache.spark.sql.types._
      val tA  = StructType(Array(StructField("a", IntegerType)))
      val tB  = StructType(Array(StructField("b", StringType)))
      val fp1 = MvCatalog.schemaFingerprint(Map("t1" -> tA, "t2" -> tB))
      val fp2 = MvCatalog.schemaFingerprint(Map("t2" -> tB, "t1" -> tA))
      fp1 shouldBe fp2
    }

    it("recorded fingerprint matches a fresh recomputation of the source schemas") {
      sql("CREATE TABLE IF NOT EXISTS t8_sch8(a INT, b STRING) USING DELTA")
      sql("CREATE MATERIALIZED VIEW mv8_sch8 AS SELECT a FROM t8_sch8")
      val meta = lookup("mv8_sch8").get
      val fresh = MvCatalog.schemaFingerprint(
        meta.sourceTables.map(t => t -> spark.table(t).schema).toMap
      )
      meta.sourceSchemaFingerprint shouldBe fresh
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (10) lastVersion is advanced past the initial -1 sentinel after CREATE
  //      and bumped after every REFRESH.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(10) lastVersion advances after CREATE and REFRESH") {

    it("CREATE leaves lastVersion at the Delta version of the initial snapshot") {
      sql("CREATE TABLE IF NOT EXISTS t10_sch10(id INT, val INT) USING DELTA")
      sql("INSERT INTO t10_sch10 VALUES (1, 100)")
      sql("CREATE MATERIALIZED VIEW mv10_sch10 AS SELECT id, val FROM t10_sch10")
      val meta = lookup("mv10_sch10").get
      meta.lastVersion should be >= 0L
    }

    it("REFRESH bumps the lastVersion forward after applying staging deltas") {
      val v0 = lookup("mv10_sch10").get.lastVersion
      sql("INSERT INTO t10_sch10 VALUES (2, 200)")
      refreshMv("mv10_sch10")
      val v1 = lookup("mv10_sch10").get.lastVersion
      v1 should be > v0
    }
  }
}
