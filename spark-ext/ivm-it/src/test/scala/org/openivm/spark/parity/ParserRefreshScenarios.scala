package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{MvCatalog, MvMetadata}
import org.openivm.spark.commands.CommandConcurrencyInjection

/** Slice of `ParserSpec` covering REFRESH idempotency, INSERT+DELETE-in-the-
  * same-window net-zero deltas, DROP+recreate with a different body, and
  * `CREATE … IF NOT EXISTS` idempotency.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
abstract class ParserRefreshScenarios extends IvmParitySpecBase("parser-refresh") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvExists(name: String): Boolean =
    MvCatalog.lookup(spark, TableIdentifier(name)).isDefined

  // ──────────────────────────────────────────────────────────────────────────
  // (10) Idempotent back-to-back REFRESH (parser.test Test 28).
  // ──────────────────────────────────────────────────────────────────────────
  describe("(10) REFRESH idempotency") {

    it("multiple back-to-back REFRESH calls with no DML in between leave the MV unchanged") {
      sql("CREATE TABLE IF NOT EXISTS idem_p10(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO idem_p10 VALUES (1,'a',10),(2,'a',20)")
      sql(
        "CREATE MATERIALIZED VIEW mv_idem_p10 AS " +
          "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total, count(val) AS cnt " +
          "FROM idem_p10 GROUP BY grp"
      )

      sql("INSERT INTO idem_p10 VALUES (3,'a',5)")
      refreshMv("mv_idem_p10")
      val snap1 = spark.table("mv_idem_p10").collect().toSet
      refreshMv("mv_idem_p10")
      refreshMv("mv_idem_p10")
      val snap2 = spark.table("mv_idem_p10").collect().toSet
      snap2 shouldBe snap1
      assertMvCorrect(
        "mv_idem_p10",
        "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total, count(val) AS cnt " +
          "FROM idem_p10 GROUP BY grp"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (11) INSERT+DELETE of same id in one cycle (parser.test Test 29) —
  //     net effect must be zero.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(11) INSERT then DELETE the same row before refresh") {

    it("net delta is zero; MV is unchanged from the pre-batch state") {
      sql("CREATE TABLE IF NOT EXISTS oscill_p11(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO oscill_p11 VALUES (1,'a',10),(2,'a',20)")
      sql(
        "CREATE MATERIALIZED VIEW mv_oscill_p11 AS " +
          "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total, count(val) AS cnt " +
          "FROM oscill_p11 GROUP BY grp"
      )
      val before = spark.table("mv_oscill_p11").collect().toSet

      sql("INSERT INTO oscill_p11 VALUES (99,'a',1)")
      sql("DELETE FROM oscill_p11 WHERE id = 99")
      refreshMv("mv_oscill_p11")

      spark.table("mv_oscill_p11").collect().toSet shouldBe before
      assertMvCorrect(
        "mv_oscill_p11",
        "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total, count(val) AS cnt " +
          "FROM oscill_p11 GROUP BY grp"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (12) DROP MATERIALIZED VIEW removes the catalog row and lets a fresh MV
  //     of the same name be created with a different SELECT body.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(12) DROP + recreate MV with a different body") {

    it("a fresh CREATE after DROP succeeds and uses the new body") {
      sql(
        "CREATE TABLE IF NOT EXISTS recreate_p12(id INT, val INT, category STRING) USING DELTA"
      )
      sql("INSERT INTO recreate_p12 VALUES (1,10,'a'),(2,20,'b')")
      sql("CREATE MATERIALIZED VIEW mv_recreate_p12 AS SELECT id, val FROM recreate_p12")
      assertMvCorrect("mv_recreate_p12", "SELECT id, val FROM recreate_p12")

      sql("DROP MATERIALIZED VIEW mv_recreate_p12")
      mvExists("mv_recreate_p12") shouldBe false

      // Re-create with an aggregate body.
      sql(
        "CREATE MATERIALIZED VIEW mv_recreate_p12 AS " +
          "SELECT category, SUM(val) AS total FROM recreate_p12 GROUP BY category"
      )
      assertMvCorrect(
        "mv_recreate_p12",
        "SELECT category, SUM(val) AS total FROM recreate_p12 GROUP BY category"
      )

      sql("INSERT INTO recreate_p12 VALUES (3, 30, 'a')")
      refreshMv("mv_recreate_p12")
      assertMvCorrect(
        "mv_recreate_p12",
        "SELECT category, SUM(val) AS total FROM recreate_p12 GROUP BY category"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (13) IF NOT EXISTS guard.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(13) IF NOT EXISTS makes CREATE idempotent") {

    it("a second CREATE … IF NOT EXISTS with the same name is a no-op") {
      sql("CREATE TABLE IF NOT EXISTS ine_p13(id INT, val INT) USING DELTA")
      sql("INSERT INTO ine_p13 VALUES (1,10)")
      sql("CREATE MATERIALIZED VIEW mv_ine_p13 AS SELECT id, val FROM ine_p13")
      val before = spark.table("mv_ine_p13").collect().toSet

      // Second CREATE IF NOT EXISTS — must NOT throw and must not modify the MV.
      sql(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ine_p13 AS SELECT id, val + 99 FROM ine_p13"
      )
      spark.table("mv_ine_p13").collect().toSet shouldBe before
    }
  }

  describe("guarded refresh") {
    val hash = "0123456789abcdef0123456789abcdef"
    def guarded(target: String, expected: String = hash) =
      sql(s"REFRESH MATERIALIZED VIEW $target EXPECT QUERY HASH '$expected' IF EXISTS").collect()

    it("fails v3 closed until target serialization is proven") {
      val request = "0123456789ab4def8abc0123456789ab"
      val error = intercept[Exception] {
        sql(s"REFRESH MATERIALIZED VIEW ensure_v3_blocked EXPECT QUERY HASH '$hash' IF EXISTS REQUEST ID '$request'")
          .collect()
      }
      error.getMessage should include("cross-driver target serialization is unverified")
      spark.catalog.tableExists("ensure_v3_blocked") shouldBe false
    }

    it("reports only truly absent names as absent") {
      val capability = sql("SET spark.openivm.capabilities").collect().head
      capability.getString(1) should include("ensure_refresh_v2")
      spark.conf.set("spark.openivm.enabled", "false")
      try {
        sql("SET spark.openivm.capabilities").collect().head.getString(1) should include("ensure_refresh_v2")
      } finally spark.conf.set("spark.openivm.enabled", "true")
      val absent = guarded("ensure_absent_p14").head
      absent.getString(0) shouldBe "absent"
      absent.getString(1) shouldBe hash
      absent.getString(2) shouldBe "`ensure_absent_p14`"
      (3 to 5).foreach(i => absent.isNullAt(i) shouldBe true)
      sql("CREATE TABLE ensure_unmanaged_p14 (id INT) USING DELTA")
      intercept[Exception](guarded("ensure_unmanaged_p14"))
    }

    ignore("returns request-bound prewrite results only for absent or physical Delta targets") { /* TODO: require cross-driver target serialization */
      val request = "0123456789ab4def8abc0123456789ab"
      def guardedV3(target: String) = sql(
        s"REFRESH MATERIALIZED VIEW $target EXPECT QUERY HASH '$hash' IF EXISTS REQUEST ID '$request'"
      )
      val absent = guardedV3("ensure_v3_absent_p14")
      absent.schema.fields.map(field => (field.name, field.nullable)).toSeq shouldBe Seq(
        ("status", false),
        ("query_hash", false),
        ("target", false),
        ("target_table_id", true),
        ("refresh_id", true),
        ("outcome", true),
        ("request_id", false)
      )
      val absentRow = absent.collect().head
      absentRow.toSeq shouldBe Seq("absent", hash, "`ensure_v3_absent_p14`", null, null, null, request)
      sql("CREATE TABLE ensure_v3_unmanaged_p14 (id INT) USING DELTA")
      val row = guardedV3("ensure_v3_unmanaged_p14").collect().head
      row.getString(0) shouldBe "present_unregistered_prewrite"
      row.getString(1) shouldBe hash
      row.getString(2) shouldBe "`ensure_v3_unmanaged_p14`"
      row.getString(3) should fullyMatch regex "[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}"
      row.isNullAt(4) shouldBe true
      row.isNullAt(5) shouldBe true
      row.getString(6) shouldBe request
      sql("CREATE VIEW ensure_v3_view_p14 AS SELECT * FROM ensure_v3_unmanaged_p14")
      intercept[Exception](guardedV3("ensure_v3_view_p14").collect())
    }

    ignore("preserves the registered v2 refresh path with a bound v3 proof") { /* TODO: require cross-driver target serialization */
      val request = "0123456789ab4def8abc0123456789ab"
      val key     = "dbt.fabricspark.materialized_view.query_hash"
      sql("CREATE TABLE ensure_v3_src_p15 (id INT) USING DELTA")
      sql("INSERT INTO ensure_v3_src_p15 VALUES (1)")
      sql(
        s"CREATE MATERIALIZED VIEW ensure_v3_mv_p15 TBLPROPERTIES ('$key' = '$hash') AS SELECT id FROM ensure_v3_src_p15"
      )
      sql("INSERT INTO ensure_v3_src_p15 VALUES (2)")
      val row = sql(
        s"REFRESH MATERIALIZED VIEW ensure_v3_mv_p15 EXPECT QUERY HASH '$hash' IF EXISTS REQUEST ID '$request'"
      ).collect().head
      row.getString(0) shouldBe "refreshed"
      row.getString(1) shouldBe hash
      row.getString(2) shouldBe "`ensure_v3_mv_p15`"
      row.getString(3) should fullyMatch regex "[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}"
      row.getString(4) should not be empty
      Set("incremental_executed", "full_refresh_executed", "no_pending_deltas") should contain(row.getString(5))
      row.getString(6) shouldBe request
      assertMvCorrect("ensure_v3_mv_p15", "SELECT id FROM ensure_v3_src_p15")
      val nextRequest = "1123456789ab4def8abc0123456789ab"
      val next = sql(
        s"REFRESH MATERIALIZED VIEW ensure_v3_mv_p15 EXPECT QUERY HASH '$hash' IF EXISTS REQUEST ID '$nextRequest'"
      ).collect().head
      next.getString(5) shouldBe "no_pending_deltas"
      sql("DROP TABLE ensure_v3_mv_p15")
      sql("CREATE TABLE ensure_v3_mv_p15 (id INT) USING DELTA")
      intercept[Exception] {
        sql(s"REFRESH MATERIALIZED VIEW ensure_v3_mv_p15 EXPECT QUERY HASH '$hash' IF EXISTS REQUEST ID '$request'")
          .collect()
      }
      spark.table("ensure_v3_mv_p15").count() shouldBe 0L
    }

    ignore("does not return refresh proof if the target is rebound during refresh") { /* TODO: require cross-driver target serialization */
      val request = "2123456789ab4def8abc0123456789ab"
      val key     = "dbt.fabricspark.materialized_view.query_hash"
      sql("CREATE TABLE ensure_v3_race_src (id INT) USING DELTA")
      sql("INSERT INTO ensure_v3_race_src VALUES (1)")
      sql(
        s"CREATE MATERIALIZED VIEW ensure_v3_race_mv TBLPROPERTIES ('$key' = '$hash') AS SELECT id FROM ensure_v3_race_src"
      )
      sql("INSERT INTO ensure_v3_race_src VALUES (2)")
      var rebound = false
      CommandConcurrencyInjection.withBeforeRefreshApply {
        rebound = true
        sql("DROP TABLE ensure_v3_race_mv")
        sql("CREATE TABLE ensure_v3_race_mv (id INT) USING DELTA")
      } {
        intercept[Exception] {
          sql(s"REFRESH MATERIALIZED VIEW ensure_v3_race_mv EXPECT QUERY HASH '$hash' IF EXISTS REQUEST ID '$request'")
            .collect()
        }
      }
      rebound shouldBe true
    }

    it("validates the live definition before writing and returns refresh proof") {
      val key = "dbt.fabricspark.materialized_view.query_hash"
      sql("CREATE TABLE ensure_src_p15 (id INT) USING DELTA")
      sql("INSERT INTO ensure_src_p15 VALUES (1)")
      sql(s"CREATE MATERIALIZED VIEW ensure_mv_p15 TBLPROPERTIES ('$key' = '$hash') AS SELECT id FROM ensure_src_p15")
      sql("INSERT INTO ensure_src_p15 VALUES (2)")
      intercept[Exception](guarded("ensure_mv_p15", "ffffffffffffffffffffffffffffffff"))
      spark.table("ensure_mv_p15").count() shouldBe 1L
      val proof = guarded("ensure_mv_p15").head
      proof.getString(0) shouldBe "refreshed"
      proof.getString(1) shouldBe hash
      proof.getString(2) shouldBe "`ensure_mv_p15`"
      (3 to 5).foreach(i => proof.getString(i) should not be empty)
      assertMvCorrect("ensure_mv_p15", "SELECT id FROM ensure_src_p15")
      val meta = MvCatalog.lookup(spark, TableIdentifier("ensure_mv_p15")).get
      try {
        val identityKey = MvMetadata.TargetDeltaTableIdKey
        MvCatalog.upsert(spark, meta.copy(properties = meta.properties - identityKey))
        intercept[Exception](guarded("ensure_mv_p15"))
        MvCatalog.upsert(spark, meta.copy(properties = meta.properties + (identityKey -> "different-table-id")))
        intercept[Exception](guarded("ensure_mv_p15"))
      } finally {
        MvCatalog.upsert(spark, meta)
      }
      sql(s"ALTER TABLE ensure_mv_p15 SET TBLPROPERTIES ('$key' = 'ffffffffffffffffffffffffffffffff')")
      sql("INSERT INTO ensure_src_p15 VALUES (3)")
      intercept[Exception](guarded("ensure_mv_p15"))
      spark.table("ensure_mv_p15").count() shouldBe 2L
    }
  }
}
