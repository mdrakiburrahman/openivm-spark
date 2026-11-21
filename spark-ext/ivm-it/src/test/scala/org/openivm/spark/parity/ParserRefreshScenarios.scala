package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{MvCatalog, MvMetadata}
import org.openivm.spark.common.rocksdb.OpenIvmStateSync

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
    def eligibility(target: String, expected: String = hash) =
      sql(s"SHOW OPENIVM REFRESH ELIGIBILITY $target EXPECT QUERY HASH '$expected'").collect().head
    def decline(target: String, reason: String, expected: String = hash): Unit = {
      val row = eligibility(target, expected)
      row.getString(0) shouldBe "lifecycle_required"
      row.getString(1) shouldBe expected
      row.getString(2) shouldBe target
      row.isNullAt(3) shouldBe true
      row.getString(4) shouldBe reason
    }

    it("reports only truly absent names as absent") {
      val capability = sql("SET spark.openivm.capabilities").collect().head
      capability.getString(1) should include("ensure_refresh_v2")
      capability.getString(1) should include("ensure_refresh_eligibility_v1")
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
      decline("`ensure_unmanaged_p14`", "registration_missing")
      MvCatalog.prepareCreate(spark, TableIdentifier("ensure_corrupt_p14", Some("default")))
      intercept[Exception](eligibility("`default`.`ensure_corrupt_p14`"))
    }

    it("validates the live definition before writing and returns refresh proof") {
      val key    = "dbt.fabricspark.materialized_view.query_hash"
      val target = "`spark_catalog`.`default`.`ensure_mv_p15`"
      sql("CREATE TABLE ensure_src_p15 (id INT) USING DELTA")
      sql("INSERT INTO ensure_src_p15 VALUES (1)")
      sql(
        s"CREATE MATERIALIZED VIEW default.ensure_mv_p15 TBLPROPERTIES ('$key' = '$hash') AS SELECT id FROM ensure_src_p15"
      )
      sql("INSERT INTO ensure_src_p15 VALUES (2)")
      val originalAsync = spark.conf.getOption("spark.openivm.stateSync.async.enabled")
      spark.conf.set("spark.openivm.stateSync.async.enabled", "true")
      decline(target, "async_unsupported")
      val asyncFailure = intercept[Exception](guarded(target))
      Iterator
        .iterate[Throwable](asyncFailure)(_.getCause)
        .takeWhile(_ != null)
        .exists(e =>
          Option(e.getMessage).exists(_.contains("requires synchronous state synchronization"))
        ) shouldBe true
      assertMvCorrect("ensure_mv_p15", "SELECT id FROM ensure_src_p15 WHERE id = 1")
      spark.conf.set("spark.openivm.stateSync.async.enabled", "false")
      try {
        val beforeMeta    = MvCatalog.lookup(spark, TableIdentifier("ensure_mv_p15", Some("default"))).get
        val beforeVersion = io.delta.tables.DeltaTable.forPath(spark, beforeMeta.location).history(1).head().getLong(0)
        var probeBackups  = 0
        OpenIvmStateSync.setStateSyncUriHookForTesting(_ => Some("file:/guard-v2-probe-backup"))
        OpenIvmStateSync.setBackupPassHookForTesting { (_, _, _) => probeBackups += 1 }
        try {
          val result = eligibility(target)
          result.getString(0) shouldBe "eligible"
          result.getString(1) shouldBe hash
          result.getString(2) shouldBe target
          result.getString(3) should not be empty
          result.isNullAt(4) shouldBe true
          decline(target, "query_hash_policy", "ffffffffffffffffffffffffffffffff")
          probeBackups shouldBe 0
        } finally {
          OpenIvmStateSync.setBackupPassHookForTesting(null)
          OpenIvmStateSync.setStateSyncUriHookForTesting(null)
        }
        MvCatalog.lookup(spark, TableIdentifier("ensure_mv_p15", Some("default"))).get shouldBe beforeMeta
        io.delta.tables.DeltaTable
          .forPath(spark, beforeMeta.location)
          .history(1)
          .head()
          .getLong(0) shouldBe beforeVersion
        intercept[Exception](guarded(target, "ffffffffffffffffffffffffffffffff"))
        spark.table("ensure_mv_p15").count() shouldBe 1L
        val proof = guarded(target).head
        proof.getString(0) shouldBe "refreshed"
        proof.getString(1) shouldBe hash
        proof.getString(2) shouldBe target
        (3 to 5).foreach(i => proof.getString(i) should not be empty)
        assertMvCorrect("ensure_mv_p15", "SELECT id FROM ensure_src_p15")
        val meta    = MvCatalog.lookup(spark, TableIdentifier("ensure_mv_p15", Some("default"))).get
        val version = io.delta.tables.DeltaTable.forPath(spark, meta.location).history(1).head().getLong(0)
        sql("INSERT INTO ensure_src_p15 VALUES (3)")
        try {
          val identityKey = MvMetadata.TargetDeltaTableIdKey
          MvCatalog.upsert(spark, meta.copy(properties = meta.properties - identityKey))
          decline(target, "legacy_identity_missing")
          intercept[Exception](guarded(target))
          MvCatalog.upsert(spark, meta.copy(properties = meta.properties + (identityKey -> "different-table-id")))
          intercept[Exception](eligibility(target))
          intercept[Exception](guarded(target))
        } finally {
          MvCatalog.upsert(spark, meta)
        }
        io.delta.tables.DeltaTable.forPath(spark, meta.location).history(1).head().getLong(0) shouldBe version
        assertMvCorrect("ensure_mv_p15", "SELECT id FROM ensure_src_p15 WHERE id <= 2")
        sql(s"ALTER TABLE ensure_mv_p15 SET TBLPROPERTIES ('$key' = 'ffffffffffffffffffffffffffffffff')")
        val changedPropertyVersion =
          io.delta.tables.DeltaTable.forPath(spark, meta.location).history(1).head().getLong(0)
        decline(target, "query_hash_policy")
        intercept[Exception](guarded(target))
        io.delta.tables.DeltaTable
          .forPath(spark, meta.location)
          .history(1)
          .head()
          .getLong(0) shouldBe changedPropertyVersion
        assertMvCorrect("ensure_mv_p15", "SELECT id FROM ensure_src_p15 WHERE id <= 2")
        sql(s"ALTER TABLE ensure_mv_p15 SET TBLPROPERTIES ('$key' = '$hash')")
        sql("REFRESH MATERIALIZED VIEW default.ensure_mv_p15")
        assertMvCorrect("ensure_mv_p15", "SELECT id FROM ensure_src_p15")
        sql("INSERT INTO ensure_src_p15 VALUES (4)")
        var attempts = 0
        OpenIvmStateSync.setStateSyncUriHookForTesting(_ => Some("file:/guard-v2-backup-failure"))
        OpenIvmStateSync.setBackupPassHookForTesting { (_, _, _) =>
          attempts += 1
          throw new java.io.IOException("guarded backup failure")
        }
        try {
          val failure = intercept[Exception](guarded(target))
          Iterator
            .iterate[Throwable](failure)(_.getCause)
            .takeWhile(_ != null)
            .exists(_.getMessage == "guarded backup failure") shouldBe true
          attempts shouldBe 1
        } finally {
          OpenIvmStateSync.setBackupPassHookForTesting(null)
          OpenIvmStateSync.setStateSyncUriHookForTesting(null)
        }
        assertMvCorrect("ensure_mv_p15", "SELECT id FROM ensure_src_p15")
        val finalVersion = io.delta.tables.DeltaTable.forPath(spark, meta.location).history(1).head().getLong(0)
        MvCatalog.remove(spark, meta.name)
        decline(target, "registration_missing")
        intercept[Exception](guarded(target))
        io.delta.tables.DeltaTable.forPath(spark, meta.location).history(1).head().getLong(0) shouldBe finalVersion
      } finally {
        originalAsync match {
          case Some(value) => spark.conf.set("spark.openivm.stateSync.async.enabled", value)
          case None        => spark.conf.unset("spark.openivm.stateSync.async.enabled")
        }
      }
    }

    it("does not create a missing Delta catalog and rejects an invalid catalog") {
      val warehouse = new java.io.File(new java.net.URI(spark.conf.get("spark.sql.warehouse.dir")))
      val root      = new java.io.File(warehouse, "eligibility_catalog")
      val metadata  = new java.io.File(root, "mv_metadata")
      metadata.exists() shouldBe false
      val oldBackend = spark.conf.getOption("spark.openivm.catalog.backend")
      val oldPath    = spark.conf.getOption("spark.openivm.catalog.path")
      spark.conf.set("spark.openivm.catalog.backend", "delta")
      spark.conf.set("spark.openivm.catalog.path", root.toString)
      try {
        decline("`default`.`ensure_delta_absent_p18`", "registration_missing")
        metadata.exists() shouldBe false
        metadata.mkdirs() shouldBe true
        intercept[Exception](eligibility("`default`.`ensure_delta_absent_p18`"))
      } finally {
        oldBackend match {
          case Some(value) => spark.conf.set("spark.openivm.catalog.backend", value)
          case None        => spark.conf.unset("spark.openivm.catalog.backend")
        }
        oldPath match {
          case Some(value) => spark.conf.set("spark.openivm.catalog.path", value)
          case None        => spark.conf.unset("spark.openivm.catalog.path")
        }
      }
    }

    it("only routes a missing public view when its backing identity is intact") {
      val key      = "dbt.fabricspark.materialized_view.query_hash"
      val target   = "`spark_catalog`.`default`.`ensure_mv_p16`"
      val oldAsync = spark.conf.getOption("spark.openivm.stateSync.async.enabled")
      spark.conf.set("spark.openivm.stateSync.async.enabled", "false")
      try {
        sql("CREATE TABLE ensure_src_p16 (grp INT, val INT) USING DELTA")
        sql("INSERT INTO ensure_src_p16 VALUES (1, 2)")
        sql(
          s"CREATE MATERIALIZED VIEW default.ensure_mv_p16 TBLPROPERTIES ('$key' = '$hash') AS " +
            "SELECT grp, SUM(val) AS total FROM ensure_src_p16 GROUP BY grp HAVING SUM(val) > 0"
        )
        val meta = MvCatalog.lookup(spark, TableIdentifier("ensure_mv_p16", Some("default"))).get
        meta.usesBackingDataTable shouldBe true
        val version = io.delta.tables.DeltaTable.forPath(spark, meta.location).history(1).head().getLong(0)
        eligibility(target).getString(0) shouldBe "eligible"
        sql("DROP VIEW default.ensure_mv_p16")
        decline(target, "public_relation_missing")
        MvCatalog.upsert(
          spark,
          meta.copy(properties = meta.properties + (MvMetadata.TargetDeltaTableIdKey -> "foreign"))
        )
        intercept[Exception](eligibility(target))
        MvCatalog.upsert(spark, meta)
        sql("CREATE VIEW default.ensure_mv_p16 AS SELECT grp, CAST(val AS BIGINT) AS total FROM ensure_src_p16")
        intercept[Exception](eligibility(target))
        intercept[Exception](guarded(target))
        io.delta.tables.DeltaTable.forPath(spark, meta.location).history(1).head().getLong(0) shouldBe version
      } finally {
        oldAsync match {
          case Some(value) => spark.conf.set("spark.openivm.stateSync.async.enabled", value)
          case None        => spark.conf.unset("spark.openivm.stateSync.async.enabled")
        }
      }
    }

    it("reports strict results after managed replacement or disappearance following eligibility") {
      val key      = "dbt.fabricspark.materialized_view.query_hash"
      val target   = "`spark_catalog`.`default`.`ensure_mv_p17`"
      val oldAsync = spark.conf.getOption("spark.openivm.stateSync.async.enabled")
      spark.conf.set("spark.openivm.stateSync.async.enabled", "false")
      try {
        sql("CREATE TABLE ensure_src_p17 (id INT) USING DELTA")
        sql("INSERT INTO ensure_src_p17 VALUES (1)")
        val create = s"CREATE MATERIALIZED VIEW default.ensure_mv_p17 TBLPROPERTIES ('$key' = '$hash') AS " +
          "SELECT id FROM ensure_src_p17"
        sql(create)
        val first = eligibility(target)
        first.getString(0) shouldBe "eligible"
        sql("DROP MATERIALIZED VIEW default.ensure_mv_p17")
        sql(create)
        val second = eligibility(target)
        second.getString(0) shouldBe "eligible"
        second.getString(3) should not be first.getString(3)
        val refreshed = guarded(target).head
        refreshed.getString(0) shouldBe "refreshed"
        refreshed.getString(3) shouldBe second.getString(3)
        eligibility(target).getString(0) shouldBe "eligible"
        sql("DROP MATERIALIZED VIEW default.ensure_mv_p17")
        guarded(target).head.getString(0) shouldBe "absent"
      } finally {
        oldAsync match {
          case Some(value) => spark.conf.set("spark.openivm.stateSync.async.enabled", value)
          case None        => spark.conf.unset("spark.openivm.stateSync.async.enabled")
        }
      }
    }
  }
}
