package org.openivm.spark.parity

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Literal
import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshSqlLogCatalog}
import org.openivm.spark.parity.base.IvmParitySpecBase

/** Regression spec for the SF=10 `gold.fact_holdings diff=18` failure.
  *
  * Symptom: `SIMPLE_PROJECTION` MV ends up MISSING rows after an incremental
  * refresh whose source-delta retracts only SOME of a set of source rows that
  * project to the SAME value-tuple (i.e. value-equal duplicates in the MV).
  *
  * Root cause: `SparkRefreshRewriter.rewriteSimpleProjectionDataInsert`
  * emits a `MERGE INTO <mv> AS v USING <neg-mult rows> AS d ON v.col IS NOT
  * DISTINCT FROM d.col WHEN MATCHED THEN DELETE`. Delta Lake MERGE
  * `WHEN MATCHED THEN DELETE` deletes ALL matching MV rows per source row,
  * not exactly `|openivm_multiplicity|`. When the MV has N > |mult| value-
  * equal copies of a tuple that the delta partially retracts, this
  * over-deletes by `N - |mult|` rows — exactly the SF=10 fact_holdings
  * `diff=18` shape.
  *
  * Reference DuckDB implementation uses `rowid` + `ROW_NUMBER()` to delete
  * exactly `|_net|` copies per (cols) group
  * (`.temp/openivm/src/upsert/refresh_compiler.cpp:828-852`). The Spark fix
  * (this spec's expected post-fix behaviour) emits a 3-statement rewrite
  * that uses `delta.<mvLocation> VERSION AS OF <pre-refresh-version>` to
  * snapshot the pre-DELETE bag-count and re-INSERT `max(0, _cur + _net)`
  * copies after the over-deleting MERGE.
  *
  * Determinism harness: `local[1]` + `shuffle.partitions=1` +
  * `adaptive.enabled=false`, matching the documented prescription in
  * `.research/OPENIVM-BUG.md` §6.
  */
abstract class SimpleProjectionBagDeleteScenarios extends IvmParitySpecBase("simple-projection-bag-delete") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.QueryLogEnabledKey -> "true")

  // ─────────────────────────────────────────────────────────────────────────
  // (1) Partial retract of value-equal duplicates — the SF=10 fact_holdings
  //     `diff=18` shape. Source has 3 rows projecting to `k=1`; batch 2
  //     deletes ONE of them. MV must end up with 2 copies, not 0 (the
  //     pre-fix MERGE-DELETE over-deletes ALL 3).
  // ─────────────────────────────────────────────────────────────────────────

  describe("(1) SIMPLE_PROJECTION partial retract of value-equal duplicates") {

    it("preserves bag-count on incremental DELETE (net=-1, 3 MV copies → 2 copies)") {
      sql("CREATE TABLE IF NOT EXISTS spbd_src1(k INT, payload STRING) USING DELTA")
      sql("CREATE MATERIALIZED VIEW spbd_mv1 AS SELECT k FROM spbd_src1")

      // Batch 1: three rows projecting to value-tuple `k=1`, different payloads.
      sql("INSERT INTO spbd_src1 VALUES (1, 'a'), (1, 'b'), (1, 'c')")
      sql("REFRESH MATERIALIZED VIEW spbd_mv1").collect()
      assertMvCorrect("spbd_mv1", "SELECT k FROM spbd_src1")

      // Batch 2: retract ONE source row. View-delta should carry mult=-1 for `k=1`.
      // Pre-fix bug: MERGE-DELETE drops ALL 3 MV copies of `k=1` → MV ends up
      // with 0 rows. Expected: 2 rows.
      sql("DELETE FROM spbd_src1 WHERE payload = 'a'")
      sql("REFRESH MATERIALIZED VIEW spbd_mv1").collect()
      assertMvCorrect("spbd_mv1", "SELECT k FROM spbd_src1")
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (2) Multi-key partial retract — multiple distinct value-tuples each
  //     have N > |mult| copies in the MV and the delta retracts a strict
  //     subset of each.
  // ─────────────────────────────────────────────────────────────────────────

  describe("(2) SIMPLE_PROJECTION partial retract across multiple value-tuples") {

    it("preserves bag-count when several distinct keys are partially retracted") {
      sql("CREATE TABLE IF NOT EXISTS spbd_src2(k INT, payload STRING) USING DELTA")
      sql("CREATE MATERIALIZED VIEW spbd_mv2 AS SELECT k FROM spbd_src2")

      // Batch 1: k=1 → 4 copies; k=2 → 3 copies; k=3 → 2 copies; k=4 → 1 copy.
      sql(
        "INSERT INTO spbd_src2 VALUES " +
          "(1,'a'),(1,'b'),(1,'c'),(1,'d')," +
          "(2,'a'),(2,'b'),(2,'c')," +
          "(3,'a'),(3,'b')," +
          "(4,'a')"
      )
      sql("REFRESH MATERIALIZED VIEW spbd_mv2").collect()
      assertMvCorrect("spbd_mv2", "SELECT k FROM spbd_src2")

      // Batch 2: retract one copy of k=1 and two copies of k=2.
      // Expected post-state: k=1 → 3, k=2 → 1, k=3 → 2, k=4 → 1.
      sql("DELETE FROM spbd_src2 WHERE (k=1 AND payload='a') OR (k=2 AND payload IN ('a','b'))")
      sql("REFRESH MATERIALIZED VIEW spbd_mv2").collect()
      assertMvCorrect("spbd_mv2", "SELECT k FROM spbd_src2")
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (3) NULL-column value-equality — matches the TPC-DI fact_holdings shape
  //     where PNDG/SBMT trades produce NULL `current_price`,
  //     `current_fee`, `current_commission`. The pre-fix value-equality
  //     MERGE (`IS NOT DISTINCT FROM`) over-collapses NULL-bearing tuples.
  // ─────────────────────────────────────────────────────────────────────────

  describe("(3) SIMPLE_PROJECTION partial retract with NULL-bearing value-tuples") {

    it("preserves bag-count when retracted value-tuple has NULL columns") {
      sql(
        "CREATE TABLE IF NOT EXISTS spbd_src3(k INT, price DOUBLE, fee DOUBLE, payload STRING) USING DELTA"
      )
      sql("CREATE MATERIALIZED VIEW spbd_mv3 AS SELECT k, price, fee FROM spbd_src3")

      // Batch 1: 5 rows projecting to (k=1, price=NULL, fee=NULL) and 1 row to (k=2, …).
      sql(
        "INSERT INTO spbd_src3 VALUES " +
          "(1, NULL, NULL, 'p1')," +
          "(1, NULL, NULL, 'p2')," +
          "(1, NULL, NULL, 'p3')," +
          "(1, NULL, NULL, 'p4')," +
          "(1, NULL, NULL, 'p5')," +
          "(NULL, NULL, NULL, 'null-1')," +
          "(NULL, NULL, NULL, 'null-2')," +
          "(2, 1.0, 0.1, 'p6')"
      )
      sql("REFRESH MATERIALIZED VIEW spbd_mv3").collect()
      assertMvCorrect("spbd_mv3", "SELECT k, price, fee FROM spbd_src3")

      // Batch 2: retract 2 of the 5 NULL-priced rows. MV must end up with
      // 3 NULL-priced rows; the pre-fix bug deletes ALL 5 because the
      // value-equality `IS NOT DISTINCT FROM` MERGE matches every NULL copy.
      sql("DELETE FROM spbd_src3 WHERE payload IN ('p1', 'p2', 'null-1')")
      sql("REFRESH MATERIALIZED VIEW spbd_mv3").collect()
      assertMvCorrect("spbd_mv3", "SELECT k, price, fee FROM spbd_src3")
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (4) Insert + delete mixed batch — non-conflicting (no value-tuple has
  //     both positive and negative net). This exercises both sides of the
  //     net-multiplicity program in one refresh.
  // ─────────────────────────────────────────────────────────────────────────

  describe("(4) SIMPLE_PROJECTION mixed insert + partial-retract batch") {

    it("preserves bag-count on a batch that both inserts and partially retracts") {
      sql("CREATE TABLE IF NOT EXISTS spbd_src4(k INT, payload STRING) USING DELTA")
      sql("CREATE MATERIALIZED VIEW spbd_mv4 AS SELECT k FROM spbd_src4")

      // Batch 1: k=1 → 3 copies, k=2 → 2 copies.
      sql("INSERT INTO spbd_src4 VALUES (1,'a'),(1,'b'),(1,'c'),(2,'a'),(2,'b')")
      sql("REFRESH MATERIALIZED VIEW spbd_mv4").collect()
      assertMvCorrect("spbd_mv4", "SELECT k FROM spbd_src4")

      // Batch 2: retract one copy of k=1 (net=-1) AND insert 2 new copies of
      // k=3 (net=+2). Different value-tuples; no conflict. Pre-fix: k=1
      // drops to 0 (over-delete), k=3 gets 2 (correct). Expected k=1 → 2.
      sql("DELETE FROM spbd_src4 WHERE k=1 AND payload='a'")
      sql("INSERT INTO spbd_src4 VALUES (3,'a'),(3,'b')")
      val beforeVersion = mvDataVersion("spbd_mv4")
      RefreshSqlLogCatalog.removeAll(spark)
      sql("REFRESH MATERIALIZED VIEW spbd_mv4").collect()
      assertMvCorrect("spbd_mv4", "SELECT k FROM spbd_src4")
      mvDataVersion("spbd_mv4") shouldBe beforeVersion + 1L
      val applySql = sql("SHOW OPENIVM QUERY LOG")
        .collect()
        .map(_.getString(9))
        .find(_.contains("__openivm_apply_delete"))
        .getOrElse(fail("missing atomic bag apply"))
      applySql should include("v.`k` IN (1)")
      applySql should include("VERSION AS OF")
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (5) Positive and negative rows for the same projected tuple. The raw
  //     signed delta conflicts, but consolidation makes it net zero. The
  //     bag-correct net-view refresh must stay incremental rather than
  //     routing the whole MV through INSERT OVERWRITE.
  // ─────────────────────────────────────────────────────────────────────────

  describe("(5) SIMPLE_PROJECTION conflicting signs for one value-tuple") {

    it("nets conflicting rows and avoids a full-refresh overwrite") {
      sql("CREATE TABLE IF NOT EXISTS spbd_src5(k INT, payload STRING) USING DELTA")
      sql("INSERT INTO spbd_src5 VALUES (1,'a'),(1,'b'),(1,'c'),(2,'a'),(2,'b')")
      sql("CREATE MATERIALIZED VIEW spbd_mv5 AS SELECT k FROM spbd_src5")
      sql(
        "CREATE MATERIALIZED VIEW spbd_mv5_counts AS " +
          "SELECT k, COUNT(*) AS cnt FROM spbd_mv5 GROUP BY k"
      )

      val meta = MvCatalog
        .lookup(spark, TableIdentifier("spbd_mv5"))
        .getOrElse(fail("spbd_mv5 metadata missing"))
      val escapedLocation = meta.location.replace("`", "``")
      val preRefreshVersion = spark
        .sql(s"DESCRIBE HISTORY delta.`$escapedLocation`")
        .selectExpr("max(version) AS version")
        .head()
        .getAs[Long]("version")

      // k=1 contributes -2 and +1 (net -1), while k=2 contributes -1 and
      // +2 (net +1). Both projected tuples therefore contain opposing raw
      // signs with a non-zero net. k=3 is an independent positive change.
      sql("DELETE FROM spbd_src5 WHERE (k=1 AND payload IN ('a','b')) OR (k=2 AND payload='a')")
      sql("INSERT INTO spbd_src5 VALUES (1,'replacement'),(2,'new-1'),(2,'new-2'),(3,'new')")
      refreshMv("spbd_mv5")
      refreshMv("spbd_mv5_counts")

      assertMvCorrect("spbd_mv5", "SELECT k FROM spbd_src5")
      assertMvCorrect(
        "spbd_mv5_counts",
        "SELECT k, COUNT(*) AS cnt FROM spbd_src5 GROUP BY k"
      )

      val unconditionalOverwrites = spark
        .sql(s"DESCRIBE HISTORY delta.`$escapedLocation`")
        .where(s"version > $preRefreshVersion")
        .collect()
        .filter { row =>
          val operation  = row.getAs[String]("operation")
          val parameters = Option(row.getAs[Map[String, String]]("operationParameters")).getOrElse(Map.empty)
          operation == "WRITE" && parameters.get("mode").contains("Overwrite")
        }

      unconditionalOverwrites shouldBe empty
    }
  }

  describe("(6) SIMPLE_PROJECTION raw retractions that cancel in the net delta") {
    it("preserves native net cancellation in a string-only projection without target deletions") {
      sql("CREATE TABLE spbd_src6(k INT, payload STRING) USING DELTA")
      sql("INSERT INTO spbd_src6 VALUES (1,'a'),(1,'b'),(2,'c')")
      val query = "SELECT CAST(k AS STRING) AS label FROM spbd_src6"
      sql(s"CREATE MATERIALIZED VIEW spbd_mv6 AS $query")
      sql("UPDATE spbd_src6 SET payload = 'changed' WHERE k=1")
      sql("INSERT INTO spbd_src6 VALUES (3,'new')")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("spbd_mv6")
      assertMvCorrect("spbd_mv6", query)
      val applySql = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9))
      // The native compiler can consolidate away the negative rows before
      // Spark sees them. Preserve that existing append path, too.
      applySql.find(_.contains("__openivm_apply_delete")) match {
        case Some(merge) => merge should include("AND (false)")
        case None        => applySql.exists(_.startsWith("MERGE INTO")) shouldBe false
      }
      val history = sql(s"DESCRIBE HISTORY delta.`${mvDataLocation("spbd_mv6")}`").head()
      val metrics = history.getAs[Map[String, String]]("operationMetrics")
      history.getAs[String]("operation") match {
        case "MERGE" =>
          metrics("numTargetRowsDeleted") shouldBe "0"
          metrics("numTargetFilesAfterSkipping") shouldBe "0"
        case "WRITE" =>
          history.getAs[Map[String, String]]("operationParameters")("mode") shouldBe "Append"
        case other => fail(s"unexpected net-only operation: $other")
      }
    }
  }

  describe("(7) SIMPLE_PROJECTION bounded string target predicates") {
    it("prunes exact string keys under both parser settings and falls back on oversized keys") {
      sql("CREATE TABLE spbd_src7(id INT, k STRING, event_date DATE, watch_cnt BIGINT) USING DELTA")
      val keys = Seq(Some("quote'key"), Some("back\\slash\\n"), Some("München☃"), None)
      val values = keys.zipWithIndex.flatMap { case (key, i) =>
        val literal =
          key.fold("NULL")(s => s"CAST(${Literal(s.getBytes(java.nio.charset.StandardCharsets.UTF_8)).sql} AS STRING)")
        val date = if (i == 2) "CAST(NULL AS DATE)" else "DATE '2026-01-01'"
        (0 until 3).map(j => s"(${i * 10 + j}, $literal, $date, 1)")
      }
      sql(s"INSERT INTO spbd_src7 VALUES ${values.mkString(", ")}")
      val query = "SELECT k, event_date, watch_cnt FROM spbd_src7"
      sql(s"CREATE MATERIALIZED VIEW spbd_mv7 AS $query")
      val parserKey       = "spark.sql.parser.escapedStringLiterals"
      val previous        = spark.conf.getOption(parserKey)
      val dateApiKey      = "spark.sql.datetime.java8API.enabled"
      val previousDateApi = spark.conf.getOption(dateApiKey)
      try {
        for (escaped <- Seq(false, true)) {
          spark.conf.set(parserKey, escaped.toString)
          spark.conf.set(dateApiKey, escaped.toString)
          val deletedSuffix = if (escaped) 1 else 0
          sql(s"DELETE FROM spbd_src7 WHERE id % 10 = $deletedSuffix")
          RefreshSqlLogCatalog.removeAll(spark)
          refreshMv("spbd_mv7")
          assertMvCorrect("spbd_mv7", query)
          val apply = sql("SHOW OPENIVM QUERY LOG")
            .collect()
            .map(_.getString(9))
            .find(_.contains("__openivm_apply_delete"))
            .getOrElse(fail("missing atomic bag apply"))
          apply should include("CAST(X'")
          apply should include("v.`k` IS NULL")
        }
      } finally {
        previous match {
          case Some(value) => spark.conf.set(parserKey, value)
          case None        => spark.conf.unset(parserKey)
        }
        previousDateApi match {
          case Some(value) => spark.conf.set(dateApiKey, value)
          case None        => spark.conf.unset(dateApiKey)
        }
      }
      sql("INSERT INTO spbd_src7 SELECT CAST(id AS INT), REPEAT('x', 1024), DATE '2026-01-01', 1 FROM range(201,204)")
      refreshMv("spbd_mv7")
      sql("DELETE FROM spbd_src7 WHERE id = 201")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("spbd_mv7")
      assertMvCorrect("spbd_mv7", query)
      val apply = sql("SHOW OPENIVM QUERY LOG")
        .collect()
        .map(_.getString(9))
        .find(_.contains("__openivm_apply_delete"))
        .getOrElse(fail("missing fallback atomic bag apply"))
      apply should not include " IN ("
    }
  }
}
