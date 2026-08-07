package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.DataFrame

/** Repro for the ivm-bench SF=10 OAT failure on `gold.fact_holdings`:
  *
  *   validation `EXCEPT ALL` symmetric diff = 18 rows
  *   mv_count = 2,712,782   expected = 2,712,800
  *
  * The MV body is a 5-table join exercising three structural risk factors
  * simultaneously:
  *
  *   1. **Multi-source delta in the same batch.** 3 of the 4 underlying
  *      base tables get new DML in batch 2 (holdings_history, dim_trade,
  *      dim_account).
  *   2. **SCD2 BETWEEN** range join on `dim_account` (`s1.create_timestamp
  *      BETWEEN a.effective_timestamp AND a.end_timestamp`).
  *   3. **Self-join** of `dim_trade` (twice, as `ct` (`USING (trade_id)`)
  *      and `pt` (`ON s1.previous_trade_id = pt.trade_id`)) — no time
  *      predicate, so each side produces a full SCD2-version product.
  *
  * Fact_holdings batch-2 forensics showed the missing/extra rows clustering
  * by event timestamps with a 2×3 (or similar) Cartesian SCD2-version
  * product per trade event — strongly suggesting at least one term in the
  * openivm multi-source view-delta UNION is computing the wrong
  * SCD2-version expansion when a self-joined dimension also gains a new
  * version in the same batch.
  *
  * This spec mirrors the `fact_holdings` join shape at a tiny per-spec
  * scale (`msj_*` prefix, UUID warehouse dir), exercising in batch 2:
  *
  *   - a NEW SCD2 version of dim_trade for a trade_id ALREADY referenced
  *     by a batch-1 holdings_history row (so the existing `s1 ⋈ ct` pair
  *     must amplify by the new ct version);
  *   - a NEW SCD2 version of dim_account for an account_id ALREADY
  *     referenced by a batch-1 holdings row (the account close-out term
  *     must not double-count);
  *   - a NEW holdings_history row whose `trade_id` AND `previous_trade_id`
  *     both fall on batch-2-introduced dim_trade SCD2 versions
  *     (cross-product over self-join under multi-source delta).
  *
  * Oracle: bidirectional `EXCEPT ALL` on user-visible columns. The MV body
  * compiles as `SIMPLE_PROJECTION` (RefreshTypeCode 2) — same classification
  * as the bench's failing `fact_holdings`, so any over- or under-counting
  * in openivm's emitted multi-source view-delta CTAS surfaces as a diff.
  */
abstract class MultiSourceScd2SelfJoinFactHoldingsScenarios
    extends IvmParitySpecBase("multi-source-scd2-self-join-fact-holdings") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    val mvCount             = mv.count()
    val expectedCount       = expected.count()
    val missingDf           = expected.exceptAll(mv)
    val spuriousDf          = mv.exceptAll(expected)
    val missing             = missingDf.count()
    val spurious            = spuriousDf.count()
    val diffSamples =
      Seq(
        if (spurious > 0) Some(s"spurious_sample=${spuriousDf.limit(20).collect().mkString("[", ", ", "]")}")
        else None,
        if (missing > 0) Some(s"missing_sample=${missingDf.limit(20).collect().mkString("[", ", ", "]")}") else None
      ).flatten.mkString(" ")
    val diagnostics =
      s"$mvName: mv_count=$mvCount, expected_count=$expectedCount, " +
        s"missing(expected\\mv)=$missing, spurious(mv\\expected)=$spurious. $diffSamples "
    withClue(diagnostics + s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      spurious shouldBe 0L
    }
    withClue(diagnostics + s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
      missing shouldBe 0L
    }
  }

  // MV body — mirrors gold/fact_holdings.sql. Uses table-name prefixed
  // identifiers to avoid collision with other parity specs running in
  // parallel-forked JVMs.
  protected val FactHoldingsBody: String =
    """SELECT
      |  ct.sk_trade_id        AS sk_current_trade_id,
      |  pt.sk_trade_id        AS sk_trade_id,
      |  a.sk_account_id       AS sk_account_id,
      |  s.sk_security_id      AS sk_security_id,
      |  TO_DATE(s1.create_timestamp) AS sk_trade_date,
      |  s1.create_timestamp   AS trade_timestamp,
      |  s1.trade_price        AS current_price,
      |  s1.quantity           AS current_holding
      |FROM msj_holdings_history s1
      |JOIN msj_dim_trade ct
      |  ON s1.trade_id = ct.trade_id
      |JOIN msj_dim_trade pt
      |  ON s1.previous_trade_id = pt.trade_id
      |JOIN msj_dim_account a
      |  ON s1.account_id = a.account_id
      | AND s1.create_timestamp BETWEEN a.effective_timestamp AND a.end_timestamp
      |JOIN msj_dim_security s
      |  ON s1.symbol = s.symbol""".stripMargin

  describe("Multi-source SCD2 self-join SIMPLE_PROJECTION (fact_holdings shape)") {

    it("preserves bag-correctness across batch 2 multi-source DML (holdings + dim_trade SCD2 + dim_account SCD2)") {
      // ── DDL ─────────────────────────────────────────────────────────────
      sql(
        """CREATE TABLE IF NOT EXISTS msj_holdings_history(
          |  trade_id           STRING,
          |  previous_trade_id  STRING,
          |  account_id         STRING,
          |  symbol             STRING,
          |  create_timestamp   TIMESTAMP,
          |  trade_price        DOUBLE,
          |  quantity           BIGINT
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS msj_dim_trade(
          |  sk_trade_id        STRING,
          |  trade_id           STRING,
          |  effective_timestamp TIMESTAMP,
          |  end_timestamp      TIMESTAMP
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS msj_dim_account(
          |  sk_account_id      STRING,
          |  account_id         STRING,
          |  effective_timestamp TIMESTAMP,
          |  end_timestamp      TIMESTAMP
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS msj_dim_security(
          |  sk_security_id     STRING,
          |  symbol             STRING
          |) USING DELTA""".stripMargin
      )

      // ── BATCH 1: seed ──────────────────────────────────────────────────
      //
      // dim_security: 2 securities, single-version (no SCD2)
      // dim_account:  A1 = 1 version, A2 = 2 versions
      // dim_trade:    T0 = 1 version; T1 = 1 version; T2 = 2 versions;
      //               T_PARENT2 = 1 version
      // holdings_history:
      //   h1: T1 (prev T0)        owned by A1, symbol S1, ts=2020-03-01
      //   h2: T2 (prev T_PARENT2) owned by A2, symbol S2, ts=2020-05-01

      sql(
        """INSERT INTO msj_dim_security VALUES
          |  ('SEC_S1', 'S1'),
          |  ('SEC_S2', 'S2')""".stripMargin
      )
      sql(
        """INSERT INTO msj_dim_account VALUES
          |  ('ACC_A1_V1', 'A1', TIMESTAMP'2020-01-01 00:00:00', TIMESTAMP'9999-12-31 00:00:00'),
          |  ('ACC_A2_V1', 'A2', TIMESTAMP'2020-01-01 00:00:00', TIMESTAMP'2020-06-30 23:59:59'),
          |  ('ACC_A2_V2', 'A2', TIMESTAMP'2020-07-01 00:00:00', TIMESTAMP'9999-12-31 00:00:00')""".stripMargin
      )
      sql(
        """INSERT INTO msj_dim_trade VALUES
          |  ('SK_T0_V1',      'T0',      TIMESTAMP'2020-01-15 00:00:00', TIMESTAMP'9999-12-31 00:00:00'),
          |  ('SK_T1_V1',      'T1',      TIMESTAMP'2020-02-01 00:00:00', TIMESTAMP'9999-12-31 00:00:00'),
          |  ('SK_T2_V1',      'T2',      TIMESTAMP'2020-02-01 00:00:00', TIMESTAMP'2020-07-15 23:59:59'),
          |  ('SK_T2_V2',      'T2',      TIMESTAMP'2020-07-16 00:00:00', TIMESTAMP'9999-12-31 00:00:00'),
          |  ('SK_TPARENT2_V1','T_PARENT2', TIMESTAMP'2020-01-20 00:00:00', TIMESTAMP'9999-12-31 00:00:00')""".stripMargin
      )
      sql(
        """INSERT INTO msj_holdings_history VALUES
          |  ('T1','T0',       'A1','S1', TIMESTAMP'2020-03-01 10:00:00', 100.0, 10),
          |  ('T2','T_PARENT2','A2','S2', TIMESTAMP'2020-05-01 11:00:00', 200.0, 20)""".stripMargin
      )

      sql(s"CREATE MATERIALIZED VIEW msj_mv AS $FactHoldingsBody")

      // Sanity: full-recompute parity after backfill.
      assertMvCorrect("msj_mv", FactHoldingsBody)

      // ── BATCH 2: multi-source DML — 3 of 4 sources get new rows ────────
      //
      // (a) dim_trade gains a NEW SCD2 version of T1 (the trade_id of h1).
      //     Since fact_holdings joins ct on trade_id WITHOUT a time
      //     predicate, the existing h1 row must amplify from 1×1 to 1×2
      //     ct.sk_trade_id values — pure cross-product expansion driven by
      //     a Δct ⋈ s1_pre term that openivm's IVM delta must emit.
      //
      // (b) dim_trade also gains a brand-new trade T3, plus a NEW SCD2
      //     version of T2 (T2 already had 2 versions; now 3).
      //
      // (c) dim_account closes A1's v1 (end_ts → 2020-08-01) and inserts
      //     A1.v2 covering [2020-08-02, 9999]. The pre-existing h1 has
      //     ts=2020-03-01, still in A1.v1 — its `a` join arity stays 1
      //     (no spurious amplification term should fire).
      //
      // (d) holdings_history adds:
      //     h3: T3 (prev T1), A2, S1, ts=2020-08-15
      //         — ct = T3.v1 only          (1)
      //         — pt = T1.v1 AND T1.v2     (2)  ← Δct rendezvous with Δs1
      //         — a  = A2.v2               (1)
      //         — s  = S1                  (1)
      //         expected fact rows for h3: 1 × 2 × 1 × 1 = 2
      //     h4: T1 (prev T0), A1, S1, ts=2020-09-15
      //         — ts > A1.v1.end (2020-08-01) → falls in A1.v2
      //         — ct = T1.v1 AND T1.v2     (2)
      //         — pt = T0.v1               (1)
      //         — a  = A1.v2 only          (1)
      //         — s  = S1                  (1)
      //         expected fact rows for h4: 2 × 1 × 1 × 1 = 2
      //
      // Net incremental delta in MV (vs full recompute):
      //   pre-existing h1: was 1×1×1×1=1 row; becomes 1×2×1×1=2 → +1 row
      //   pre-existing h2: unchanged (T2 gets new v3 but for ts=2020-05-01
      //                              T2.v1 still solely matches)
      //   new h3 and h4:   +2 + +2 = +4 rows
      //   total +5

      // Update dim_trade: add T3.v1; add T1.v2 (split T1); add T2.v3.
      // (For T1 SCD2 split, we do NOT close T1.v1 because fact_holdings
      // joins dim_trade WITHOUT time bounds — the row stays valid forever.)
      sql(
        """INSERT INTO msj_dim_trade VALUES
          |  ('SK_T3_V1', 'T3', TIMESTAMP'2020-08-15 00:00:00', TIMESTAMP'9999-12-31 00:00:00'),
          |  ('SK_T1_V2', 'T1', TIMESTAMP'2020-08-01 00:00:00', TIMESTAMP'9999-12-31 00:00:00'),
          |  ('SK_T2_V3', 'T2', TIMESTAMP'2020-09-01 00:00:00', TIMESTAMP'9999-12-31 00:00:00')""".stripMargin
      )

      // Update dim_account: close A1.v1, insert A1.v2.
      // SCD2 close-out has to be a MERGE/UPDATE for openivm to track it.
      // We model it as a row replacement: delete old v1, insert new v1
      // (closed end_ts), insert new v2.
      sql(
        s"""MERGE INTO msj_dim_account a
           |USING (SELECT 'ACC_A1_V1' AS sk_account_id) src
           |  ON a.sk_account_id = src.sk_account_id
           |WHEN MATCHED THEN UPDATE SET end_timestamp = TIMESTAMP'2020-08-01 23:59:59'""".stripMargin
      )
      sql(
        """INSERT INTO msj_dim_account VALUES
          |  ('ACC_A1_V2', 'A1', TIMESTAMP'2020-08-02 00:00:00', TIMESTAMP'9999-12-31 00:00:00')""".stripMargin
      )

      // New holdings rows.
      sql(
        """INSERT INTO msj_holdings_history VALUES
          |  ('T3','T1','A2','S1', TIMESTAMP'2020-08-15 10:00:00', 300.0, 30),
          |  ('T1','T0','A1','S1', TIMESTAMP'2020-09-15 10:00:00', 110.0, 11)""".stripMargin
      )

      sql("REFRESH MATERIALIZED VIEW msj_mv").collect()
      assertMvCorrect("msj_mv", FactHoldingsBody)

      // Batch 3 deliberately mixes conflicting DML before one refresh. This
      // exercises every lineage arm, including the second dim_trade
      // occurrence, and verifies that the affected-key envelope covers both
      // preimages and current rows.
      sql(
        """UPDATE msj_holdings_history
          |SET trade_price = 301.0
          |WHERE trade_id = 'T3' AND previous_trade_id = 'T1'""".stripMargin
      )
      sql(
        """DELETE FROM msj_holdings_history
          |WHERE trade_id = 'T3' AND previous_trade_id = 'T1'""".stripMargin
      )
      sql(
        """INSERT INTO msj_holdings_history VALUES
          |  ('T3','T1','A2','S1', TIMESTAMP'2020-08-15 10:00:00', 302.0, 32)""".stripMargin
      )

      // Replace a row read through the previous-trade alias.
      sql("DELETE FROM msj_dim_trade WHERE sk_trade_id = 'SK_T0_V1'")
      sql(
        """INSERT INTO msj_dim_trade VALUES
          |  ('SK_T0_V2', 'T0', TIMESTAMP'2020-01-15 00:00:00', TIMESTAMP'9999-12-31 00:00:00')""".stripMargin
      )

      // Replace projected values on two other lookup arms without changing
      // their join keys.
      sql(
        """UPDATE msj_dim_account
          |SET sk_account_id = 'ACC_A1_V2_REKEY'
          |WHERE sk_account_id = 'ACC_A1_V2'""".stripMargin
      )
      sql("DELETE FROM msj_dim_security WHERE symbol = 'S1'")
      sql("INSERT INTO msj_dim_security VALUES ('SEC_S1_V2', 'S1')")

      sql("REFRESH MATERIALIZED VIEW msj_mv").collect()
      assertMvCorrect("msj_mv", FactHoldingsBody)
    }
  }
}
