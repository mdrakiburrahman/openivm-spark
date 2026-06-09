package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.DataFrame

/** Repro for the OAT-discovered SF=10 failure
  *   `gold.fact_holdings diff=18`
  * (observed in `.temp/ivm-bench/mount/results/10/dbt-server/validation-spark-openivm-batch2.json`).
  *
  * The `gold.fact_holdings` model in `.temp/ivm-bench/.../models/gold/fact_holdings.sql`
  * is a 5-way `SIMPLE_PROJECTION` MV with two structurally fragile join shapes:
  *
  *   1. Self-join on `gold.dim_trade` (aliased `ct using(trade_id)` and
  *      `pt on previous_trade_id = pt.trade_id`).
  *   2. Range-band join on `gold.dim_account`
  *      (`s1.create_timestamp between a.effective_timestamp and a.end_timestamp`).
  *
  * The captured `TpcDiSpec` fixture (≤100 rows / table) cannot reproduce this
  * bug because reference dimensions (`silver.accounts`, `silver.trades_history`)
  * are empty in that slice, so `fact_holdings` is trivially-correct (∅ = ∅).
  *
  * This spec runs the MV in isolation against minimal Delta tables matching
  * the production join shape:
  *
  *   - `sjrb_holdings(trade_id, previous_trade_id, account_id, symbol, create_ts, qty)`
  *   - `sjrb_dim_trade(trade_id, sk_trade_id)`             — multiple SK rows per trade_id
  *   - `sjrb_dim_account(account_id, sk_account_id, eff_ts, end_ts)`  — SCD-2 ranges
  *   - `sjrb_dim_security(symbol, sk_security_id)`         — multiple SK rows per symbol
  *
  * Harness keeps `local[1]` isolation but uses `shuffle.partitions=4` and
  * `adaptive.enabled=true` to mirror the SF=10 bench settings more closely.
  */
abstract class SimpleProjectionSelfJoinRangeBandScenarios
    extends IvmParitySpecBase("simple-projection-self-join-range-band") {
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

  // The MV body mirrors gold.fact_holdings's join shape:
  //   self-join on dim_trade (ct = current, pt = previous) + range-band on dim_account.
  protected val SelfJoinRangeBandSql: String =
    """SELECT
      |  ct.sk_trade_id   AS sk_current_trade_id,
      |  pt.sk_trade_id   AS sk_previous_trade_id,
      |  a.sk_account_id  AS sk_account_id,
      |  s.sk_security_id AS sk_security_id,
      |  h.qty            AS current_holding
      |FROM sjrb_holdings h
      |JOIN sjrb_dim_trade ct
      |  ON h.trade_id = ct.trade_id
      |JOIN sjrb_dim_trade pt
      |  ON h.previous_trade_id = pt.trade_id
      |JOIN sjrb_dim_account a
      |  ON h.account_id = a.account_id
      | AND h.create_ts BETWEEN a.eff_ts AND a.end_ts
      |JOIN sjrb_dim_security s
      |  ON h.symbol = s.symbol""".stripMargin

  describe("SIMPLE_PROJECTION MV with self-join + range-band joins") {

    it("preserves cross-product expansion on incremental INSERT of one holdings row") {
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb_holdings(
          |  trade_id BIGINT, previous_trade_id BIGINT, account_id BIGINT,
          |  symbol STRING, create_ts TIMESTAMP, qty INT
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb_dim_trade(
          |  trade_id BIGINT, sk_trade_id STRING
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb_dim_account(
          |  account_id BIGINT, sk_account_id STRING,
          |  eff_ts TIMESTAMP, end_ts TIMESTAMP
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb_dim_security(
          |  symbol STRING, sk_security_id STRING
          |) USING DELTA""".stripMargin
      )

      // ── BATCH 1: seed a baseline. Tracks the bench's "batch-1 backfill" shape.
      //
      // 3 SCD-2 versions per trade_id  → self-join multiplies to 3 × 3 = 9 per holdings row
      // 1 account version             → 1× from account
      // 2 security versions per symbol → 2× from security
      // 1 holdings row                 → 1 × 9 × 1 × 2 = 18 result rows ← matches SF=10 diff size
      sql(
        """INSERT INTO sjrb_dim_trade VALUES
          |  (4242, 'sk_x_v1'), (4242, 'sk_x_v2'), (4242, 'sk_x_v3'),
          |  (4243, 'sk_y_v1'), (4243, 'sk_y_v2'), (4243, 'sk_y_v3')""".stripMargin
      )
      sql(
        """INSERT INTO sjrb_dim_account VALUES
          |  (13, 'sk_acct_13_v1', TIMESTAMP'2000-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59')""".stripMargin
      )
      sql(
        """INSERT INTO sjrb_dim_security VALUES
          |  ('AAAAAAAAAAAACPQ', 'sk_sec_v1'),
          |  ('AAAAAAAAAAAACPQ', 'sk_sec_v2')""".stripMargin
      )

      // Create the MV BEFORE any holdings exist (mirrors the batch-1 backfill on empty source).
      sql(s"CREATE MATERIALIZED VIEW sjrb_mv AS $SelfJoinRangeBandSql")

      // ── BATCH 2: one new holdings row referencing X and Y (both with multi-version dim_trade).
      //
      // Expected fact rows from this delta:
      //   ct ∈ {sk_x_v1, sk_x_v2, sk_x_v3}  (3 versions)
      // × pt ∈ {sk_y_v1, sk_y_v2, sk_y_v3}  (3 versions)
      // × a  ∈ {sk_acct_13_v1}              (1 version, covers timestamp 2017-04-10)
      // × s  ∈ {sk_sec_v1, sk_sec_v2}       (2 versions of the symbol)
      // = 3 × 3 × 1 × 2 = 18 rows
      //
      // On the buggy openivm-side, the SIMPLE_PROJECTION delta-rule under
      // self-join + range-band drops some of these expansions.
      sql(
        """INSERT INTO sjrb_holdings VALUES
          |  (4242, 4243, 13, 'AAAAAAAAAAAACPQ', TIMESTAMP'2017-04-10 11:39:40', 0)""".stripMargin
      )

      sql("REFRESH MATERIALIZED VIEW sjrb_mv").collect()

      assertMvCorrect("sjrb_mv", SelfJoinRangeBandSql)
    }
    it("propagates upstream DELETE+INSERT on dim_trade (the OPENIVM-BUG.md fact_watches class)") {
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb2_holdings(
          |  trade_id BIGINT, previous_trade_id BIGINT, account_id BIGINT,
          |  symbol STRING, create_ts TIMESTAMP, qty INT
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb2_dim_trade(
          |  trade_id BIGINT, sk_trade_id STRING
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb2_dim_account(
          |  account_id BIGINT, sk_account_id STRING,
          |  eff_ts TIMESTAMP, end_ts TIMESTAMP
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb2_dim_security(
          |  symbol STRING, sk_security_id STRING
          |) USING DELTA""".stripMargin
      )

      val mvSql =
        """SELECT
          |  ct.sk_trade_id   AS sk_current_trade_id,
          |  pt.sk_trade_id   AS sk_previous_trade_id,
          |  a.sk_account_id  AS sk_account_id,
          |  s.sk_security_id AS sk_security_id,
          |  h.qty            AS current_holding
          |FROM sjrb2_holdings h
          |JOIN sjrb2_dim_trade ct
          |  ON h.trade_id = ct.trade_id
          |JOIN sjrb2_dim_trade pt
          |  ON h.previous_trade_id = pt.trade_id
          |JOIN sjrb2_dim_account a
          |  ON h.account_id = a.account_id
          | AND h.create_ts BETWEEN a.eff_ts AND a.end_ts
          |JOIN sjrb2_dim_security s
          |  ON h.symbol = s.symbol""".stripMargin

      // ── BATCH 1: baseline state — 1 version per dim_trade key + 1 holdings row.
      sql("""INSERT INTO sjrb2_dim_trade VALUES (4242, 'sk_x_v1'), (4243, 'sk_y_v1')""")
      sql(
        """INSERT INTO sjrb2_dim_account VALUES
          |  (13, 'sk_acct_13_v1', TIMESTAMP'2000-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59')""".stripMargin
      )
      sql("""INSERT INTO sjrb2_dim_security VALUES ('AAAAAAAAAAAACPQ', 'sk_sec_v1')""")
      sql(
        """INSERT INTO sjrb2_holdings VALUES
          |  (4242, 4243, 13, 'AAAAAAAAAAAACPQ', TIMESTAMP'2017-04-10 11:39:40', 0)""".stripMargin
      )
      sql(s"CREATE MATERIALIZED VIEW sjrb2_mv AS $mvSql")
      assertMvCorrect("sjrb2_mv", mvSql)

      // ── BATCH 2: simulate upstream silver MV emitting DELETE-old + INSERT-new for
      //    the `4242` dim_trade row (semantically: "this trade got a new SCD2 version").
      //    A correct downstream MV must DELETE the old 1 row keyed on sk_x_v1 and
      //    INSERT 1 new row keyed on sk_x_v2.
      //
      //    The OPENIVM-BUG.md failure mode: openivm-spark drops the propagation —
      //    sjrb2_mv ends up with neither the deleted row removed nor the new row
      //    added, so it stays at the old (sk_x_v1, sk_y_v1) tuple.
      sql("""DELETE FROM sjrb2_dim_trade WHERE trade_id = 4242 AND sk_trade_id = 'sk_x_v1'""")
      sql("""INSERT INTO sjrb2_dim_trade VALUES (4242, 'sk_x_v2')""")

      sql("REFRESH MATERIALIZED VIEW sjrb2_mv").collect()
      assertMvCorrect("sjrb2_mv", mvSql)
    }
    it("propagates upstream MIN/MAX aggregate mutations through BETWEEN range joins") {
      // events(k, watch_ts, action_type) — base table whose rows in batch 2
      // mutate the conditional-NULL projection columns in the upstream MV.
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb3_events(
          |  k BIGINT, watch_ts TIMESTAMP, action_type STRING
          |) USING DELTA""".stripMargin
      )
      // dim(k, eff_ts, end_ts) — SCD2 dim that the downstream MV joins against.
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb3_dim(
          |  k BIGINT, sk_dim STRING, eff_ts TIMESTAMP, end_ts TIMESTAMP
          |) USING DELTA""".stripMargin
      )

      // Upstream: GROUP BY k with conditional-NULL projection. Each (k) collapses
      // to one row that holds MIN(placed_timestamp) and MAX(removed_timestamp).
      // When a Cancelled event arrives for an existing (k) that previously only
      // had an Activate event, removed_timestamp flips from NULL to the cancel
      // timestamp. This emits DELETE-old + INSERT-new on the upstream MV.
      val upstreamSql =
        """SELECT k,
          |       MIN(CASE WHEN action_type = 'Activate'  THEN watch_ts END) AS placed_ts,
          |       MAX(CASE WHEN action_type = 'Cancelled' THEN watch_ts END) AS removed_ts
          |FROM sjrb3_events
          |GROUP BY k""".stripMargin

      // Downstream: BETWEEN range join on placed_ts against the SCD2 dim.
      val downstreamSql =
        """SELECT u.k AS k, u.placed_ts AS placed_ts, u.removed_ts AS removed_ts, d.sk_dim AS sk_dim
          |FROM sjrb3_upstream u
          |JOIN sjrb3_dim d
          |  ON u.k = d.k
          | AND u.placed_ts BETWEEN d.eff_ts AND d.end_ts""".stripMargin

      // Set up the SCD2 dim with one version covering all timestamps.
      sql(
        """INSERT INTO sjrb3_dim VALUES
          |  (1, 'sk_dim_1_v1', TIMESTAMP'2000-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59')""".stripMargin
      )

      // ── BATCH 1: one Activate event for k=1 → upstream emits 1 row
      // (placed_ts=2016-01-01, removed_ts=NULL). Downstream BETWEEN join hits
      // the dim version and produces 1 fact row.
      sql(
        """INSERT INTO sjrb3_events VALUES
          |  (1, TIMESTAMP'2016-01-01 12:00:00', 'Activate')""".stripMargin
      )
      sql(s"CREATE MATERIALIZED VIEW sjrb3_upstream AS $upstreamSql")
      sql(s"CREATE MATERIALIZED VIEW sjrb3_downstream AS $downstreamSql")

      assertMvCorrect("sjrb3_upstream", upstreamSql)
      assertMvCorrect("sjrb3_downstream", downstreamSql)

      // ── BATCH 2: one Cancelled event for the SAME k=1. Upstream's row mutates
      // from (placed=2016, removed=NULL) → (placed=2016, removed=2017). This is
      // the canonical OPENIVM-BUG.md trigger: an upstream UPDATE that propagates
      // as DELETE-old + INSERT-new and the downstream MV must mirror the change.
      sql(
        """INSERT INTO sjrb3_events VALUES
          |  (1, TIMESTAMP'2017-01-01 12:00:00', 'Cancelled')""".stripMargin
      )

      sql("REFRESH MATERIALIZED VIEW sjrb3_upstream").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb3_downstream").collect()

      assertMvCorrect("sjrb3_upstream", upstreamSql)
      assertMvCorrect("sjrb3_downstream", downstreamSql)
    }
    it("propagates upstream INSERT (new key) through BETWEEN range joins on 2-deep cascade") {
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb4_events(
          |  k BIGINT, watch_ts TIMESTAMP, action_type STRING
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb4_dim(
          |  k BIGINT, sk_dim STRING, eff_ts TIMESTAMP, end_ts TIMESTAMP
          |) USING DELTA""".stripMargin
      )

      val upstreamSql =
        """SELECT k,
          |       MIN(CASE WHEN action_type = 'Activate'  THEN watch_ts END) AS placed_ts,
          |       MAX(CASE WHEN action_type = 'Cancelled' THEN watch_ts END) AS removed_ts
          |FROM sjrb4_events
          |GROUP BY k""".stripMargin

      val downstreamSql =
        """SELECT u.k AS k, u.placed_ts AS placed_ts, u.removed_ts AS removed_ts, d.sk_dim AS sk_dim
          |FROM sjrb4_upstream u
          |JOIN sjrb4_dim d
          |  ON u.k = d.k
          | AND u.placed_ts BETWEEN d.eff_ts AND d.end_ts""".stripMargin

      // SCD2 dim with versions for both keys.
      sql(
        """INSERT INTO sjrb4_dim VALUES
          |  (1, 'sk_dim_1_v1', TIMESTAMP'2000-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59'),
          |  (2, 'sk_dim_2_v1', TIMESTAMP'2000-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59')""".stripMargin
      )

      // ── BATCH 1: one Activate event for k=1.
      sql(
        """INSERT INTO sjrb4_events VALUES
          |  (1, TIMESTAMP'2016-01-01 12:00:00', 'Activate')""".stripMargin
      )
      sql(s"CREATE MATERIALIZED VIEW sjrb4_upstream AS $upstreamSql")
      sql(s"CREATE MATERIALIZED VIEW sjrb4_downstream AS $downstreamSql")

      assertMvCorrect("sjrb4_upstream", upstreamSql)
      assertMvCorrect("sjrb4_downstream", downstreamSql)

      // ── BATCH 2: brand-new key k=2 Activate event. Upstream emits a pure
      // INSERT (no DELETE — k=2 didn't exist before). Downstream BETWEEN join
      // must produce a new fact row for (k=2, sk_dim_2_v1, placed=2017).
      sql(
        """INSERT INTO sjrb4_events VALUES
          |  (2, TIMESTAMP'2017-07-08 12:00:00', 'Activate')""".stripMargin
      )

      sql("REFRESH MATERIALIZED VIEW sjrb4_upstream").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb4_downstream").collect()

      assertMvCorrect("sjrb4_upstream", upstreamSql)
      assertMvCorrect("sjrb4_downstream", downstreamSql)
    }
    it("propagates batch-2 trade-history INSERT through depth-4 cascade with WINDOW_PARTITION middle") {
      // ── Base layers
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb5_events(
          |  trade_id BIGINT, eff_ts TIMESTAMP, status STRING, price DOUBLE
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb5_holdings_base(
          |  trade_id BIGINT, previous_trade_id BIGINT, account_id BIGINT, symbol STRING, qty INT
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb5_dim_trade(
          |  trade_id BIGINT, sk_trade_id STRING
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb5_dim_account(
          |  account_id BIGINT, sk_account_id STRING,
          |  eff_ts TIMESTAMP, end_ts TIMESTAMP
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb5_dim_security(
          |  symbol STRING, sk_security_id STRING
          |) USING DELTA""".stripMargin
      )

      // L1: SIMPLE_PROJECTION (rename-only, mirrors bronze rename layer)
      val tradeHistorySql =
        """SELECT trade_id, eff_ts AS effective_timestamp, status, price
          |FROM sjrb5_events""".stripMargin

      // L2: WINDOW_PARTITION (mirrors silver.trades's select distinct min/max OVER trade_id)
      val tradesSql =
        """SELECT DISTINCT
          |  trade_id,
          |  price,
          |  MIN(effective_timestamp) OVER (PARTITION BY trade_id) AS create_timestamp,
          |  MAX(effective_timestamp) OVER (PARTITION BY trade_id) AS close_timestamp
          |FROM sjrb5_trade_history""".stripMargin

      // L3: SIMPLE_PROJECTION JOIN (mirrors silver.holdings_history)
      val holdingsSql =
        """SELECT
          |  h.trade_id, h.previous_trade_id, h.account_id, h.symbol, h.qty,
          |  ct.create_timestamp, ct.price
          |FROM sjrb5_holdings_base h
          |JOIN sjrb5_trades ct
          |  ON h.trade_id = ct.trade_id""".stripMargin

      // L4: SIMPLE_PROJECTION self-join + range-band (mirrors gold.fact_holdings)
      val factSql =
        """SELECT
          |  ct.sk_trade_id   AS sk_current_trade_id,
          |  pt.sk_trade_id   AS sk_previous_trade_id,
          |  a.sk_account_id  AS sk_account_id,
          |  s.sk_security_id AS sk_security_id,
          |  h.qty            AS current_holding,
          |  h.price          AS current_price
          |FROM sjrb5_holdings h
          |JOIN sjrb5_dim_trade ct
          |  ON h.trade_id = ct.trade_id
          |JOIN sjrb5_dim_trade pt
          |  ON h.previous_trade_id = pt.trade_id
          |JOIN sjrb5_dim_account a
          |  ON h.account_id = a.account_id
          | AND h.create_timestamp BETWEEN a.eff_ts AND a.end_ts
          |JOIN sjrb5_dim_security s
          |  ON h.symbol = s.symbol""".stripMargin

      // ── BATCH 1: seed everything with a baseline matching one fact row.
      // One trade_id=100 with two SCD2 versions (status PNDG / CMPT), price changes.
      sql(
        """INSERT INTO sjrb5_events VALUES
          |  (100, TIMESTAMP'2017-04-10 11:00:00', 'PNDG', 5.0),
          |  (100, TIMESTAMP'2017-04-10 11:39:40', 'CMPT', 5.53),
          |  (101, TIMESTAMP'2017-04-09 10:00:00', 'CMPT', 4.99)""".stripMargin
      )
      sql(
        """INSERT INTO sjrb5_dim_trade VALUES (100, 'sk_t100_v1'), (101, 'sk_t101_v1')""".stripMargin
      )
      sql(
        """INSERT INTO sjrb5_dim_account VALUES
          |  (13, 'sk_acct_13_v1', TIMESTAMP'2000-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59')""".stripMargin
      )
      sql("""INSERT INTO sjrb5_dim_security VALUES ('AAA', 'sk_sec_AAA_v1')""")
      sql(
        """INSERT INTO sjrb5_holdings_base VALUES
          |  (100, 101, 13, 'AAA', 0)""".stripMargin
      )

      sql(s"CREATE MATERIALIZED VIEW sjrb5_trade_history AS $tradeHistorySql")
      sql(s"CREATE MATERIALIZED VIEW sjrb5_trades AS $tradesSql")
      sql(s"CREATE MATERIALIZED VIEW sjrb5_holdings AS $holdingsSql")
      sql(s"CREATE MATERIALIZED VIEW sjrb5_fact AS $factSql")

      assertMvCorrect("sjrb5_trade_history", tradeHistorySql)
      assertMvCorrect("sjrb5_trades", tradesSql)
      assertMvCorrect("sjrb5_holdings", holdingsSql)
      assertMvCorrect("sjrb5_fact", factSql)

      // ── BATCH 2: insert a NEW status event for an EXISTING trade_id=100. This
      // mutates silver.trades's min/max(eff_ts) for trade_id=100, which in turn
      // propagates to silver.holdings_history (existing holdings row's
      // create_timestamp shifts) and then to fact_holdings.
      //
      // The depth-4 cascade with a WINDOW_PARTITION middle is where openivm-spark
      // is documented to be flaky. We refresh in DAG order; each REFRESH must
      // produce a correct delta for the next layer.
      sql(
        """INSERT INTO sjrb5_events VALUES
          |  (100, TIMESTAMP'2017-04-10 10:50:00', 'PNDG', 4.95)""".stripMargin
      )

      sql("REFRESH MATERIALIZED VIEW sjrb5_trade_history").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb5_trades").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb5_holdings").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb5_fact").collect()

      assertMvCorrect("sjrb5_trade_history", tradeHistorySql)
      assertMvCorrect("sjrb5_trades", tradesSql)
      assertMvCorrect("sjrb5_holdings", holdingsSql)
      assertMvCorrect("sjrb5_fact", factSql)
    }
    it("propagates batch-2 holdings INSERT through depth-4 cascade with WINDOW_PARTITION middle") {
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb6_events(
          |  trade_id BIGINT, eff_ts TIMESTAMP, status STRING, price DOUBLE
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb6_holdings_base(
          |  trade_id BIGINT, previous_trade_id BIGINT, account_id BIGINT, symbol STRING, qty INT
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb6_dim_trade(
          |  trade_id BIGINT, sk_trade_id STRING
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb6_dim_account(
          |  account_id BIGINT, sk_account_id STRING,
          |  eff_ts TIMESTAMP, end_ts TIMESTAMP
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb6_dim_security(
          |  symbol STRING, sk_security_id STRING
          |) USING DELTA""".stripMargin
      )

      val tradeHistorySql =
        """SELECT trade_id, eff_ts AS effective_timestamp, status, price
          |FROM sjrb6_events""".stripMargin

      val tradesSql =
        """SELECT DISTINCT
          |  trade_id,
          |  price,
          |  MIN(effective_timestamp) OVER (PARTITION BY trade_id) AS create_timestamp,
          |  MAX(effective_timestamp) OVER (PARTITION BY trade_id) AS close_timestamp
          |FROM sjrb6_trade_history""".stripMargin

      val holdingsSql =
        """SELECT
          |  h.trade_id, h.previous_trade_id, h.account_id, h.symbol, h.qty,
          |  ct.create_timestamp, ct.price
          |FROM sjrb6_holdings_base h
          |JOIN sjrb6_trades ct
          |  ON h.trade_id = ct.trade_id""".stripMargin

      val factSql =
        """SELECT
          |  ct.sk_trade_id   AS sk_current_trade_id,
          |  pt.sk_trade_id   AS sk_previous_trade_id,
          |  a.sk_account_id  AS sk_account_id,
          |  s.sk_security_id AS sk_security_id,
          |  h.qty            AS current_holding,
          |  h.price          AS current_price
          |FROM sjrb6_holdings h
          |JOIN sjrb6_dim_trade ct
          |  ON h.trade_id = ct.trade_id
          |JOIN sjrb6_dim_trade pt
          |  ON h.previous_trade_id = pt.trade_id
          |JOIN sjrb6_dim_account a
          |  ON h.account_id = a.account_id
          | AND h.create_timestamp BETWEEN a.eff_ts AND a.end_ts
          |JOIN sjrb6_dim_security s
          |  ON h.symbol = s.symbol""".stripMargin

      // ── BATCH 1: seed events for trade_ids 200, 201, 202 — each with 3 status versions.
      //   trade 200: PNDG → SBMT → CMPT (price changes each step)
      //   trade 201: PNDG → SBMT → CMPT
      //   trade 202: PNDG → SBMT → CMPT
      sql(
        """INSERT INTO sjrb6_events VALUES
          |  (200, TIMESTAMP'2017-04-10 11:00:00', 'PNDG', 5.0),
          |  (200, TIMESTAMP'2017-04-10 11:20:00', 'SBMT', 5.25),
          |  (200, TIMESTAMP'2017-04-10 11:39:40', 'CMPT', 5.53),
          |  (201, TIMESTAMP'2017-04-09 10:00:00', 'PNDG', 4.99),
          |  (201, TIMESTAMP'2017-04-09 10:10:00', 'SBMT', 5.00),
          |  (201, TIMESTAMP'2017-04-09 10:30:00', 'CMPT', 5.02),
          |  (202, TIMESTAMP'2017-04-08 12:00:00', 'PNDG', 6.10),
          |  (202, TIMESTAMP'2017-04-08 12:15:00', 'SBMT', 6.12),
          |  (202, TIMESTAMP'2017-04-08 12:30:00', 'CMPT', 6.14)""".stripMargin
      )
      // dim_trade: 1 SK per trade_id.
      sql(
        """INSERT INTO sjrb6_dim_trade VALUES
          |  (200, 'sk_t200_v1'),
          |  (201, 'sk_t201_v1'),
          |  (202, 'sk_t202_v1')""".stripMargin
      )
      sql(
        """INSERT INTO sjrb6_dim_account VALUES
          |  (13, 'sk_acct_13_v1', TIMESTAMP'2000-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59')""".stripMargin
      )
      sql("""INSERT INTO sjrb6_dim_security VALUES ('AAA', 'sk_sec_AAA_v1')""")
      // No holdings in batch 1.

      sql(s"CREATE MATERIALIZED VIEW sjrb6_trade_history AS $tradeHistorySql")
      sql(s"CREATE MATERIALIZED VIEW sjrb6_trades AS $tradesSql")
      sql(s"CREATE MATERIALIZED VIEW sjrb6_holdings AS $holdingsSql")
      sql(s"CREATE MATERIALIZED VIEW sjrb6_fact AS $factSql")

      assertMvCorrect("sjrb6_trade_history", tradeHistorySql)
      assertMvCorrect("sjrb6_trades", tradesSql)
      assertMvCorrect("sjrb6_holdings", holdingsSql)
      assertMvCorrect("sjrb6_fact", factSql)

      // ── BATCH 2: insert a new holdings_base row referencing trade 200 (current)
      // and trade 201 (previous). silver.holdings_history (sjrb6_holdings) needs
      // to multiply this against the WP-derived sjrb6_trades rows for trade_id=200,
      // and fact_holdings must downstream-propagate to a single new row.
      sql(
        """INSERT INTO sjrb6_holdings_base VALUES
          |  (200, 201, 13, 'AAA', 100)""".stripMargin
      )

      sql("REFRESH MATERIALIZED VIEW sjrb6_trade_history").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb6_trades").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb6_holdings").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb6_fact").collect()

      assertMvCorrect("sjrb6_trade_history", tradeHistorySql)
      assertMvCorrect("sjrb6_trades", tradesSql)
      assertMvCorrect("sjrb6_holdings", holdingsSql)
      assertMvCorrect("sjrb6_fact", factSql)
    }

    it(
      "repros fact_holdings 18-row diff: NULL-priced silver.trades × multi-version dim_trade × range-band dim_account"
    ) {
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb7_trade_events(
          |  trade_id BIGINT,
          |  effective_timestamp TIMESTAMP,
          |  status STRING,
          |  trade_price DOUBLE,
          |  fee DOUBLE,
          |  commission DOUBLE,
          |  sk_trade_id STRING
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb7_brokerage_holding_history(
          |  trade_id BIGINT,
          |  previous_trade_id BIGINT,
          |  account_id BIGINT,
          |  symbol STRING,
          |  quantity INT,
          |  bid_price DOUBLE
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb7_account_versions(
          |  account_id BIGINT,
          |  sk_customer_id STRING,
          |  sk_account_id STRING,
          |  effective_timestamp TIMESTAMP,
          |  end_timestamp TIMESTAMP
          |) USING DELTA""".stripMargin
      )
      sql(
        """CREATE TABLE IF NOT EXISTS sjrb7_security_versions(
          |  symbol STRING,
          |  sk_security_id STRING,
          |  effective_timestamp TIMESTAMP,
          |  end_timestamp TIMESTAMP
          |) USING DELTA""".stripMargin
      )

      val tradesHistorySql =
        """SELECT
          |  trade_id,
          |  effective_timestamp,
          |  status,
          |  trade_price,
          |  fee,
          |  commission,
          |  sk_trade_id
          |FROM sjrb7_trade_events""".stripMargin

      val tradesSql =
        """SELECT DISTINCT
          |  trade_id,
          |  status,
          |  trade_price,
          |  fee,
          |  commission,
          |  MIN(effective_timestamp) OVER (PARTITION BY trade_id) AS create_timestamp,
          |  MAX(effective_timestamp) OVER (PARTITION BY trade_id) AS close_timestamp
          |FROM sjrb7_trades_history""".stripMargin

      val dimTradeSql =
        """SELECT
          |  trade_id,
          |  effective_timestamp,
          |  sk_trade_id
          |FROM sjrb7_trades_history""".stripMargin

      val dimAccountSql =
        """SELECT
          |  account_id,
          |  sk_customer_id,
          |  sk_account_id,
          |  effective_timestamp,
          |  end_timestamp
          |FROM sjrb7_account_versions""".stripMargin

      val dimSecuritySql =
        """SELECT
          |  symbol,
          |  sk_security_id,
          |  effective_timestamp,
          |  end_timestamp
          |FROM sjrb7_security_versions""".stripMargin

      val holdingsHistorySql =
        """SELECT
          |  h.trade_id,
          |  h.previous_trade_id,
          |  h.account_id,
          |  h.symbol,
          |  h.quantity,
          |  h.bid_price,
          |  t.status,
          |  t.trade_price,
          |  t.fee,
          |  t.commission,
          |  t.create_timestamp,
          |  t.close_timestamp
          |FROM sjrb7_brokerage_holding_history h
          |JOIN sjrb7_trades t
          |  ON h.trade_id = t.trade_id""".stripMargin

      val factHoldingsSql =
        """SELECT
          |  ct.sk_trade_id AS sk_current_trade_id,
          |  pt.sk_trade_id AS sk_trade_id,
          |  a.sk_customer_id AS sk_customer_id,
          |  a.sk_account_id AS sk_account_id,
          |  s.sk_security_id AS sk_security_id,
          |  CAST(h.create_timestamp AS DATE) AS sk_trade_date,
          |  h.create_timestamp AS trade_timestamp,
          |  h.trade_price AS current_price,
          |  h.quantity AS current_holding,
          |  h.bid_price AS current_bid_price,
          |  h.fee AS current_fee,
          |  h.commission AS current_commission
          |FROM sjrb7_holdings_history h
          |JOIN sjrb7_dim_trade ct
          |  ON h.trade_id = ct.trade_id
          |JOIN sjrb7_dim_trade pt
          |  ON h.previous_trade_id = pt.trade_id
          |JOIN sjrb7_dim_account a
          |  ON h.account_id = a.account_id
          | AND h.create_timestamp BETWEEN a.effective_timestamp AND a.end_timestamp
          |JOIN sjrb7_dim_security s
          |  ON h.symbol = s.symbol""".stripMargin

      sql(
        """INSERT INTO sjrb7_trade_events VALUES
          |  (42, TIMESTAMP'2017-04-10 11:00:00', 'SBMT', NULL, NULL, NULL, 'sk_t42_sbmt'),
          |  (42, TIMESTAMP'2017-04-10 11:20:00', 'PNDG', NULL, NULL, NULL, 'sk_t42_pndg'),
          |  (99, TIMESTAMP'2017-04-09 09:00:00', 'SBMT', NULL, NULL, NULL, 'sk_t99_sbmt'),
          |  (99, TIMESTAMP'2017-04-09 09:15:00', 'PNDG', NULL, NULL, NULL, 'sk_t99_pndg'),
          |  (99, TIMESTAMP'2017-04-09 09:45:00', 'CMPT', 6.66, 12.34, 5.67, 'sk_t99_cmpt')""".stripMargin
      )
      sql(
        """INSERT INTO sjrb7_account_versions VALUES
          |  (777, 'sk_cust_777_old', 'sk_acct_777_old',
          |   TIMESTAMP'2016-01-01 00:00:00', TIMESTAMP'2017-04-01 00:00:00')""".stripMargin
      )
      sql(
        """INSERT INTO sjrb7_security_versions VALUES
          |  ('AAATEST', 'sk_sec_AAATEST_v1',
          |   TIMESTAMP'2010-01-01 00:00:00', TIMESTAMP'2017-04-15 00:00:00')""".stripMargin
      )

      sql(s"CREATE MATERIALIZED VIEW sjrb7_trades_history AS $tradesHistorySql")
      sql(s"CREATE MATERIALIZED VIEW sjrb7_trades AS $tradesSql")
      sql(s"CREATE MATERIALIZED VIEW sjrb7_dim_trade AS $dimTradeSql")
      sql(s"CREATE MATERIALIZED VIEW sjrb7_dim_account AS $dimAccountSql")
      sql(s"CREATE MATERIALIZED VIEW sjrb7_dim_security AS $dimSecuritySql")
      sql(s"CREATE MATERIALIZED VIEW sjrb7_holdings_history AS $holdingsHistorySql")
      sql(s"CREATE MATERIALIZED VIEW sjrb7_fact_holdings AS $factHoldingsSql")

      assertMvCorrect("sjrb7_trades_history", tradesHistorySql)
      assertMvCorrect("sjrb7_trades", tradesSql)
      assertMvCorrect("sjrb7_dim_trade", dimTradeSql)
      assertMvCorrect("sjrb7_dim_account", dimAccountSql)
      assertMvCorrect("sjrb7_dim_security", dimSecuritySql)
      assertMvCorrect("sjrb7_holdings_history", holdingsHistorySql)
      assertMvCorrect("sjrb7_fact_holdings", factHoldingsSql)

      sql(
        """INSERT INTO sjrb7_brokerage_holding_history VALUES
          |  (42, 99, 777, 'AAATEST', 18, 5.50)""".stripMargin
      )
      sql(
        """INSERT INTO sjrb7_account_versions VALUES
          |  (777, 'sk_cust_777_current', 'sk_acct_777_current',
          |   TIMESTAMP'2017-04-01 00:00:01', TIMESTAMP'9999-12-31 23:59:59')""".stripMargin
      )
      sql(
        """INSERT INTO sjrb7_security_versions VALUES
          |  ('AAATEST', 'sk_sec_AAATEST_v2',
          |   TIMESTAMP'2017-04-05 00:00:00', TIMESTAMP'9999-12-31 23:59:59')""".stripMargin
      )
      sql(
        """INSERT INTO sjrb7_trade_events VALUES
          |  (42, TIMESTAMP'2017-04-10 11:39:40', 'CMPT', 5.54, 53.84, 10.79, 'sk_t42_cmpt')""".stripMargin
      )

      sql("REFRESH MATERIALIZED VIEW sjrb7_trades_history").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb7_trades").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb7_dim_trade").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb7_dim_account").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb7_dim_security").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb7_holdings_history").collect()
      sql("REFRESH MATERIALIZED VIEW sjrb7_fact_holdings").collect()

      assertMvCorrect("sjrb7_trades_history", tradesHistorySql)
      assertMvCorrect("sjrb7_trades", tradesSql)
      assertMvCorrect("sjrb7_dim_trade", dimTradeSql)
      assertMvCorrect("sjrb7_dim_account", dimAccountSql)
      assertMvCorrect("sjrb7_dim_security", dimSecuritySql)
      assertMvCorrect("sjrb7_holdings_history", holdingsHistorySql)
      assertMvCorrect("sjrb7_fact_holdings", factHoldingsSql)
    }
  }
}
