package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.MvCatalog

/** Parity coverage for the compile-bridge shims that keep Spark's 1-arg / 2-arg
  * `to_date`, 1-arg / 2-arg `to_timestamp`, `date_format`, the Spark-only
  * `last_value(expr, ignoreNulls)` / `first_value(expr, ignoreNulls)` window
  * forms, `current_date()` / `current_timestamp()` (incl.
  * `CAST(CURRENT_TIMESTAMP() AS TIMESTAMP)`), and Spark's backslash-escaped
  * string-literal convention (e.g. the nested-REPLACE `normalize_os_name`
  * fragment) working end-to-end through Spark SQL parse -> compile bridge ->
  * DuckDB compile -> refresh SQL -> Spark execution.
  *
  * All table / MV names are prefixed with `cbdf_` so parallel forked specs do
  * not collide on Delta warehouse paths.
  */
abstract class CompileBridgeDateFunctionsScenarios extends IvmParitySpecBase("compile-bridge-date-functions") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def assertIncremental(mvName: String): Unit = {
    val id   = spark.sessionState.sqlParser.parseTableIdentifier(mvName)
    val meta = MvCatalog.lookup(spark, id).getOrElse(fail(s"Missing MV metadata for $mvName"))
    meta.refreshTypeName should not equal "FULL_REFRESH"
  }

  describe("compile-bridge shim: to_date(raw)") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      sql("CREATE TABLE cbdf_to_date_iso_src (id INT, trade_date_raw STRING) USING DELTA")
      sql(
        "INSERT INTO cbdf_to_date_iso_src VALUES (1, '2024-01-01'), (2, '2024-01-15'), (3, '2024-02-01')"
      )

      val mvName   = "cbdf_mv_to_date_iso"
      val viewBody = "SELECT id, to_date(trade_date_raw) AS trade_date FROM cbdf_to_date_iso_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)

      sql("INSERT INTO cbdf_to_date_iso_src VALUES (4, '2024-02-15'), (5, '2024-03-01')")
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: to_date(timestamp_col) in gold-style CTE + join") {
    it("compiles and stays correct after INSERT + REFRESH") {
      sql(
        "CREATE TABLE cbdf_cash_transactions_src (account_id INT, transaction_timestamp TIMESTAMP, amount DOUBLE, description STRING) USING DELTA"
      )
      sql(
        "CREATE TABLE cbdf_dim_account_src (account_id INT, effective_timestamp TIMESTAMP, end_timestamp TIMESTAMP, sk_customer_id INT, sk_account_id INT) USING DELTA"
      )
      sql(
        "INSERT INTO cbdf_dim_account_src VALUES " +
          "(10, TIMESTAMP'2023-12-01 00:00:00', TIMESTAMP'2024-12-31 23:59:59', 100, 1000), " +
          "(20, TIMESTAMP'2023-12-01 00:00:00', TIMESTAMP'2024-12-31 23:59:59', 200, 2000)"
      )
      sql(
        "INSERT INTO cbdf_cash_transactions_src VALUES " +
          "(10, TIMESTAMP'2024-01-01 09:15:00', 125.5D, 'seed cash'), " +
          "(10, TIMESTAMP'2024-01-15 10:30:45', 20.0D, 'fee rebate'), " +
          "(20, TIMESTAMP'2024-02-01 00:00:00', 99.9D, 'wire in')"
      )

      val mvName = "cbdf_mv_fact_cash_transactions"
      val viewBody =
        """WITH s1 AS (
          |  SELECT
          |    *,
          |    to_date(transaction_timestamp) AS sk_transaction_date
          |  FROM cbdf_cash_transactions_src
          |)
          |SELECT
          |  sk_customer_id,
          |  sk_account_id,
          |  sk_transaction_date,
          |  transaction_timestamp,
          |  amount,
          |  description
          |FROM s1
          |JOIN cbdf_dim_account_src a
          |  ON s1.account_id = a.account_id
          | AND s1.transaction_timestamp BETWEEN a.effective_timestamp AND a.end_timestamp""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")

      // The translated `to_date(timestamp_col)` body now keeps the MV
      // incremental even through the multi-source CTE + JOIN shape.
      val createMeta = MvCatalog
        .lookup(
          spark,
          spark.sessionState.sqlParser.parseTableIdentifier(mvName)
        )
        .getOrElse(fail(s"Missing MV metadata for $mvName"))
      createMeta.refreshTypeName should not equal "FULL_REFRESH"
      assertMvCorrect(mvName, viewBody)

      sql(
        "INSERT INTO cbdf_cash_transactions_src VALUES " +
          "(10, TIMESTAMP'2024-02-15 12:00:00', 75.25D, 'cash out'), " +
          "(20, TIMESTAMP'2024-03-01 18:45:00', 250.0D, 'bonus')"
      )
      refreshMv(mvName)
      val refreshedMeta = MvCatalog
        .lookup(
          spark,
          spark.sessionState.sqlParser.parseTableIdentifier(mvName)
        )
        .getOrElse(fail(s"Missing MV metadata for $mvName"))
      refreshedMeta.refreshTypeName should not equal "FULL_REFRESH"
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: to_date(raw, fmt)") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      sql("CREATE TABLE cbdf_to_date_src (id INT, trade_date_raw STRING) USING DELTA")
      sql(
        "INSERT INTO cbdf_to_date_src VALUES (1, '20240101'), (2, '20240115'), (3, '20240201')"
      )

      val mvName   = "cbdf_mv_to_date"
      val viewBody = "SELECT id, to_date(trade_date_raw, 'yyyyMMdd') AS trade_date FROM cbdf_to_date_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)

      sql("INSERT INTO cbdf_to_date_src VALUES (4, '20240215'), (5, '20240301')")
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: to_timestamp(raw, fmt)") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      sql("CREATE TABLE cbdf_to_timestamp_src (id INT, action_ts_raw STRING) USING DELTA")
      sql(
        "INSERT INTO cbdf_to_timestamp_src VALUES " +
          "(1, '2024-01-01 09:15:00'), (2, '2024-01-01 09:30:45'), (3, '2024-01-02 10:45:59')"
      )

      val mvName = "cbdf_mv_to_timestamp"
      val viewBody =
        "SELECT id, to_timestamp(action_ts_raw, 'yyyy-MM-dd HH:mm:ss') AS action_ts FROM cbdf_to_timestamp_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)

      sql(
        "INSERT INTO cbdf_to_timestamp_src VALUES (4, '2024-01-03 00:00:00'), (5, '2024-01-03 12:34:56')"
      )
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: to_timestamp(bigint_col) in bronze-style UNION ALL") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      sql(
        "CREATE TABLE cbdf_customer_cdc_src (cdc_dsn BIGINT, cdc_flag STRING, customerid BIGINT, taxid STRING, gender STRING, tier INT, dob DATE, lastname STRING, firstname STRING, middleinitial STRING, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, email1 STRING, email2 STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, lcl_tx_id STRING, nat_tx_id STRING) USING DELTA"
      )
      sql(
        "CREATE TABLE cbdf_account_cdc_src (cdc_dsn BIGINT, cdc_flag STRING, ca_c_id BIGINT, accountid BIGINT, taxstatus INT, ca_b_id BIGINT, accountdesc STRING) USING DELTA"
      )
      sql(
        "INSERT INTO cbdf_customer_cdc_src VALUES " +
          "(1704100500, 'I', 1, 'TAX-1', 'F', 1, DATE'1990-01-01', 'Doe', 'Jane', 'Q', '1 Main', 'Apt 2', '11111', 'Seattle', 'WA', 'US', 'jane@example.com', 'jane.alt@example.com', '1', '206', '5550101', '11', '1', '206', '5550102', '12', '1', '206', '5550103', '13', 'LCL-1', 'NAT-1'), " +
          "(1704101445, 'U', 2, 'TAX-2', 'M', 2, DATE'1988-02-02', 'Roe', 'John', 'R', '2 Pine', NULL, '22222', 'Portland', 'OR', 'US', 'john@example.com', 'john.alt@example.com', '1', '503', '5550201', NULL, '1', '503', '5550202', NULL, '1', '503', '5550203', NULL, 'LCL-2', 'NAT-2')"
      )
      sql(
        "INSERT INTO cbdf_account_cdc_src VALUES " +
          "(1704100500, 'I', 1, 1001, 1, 77, 'Checking'), " +
          "(1704101445, 'U', 2, 2002, 2, 88, 'Brokerage')"
      )

      val mvName = "cbdf_mv_crm_customer_mgmt"
      val viewBody =
        """WITH staging_customers AS (
          |  SELECT
          |    to_timestamp(cdc_dsn) AS action_ts,
          |    CASE cdc_flag WHEN 'I' THEN 'NEW' WHEN 'U' THEN 'UPDCUST' END AS action_type,
          |    CAST(customerid AS BIGINT) AS c_id,
          |    taxid AS c_tax_id,
          |    gender AS c_gndr,
          |    CAST(tier AS INT) AS c_tier,
          |    dob AS c_dob,
          |    lastname AS c_l_name,
          |    firstname AS c_f_name,
          |    middleinitial AS c_m_name,
          |    addressline1 AS c_adline1,
          |    addressline2 AS c_adline2,
          |    postalcode AS c_zipcode,
          |    city AS c_city,
          |    stateprov AS c_state_prov,
          |    country AS c_ctry,
          |    email1 AS c_prim_email,
          |    email2 AS c_alt_email,
          |    concat_ws('-', c_ctry_1, c_area_1, c_local_1, c_ext_1) AS c_phone_1,
          |    concat_ws('-', c_ctry_2, c_area_2, c_local_2, c_ext_2) AS c_phone_2,
          |    concat_ws('-', c_ctry_3, c_area_3, c_local_3, c_ext_3) AS c_phone_3,
          |    lcl_tx_id AS c_lcl_tx_id,
          |    nat_tx_id AS c_nat_tx_id,
          |    CAST(NULL AS BIGINT) AS ca_id,
          |    CAST(NULL AS INT) AS ca_tax_st,
          |    CAST(NULL AS BIGINT) AS ca_b_id,
          |    CAST(NULL AS STRING) AS ca_name
          |  FROM cbdf_customer_cdc_src
          |  WHERE cdc_flag IN ('I', 'U')
          |),
          |staging_accounts AS (
          |  SELECT
          |    to_timestamp(cdc_dsn) AS action_ts,
          |    CASE cdc_flag WHEN 'I' THEN 'ADDACCT' WHEN 'U' THEN 'UPDACCT' END AS action_type,
          |    CAST(ca_c_id AS BIGINT) AS c_id,
          |    CAST(NULL AS STRING) AS c_tax_id,
          |    CAST(NULL AS STRING) AS c_gndr,
          |    CAST(NULL AS INT) AS c_tier,
          |    CAST(NULL AS DATE) AS c_dob,
          |    CAST(NULL AS STRING) AS c_l_name,
          |    CAST(NULL AS STRING) AS c_f_name,
          |    CAST(NULL AS STRING) AS c_m_name,
          |    CAST(NULL AS STRING) AS c_adline1,
          |    CAST(NULL AS STRING) AS c_adline2,
          |    CAST(NULL AS STRING) AS c_zipcode,
          |    CAST(NULL AS STRING) AS c_city,
          |    CAST(NULL AS STRING) AS c_state_prov,
          |    CAST(NULL AS STRING) AS c_ctry,
          |    CAST(NULL AS STRING) AS c_prim_email,
          |    CAST(NULL AS STRING) AS c_alt_email,
          |    CAST(NULL AS STRING) AS c_phone_1,
          |    CAST(NULL AS STRING) AS c_phone_2,
          |    CAST(NULL AS STRING) AS c_phone_3,
          |    CAST(NULL AS STRING) AS c_lcl_tx_id,
          |    CAST(NULL AS STRING) AS c_nat_tx_id,
          |    CAST(accountid AS BIGINT) AS ca_id,
          |    CAST(taxstatus AS INT) AS ca_tax_st,
          |    CAST(ca_b_id AS BIGINT) AS ca_b_id,
          |    accountdesc AS ca_name
          |  FROM cbdf_account_cdc_src
          |  WHERE cdc_flag IN ('I', 'U')
          |)
          |SELECT * FROM staging_customers
          |UNION ALL
          |SELECT * FROM staging_accounts""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)

      sql(
        "INSERT INTO cbdf_customer_cdc_src VALUES " +
          "(1704240000, 'I', 3, 'TAX-3', 'F', 3, DATE'1995-03-03', 'Smith', 'Alex', NULL, '3 Cedar', NULL, '33333', 'San Francisco', 'CA', 'US', 'alex@example.com', NULL, '1', '415', '5550301', NULL, '1', '415', '5550302', NULL, '1', '415', '5550303', NULL, 'LCL-3', 'NAT-3')"
      )
      sql(
        "INSERT INTO cbdf_account_cdc_src VALUES (1704285296, 'I', 3, 3003, 1, 99, 'Retirement')"
      )
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: date_format(ts, fmt)") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      sql("CREATE TABLE cbdf_date_format_src (id INT, action_ts TIMESTAMP) USING DELTA")
      sql(
        "INSERT INTO cbdf_date_format_src VALUES " +
          "(1, TIMESTAMP'2024-01-01 09:15:00'), (2, TIMESTAMP'2024-01-01 09:30:45'), " +
          "(3, TIMESTAMP'2024-01-02 10:45:59')"
      )

      val mvName   = "cbdf_mv_date_format"
      val viewBody = "SELECT id, date_format(action_ts, 'yyyyMMdd') AS action_day FROM cbdf_date_format_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)

      sql(
        "INSERT INTO cbdf_date_format_src VALUES (4, TIMESTAMP'2024-01-03 00:00:00'), (5, TIMESTAMP'2024-01-03 12:34:56')"
      )
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: last_value(expr, ignoreNulls)") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      sql(
        "CREATE TABLE cbdf_last_value_src (id INT, customer_id INT, effective_ts TIMESTAMP, status STRING) USING DELTA"
      )
      sql(
        "INSERT INTO cbdf_last_value_src VALUES " +
          "(1, 10, TIMESTAMP'2024-01-01 09:00:00', 'bronze'), " +
          "(2, 10, TIMESTAMP'2024-01-02 09:00:00', 'silver'), " +
          "(3, 20, TIMESTAMP'2024-01-01 12:00:00', 'starter')"
      )

      // Literal `ignoreNulls = true` is normalized to DuckDB's native
      // `IGNORE NULLS` modifier and restored to Spark's 2-arg spelling.
      val mvName = "cbdf_mv_last_value"
      val viewBody =
        "SELECT id, customer_id, effective_ts, " +
          "last_value(status, true) OVER (PARTITION BY customer_id ORDER BY effective_ts) AS carried_status " +
          "FROM cbdf_last_value_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)

      sql(
        "INSERT INTO cbdf_last_value_src VALUES " +
          "(4, 10, TIMESTAMP'2024-01-03 09:00:00', 'gold'), " +
          "(5, 20, TIMESTAMP'2024-01-02 12:00:00', 'growth')"
      )
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: first_value(expr, ignoreNulls)") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      sql(
        "CREATE TABLE cbdf_first_value_src (id INT, customer_id INT, effective_ts TIMESTAMP, status STRING) USING DELTA"
      )
      sql(
        "INSERT INTO cbdf_first_value_src VALUES " +
          "(1, 10, TIMESTAMP'2024-01-01 09:00:00', 'bronze'), " +
          "(2, 10, TIMESTAMP'2024-01-02 09:00:00', 'silver'), " +
          "(3, 20, TIMESTAMP'2024-01-01 12:00:00', 'starter')"
      )

      // Mirrors the last_value(expr, ignoreNulls) case above: Spark's
      // FIRST_VALUE(expr, true) is normalized to DuckDB's native
      // `IGNORE NULLS` modifier and restored to Spark's 2-arg spelling.
      val mvName = "cbdf_mv_first_value"
      val viewBody =
        "SELECT id, customer_id, effective_ts, " +
          "first_value(status, true) OVER (PARTITION BY customer_id ORDER BY effective_ts) AS onboarded_status " +
          "FROM cbdf_first_value_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)

      sql(
        "INSERT INTO cbdf_first_value_src VALUES " +
          "(4, 10, TIMESTAMP'2024-01-03 09:00:00', 'gold'), " +
          "(5, 20, TIMESTAMP'2024-01-02 12:00:00', 'growth')"
      )
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: current_date()") {
    it("compiles and refreshes without error, and returns today's date") {
      sql("CREATE TABLE cbdf_current_date_src (id INT, region STRING) USING DELTA")
      sql("INSERT INTO cbdf_current_date_src VALUES (1, 'east'), (2, 'west')")

      // Not asserted via assertIncremental: current_date() is a Spark
      // volatile/non-deterministic-per-refresh expression, so which refresh
      // classification openivm assigns it is an implementation detail, not
      // a contract this test should pin down.
      val mvName   = "cbdf_mv_current_date"
      val viewBody = "SELECT id, region, current_date() AS loaded_on FROM cbdf_current_date_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertMvCorrect(mvName, viewBody)

      sql("INSERT INTO cbdf_current_date_src VALUES (3, 'north')")
      refreshMv(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }

  describe("compile-bridge shim: current_timestamp() and CAST(CURRENT_TIMESTAMP() AS TIMESTAMP)") {
    it("compiles and refreshes without error, producing a fresh non-null timestamp for both spellings") {
      sql("CREATE TABLE cbdf_current_timestamp_src (id INT, region STRING) USING DELTA")
      sql("INSERT INTO cbdf_current_timestamp_src VALUES (1, 'east'), (2, 'west')")

      // Both Spark spellings of "now" appear in the benchmark corpus and
      // must independently survive the compile bridge. Values are not
      // compared against a freshly re-evaluated `expectedSql` (unlike
      // assertMvCorrect elsewhere in this file) because two independent
      // CURRENT_TIMESTAMP evaluations are never bit-identical; instead we
      // assert non-null and a recent wall-clock value.
      val mvName = "cbdf_mv_current_timestamp"
      val viewBody =
        "SELECT id, region, current_timestamp() AS loaded_at, " +
          "CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS loaded_at_cast FROM cbdf_current_timestamp_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")

      def assertFreshTimestamps(expectedCount: Long): Unit = {
        val mv = spark.table(mvName)
        mv.count() shouldBe expectedCount
        mv.filter("loaded_at IS NULL OR loaded_at_cast IS NULL").count() shouldBe 0L
        val stale = mv.filter(
          "loaded_at < current_timestamp() - INTERVAL 1 HOUR OR loaded_at_cast < current_timestamp() - INTERVAL 1 HOUR"
        )
        withClue("materialized loaded_at/loaded_at_cast should reflect a recent CURRENT_TIMESTAMP() evaluation: ") {
          stale.count() shouldBe 0L
        }
      }
      assertFreshTimestamps(2L)

      sql("INSERT INTO cbdf_current_timestamp_src VALUES (3, 'north')")
      refreshMv(mvName)
      assertFreshTimestamps(3L)
    }
  }

  describe("compile-bridge shim: Spark backslash-escaped string literals (normalize_os_name)") {
    it("keeps the MV incremental and correct after INSERT + REFRESH") {
      sql("CREATE TABLE cbdf_os_name_src (id INT, os_name STRING) USING DELTA")
      sql(
        "INSERT INTO cbdf_os_name_src VALUES " +
          "(1, 'Red Hat Enterprise Linux 8.6 (Ootpa)'), " +
          "(2, 'Windows Server 2019 - Standard'), " +
          "(3, 'macOS/Big Sur, v11.6: patch\\'s applied')"
      )

      // Verbatim (modulo Jinja `{{ expr }}` -> `os_name` substitution) from
      // dbt-server's codegen/macros/spark/os_helpers.sql normalize_os_name
      // macro -- the exact fragment named in the task, including the
      // backslash-escaped '\\' and single-quote-escaped '\'' literals.
      val mvName = "cbdf_mv_os_name"
      val viewBody =
        """SELECT id, TRIM('_' FROM
          |        REPLACE(REPLACE(REPLACE(REPLACE(
          |            LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(os_name,
          |                ' ', '_'), '-', '_'), '.', '_'), '/', '_'), '\\', '_'), '(', '_'), ')', '_'), ',', '_'), ':', '_'), '\'', '_'))
          |        , '____', '_'), '___', '_'), '__', '_'), '__', '_')
          |    ) AS normalized_os_name
          |FROM cbdf_os_name_src""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
      refreshMv(mvName)
      assertIncremental(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }
}
