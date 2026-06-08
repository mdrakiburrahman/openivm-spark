package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** Parity port of `openivm/test/sql/inner_join_fk.test`.
  *
  * == What the openivm test exercises ==
  *
  * The DuckDB test in openivm walks the inclusion-exclusion (Möbius) join rule
  * (`src/rules/join.cpp:396-447`) through a star schema where every dimension
  * column is declared with a foreign key.  When `openivm_fk_pruning=true`
  * (default), openivm prunes 2^N-1 Möbius terms whose insert-only dimension
  * deltas cannot generate any new join rows: a newly-inserted PK that has no
  * referencing fact row contributes nothing to the view delta, so the term is
  * skipped.  Updates and deletes on the referenced side must NOT be pruned
  * (deletes can drop already-joined rows; updates split into delete+insert and
  * the delete arm still affects the view).
  *
  * == Porting strategy on openivm-spark ==
  *
  *   - Spark 3.5 / Delta Lake do not have native FOREIGN KEY constraints, and
  *     openivm-spark does not enforce or read FK metadata from Delta — multi-
  *     source JOIN views are routed through `RefreshMaterializedViewCommand`
  *     and frequently demoted to FULL_REFRESH by `hasRealDelta`.  In other
  *     words: openivm-spark never performs the FK-pruning optimization itself.
  *
  *   - What we DO have, and what the openivm CLAUDE.md rule mandates we test:
  *     every refresh must leave the MV bag-equal to the live view body via
  *     bidirectional EXCEPT ALL.  This spec replays every DML scenario in
  *     `inner_join_fk.test` — INSERT into dim only (FK-prunable in openivm),
  *     INSERT into fact (referencing side), UPDATE / DELETE on dim (NOT
  *     prunable), simultaneous fact+dim inserts, batched mixed DML — and
  *     verifies correctness regardless of whether the underlying engine
  *     chooses an incremental or full-recompute path.
  *
  * Tests 1–6 mirror the 3-way star schema scenarios.  Test 7 is the 2-table
  * dept/employees scenario.  Test 8 is the no-FK control join (FK pruning
  * never applies; only correctness matters).
  *
  * See `.temp/openivm-spark-research/prompts/PLAN.md` §12 (out of scope items
  * — declared FK metadata is not consumed) for why we do NOT assert FK-pruning
  * mechanics: those are an openivm-only optimization.  Correctness via
  * bidirectional EXCEPT ALL is what we guarantee on Spark.
  */
abstract class InnerJoinFkScenarios extends IvmParitySpecBase("inner-join-fk") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Looks up the recorded refresh type for `name` via the MV catalog. */
  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ============================================================================
  // 3-way star schema: fact_sales + dim_product + dim_region
  // ============================================================================
  //
  // We CREATE the star schema and the MV once in beforeAll-style setup, then
  // each `it(...)` exercises one DML scenario.  This mirrors the openivm test
  // structure which threads its scenarios through a single MV.
  //
  // Because tests within a `describe` block don't share state across describes,
  // we keep all star-schema scenarios under one describe so the same MV is
  // mutated in sequence — same as the openivm test.

  describe("(1) Star schema (fact_sales, dim_product, dim_region) — FK-pruning scenarios") {

    it("(1a) initial load: MV = base join") {
      sql("CREATE TABLE IF NOT EXISTS dim_product(product_id INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS dim_region(region_id INT, region_name STRING) USING DELTA")
      sql(
        "CREATE TABLE IF NOT EXISTS fact_sales(sale_id INT, product_id INT, region_id INT, amount DECIMAL(10,2)) USING DELTA"
      )

      sql("INSERT INTO dim_product VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey')")
      sql("INSERT INTO dim_region VALUES (10, 'North'), (20, 'South')")
      sql("INSERT INTO fact_sales VALUES (1, 1, 10, 100.0), (2, 2, 20, 200.0), (3, 1, 20, 150.0)")

      sql(
        "CREATE MATERIALIZED VIEW fk_star AS " +
          "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales f " +
          "JOIN dim_product p ON f.product_id = p.product_id " +
          "JOIN dim_region r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )

      // openivm classifies as AGGREGATE_GROUP, openivm-spark may demote to
      // GroupRecompute or FullRefresh.  All are acceptable shapes.
      val rt = mvRefreshType("fk_star")
      Seq(
        RefreshTypeCode.AggregateGroup,
        RefreshTypeCode.GroupRecompute,
        RefreshTypeCode.FullRefresh
      ) should contain(rt)

      assertMvCorrect(
        "fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales f " +
          "JOIN dim_product p ON f.product_id = p.product_id " +
          "JOIN dim_region r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )
    }

    // (1b) INSERT into dimension only — in openivm this is the FK-prunable
    // case: the new PK has no referencing facts so no Möbius term contributes.
    it("(1b) INSERT into dim_product alone — no MV change") {
      sql("INSERT INTO dim_product VALUES (4, 'Thingamajig')")
      refreshMv("fk_star")
      assertMvCorrect(
        "fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales f " +
          "JOIN dim_product p ON f.product_id = p.product_id " +
          "JOIN dim_region r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )
    }

    // (1c) INSERT into fact referencing the freshly-inserted dim row.
    it("(1c) INSERT into fact referencing the new dim_product — new group appears") {
      sql("INSERT INTO fact_sales VALUES (4, 4, 10, 300.0)")
      refreshMv("fk_star")
      assertMvCorrect(
        "fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales f " +
          "JOIN dim_product p ON f.product_id = p.product_id " +
          "JOIN dim_region r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )
      // Spot-check the new (Thingamajig, North) row materialised
      val row = spark
        .table("fk_star")
        .selectExpr("name", "region_name")
        .where("name = 'Thingamajig'")
        .collect()
      row.length shouldBe 1
      row.head.getString(0) shouldBe "Thingamajig"
      row.head.getString(1) shouldBe "North"
    }

    // (1d) UPDATE dim_product: openivm splits this into delete+insert and FK
    // pruning must NOT apply (the delete arm can drop matched rows).
    it("(1d) UPDATE dim_product (delete + insert delta) — name change propagates") {
      sql("UPDATE dim_product SET name = 'SuperWidget' WHERE product_id = 1")
      refreshMv("fk_star")
      assertMvCorrect(
        "fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales f " +
          "JOIN dim_product p ON f.product_id = p.product_id " +
          "JOIN dim_region r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )
      spark.table("fk_star").where("name = 'Widget'").count() shouldBe 0L
      spark.table("fk_star").where("name = 'SuperWidget'").count() shouldBe 2L
    }

    // (1e) DELETE from dim_product — FK pruning must NOT apply (delete can
    // remove matched join rows).
    it("(1e) DELETE from dim_product + cascading delete on fact_sales") {
      sql("DELETE FROM fact_sales WHERE product_id = 3")
      sql("DELETE FROM dim_product WHERE product_id = 3")
      refreshMv("fk_star")
      assertMvCorrect(
        "fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales f " +
          "JOIN dim_product p ON f.product_id = p.product_id " +
          "JOIN dim_region r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )
    }

    // (1f) Simultaneous fact + dim inserts (both insert-only).
    it("(1f) Simultaneous INSERT into fact_sales + dim_region — new region group") {
      sql("INSERT INTO dim_region VALUES (30, 'East')")
      sql("INSERT INTO fact_sales VALUES (5, 2, 30, 500.0)")
      refreshMv("fk_star")
      assertMvCorrect(
        "fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales f " +
          "JOIN dim_product p ON f.product_id = p.product_id " +
          "JOIN dim_region r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )
      val row = spark
        .table("fk_star")
        .selectExpr("name", "region_name")
        .where("region_name = 'East'")
        .collect()
      row.length shouldBe 1
      row.head.getString(0) shouldBe "Gadget"
      row.head.getString(1) shouldBe "East"
    }

    // (1g) Stress test — batched mixed DML on fact and dims followed by ONE
    // refresh.  Per CLAUDE.md: stress tests must batch many ops, not do
    // one-op-at-a-time.
    it("(1g) Stress — batched INSERTs/DELETE/UPDATE on fact + insert-only dims, single REFRESH") {
      sql("INSERT INTO dim_product VALUES (5, 'Gizmo'), (6, 'Contraption')")
      sql("INSERT INTO dim_region VALUES (40, 'West')")
      sql(
        "INSERT INTO fact_sales VALUES " +
          "(10, 5, 10, 50.0), (11, 5, 20, 75.0), (12, 6, 30, 200.0), " +
          "(13, 2, 40, 125.0), (14, 4, 40, 350.0)"
      )
      sql("DELETE FROM fact_sales WHERE sale_id = 2")
      sql("UPDATE fact_sales SET amount = 999.0 WHERE sale_id = 1")
      refreshMv("fk_star")
      assertMvCorrect(
        "fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales f " +
          "JOIN dim_product p ON f.product_id = p.product_id " +
          "JOIN dim_region r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )
    }
  }

  // ============================================================================
  // 2-table FK join: departments + employees (simpler case)
  // ============================================================================

  describe("(2) 2-table FK join (departments, employees)") {
    it("INSERT into departments alone → no MV change; subsequent employee INSERT → new group") {
      sql("CREATE TABLE IF NOT EXISTS departments(dept_id INT, dept_name STRING) USING DELTA")
      sql(
        "CREATE TABLE IF NOT EXISTS employees(emp_id INT, dept_id INT, salary INT) USING DELTA"
      )
      sql("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
      sql("INSERT INTO employees VALUES (1, 1, 100000), (2, 1, 120000), (3, 2, 80000)")

      sql(
        "CREATE MATERIALIZED VIEW dept_costs AS " +
          "SELECT d.dept_name, SUM(e.salary) AS total_salary, COUNT(*) AS headcount " +
          "FROM employees e JOIN departments d ON e.dept_id = d.dept_id " +
          "GROUP BY d.dept_name"
      )

      assertMvCorrect(
        "dept_costs",
        "SELECT d.dept_name, SUM(e.salary) AS total_salary, COUNT(*) AS headcount " +
          "FROM employees e JOIN departments d ON e.dept_id = d.dept_id " +
          "GROUP BY d.dept_name"
      )

      // Insert-only dimension change — FK pruning applies in openivm
      sql("INSERT INTO departments VALUES (3, 'Marketing')")
      refreshMv("dept_costs")
      assertMvCorrect(
        "dept_costs",
        "SELECT d.dept_name, SUM(e.salary) AS total_salary, COUNT(*) AS headcount " +
          "FROM employees e JOIN departments d ON e.dept_id = d.dept_id " +
          "GROUP BY d.dept_name"
      )

      // Now an employee in the new department
      sql("INSERT INTO employees VALUES (4, 3, 90000)")
      refreshMv("dept_costs")
      assertMvCorrect(
        "dept_costs",
        "SELECT d.dept_name, SUM(e.salary) AS total_salary, COUNT(*) AS headcount " +
          "FROM employees e JOIN departments d ON e.dept_id = d.dept_id " +
          "GROUP BY d.dept_name"
      )

      // Spot-check expected groups (sorted by dept_name)
      val rows = spark
        .table("dept_costs")
        .selectExpr("dept_name", "total_salary", "headcount")
        .orderBy("dept_name")
        .collect()
      rows.map(r => (r.getString(0), r.getLong(1), r.getLong(2))).toSeq shouldBe Seq(
        ("Engineering", 220000L, 2L),
        ("Marketing", 90000L, 1L),
        ("Sales", 80000L, 1L)
      )
    }
  }

  // ============================================================================
  // 2-table NO-FK join — pruning never applies; correctness still required
  // ============================================================================

  describe("(3) 2-table join WITHOUT FK metadata — pruning never applies, correctness must hold") {
    it("MV stays bag-equal under simultaneous inserts on both sides") {
      sql("CREATE TABLE IF NOT EXISTS t_left_nofk(a INT, b INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS t_right_nofk(b INT, c INT) USING DELTA")
      sql("INSERT INTO t_left_nofk VALUES (1, 10), (2, 20)")
      sql("INSERT INTO t_right_nofk VALUES (10, 100), (20, 200)")

      sql(
        "CREATE MATERIALIZED VIEW mv_no_fk AS " +
          "SELECT t_left_nofk.a, t_right_nofk.c FROM t_left_nofk JOIN t_right_nofk ON t_left_nofk.b = t_right_nofk.b"
      )

      assertMvCorrect(
        "mv_no_fk",
        "SELECT t_left_nofk.a, t_right_nofk.c FROM t_left_nofk JOIN t_right_nofk ON t_left_nofk.b = t_right_nofk.b"
      )

      // Simultaneous inserts on both sides
      sql("INSERT INTO t_right_nofk VALUES (30, 300)")
      sql("INSERT INTO t_left_nofk VALUES (3, 30)")
      refreshMv("mv_no_fk")
      assertMvCorrect(
        "mv_no_fk",
        "SELECT t_left_nofk.a, t_right_nofk.c FROM t_left_nofk JOIN t_right_nofk ON t_left_nofk.b = t_right_nofk.b"
      )
    }
  }
}
