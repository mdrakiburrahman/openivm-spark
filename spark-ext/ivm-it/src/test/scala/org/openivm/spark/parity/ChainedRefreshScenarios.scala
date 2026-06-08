package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Split from the original parity spec.  Scope:
  * Depth-2 projection/join chains and net-zero upstream-delta refresh scenarios from `chained.test`.
  *
  * Includes sections: (C), (D), (F), (G), (J).
  */
abstract class ChainedRefreshScenarios extends IvmParitySpecBase("chained-refresh") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Issue refreshes in dependency order: every name in `mvs` is refreshed
    * after the ones before it. */
  protected def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  // ──────────────────────────────────────────────────────────────────────────
  // (C) Two-level chain: projection MV + JOIN MV (chained.test L78–L122)
  // Source table feeds a pure projection MV, which then feeds a join MV.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(C) Two-level chain: compact_src → compact_proj → compact_join") {

    it("a single-row INSERT propagates from src → proj → join (with USING(k))") {
      sql("CREATE TABLE IF NOT EXISTS chr_compact_src(id INT, k INT, v INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS chr_compact_dim(k INT, label STRING) USING DELTA")
      sql("INSERT INTO chr_compact_src VALUES (1,10,100),(2,20,200),(3,30,300)")
      sql("INSERT INTO chr_compact_dim VALUES (10,'a'),(20,'b'),(30,'c'),(40,'d')")
      sql("CREATE MATERIALIZED VIEW chr_compact_proj AS SELECT id, k, v FROM chr_compact_src")
      sql(
        "CREATE MATERIALIZED VIEW chr_compact_join AS " +
          "SELECT p.id, p.k, d.label FROM chr_compact_proj p JOIN chr_compact_dim d USING (k)"
      )
      sql("INSERT INTO chr_compact_src VALUES (4,40,400)")
      refreshChain("chr_compact_proj", "chr_compact_join")
      assertMvCorrect("chr_compact_proj", "SELECT id, k, v FROM chr_compact_src")
      assertMvCorrect(
        "chr_compact_join",
        "SELECT p.id, p.k, d.label FROM chr_compact_src p JOIN chr_compact_dim d USING (k)"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (D) Canceling source deltas: INSERT + DELETE on the same row cancels
  //     out so the projection MV's delta is empty (chained.test L124–L168).
  // ──────────────────────────────────────────────────────────────────────────

  describe("(D) Two-level chain: canceling deltas yield no observable change downstream") {

    it("INSERT (id=3) immediately followed by DELETE leaves both MVs at the original state") {
      sql("CREATE TABLE IF NOT EXISTS chr_cancel_src(id INT, k INT, v INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS chr_cancel_dim(k INT, label STRING) USING DELTA")
      sql("INSERT INTO chr_cancel_src VALUES (1,10,100),(2,20,200)")
      sql("INSERT INTO chr_cancel_dim VALUES (10,'a'),(20,'b'),(30,'c')")
      sql("CREATE MATERIALIZED VIEW chr_cancel_proj AS SELECT id, k, v FROM chr_cancel_src")
      sql(
        "CREATE MATERIALIZED VIEW chr_cancel_join AS " +
          "SELECT p.id, p.k, d.label FROM chr_cancel_proj p JOIN chr_cancel_dim d USING (k)"
      )
      // Net-zero on the source: INSERT then DELETE of the same row.
      sql("INSERT INTO chr_cancel_src VALUES (3,30,300)")
      sql("DELETE FROM chr_cancel_src WHERE id = 3")
      refreshChain("chr_cancel_proj", "chr_cancel_join")
      assertMvCorrect("chr_cancel_proj", "SELECT id, k, v FROM chr_cancel_src")
      assertMvCorrect(
        "chr_cancel_join",
        "SELECT p.id, p.k, d.label FROM chr_cancel_src p JOIN chr_cancel_dim d USING (k)"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (F) Two-level chain with JOIN at the base: customers + purchases (JOIN MV)
  //     → spend_by_customer (group-agg MV) — chained.test L553–L651.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(F) Two-level chain: customers ⋈ purchases → customer_purchases → spend_by_customer") {

    it("first batch: insert into the join's right side propagates to the agg MV") {
      sql("CREATE TABLE IF NOT EXISTS chr_customers(id INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS chr_purchases(customer_id INT, product STRING, amount INT) USING DELTA")
      sql("INSERT INTO chr_customers VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')")
      sql(
        "CREATE MATERIALIZED VIEW chr_customer_purchases AS " +
          "SELECT c.name, p.product, p.amount FROM chr_customers c INNER JOIN chr_purchases p ON c.id = p.customer_id"
      )
      sql(
        "CREATE MATERIALIZED VIEW chr_spend_by_customer AS " +
          "SELECT name, SUM(amount) AS total_spend FROM chr_customer_purchases GROUP BY name"
      )
      sql("INSERT INTO chr_purchases VALUES (1,'book',20),(2,'pen',5),(1,'laptop',1000)")
      refreshChain("chr_customer_purchases", "chr_spend_by_customer")
      assertMvCorrect(
        "chr_customer_purchases",
        "SELECT c.name, p.product, p.amount FROM chr_customers c INNER JOIN chr_purchases p ON c.id = p.customer_id"
      )
      assertMvCorrect(
        "chr_spend_by_customer",
        "SELECT c.name, SUM(p.amount) AS total_spend " +
          "FROM chr_customers c INNER JOIN chr_purchases p ON c.id = p.customer_id GROUP BY c.name"
      )
    }

    it("second batch surfaces a new customer (Charlie)") {
      sql("INSERT INTO chr_purchases VALUES (3,'tablet',500),(1,'mouse',30)")
      refreshChain("chr_customer_purchases", "chr_spend_by_customer")
      assertMvCorrect(
        "chr_customer_purchases",
        "SELECT c.name, p.product, p.amount FROM chr_customers c INNER JOIN chr_purchases p ON c.id = p.customer_id"
      )
      assertMvCorrect(
        "chr_spend_by_customer",
        "SELECT c.name, SUM(p.amount) AS total_spend " +
          "FROM chr_customers c INNER JOIN chr_purchases p ON c.id = p.customer_id GROUP BY c.name"
      )
    }

    it("DELETE removes the matching join output and zeroes Bob's group") {
      sql("DELETE FROM chr_purchases WHERE product = 'pen'")
      refreshChain("chr_customer_purchases", "chr_spend_by_customer")
      assertMvCorrect(
        "chr_customer_purchases",
        "SELECT c.name, p.product, p.amount FROM chr_customers c INNER JOIN chr_purchases p ON c.id = p.customer_id"
      )
      assertMvCorrect(
        "chr_spend_by_customer",
        "SELECT c.name, SUM(p.amount) AS total_spend " +
          "FROM chr_customers c INNER JOIN chr_purchases p ON c.id = p.customer_id GROUP BY c.name"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (G) Two-level chain: JOIN MV → multi-aggregate MV (chained.test L653–L741)
  // ──────────────────────────────────────────────────────────────────────────

  describe("(G) Two-level chain: departments ⋈ employees → emp_dept → dept_totals") {

    it("first batch: SUM + COUNT aggregates over the joined emp/dept view") {
      sql("CREATE TABLE IF NOT EXISTS chr_departments(id INT, dept_name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS chr_employees(id INT, name STRING, dept_id INT, salary INT) USING DELTA")
      sql("INSERT INTO chr_departments VALUES (1,'Engineering'),(2,'Sales')")
      sql(
        "CREATE MATERIALIZED VIEW chr_emp_dept AS " +
          "SELECT e.name, d.dept_name, e.salary " +
          "FROM chr_employees e INNER JOIN chr_departments d ON e.dept_id = d.id"
      )
      sql(
        "CREATE MATERIALIZED VIEW chr_dept_totals AS " +
          "SELECT dept_name, SUM(salary) AS total_salary, COUNT(*) AS headcount " +
          "FROM chr_emp_dept GROUP BY dept_name"
      )
      sql("INSERT INTO chr_employees VALUES (1,'Alice',1,100),(2,'Bob',2,80),(3,'Charlie',1,120)")
      refreshChain("chr_emp_dept", "chr_dept_totals")
      assertMvCorrect(
        "chr_emp_dept",
        "SELECT e.name, d.dept_name, e.salary " +
          "FROM chr_employees e INNER JOIN chr_departments d ON e.dept_id = d.id"
      )
      assertMvCorrect(
        "chr_dept_totals",
        "SELECT d.dept_name, SUM(e.salary) AS total_salary, COUNT(*) AS headcount " +
          "FROM chr_employees e INNER JOIN chr_departments d ON e.dept_id = d.id GROUP BY d.dept_name"
      )
    }

    it("second batch with employee additions and a deletion (Bob)") {
      sql("INSERT INTO chr_employees VALUES (4,'Diana',2,90),(5,'Eve',1,110)")
      refreshChain("chr_emp_dept", "chr_dept_totals")
      assertMvCorrect(
        "chr_dept_totals",
        "SELECT d.dept_name, SUM(e.salary) AS total_salary, COUNT(*) AS headcount " +
          "FROM chr_employees e INNER JOIN chr_departments d ON e.dept_id = d.id GROUP BY d.dept_name"
      )

      sql("DELETE FROM chr_employees WHERE name = 'Bob'")
      refreshChain("chr_emp_dept", "chr_dept_totals")
      assertMvCorrect(
        "chr_dept_totals",
        "SELECT d.dept_name, SUM(e.salary) AS total_salary, COUNT(*) AS headcount " +
          "FROM chr_employees e INNER JOIN chr_departments d ON e.dept_id = d.id GROUP BY d.dept_name"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (J) Two-level chain with a net-zero upstream delta (chained.test L1059–L1118)
  //     INSERT then DELETE the same row → upstream delta should compact to empty
  //     and the downstream refresh should observe no change.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(J) Two-level chain with net-zero upstream delta") {

    it("INSERT (3,999) immediately undone by DELETE leaves both MVs at the pre-insert state") {
      sql("CREATE TABLE IF NOT EXISTS chr_noop_base(id INT, val INT) USING DELTA")
      sql("INSERT INTO chr_noop_base VALUES (1,10),(2,20)")
      sql(
        "CREATE MATERIALIZED VIEW chr_noop_l1 AS " +
          "SELECT id, SUM(val) AS total FROM chr_noop_base GROUP BY id"
      )
      sql(
        "CREATE MATERIALIZED VIEW chr_noop_l2 AS " +
          "SELECT SUM(total) AS grand FROM chr_noop_l1"
      )
      // Net-zero delta sequence on the source.
      sql("INSERT INTO chr_noop_base VALUES (3,999)")
      sql("DELETE FROM chr_noop_base WHERE id = 3")
      refreshChain("chr_noop_l1", "chr_noop_l2")
      assertMvCorrect("chr_noop_l1", "SELECT id, SUM(val) AS total FROM chr_noop_base GROUP BY id")
      assertMvCorrect(
        "chr_noop_l2",
        "SELECT SUM(total) AS grand FROM (" +
          "SELECT id, SUM(val) AS total FROM chr_noop_base GROUP BY id) t"
      )
    }
  }
}
