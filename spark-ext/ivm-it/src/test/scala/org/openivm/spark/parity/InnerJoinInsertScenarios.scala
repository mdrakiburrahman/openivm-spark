package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Insert-and-multi-table slice of the openivm `inner_join.test` parity port.
  *
  * Hosts the named_payments 11-cycle DML walk, the gods⨝god_cities⨝cities
  * 3-way join (which reuses `gods` from named_payments under the same
  * SparkSession), the multi-column DECIMAL join, NULL-key exclusion, the
  * 4-table 15-Möbius-term join, and the CROSS-JOIN / unmatched-key scenarios
  * (mv_any_join → mv_cross_join share `any_w` / `any_d` and must stay together).
  * See [[InnerJoinDeleteSpec]] for the delete-heavy, self-join cascade,
  * net-zero, DISTINCT, filter, and batched-UPDATE scenarios.
  */
abstract class InnerJoinInsertScenarios extends IvmParitySpecBase("inner-join-insert") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ============================================================================
  // (1) named_payments — 2-way INNER JOIN projection.
  //     Mirrors openivm tests 1–11 (lines 7–235).
  //
  //     Extracted to [[InnerJoinInsertHeavyAllDmlSpec]] (~7m wall) so it runs
  //     in its own forked JVM and does not bottleneck the rest of this spec.
  // ============================================================================

  // ============================================================================
  // (2) god_locations — 3-table INNER JOIN (2^3-1 = 7 Möbius terms).
  //     Mirrors openivm test 12 (lines 237–302).
  //     NOTE: previously re-used the `gods` table populated by (1).  After (1)
  //     was extracted, this test now creates and populates `gods` itself so it
  //     remains self-sufficient — the openivm input values are preserved.
  // ============================================================================

  describe("(2) god_locations — 3-table INNER JOIN (gods ⨝ god_cities ⨝ cities)") {
    it("maintains 3-way join across inserts to middle/rightmost tables and middle-table deletes") {
      // `gods` was originally populated by (1); since (1) now lives in its own
      // forked JVM, re-create the same input data here so this test stays
      // self-sufficient and the join surfaces real matched rows.
      sql("CREATE TABLE IF NOT EXISTS gods (uid INT, user_name STRING) USING DELTA")
      sql(
        "INSERT INTO gods VALUES (1, 'Apollo'), (2, 'Artemis'), (3, 'Dionysus'), (4, 'Poseidon'), (5, 'Zeus')"
      )

      sql("CREATE TABLE IF NOT EXISTS cities (city_id INT, city_name STRING) USING DELTA")
      sql("INSERT INTO cities VALUES (1, 'Athens'), (2, 'Sparta'), (3, 'Thebes')")

      sql("CREATE TABLE IF NOT EXISTS god_cities (uid INT, city_id INT) USING DELTA")
      sql("INSERT INTO god_cities VALUES (1, 1), (2, 2), (3, 1), (5, 3)")

      sql(
        "CREATE MATERIALIZED VIEW god_locations AS " +
          "SELECT g.user_name, c.city_name " +
          "FROM gods AS g " +
          "INNER JOIN god_cities AS gc ON gc.uid = g.uid " +
          "INNER JOIN cities AS c ON c.city_id = gc.city_id"
      )

      val viewBody =
        "SELECT g.user_name, c.city_name " +
          "FROM gods AS g " +
          "INNER JOIN god_cities AS gc ON gc.uid = g.uid " +
          "INNER JOIN cities AS c ON c.city_id = gc.city_id"

      assertMvCorrect("god_locations", viewBody)

      // Insert into middle table (god_cities) — links Poseidon (uid=4) to Sparta
      sql("INSERT INTO god_cities VALUES (4, 2)")
      refreshMv("god_locations")
      assertMvCorrect("god_locations", viewBody)

      // Insert into rightmost table (cities) + link table — Apollo also in Corinth
      sql("INSERT INTO cities VALUES (4, 'Corinth')")
      sql("INSERT INTO god_cities VALUES (1, 4)")
      refreshMv("god_locations")
      assertMvCorrect("god_locations", viewBody)

      // Delete from middle table — Poseidon/Sparta link gone
      sql("DELETE FROM god_cities WHERE uid = 4 AND city_id = 2")
      refreshMv("god_locations")
      assertMvCorrect("god_locations", viewBody)
    }
  }

  // ============================================================================
  // (3) order_totals — multi-column JOIN condition (region, product) with
  //     DECIMAL(10,2) prices. Mirrors openivm lines 304–370.
  // ============================================================================

  describe("(3) order_totals — multi-column INNER JOIN on (region, product) with DECIMAL prices") {
    it("incrementally maintains joins on composite keys with decimal-typed columns") {
      sql(
        "CREATE TABLE IF NOT EXISTS orders_j (region STRING, product STRING, qty INT) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS prices (region STRING, product STRING, price DECIMAL(10,2)) USING DELTA"
      )
      sql("INSERT INTO prices VALUES ('US', 'A', 10.00), ('US', 'B', 20.00), ('EU', 'A', 12.00)")
      sql("INSERT INTO orders_j VALUES ('US', 'A', 5), ('US', 'B', 3), ('EU', 'A', 10)")

      sql(
        "CREATE MATERIALIZED VIEW order_totals AS " +
          "SELECT o.region, o.product, o.qty, p.price " +
          "FROM orders_j o INNER JOIN prices p " +
          "ON o.region = p.region AND o.product = p.product"
      )

      val viewBody =
        "SELECT o.region, o.product, o.qty, p.price " +
          "FROM orders_j o INNER JOIN prices p " +
          "ON o.region = p.region AND o.product = p.product"

      assertMvCorrect("order_totals", viewBody)

      // Batched INSERT — two new orders matching existing prices
      sql("INSERT INTO orders_j VALUES ('US', 'A', 2), ('EU', 'A', 7)")
      refreshMv("order_totals")
      assertMvCorrect("order_totals", viewBody)

      // Delete from join
      sql("DELETE FROM orders_j WHERE region = 'US' AND product = 'A' AND qty = 5")
      refreshMv("order_totals")
      assertMvCorrect("order_totals", viewBody)
    }
  }

  // ============================================================================
  // (7) mv_4way — 4-table INNER JOIN (2^4-1 = 15 Möbius terms).
  //     Mirrors openivm lines 654–781.
  // ============================================================================

  describe("(7) mv_4way — 4-table INNER JOIN (products × inventory × warehouses × suppliers)") {
    it("incrementally maintains a 4-way join (15 Möbius terms) across batched multi-table DML") {
      sql("CREATE TABLE IF NOT EXISTS t4_products (pid INT, pname STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS t4_warehouses (wid INT, wname STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS t4_inventory (pid INT, wid INT, qty INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS t4_suppliers (sid INT, pid INT, sname STRING) USING DELTA")

      sql("INSERT INTO t4_products VALUES (1, 'Widget'), (2, 'Gadget')")
      sql("INSERT INTO t4_warehouses VALUES (10, 'East'), (20, 'West')")
      sql("INSERT INTO t4_inventory VALUES (1, 10, 100), (2, 20, 50)")
      sql("INSERT INTO t4_suppliers VALUES (100, 1, 'Acme'), (200, 2, 'Globex')")

      sql(
        "CREATE MATERIALIZED VIEW mv_4way AS " +
          "SELECT p.pname, w.wname, i.qty, s.sname " +
          "FROM t4_products p " +
          "INNER JOIN t4_inventory i ON i.pid = p.pid " +
          "INNER JOIN t4_warehouses w ON w.wid = i.wid " +
          "INNER JOIN t4_suppliers s ON s.pid = p.pid"
      )

      val viewBody =
        "SELECT p.pname, w.wname, i.qty, s.sname " +
          "FROM t4_products p " +
          "INNER JOIN t4_inventory i ON i.pid = p.pid " +
          "INNER JOIN t4_warehouses w ON w.wid = i.wid " +
          "INNER JOIN t4_suppliers s ON s.pid = p.pid"

      assertMvCorrect("mv_4way", viewBody)

      // Add inventory linking existing product+warehouse
      sql("INSERT INTO t4_inventory VALUES (1, 20, 30)")
      refreshMv("mv_4way")
      assertMvCorrect("mv_4way", viewBody)

      // Insert new product + supplier + inventory all at once (3-table batched insert)
      sql("INSERT INTO t4_products VALUES (3, 'Doohickey')")
      sql("INSERT INTO t4_suppliers VALUES (300, 3, 'Initech')")
      sql("INSERT INTO t4_inventory VALUES (3, 10, 75)")
      refreshMv("mv_4way")
      assertMvCorrect("mv_4way", viewBody)

      // Delete from the middle (inventory) — should remove all related rows
      sql("DELETE FROM t4_inventory WHERE pid = 1 AND wid = 10")
      refreshMv("mv_4way")
      assertMvCorrect("mv_4way", viewBody)
    }
  }

  // ============================================================================
  // (10) mv_any_join — LOGICAL_ANY_JOIN (CROSS JOIN + WHERE).
  //      Mirrors openivm lines 921–990.
  // ============================================================================

  describe("(10) mv_any_join — LOGICAL_ANY_JOIN: CROSS JOIN ... WHERE equi-predicate") {
    it("CROSS JOIN with a WHERE predicate refreshes incrementally across batched DML") {
      sql("CREATE TABLE IF NOT EXISTS any_w (W_ID INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS any_d (D_ID INT, D_W_ID INT) USING DELTA")
      sql("INSERT INTO any_w VALUES (1), (2)")
      sql("INSERT INTO any_d VALUES (10, 1), (20, 1), (30, 2)")

      sql(
        "CREATE MATERIALIZED VIEW mv_any_join AS " +
          "SELECT w.W_ID, d.D_ID FROM any_w w CROSS JOIN any_d d WHERE d.D_W_ID = w.W_ID"
      )

      val viewBody =
        "SELECT w.W_ID, d.D_ID FROM any_w w CROSS JOIN any_d d WHERE d.D_W_ID = w.W_ID"

      assertMvCorrect("mv_any_join", viewBody)

      // Batched DML across both sides + a delete
      sql("INSERT INTO any_w VALUES (3)")
      sql("INSERT INTO any_d VALUES (40, 3), (50, 2)")
      sql("DELETE FROM any_d WHERE D_ID = 10")
      refreshMv("mv_any_join")
      assertMvCorrect("mv_any_join", viewBody)
    }
  }

  // ============================================================================
  // (11) mv_cross_join — plain CROSS JOIN (no WHERE).
  //      Mirrors openivm lines 992–1009 and the final any_w insert at line 1157.
  // ============================================================================

  describe("(11) mv_cross_join — plain CROSS JOIN (no predicate)") {
    it("Cartesian product is bag-equal across one-sided inserts") {
      sql("CREATE TABLE IF NOT EXISTS any_w (W_ID INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS any_d (D_ID INT, D_W_ID INT) USING DELTA")
      // Note: tables created in (10) above persist for this describe under the
      // same SparkSession. Use IF NOT EXISTS and just append more rows.

      sql(
        "CREATE MATERIALIZED VIEW mv_cross_join AS " +
          "SELECT w.W_ID, d.D_ID FROM any_w w CROSS JOIN any_d d"
      )

      val viewBody = "SELECT w.W_ID, d.D_ID FROM any_w w CROSS JOIN any_d d"

      assertMvCorrect("mv_cross_join", viewBody)

      // INSERT a new w-row — Cartesian grows by |any_d|
      sql("INSERT INTO any_w VALUES (4)")
      refreshMv("mv_cross_join")
      assertMvCorrect("mv_cross_join", viewBody)
    }
  }

  // ============================================================================
  // (13) mv_skip_join_key — empty-delta proof for unmatched join keys.
  //      Mirrors openivm lines 1069–1155.
  // ============================================================================

  describe("(13) mv_skip_join_key — inserts on unmatched join keys leave MV bag-equal") {
    it("inserting rows whose join key matches nothing on the other side does not change the MV body") {
      sql("CREATE TABLE IF NOT EXISTS skj_left (id INT, v INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS skj_right (id INT, w INT) USING DELTA")
      sql("INSERT INTO skj_left VALUES (1, 10)")
      sql("INSERT INTO skj_right VALUES (1, 100)")

      sql(
        "CREATE MATERIALIZED VIEW mv_skip_join_key AS " +
          "SELECT l.id, l.v, r.w FROM skj_left l JOIN skj_right r ON l.id = r.id"
      )

      val viewBody = "SELECT l.id, l.v, r.w FROM skj_left l JOIN skj_right r ON l.id = r.id"

      assertMvCorrect("mv_skip_join_key", viewBody)

      // LEFT-only insert with no matching RIGHT key
      sql("INSERT INTO skj_left VALUES (999, 999)")
      refreshMv("mv_skip_join_key")
      assertMvCorrect("mv_skip_join_key", viewBody)

      // Both-sided inserts with no overlapping keys
      sql("INSERT INTO skj_left VALUES (1000, 1000)")
      sql("INSERT INTO skj_right VALUES (2000, 2000)")
      refreshMv("mv_skip_join_key")
      assertMvCorrect("mv_skip_join_key", viewBody)
    }
  }
}
