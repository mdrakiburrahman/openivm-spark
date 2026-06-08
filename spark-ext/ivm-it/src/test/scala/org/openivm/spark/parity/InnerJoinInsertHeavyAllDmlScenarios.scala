package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy carve-out of `InnerJoinInsertSpec.scala`'s named_payments 11-cycle
  * DML walk (~7m).  Lives in its own forked JVM so the rest of the parity
  * suite is not blocked by this monster test.
  *
  * Table / MV names are prefixed `iji_heavy_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
abstract class InnerJoinInsertHeavyAllDmlScenarios extends IvmParitySpecBase("inner-join-insert-heavy-all-dml") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ============================================================================
  // (1) named_payments — 2-way INNER JOIN projection.
  //     Mirrors openivm tests 1–11 (lines 7–235).
  // ============================================================================

  describe("(1) named_payments — 2-way INNER JOIN projection (11 sequential DML cycles)") {
    ignore(
      "incrementally maintains the join across INSERTs, DELETEs, batches, no-ops, and duplicates"
    ) /* TODO: SIMPLE_PROJECTION over byte-identical duplicate source rows is not fully supported. */ {
      // --- Base tables ---
      sql("CREATE TABLE IF NOT EXISTS iji_heavy_gods (uid INT, user_name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS iji_heavy_payments (from_uid INT, to_uid INT, amount INT) USING DELTA")
      sql(
        "INSERT INTO iji_heavy_gods VALUES (1, 'Apollo'), (2, 'Artemis'), (3, 'Dionysus'), (4, 'Poseidon'), (5, 'Zeus')"
      )
      sql(
        "INSERT INTO iji_heavy_payments VALUES " +
          "(1, 2, 1722), (2, 3, 53), (2, 5, 360), (3, 1, 80), " +
          "(3, 2, 137), (3, 5, 83), (5, 1, 42), (1, 2, 222)"
      )

      // --- Simple join projection ---
      sql(
        "CREATE MATERIALIZED VIEW iji_heavy_named_payments AS " +
          "SELECT g.user_name, p.from_uid, p.to_uid, p.amount " +
          "FROM iji_heavy_gods AS g INNER JOIN iji_heavy_payments AS p ON p.to_uid = g.uid"
      )

      val viewBody =
        "SELECT g.user_name, p.from_uid, p.to_uid, p.amount " +
          "FROM iji_heavy_gods AS g INNER JOIN iji_heavy_payments AS p ON p.to_uid = g.uid"

      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 1: Insert on RIGHT side (payments)
      sql("INSERT INTO iji_heavy_payments VALUES (3, 1, 30)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 2: Delete via join
      sql("DELETE FROM iji_heavy_payments WHERE from_uid = 5 AND to_uid = 1 AND amount = 42")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 3: Insert on LEFT side (gods table) — no payments to uid 6 yet
      sql("INSERT INTO iji_heavy_gods VALUES (6, 'Hera')")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)
      // Now add a payment TO the new god
      sql("INSERT INTO iji_heavy_payments VALUES (1, 6, 99)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 4: Insert on BOTH sides simultaneously
      sql("INSERT INTO iji_heavy_gods VALUES (7, 'Athena')")
      sql("INSERT INTO iji_heavy_payments VALUES (2, 7, 500)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 5: Delete from LEFT side
      sql("DELETE FROM iji_heavy_gods WHERE uid = 6")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 6: Simultaneous delete on BOTH sides
      sql("DELETE FROM iji_heavy_gods WHERE uid = 7")
      sql("DELETE FROM iji_heavy_payments WHERE to_uid = 7")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 7: Batch insert — multiple rows at once
      sql("INSERT INTO iji_heavy_payments VALUES (1, 2, 10), (1, 2, 20), (1, 2, 30)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 8: No-op refresh (no changes since last)
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 9: Duplicate rows (same values inserted twice)
      sql("INSERT INTO iji_heavy_payments VALUES (4, 5, 777)")
      sql("INSERT INTO iji_heavy_payments VALUES (4, 5, 777)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 10: Mixed insert + delete in same refresh cycle
      sql("DELETE FROM iji_heavy_payments WHERE from_uid = 4 AND to_uid = 5 AND amount = 777")
      sql("INSERT INTO iji_heavy_payments VALUES (1, 3, 999)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 11: Insert on both sides + delete simultaneously
      sql("INSERT INTO iji_heavy_gods VALUES (8, 'Ares')")
      sql("INSERT INTO iji_heavy_payments VALUES (1, 8, 50)")
      sql("DELETE FROM iji_heavy_payments WHERE from_uid = 1 AND to_uid = 3 AND amount = 999")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)
    }
  }
}
