package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy carve-out of `FullOuterJoinSpec.scala` §(1) — the emp_projects FULL
  * OUTER JOIN projection walk through matched / unmatched-left /
  * unmatched-right rows under repeated DML (~4m44).  Lives in its own forked
  * JVM so the rest of the parity suite is not blocked by this monster test.
  *
  * Table / MV names are prefixed `foj_heavy_outer_` to guarantee no
  * Delta-path collision with the host spec or any other parallel forked JVM.
  */
abstract class FullOuterJoinHeavyOuterSidesScenarios extends IvmParitySpecBase("full-outer-join-heavy-outer-sides") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ============================================================================
  // (1) Basic FULL OUTER JOIN projection
  //     openivm/test/sql/full_outer_join.test L26–L269
  // ============================================================================

  describe("(1) FULL OUTER JOIN projection: employees ⟗ projects") {
    it("maintains matched / unmatched-left / unmatched-right rows through repeated DML") {
      sql("CREATE TABLE IF NOT EXISTS foj_heavy_outer_employees(id INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS foj_heavy_outer_projects(id INT, emp_id INT, title STRING) USING DELTA")
      sql("INSERT INTO foj_heavy_outer_employees VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      sql("INSERT INTO foj_heavy_outer_projects VALUES (10, 1, 'Alpha'), (20, 1, 'Beta'), (30, 4, 'Gamma')")

      sql(
        "CREATE MATERIALIZED VIEW foj_heavy_outer_emp_projects AS " +
          "SELECT e.name, p.title " +
          "FROM foj_heavy_outer_employees e FULL OUTER JOIN foj_heavy_outer_projects p ON e.id = p.emp_id"
      )

      val viewBody =
        "SELECT e.name, p.title " +
          "FROM foj_heavy_outer_employees e FULL OUTER JOIN foj_heavy_outer_projects p ON e.id = p.emp_id"

      // Initial: Alice matched (2 projects), Bob/Charlie unmatched-left, Gamma unmatched-right
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Insert into right side: Bob gets a project (unmatched-left → matched)
      sql("INSERT INTO foj_heavy_outer_projects VALUES (40, 2, 'Delta')")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Insert into right side with no match: new unmatched-right row
      sql("INSERT INTO foj_heavy_outer_projects VALUES (50, 99, 'Epsilon')")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Delete from right side: remove Alpha (matched pair removed; Alice still has Beta)
      sql("DELETE FROM foj_heavy_outer_projects WHERE id = 10")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Delete from right side: remove Beta — Alice becomes unmatched-left (Alice, NULL)
      sql("DELETE FROM foj_heavy_outer_projects WHERE id = 20")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Insert into left: new employee with no project (unmatched-left)
      sql("INSERT INTO foj_heavy_outer_employees VALUES (5, 'Eve')")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Delete unmatched-right: remove Gamma (emp_id=4, no matching employee)
      sql("DELETE FROM foj_heavy_outer_projects WHERE id = 30")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)

      // Stress: batch mixed DML on both sides before a single refresh
      sql("INSERT INTO foj_heavy_outer_employees VALUES (6, 'Frank')")
      sql("INSERT INTO foj_heavy_outer_projects VALUES (60, 6, 'Zeta'), (70, 5, 'Eta'), (80, 100, 'Theta')")
      sql("DELETE FROM foj_heavy_outer_employees WHERE id = 2")
      sql("DELETE FROM foj_heavy_outer_projects WHERE id = 50")
      refreshMv("foj_heavy_outer_emp_projects")
      assertMvCorrect("foj_heavy_outer_emp_projects", viewBody)
    }
  }
}
