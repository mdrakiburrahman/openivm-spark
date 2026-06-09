package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy carve-out of `ProjectionSpec.scala` §(1) — the emp_names pure
  * projection walk through insert / delete / no-op / batch / mixed-DML rounds
  * (~3m42).  Lives in its own forked JVM so the rest of the parity suite is
  * not blocked by this monster test.
  *
  * Table / MV names are prefixed `proj_heavy_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
abstract class ProjectionHeavyDmlScenarios extends IvmParitySpecBase("projection-heavy-dml") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ============================================================================
  // (1) Pure projection: SELECT id, name FROM employees
  //     openivm projection.test:1-90  — emp_names walk-through
  // ============================================================================
  describe("(1) Pure projection: SELECT id, name FROM employees") {
    it("incremental refresh propagates insert, delete, no-op, batch, and mixed DML") {
      sql("CREATE TABLE IF NOT EXISTS proj_heavy_employees(id INT, name STRING, dept STRING) USING DELTA")
      sql("INSERT INTO proj_heavy_employees VALUES (1, 'Alice', 'eng'), (2, 'Bob', 'sales')")
      sql("CREATE MATERIALIZED VIEW proj_heavy_emp_names AS SELECT id, name FROM proj_heavy_employees")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")

      // Insert a new employee
      sql("INSERT INTO proj_heavy_employees VALUES (3, 'Charlie', 'eng')")
      refreshMv("proj_heavy_emp_names")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")

      // Delete an employee
      sql("DELETE FROM proj_heavy_employees WHERE id = 1")
      refreshMv("proj_heavy_emp_names")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")

      // No-op refresh
      refreshMv("proj_heavy_emp_names")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")

      // Batch insert
      sql("INSERT INTO proj_heavy_employees VALUES (4, 'Diana', 'eng'), (5, 'Eve', 'sales')")
      refreshMv("proj_heavy_emp_names")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")

      // Mixed INSERT + DELETE before a single refresh
      sql("INSERT INTO proj_heavy_employees VALUES (6, 'Frank', 'ops')")
      sql("DELETE FROM proj_heavy_employees WHERE id = 2")
      refreshMv("proj_heavy_emp_names")
      assertMvCorrect("proj_heavy_emp_names", "SELECT id, name FROM proj_heavy_employees")
    }
  }
}
