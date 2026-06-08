package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.openivm.spark.common.MvCatalog

/** Slice of `ParserSpec` covering the IvmParser's error surface: malformed
  * DDL, analyzer-time failures, duplicate-name rejection and DROP MATERIALIZED
  * VIEW IF EXISTS semantics.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
abstract class ParserErrorScenarios extends IvmParitySpecBase("parser-error") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvExists(name: String): Boolean =
    MvCatalog.lookup(spark, TableIdentifier(name)).isDefined

  // ──────────────────────────────────────────────────────────────────────────
  // (5) Parser error surface (parser.test Tests 12-15): the IvmParser rejects
  //     malformed DDL with ParseException; analyzer-time errors (non-existent
  //     table) surface as AnalysisException.
  //
  // NOTE: The original ParserSpec relied on `sales_p1` having been created by
  // describe(1) earlier in the same JVM. The first two tests below reference
  // `sales_p1` only by NAME in the SQL text — they fail at PARSE time (missing
  // AS keyword / missing view name) before any catalog lookup, so the table
  // does not need to exist for the assertion to hold.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(5) Error surface — malformed DDL and analyzer-time failures") {

    it("CREATE with the AS keyword missing raises ParseException") {
      an[ParseException] should be thrownBy {
        sql(
          "CREATE MATERIALIZED VIEW mv_no_as SELECT region, sum(amount) FROM sales_p1 GROUP BY region"
        )
      }
    }

    it("CREATE missing the view name raises ParseException") {
      an[ParseException] should be thrownBy {
        sql(
          "CREATE MATERIALIZED VIEW AS SELECT region FROM sales_p1"
        )
      }
    }

    it("Typo 'MATERIALIZD' falls through to Spark's parser and fails") {
      // Our keyword regex requires 'materialized' exactly; the typo skips our
      // grammar and is rejected by Spark.
      a[ParseException] should be thrownBy {
        sql("CREATE MATERIALIZD VIEW mv_typo AS SELECT 1")
      }
    }

    it("Typo 'VEIW' is rejected by the IvmParser grammar") {
      a[ParseException] should be thrownBy {
        sql("CREATE MATERIALIZED VEIW mv_typo2 AS SELECT 1")
      }
    }

    it("CREATE referencing a non-existent base table raises AnalysisException at execute time") {
      // The IvmParser produces a valid CreateMaterializedViewCommand; the
      // command itself fails in `run` when collectSourceSchemas analyzes the
      // missing table. Spark wraps the lookup miss in AnalysisException.
      an[AnalysisException] should be thrownBy {
        sql(
          "CREATE MATERIALIZED VIEW mv_ghost AS SELECT x FROM does_not_exist GROUP BY x"
        )
      }
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (6) Duplicate-name rejection (parser.test Test 17): a second
  //     CREATE MATERIALIZED VIEW with the same name raises with
  //     TABLE_OR_VIEW_ALREADY_EXISTS and the original is unchanged.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(6) Duplicate MV name") {

    it("a second CREATE for the same MV name fails and leaves the existing MV intact") {
      sql("CREATE TABLE IF NOT EXISTS dup_p6(id INT, val INT) USING DELTA")
      sql("INSERT INTO dup_p6 VALUES (1,10),(2,20)")
      sql("CREATE MATERIALIZED VIEW mv_dup_p6 AS SELECT id, val FROM dup_p6")
      val before = spark.table("mv_dup_p6").collect().toSet

      val ex = the[AnalysisException] thrownBy {
        sql("CREATE MATERIALIZED VIEW mv_dup_p6 AS SELECT id, val + 1 FROM dup_p6")
      }
      ex.getMessage.toLowerCase should (include("exists") or include("already"))

      // Original MV row set must be untouched.
      spark.table("mv_dup_p6").collect().toSet shouldBe before
      mvExists("mv_dup_p6") shouldBe true
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (14) DROP MATERIALIZED VIEW IF EXISTS on a missing MV is a no-op.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(14) DROP MATERIALIZED VIEW IF EXISTS") {

    it("dropping a non-existent MV with IF EXISTS does not throw") {
      mvExists("mv_never_existed_p14") shouldBe false
      noException should be thrownBy {
        sql("DROP MATERIALIZED VIEW IF EXISTS mv_never_existed_p14")
      }
    }

    it("dropping a non-existent MV WITHOUT IF EXISTS raises an analysis error") {
      // The DropMaterializedViewCommand surfaces the missing MV as an
      // AnalysisException with TABLE_OR_VIEW_NOT_FOUND.
      an[AnalysisException] should be thrownBy {
        sql("DROP MATERIALIZED VIEW mv_definitely_not_there_p14")
      }
    }
  }
}
