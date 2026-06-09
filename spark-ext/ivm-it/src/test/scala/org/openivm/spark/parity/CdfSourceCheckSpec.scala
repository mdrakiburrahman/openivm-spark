package org.openivm.spark.parity

import org.apache.spark.sql.AnalysisException
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

/** Validates that under CDF mode, `CREATE MATERIALIZED VIEW` fails loudly
  * with a clear error when any source Delta table does NOT have
  * `delta.enableChangeDataFeed = true`.  Tests are CDF-only because the
  * intercept path does not enforce the check.
  */
class CdfSourceCheckSpec extends IvmParitySpecBase("cdf-source-check") with CdfMode {

  /** Bypass the base's CDF-injection of TBLPROPERTIES — we want to deliberately
    * create a Delta source WITHOUT change-data-feed enabled to assert the
    * validation hook trips.
    */
  private def sqlRaw(q: String): Unit = {
    spark.sql(q).collect()
    ()
  }

  describe("CDF mode source-property validation") {

    it("CREATE MV throws AnalysisException when the source has no CDF") {
      sqlRaw("CREATE TABLE cdf_chk_src(id INT, v INT) USING DELTA")
      sqlRaw("INSERT INTO cdf_chk_src VALUES (1, 10)")

      val ex = intercept[AnalysisException] {
        sqlRaw("CREATE MATERIALIZED VIEW cdf_chk_mv AS SELECT id, v FROM cdf_chk_src")
      }
      val msg = Option(ex.getMessage).getOrElse("")
      withClue(s"actual exception message: $msg") {
        msg.toLowerCase should include("change")
      }
    }

    it("CREATE MV lists EVERY missing source in a single error") {
      sqlRaw("CREATE TABLE cdf_chk_a(id INT, v INT) USING DELTA")
      sqlRaw("CREATE TABLE cdf_chk_b(id INT, v INT) USING DELTA")
      sqlRaw("INSERT INTO cdf_chk_a VALUES (1, 10)")
      sqlRaw("INSERT INTO cdf_chk_b VALUES (1, 20)")

      val ex = intercept[AnalysisException] {
        sqlRaw(
          "CREATE MATERIALIZED VIEW cdf_chk_join AS " +
            "SELECT a.id, a.v + b.v AS s FROM cdf_chk_a a JOIN cdf_chk_b b ON a.id = b.id"
        )
      }
      val msg = Option(ex.getMessage).getOrElse("")
      withClue(s"actual exception message: $msg") {
        msg should include("cdf_chk_a")
        msg should include("cdf_chk_b")
      }
    }

    it("CREATE MV succeeds when CDF is enabled (sanity)") {
      sqlRaw(
        "CREATE TABLE cdf_chk_ok(id INT, v INT) USING DELTA " +
          "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
      )
      sqlRaw("INSERT INTO cdf_chk_ok VALUES (1, 10)")
      sqlRaw("CREATE MATERIALIZED VIEW cdf_chk_ok_mv AS SELECT id, v FROM cdf_chk_ok")

      assertMvCorrect("cdf_chk_ok_mv", "SELECT id, v FROM cdf_chk_ok")
    }
  }
}
