package org.openivm.spark.parity

import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

/** End-to-end CDF cascade: a two-MV chain where the downstream MV is built
  * on top of the upstream MV.  Under CDF mode the upstream's backing Delta
  * table is automatically CDF-enabled (via
  * `FeatureGate.buildMvDataTblProperties` adding the property in CDF mode),
  * so the downstream MV reads its inputs purely off Delta CDF — no synthetic
  * MV_VIEW_DELTA staging row is required.
  */
class CascadeUnderCdfSpec extends IvmParitySpecBase("cdf-cascade") with CdfMode {

  describe("CDF cascade — MV-over-MV at depth 2") {

    it("propagates base INSERT through upstream MV to downstream MV under CDF") {
      sql("CREATE TABLE cdf_csc_src(id INT, grp STRING, v INT) USING DELTA")
      sql("INSERT INTO cdf_csc_src VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)")

      sql(
        "CREATE MATERIALIZED VIEW cdf_csc_mv_up AS " +
          "SELECT grp, SUM(v) AS s, COUNT(*) AS c FROM cdf_csc_src GROUP BY grp"
      )

      sql(
        "CREATE MATERIALIZED VIEW cdf_csc_mv_dn AS " +
          "SELECT grp, s + 1 AS s_plus_one FROM cdf_csc_mv_up"
      )

      assertMvCorrect(
        "cdf_csc_mv_dn",
        "SELECT grp, SUM(v) + 1 AS s_plus_one FROM cdf_csc_src GROUP BY grp"
      )

      sql("INSERT INTO cdf_csc_src VALUES (4, 'a', 5), (5, 'c', 100)")
      sql("REFRESH MATERIALIZED VIEW cdf_csc_mv_up")
      sql("REFRESH MATERIALIZED VIEW cdf_csc_mv_dn")

      assertMvCorrect(
        "cdf_csc_mv_dn",
        "SELECT grp, SUM(v) + 1 AS s_plus_one FROM cdf_csc_src GROUP BY grp"
      )
    }

    it("propagates base DELETE through cascade under CDF") {
      sql("CREATE TABLE cdf_csc_src2(id INT, grp STRING, v INT) USING DELTA")
      sql("INSERT INTO cdf_csc_src2 VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40)")

      sql(
        "CREATE MATERIALIZED VIEW cdf_csc_mv_up2 AS " +
          "SELECT grp, SUM(v) AS s FROM cdf_csc_src2 GROUP BY grp"
      )
      sql(
        "CREATE MATERIALIZED VIEW cdf_csc_mv_dn2 AS " +
          "SELECT grp, s FROM cdf_csc_mv_up2 WHERE s > 20"
      )

      sql("DELETE FROM cdf_csc_src2 WHERE id = 1")
      sql("REFRESH MATERIALIZED VIEW cdf_csc_mv_up2")
      sql("REFRESH MATERIALIZED VIEW cdf_csc_mv_dn2")

      assertMvCorrect(
        "cdf_csc_mv_dn2",
        "SELECT grp, SUM(v) AS s FROM cdf_csc_src2 GROUP BY grp HAVING SUM(v) > 20"
      )
    }

    it("MV backing Delta table has delta.enableChangeDataFeed=true under CDF mode") {
      sql("CREATE TABLE cdf_csc_src3(id INT, grp STRING, v INT) USING DELTA")
      sql("INSERT INTO cdf_csc_src3 VALUES (1, 'a', 10)")
      sql("CREATE MATERIALIZED VIEW cdf_csc_mv_props AS SELECT grp, SUM(v) AS s FROM cdf_csc_src3 GROUP BY grp")

      val props = spark
        .sql("DESCRIBE DETAIL `cdf_csc_mv_props`")
        .select("properties")
        .head()
        .getAs[Map[String, String]]("properties")

      withClue(s"MV properties: $props") {
        props.get("delta.enableChangeDataFeed").map(_.toLowerCase) shouldBe Some("true")
      }
    }
  }
}
