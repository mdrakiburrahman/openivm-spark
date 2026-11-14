package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshProfileCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class DateFormatGroupRecomputeScenarios extends IvmParitySpecBase("date-format-group-recompute") {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.ProfileRefreshKey                       -> "true",
    "spark.openivm.compile.classificationCache.enabled" -> "true",
    "spark.sql.session.timeZone"                        -> "UTC"
  )

  it("maintains formatted dates in grouped recompute across mixed changes") {
    sql("CREATE TABLE dfgr_source (id INT, ts TIMESTAMP, amount INT) USING DELTA")
    sql("""INSERT INTO dfgr_source VALUES
      |(1, TIMESTAMP '2024-01-01 01:00:00', 10),
      |(2, TIMESTAMP '2024-01-01 02:00:00', 20),
      |(3, NULL, 5)""".stripMargin)
    val query = """SELECT date_format(ts, 'yyyy-MM-dd') AS day, sum(DISTINCT amount) AS total
      |FROM dfgr_source GROUP BY date_format(ts, 'yyyy-MM-dd')""".stripMargin
    sql(s"CREATE MATERIALIZED VIEW dfgr_mv AS $query")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("dfgr_mv")
    MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.GroupRecompute
    assertMvCorrect("dfgr_mv", query)
    sql("UPDATE dfgr_source SET ts = TIMESTAMP '2024-02-01 00:00:00' WHERE id = 1")
    sql("DELETE FROM dfgr_source WHERE id = 2")
    sql("INSERT INTO dfgr_source VALUES (4, TIMESTAMP '2024-01-01 03:00:00', 15)")
    RefreshProfileCatalog.removeAll(spark)
    refreshMv("dfgr_mv")
    RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
    assertMvCorrect("dfgr_mv", query)
  }
}

class DateFormatGroupRecomputeCdfIntegrationSpec extends DateFormatGroupRecomputeScenarios with CdfMode
class DateFormatGroupRecomputeInterceptIntegrationSpec extends DateFormatGroupRecomputeScenarios with InterceptMode
