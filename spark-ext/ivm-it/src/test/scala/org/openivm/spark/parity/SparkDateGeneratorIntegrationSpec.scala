package org.openivm.spark.parity

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class SparkDateGeneratorScenarios extends IvmParitySpecBase("date-generator") {
  self: IvmParityMode =>

  it("incrementally expands daily date sequences and preserves invalid bounds") {
    Seq("UTC", "America/Los_Angeles").zipWithIndex.foreach { case (zone, index) =>
      spark.conf.set("spark.sql.session.timeZone", zone)
      val source = s"date_gen_source_$index"
      val mv = s"date_gen_mv_$index"
      sql(s"CREATE TABLE $source (id INT, start_date DATE, end_date DATE) USING DELTA")
      sql(s"""INSERT INTO $source VALUES
        |(1, DATE '2024-03-09', DATE '2024-03-12'),
        |(2, NULL, DATE '2024-03-11'),
        |(3, DATE '2024-11-02', DATE '2024-11-04')""".stripMargin)
      val query = s"""SELECT t.id, explode(sequence(t.start_date, t.end_date, INTERVAL '1' DAY)) AS d
        |FROM $source t""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(mv)
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
      assertMvCorrect(mv, query)
      sql(s"UPDATE $source SET end_date = DATE '2024-03-14' WHERE id = 1")
      sql(s"DELETE FROM $source WHERE id = 3")
      sql(s"INSERT INTO $source VALUES (4, DATE '2024-03-15', DATE '2024-03-15')")
      refreshMv(mv)
      assertMvCorrect(mv, query)
      sql(s"INSERT INTO $source VALUES (5, DATE '2024-03-20', DATE '2024-03-19')")
      val level = LogManager.getRootLogger.getLevel
      Configurator.setRootLevel(Level.FATAL)
      try {
        intercept[Exception] { sql(query).collect() }
        intercept[Exception] { refreshMv(mv) }
      } finally Configurator.setRootLevel(level)
    }
  }
}

class SparkDateGeneratorCdfIntegrationSpec extends SparkDateGeneratorScenarios with CdfMode
class SparkDateGeneratorInterceptIntegrationSpec extends SparkDateGeneratorScenarios with InterceptMode
