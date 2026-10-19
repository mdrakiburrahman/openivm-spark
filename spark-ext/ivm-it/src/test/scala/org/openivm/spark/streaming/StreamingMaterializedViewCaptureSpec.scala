package org.openivm.spark.streaming

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.analyzer.SourceChangeLedger
import org.openivm.spark.common.{MvCatalog, StagingCatalog, StagingDelta}
import org.scalatest.funspec.AnyFunSpec

/** Materialized views over availableNow streaming tables observe every committed streaming append. */
class StreamingMaterializedViewCaptureSpec extends AnyFunSpec with StreamingSqlTestSupport {

  private def declaration(source: String, target: String): String =
    s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
       |OPTIONS ('trigger' = 'availableNow', 'outputMode' = 'append')
       |AS SELECT id, value FROM STREAM ${quoteIdentifier(source)}""".stripMargin

  private def assertMatches(view: String, stream: String): Unit = {
    val actual   = spark.table(view).select("value", "n")
    val expected = spark.sql(s"SELECT value, count(*) AS n FROM $stream GROUP BY value")
    withClue(s"actual=${actual.collect().mkString(",")} expected=${expected.collect().mkString(",")}") {
      actual.exceptAll(expected).count() shouldBe 0L
      expected.exceptAll(actual).count() shouldBe 0L
    }
  }

  describe("a materialized view over an availableNow streaming table") {
    it("applies rows appended by later streaming runs as incremental inserts") {
      val source = "capture_stream_source"
      val stream = "capture_stream_target"
      val view   = "capture_stream_mv"
      createDeltaSource(source, "id INT, value STRING")
      insertRows(source, "(1, 'one'), (2, 'two')")
      createStreamingSqlToCompletion(declaration(source, stream))
      spark.sql(s"CREATE MATERIALIZED VIEW $view AS SELECT value, count(*) AS n FROM $stream GROUP BY value").collect()
      val sources = MvCatalog.lookup(spark, TableIdentifier(view)).get.sourceTables

      Seq("(3, 'one'), (4, 'three')", "(5, 'two'), (6, 'three'), (7, 'four')").foreach { rows =>
        insertRows(source, rows)
        createStreamingSqlToCompletion(declaration(source, stream)).inputRows should be > 0L
        SourceChangeLedger.reconcile(spark, sources)
        StagingCatalog.collectFor(spark, view, sources).map(_.opType).distinct shouldBe Seq(
          StagingDelta.OpTypes.Insert
        )
        spark.sql(s"REFRESH MATERIALIZED VIEW $view").collect()
        assertMatches(view, stream)
      }
    }
  }
}
