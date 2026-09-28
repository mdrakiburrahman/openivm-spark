package org.openivm.spark.parser

import java.io.File
import java.util.UUID

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedSubqueryColumnAliases}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{
  EventTimeWatermark,
  LocalRelation,
  LogicalPlan,
  Project,
  SubqueryAlias
}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.openivm.spark.commands.CreateStreamingTableCommand
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StreamingWatermarkParserSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("StreamingWatermarkParserSpec")
      .config("spark.sql.extensions", "org.openivm.spark.OpenIvmSparkExtensions")
      .config("spark.openivm.enabled", "true")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.warehouse.dir",
        new File(
          s"target/test-warehouse-streaming-watermark-${UUID.randomUUID().toString.take(8)}"
        ).getCanonicalPath
      )
      .getOrCreate()
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
    } finally {
      super.afterAll()
    }

  private def parseQuery(query: String): LogicalPlan =
    spark.sessionState.sqlParser
      .parsePlan(s"CREATE STREAMING TABLE sink AS $query")
      .asInstanceOf[CreateStreamingTableCommand]
      .spec
      .query

  describe("watermark parsing") {
    it("places the relation alias below the unresolved watermark") {
      val plan = parseQuery(
        """SELECT e.id
          |FROM STREAM events
          |WITH ('maxFilesPerTrigger' = '2')
          |WATERMARK e.event_time DELAY OF INTERVAL 10 MINUTES AS e""".stripMargin
      )

      val watermark =
        plan.collectWithSubqueries { case value: UnresolvedStreamingWatermark => value }.head
      watermark.eventTimeColExpr shouldBe UnresolvedAttribute(Seq("e", "event_time"))
      watermark.child shouldBe a[SubqueryAlias]
      watermark.child.asInstanceOf[SubqueryAlias].alias shouldBe "e"
      watermark.child
        .collectFirst { case relation: org.apache.spark.sql.catalyst.analysis.UnresolvedRelation =>
          relation
        }
        .get
        .options
        .get("maxFilesPerTrigger") shouldBe "2"
    }

    it("keeps an explicitly named derived timestamp expression") {
      val plan = parseQuery(
        """SELECT e.event_time
          |FROM STREAM events
          |WATERMARK timestamp_seconds(epoch) AS event_time
          |DELAY OF INTERVAL 30 SECONDS AS e""".stripMargin
      )

      val watermark =
        plan.collectWithSubqueries { case value: UnresolvedStreamingWatermark => value }.head
      watermark.eventTimeColExpr shouldBe a[Alias]
      watermark.eventTimeColExpr.name shouldBe "event_time"
      IntervalUtils.getDuration(watermark.delay, java.util.concurrent.TimeUnit.SECONDS) shouldBe 30L
    }

    it("attaches a watermark to a derived streaming relation") {
      val plan = parseQuery(
        """SELECT d.id
          |FROM (
          |  SELECT id, event_time FROM STREAM raw_events
          |)
          |WATERMARK event_time DELAY OF INTERVAL 1 MINUTE AS d""".stripMargin
      )

      val watermark =
        plan.collectWithSubqueries { case value: UnresolvedStreamingWatermark => value }.head
      watermark.child shouldBe a[SubqueryAlias]
      watermark.child.asInstanceOf[SubqueryAlias].alias shouldBe "d"
      watermark.child.exists(_.isStreaming) shouldBe true
    }

    it("preserves relation column aliases below the watermark") {
      val plan = parseQuery(
        """SELECT e.event_time
          |FROM STREAM events
          |WATERMARK event_time DELAY OF INTERVAL 5 SECONDS
          |AS e(id, event_time)""".stripMargin
      )

      val watermark =
        plan.collectWithSubqueries { case value: UnresolvedStreamingWatermark => value }.head
      val alias = watermark.child.asInstanceOf[SubqueryAlias]
      alias.alias shouldBe "e"
      alias.child shouldBe a[UnresolvedSubqueryColumnAliases]
      alias.child
        .asInstanceOf[UnresolvedSubqueryColumnAliases]
        .outputColumnNames shouldBe Seq("id", "event_time")
    }

    it("binds independent watermarks to both sides of a stream-stream join") {
      val plan = parseQuery(
        """SELECT a.id
          |FROM STREAM events_a
          |WATERMARK a.ts DELAY OF INTERVAL 10 SECONDS AS a
          |JOIN STREAM(events_b)
          |WATERMARK b.ts DELAY OF INTERVAL 20 SECONDS AS b
          |ON a.id = b.id""".stripMargin
      )

      val watermarks =
        plan.collectWithSubqueries { case value: UnresolvedStreamingWatermark => value }
      watermarks.size shouldBe 2
      watermarks.map(_.child.asInstanceOf[SubqueryAlias].alias).toSet shouldBe Set("a", "b")
    }

    it("rejects negative and malformed watermark delays") {
      an[ParseException] should be thrownBy {
        parseQuery(
          "SELECT * FROM STREAM events WATERMARK ts DELAY OF INTERVAL -1 SECOND"
        )
      }
      an[ParseException] should be thrownBy {
        parseQuery("SELECT * FROM STREAM events WATERMARK ts DELAY INTERVAL 1 SECOND")
      }
      an[ParseException] should be thrownBy {
        parseQuery(
          "SELECT * FROM STREAM events AS e WATERMARK e.ts DELAY OF INTERVAL 1 SECOND"
        )
      }
    }
  }

  describe("Spark 3.5 watermark lowering") {
    it("resolves an alias-qualified named timestamp to EventTimeWatermark") {
      val eventTime = AttributeReference("event_time", TimestampType)()
      val input = SubqueryAlias(
        "e",
        LocalRelation(output = Seq(eventTime), isStreaming = true)
      )
      val unresolved = UnresolvedStreamingWatermark(
        UnresolvedAttribute(Seq("e", "event_time")),
        IntervalUtils.fromIntervalString("INTERVAL 10 SECONDS"),
        input
      )

      val analyzed = spark.sessionState.analyzer.execute(unresolved)
      analyzed shouldBe a[EventTimeWatermark]
      analyzed.asInstanceOf[EventTimeWatermark].eventTime.name shouldBe "event_time"
    }

    it("projects an explicitly aliased derived timestamp before watermarking it") {
      val epoch = AttributeReference("epoch_seconds", LongType)()
      val derivedTimestamp = Alias(
        Cast(UnresolvedAttribute("events.epoch_seconds"), TimestampType),
        "event_time"
      )()
      val unresolved = UnresolvedStreamingWatermark(
        derivedTimestamp,
        IntervalUtils.fromIntervalString("INTERVAL 10 SECONDS"),
        SubqueryAlias(
          "events",
          LocalRelation(output = Seq(epoch), isStreaming = true)
        )
      )

      val analyzed  = spark.sessionState.analyzer.execute(unresolved)
      val watermark = analyzed.asInstanceOf[EventTimeWatermark]
      watermark.eventTime.name shouldBe "event_time"
      watermark.eventTime.qualifier shouldBe Seq("events")
      watermark.child shouldBe a[Project]
      watermark.child.output.map(_.name) should contain("event_time")
    }

    it("requires an explicit name for a derived watermark expression") {
      val epoch = AttributeReference("epoch_seconds", LongType)()
      val unresolved = UnresolvedStreamingWatermark(
        UnresolvedAlias(Cast(epoch, TimestampType)),
        IntervalUtils.fromIntervalString("INTERVAL 10 SECONDS"),
        LocalRelation(output = Seq(epoch), isStreaming = true)
      )

      an[AnalysisException] should be thrownBy {
        spark.sessionState.analyzer.execute(unresolved)
      }
    }
  }
}
