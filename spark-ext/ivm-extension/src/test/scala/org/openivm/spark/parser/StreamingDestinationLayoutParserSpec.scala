package org.openivm.spark.parser

import java.io.File
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParseException
import org.openivm.spark.commands.CreateStreamingTableCommand
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StreamingDestinationLayoutParserSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("StreamingDestinationLayoutParserSpec")
      .config("spark.sql.extensions", "org.openivm.spark.OpenIvmSparkExtensions")
      .config("spark.openivm.enabled", "true")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.warehouse.dir",
        new File(
          s"target/test-warehouse-streaming-layout-parser-${UUID.randomUUID().toString.take(8)}"
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

  private def parseCreate(sqlText: String): CreateStreamingTableCommand =
    spark.sessionState.sqlParser
      .parsePlan(sqlText)
      .asInstanceOf[CreateStreamingTableCommand]

  describe("streaming destination layout parsing") {
    it("defaults clustering to empty") {
      val command =
        parseCreate("CREATE STREAMING TABLE target AS SELECT 1 AS id")

      command.spec.partitionColumns shouldBe empty
      command.spec.clusterColumns shouldBe empty
    }

    it("preserves clustering identifier segments and quoted dotted names") {
      val command = parseCreate(
        """CREATE STREAMING TABLE `target`
          |CLUSTER BY (`region.name`, payload.region, event_date)
          |AS
          |SELECT
          |  'east' AS `region.name`,
          |  named_struct('region', 'north') AS payload,
          |  DATE '2026-09-19' AS event_date""".stripMargin
      )

      command.spec.clusterColumns shouldBe Seq(
        Seq("region.name"),
        Seq("payload", "region"),
        Seq("event_date")
      )
    }

    it("captures partitioning and clustering separately for runtime validation") {
      val command = parseCreate(
        """CREATE STREAMING TABLE target
          |PARTITIONED BY (event_date)
          |CLUSTER BY (region, event_date)
          |AS SELECT 'east' AS region, DATE '2026-09-19' AS event_date""".stripMargin
      )

      command.spec.partitionColumns shouldBe Seq("event_date")
      command.spec.clusterColumns shouldBe Seq(Seq("region"), Seq("event_date"))
    }

    it("rejects duplicate destination layout clauses") {
      an[ParseException] should be thrownBy {
        parseCreate(
          """CREATE STREAMING TABLE target
            |CLUSTER BY (region)
            |CLUSTER BY (event_date)
            |AS SELECT 'east' AS region, DATE '2026-09-19' AS event_date""".stripMargin
        )
      }
      an[ParseException] should be thrownBy {
        parseCreate(
          """CREATE STREAMING TABLE target
            |PARTITIONED BY (region)
            |PARTITIONED BY (event_date)
            |AS SELECT 'east' AS region, DATE '2026-09-19' AS event_date""".stripMargin
        )
      }
    }
  }
}
