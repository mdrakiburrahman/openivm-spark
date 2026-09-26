package org.openivm.spark.parser

import java.io.File
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.openivm.spark.commands.{
  CreateMaterializedViewCommand,
  CreateStreamingTableCommand,
  DropStreamingTableCommand,
  ShowStreamingTablesCommand,
  StopStreamingTableCommand
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StreamingDdlParserSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("StreamingDdlParserSpec")
      .config("spark.sql.extensions", "org.openivm.spark.OpenIvmSparkExtensions")
      .config("spark.openivm.enabled", "true")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.warehouse.dir",
        new File(
          s"target/test-warehouse-streaming-ddl-${UUID.randomUUID().toString.take(8)}"
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

  private def parseCreate(sql: String): CreateStreamingTableCommand =
    spark.sessionState.sqlParser.parsePlan(sql).asInstanceOf[CreateStreamingTableCommand]

  describe("CREATE STREAMING TABLE") {
    it("parses defaults and delegates the complete SELECT to Spark") {
      val command = parseCreate(
        "CREATE STREAMING TABLE sink AS SELECT id FROM source WHERE id > 0"
      )

      command.spec.name shouldBe Seq("sink")
      command.spec.provider shouldBe None
      command.spec.location shouldBe None
      command.spec.partitionColumns shouldBe empty
      command.spec.tableProperties shouldBe empty
      command.spec.options shouldBe empty
      command.spec.ifNotExists shouldBe false
      command.spec.queryText shouldBe "SELECT id FROM source WHERE id > 0"
      command.spec.query shouldBe a[Project]
    }

    it("captures all clauses for runtime validation in flexible order") {
      val command = parseCreate(
        """CREATE STREAMING TABLE IF NOT EXISTS `cat`.`db`.`sink.table`
          |USING parquet
          |LOCATION '/warehouse/it''s-here'
          |PARTITIONED BY (`event.date`, bucket)
          |OPTIONS ('outputMode' = 'append', 'triggerInterval' = '10 seconds')
          |TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
          |AS SELECT 1 AS id""".stripMargin
      )

      command.spec.name shouldBe Seq("cat", "db", "sink.table")
      command.spec.provider shouldBe Some("parquet")
      command.spec.location shouldBe Some("/warehouse/it's-here")
      command.spec.partitionColumns shouldBe Seq("event.date", "bucket")
      command.spec.tableProperties shouldBe Map("delta.enableChangeDataFeed" -> "true")
      command.spec.options shouldBe Map(
        "outputMode"      -> "append",
        "triggerInterval" -> "10 seconds"
      )
      command.spec.ifNotExists shouldBe true
    }

    it("accepts case-insensitive keywords, comments, and a trailing semicolon") {
      val command = parseCreate(
        """/* lead */ cReAtE /* head */ sTrEaMiNg TABLE `sink`
          |OPTIONS ('escaped' = 'a''b') AS
          |SELECT 'STREAM fake WATERMARK ts DELAY OF INTERVAL 1 DAY' AS text;""".stripMargin
      )

      command.spec.name shouldBe Seq("sink")
      command.spec.options shouldBe Map("escaped" -> "a'b")
      command.spec.queryText should include("STREAM fake WATERMARK")
    }

    it("rejects duplicate option names case-insensitively") {
      an[ParseException] should be thrownBy {
        parseCreate(
          "CREATE STREAMING TABLE sink OPTIONS ('outputMode'='append', 'OUTPUTMODE'='complete') " +
            "AS SELECT 1"
        )
      }
    }

    it("rejects repeated declaration clauses") {
      an[ParseException] should be thrownBy {
        parseCreate(
          "CREATE STREAMING TABLE sink USING delta USING parquet AS SELECT 1"
        )
      }
    }
  }

  describe("streaming lifecycle statements") {
    it("parses SHOW with and without a multipart namespace") {
      spark.sessionState.sqlParser
        .parsePlan("SHOW STREAMING TABLES")
        .asInstanceOf[ShowStreamingTablesCommand]
        .namespace shouldBe None

      spark.sessionState.sqlParser
        .parsePlan("SHOW STREAMING TABLES IN `cat`.`analytics.db`;")
        .asInstanceOf[ShowStreamingTablesCommand]
        .namespace shouldBe Some(Seq("cat", "analytics.db"))
    }

    it("parses ALTER STOP and DROP IF EXISTS") {
      spark.sessionState.sqlParser
        .parsePlan("ALTER STREAMING TABLE `db`.`events` STOP")
        .asInstanceOf[StopStreamingTableCommand]
        .name shouldBe Seq("db", "events")

      val drop = spark.sessionState.sqlParser
        .parsePlan("DROP STREAMING TABLE IF EXISTS cat.db.events")
        .asInstanceOf[DropStreamingTableCommand]
      drop.name shouldBe Seq("cat", "db", "events")
      drop.ifExists shouldBe true
    }

    it("rejects malformed lifecycle and declaration clauses") {
      an[ParseException] should be thrownBy {
        spark.sessionState.sqlParser.parsePlan("ALTER STREAMING TABLE events")
      }
      an[ParseException] should be thrownBy {
        spark.sessionState.sqlParser.parsePlan(
          "CREATE STREAMING TABLE sink LOCATION 123 AS SELECT 1"
        )
      }
    }
  }

  describe("existing parser behavior") {
    it("continues to delegate ordinary SQL and parse materialized-view SQL") {
      spark.sessionState.sqlParser.parsePlan("SELECT 1") shouldBe a[Project]
      val materializedView =
        spark.sessionState.sqlParser.parsePlan("CREATE MATERIALIZED VIEW mv AS SELECT 1")
      materializedView shouldBe a[CreateMaterializedViewCommand]
    }
  }
}
