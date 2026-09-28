package org.openivm.spark.parser

import java.io.File
import java.util.{Collections, IdentityHashMap, UUID}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedSubqueryColumnAliases}
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.openivm.spark.commands.CreateStreamingTableCommand
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StreamingQueryParserSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("StreamingQueryParserSpec")
      .config("spark.sql.extensions", "org.openivm.spark.OpenIvmSparkExtensions")
      .config("spark.openivm.enabled", "true")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.warehouse.dir",
        new File(
          s"target/test-warehouse-streaming-query-${UUID.randomUUID().toString.take(8)}"
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

  private def relations(plan: LogicalPlan): Seq[UnresolvedRelation] = {
    val visited = Collections.newSetFromMap(
      new IdentityHashMap[LogicalPlan, java.lang.Boolean]()
    )

    def collect(current: LogicalPlan): Seq[UnresolvedRelation] = {
      if (!visited.add(current)) return Seq.empty
      val atNode = current match {
        case relation: UnresolvedRelation => Seq(relation)
        case _                            => Seq.empty
      }
      val nestedPlans = current.expressions
        .flatMap(_.collect { case expression: SubqueryExpression => expression.plan })
      val innerPlans = current.innerChildren.collect { case logical: LogicalPlan =>
        logical
      }
      atNode ++
        (current.children ++ innerPlans).distinct.flatMap(collect) ++
        nestedPlans.flatMap(collect)
    }
    collect(plan)
  }

  describe("stream relation binding") {
    it("parses bare and parenthesized STREAM forms as native streaming relations") {
      val plan = parseQuery(
        "SELECT * FROM STREAM catalog.db.left_events l " +
          "JOIN STREAM(catalog.db.right_events) r ON l.id = r.id"
      )

      val streaming = relations(plan).filter(_.isStreaming)
      plan.isStreaming shouldBe true
      streaming.map(_.multipartIdentifier) should contain theSameElementsAs Seq(
        Seq("catalog", "db", "left_events"),
        Seq("catalog", "db", "right_events")
      )
    }

    it("keeps a repeated ordinary relation static") {
      val plan = parseQuery(
        "SELECT s.id FROM STREAM db.events s JOIN db.events b ON s.id = b.id"
      )
      val eventRelations = relations(plan).filter(_.multipartIdentifier == Seq("db", "events"))

      eventRelations.count(_.isStreaming) shouldBe 1
      eventRelations.count(relation => !relation.isStreaming) shouldBe 1
    }

    it("binds different reader options to repeated source occurrences") {
      val plan = parseQuery(
        """SELECT a.id
          |FROM STREAM events
          |WITH ('maxFilesPerTrigger' = '1', 'escaped' = 'a''b') AS a
          |JOIN STREAM(events) WITH ('maxFilesPerTrigger' = '7') AS b
          |ON a.id = b.id""".stripMargin
      )

      val streaming = relations(plan).filter(_.isStreaming)
      streaming.map(_.options.get("maxFilesPerTrigger")) should contain theSameElementsAs Seq("1", "7")
      streaming
        .find(_.options.get("maxFilesPerTrigger") == "1")
        .get
        .options
        .get("escaped") shouldBe "a'b"
    }

    it("distinguishes a CTE WITH from relation-local reader WITH") {
      val plan = parseQuery(
        """WITH recent AS (
          |  WITH source_rows AS (
          |    SELECT * FROM STREAM `cat`.`db`.`events`
          |    WITH ('startingVersion' = '12')
          |  )
          |  SELECT * FROM source_rows
          |)
          |SELECT * FROM recent""".stripMargin
      )

      val source = relations(plan).find(_.isStreaming).get
      source.multipartIdentifier shouldBe Seq("cat", "db", "events")
      source.options.get("startingVersion") shouldBe "12"
    }

    it("binds sources inside nested subquery expressions") {
      val plan = parseQuery(
        """SELECT o.id
          |FROM outer_events o
          |WHERE EXISTS (
          |  SELECT 1 FROM STREAM nested_events n WHERE n.id = o.id
          |)""".stripMargin
      )

      relations(plan).count(relation =>
        relation.isStreaming && relation.multipartIdentifier == Seq("nested_events")
      ) shouldBe 1
    }

    it("ignores STREAM and WITH text inside literals and comments") {
      val plan = parseQuery(
        """SELECT 'FROM STREAM fake WITH (''x''=''y'')' AS text
          |FROM /* STREAM ignored */ STREAM real_events AS r
          |WHERE r.note <> 'WATERMARK ts DELAY OF INTERVAL 1 DAY'""".stripMargin
      )

      val streaming = relations(plan).filter(_.isStreaming)
      streaming.map(_.multipartIdentifier) shouldBe Seq(Seq("real_events"))
    }

    it("preserves quoted identifiers, relation aliases, and column alias lists") {
      val plan = parseQuery(
        "SELECT e.id FROM STREAM `cat`.`db.with.dot`.`events` AS `e`(`id`, `ts`)"
      )

      val source = relations(plan).find(_.isStreaming).get
      source.multipartIdentifier shouldBe Seq("cat", "db.with.dot", "events")
      val alias = plan.collectFirst {
        case value: SubqueryAlias if value.alias == "e" => value
      }.get
      alias.child shouldBe a[UnresolvedSubqueryColumnAliases]
      alias.child
        .asInstanceOf[UnresolvedSubqueryColumnAliases]
        .outputColumnNames shouldBe Seq("id", "ts")
    }
  }

  describe("reader option validation") {
    it("rejects duplicate keys and contradictory starting offsets") {
      an[ParseException] should be thrownBy {
        parseQuery(
          "SELECT * FROM STREAM events WITH ('ignoreDeletes'='true', 'IGNOREDELETES'='false')"
        )
      }

      an[ParseException] should be thrownBy {
        parseQuery(
          "SELECT * FROM STREAM events " +
            "WITH ('startingVersion'='0', 'startingTimestamp'='2024-01-01')"
        )
      }
    }

    it("rejects malformed STREAM and reader WITH clauses") {
      an[ParseException] should be thrownBy {
        parseQuery("SELECT * FROM STREAM()")
      }
      an[ParseException] should be thrownBy {
        parseQuery("SELECT * FROM STREAM events WITH ('x')")
      }
    }
  }
}
