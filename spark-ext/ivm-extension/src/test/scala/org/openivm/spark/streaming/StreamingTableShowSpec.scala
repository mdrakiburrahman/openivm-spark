package org.openivm.spark.streaming

import org.openivm.spark.commands.ShowStreamingTablesCommand
import org.scalatest.funspec.AnyFunSpec

class StreamingTableShowSpec extends AnyFunSpec with StreamingTableTestFixture {
  describe("filtered streaming status") {
    it("parses quoted namespaces and optional Spark SHOW patterns") {
      val command = spark.sessionState.sqlParser
        .parsePlan("SHOW STREAMING TABLES IN `catalog`.`schema with spaces` LIKE 'target*|other';")
        .asInstanceOf[ShowStreamingTablesCommand]
      command.namespace shouldBe Some(Seq("catalog", "schema with spaces"))
      command.pattern shouldBe Some("target*|other")
      spark.sessionState.sqlParser
        .parsePlan("SHOW STREAMING TABLES LIKE 'target'")
        .asInstanceOf[ShowStreamingTablesCommand]
        .namespace shouldBe None
      spark.sessionState.sqlParser
        .parsePlan("SHOW STREAMING TABLES")
        .asInstanceOf[ShowStreamingTablesCommand]
        .pattern shouldBe None
      an[org.apache.spark.sql.catalyst.parser.ParseException] should be thrownBy {
        spark.sessionState.sqlParser.parsePlan("SHOW STREAMING TABLES LIKE target")
      }
    }

    it("resolves one of seventy candidates before any target metadata lookup") {
      val names = (1 to 70).map(index => Seq("catalog", "schema", s"target_$index"))
      var reads = Vector.empty[Seq[String]]
      def lookup(name: Seq[String]): Option[String] = {
        reads :+= name
        Some(name.last)
      }
      StreamingTableManager.resolveShowCandidates(names, Some("target_42"))(lookup) shouldBe Seq("target_42")
      reads shouldBe Vector(Seq("catalog", "schema", "target_42"))
      reads = Vector.empty
      StreamingTableManager.resolveShowCandidates(names, Some("absent"))(lookup) shouldBe empty
      reads shouldBe empty
      StreamingTableManager.resolveShowCandidates(names, None)(lookup).size shouldBe 70
      reads.size shouldBe 70
      StreamingTableManager.resolveShowCandidates(names, Some("TARGET_42|target_43"))(lookup) shouldBe
        Seq("target_42", "target_43")
    }

    it("preserves selected-target metadata validation, status schema and unfiltered enumeration") {
      createSource("show_source")
      appendRows("show_source", "(1, 'one', 'p')")
      val status = createStreaming(
        "show_target",
        readStream("show_source"),
        "SELECT id, value, part FROM STREAM show_source",
        options = Map("trigger" -> "availableNow")
      )
      spark.streams.get(status.queryId.get).awaitTermination(30000L) shouldBe true
      val filtered = spark.sql("SHOW STREAMING TABLES LIKE 'show_target'")
      filtered.columns.toSeq shouldBe Seq(
        "table_name",
        "query_id",
        "run_id",
        "status",
        "is_active",
        "checkpoint_location",
        "definition_hash",
        "last_progress",
        "last_failure"
      )
      val row = filtered.collect().head
      row.getAs[String]("status") shouldBe "stopped"
      row.getAs[Boolean]("is_active") shouldBe false
      row.getAs[String]("query_id") shouldBe status.queryId.get
      row.getAs[String]("run_id") shouldBe status.runId.get
      row.getAs[String]("checkpoint_location") shouldBe status.checkpointLocation.get
      row.getAs[String]("definition_hash") shouldBe status.definitionHash.get
      spark.sql("SHOW STREAMING TABLES").collect().map(_.getAs[String]("table_name")) should contain(status.tableName)
      spark.sql("SHOW STREAMING TABLES LIKE 'absent'").collect() shouldBe empty
      spark.sql("ALTER TABLE show_target SET TBLPROPERTIES ('openivm.streaming.delta-id' = 'replaced')").collect()
      val invalid = spark.sql("SHOW STREAMING TABLES LIKE 'show_target'").collect().head
      invalid.getAs[String]("status") shouldBe "metadata_error"
      invalid.getAs[String]("last_failure") should not be null
    }
  }
}
