package org.openivm.spark.streaming

import org.openivm.spark.common.{MvCatalog, StreamingDependencyCatalog}
import org.scalatest.funspec.AnyFunSpec

class ManagedTableCascadeSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("managed streaming and materialized table cascades") {
    it("drops transitive materialized-view descendants before their upstream MV") {
      createDeltaSource("managed_mv_chain_source", "id INT, value STRING")
      insertRows("managed_mv_chain_source", "(1, 'one')")
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_mv_chain_a AS " +
            "SELECT id, value FROM managed_mv_chain_source"
        )
        .collect()
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_mv_chain_b AS " +
            "SELECT id, value FROM managed_mv_chain_a"
        )
        .collect()
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_mv_chain_c AS " +
            "SELECT id, value FROM managed_mv_chain_b"
        )
        .collect()

      spark.sql("DROP MATERIALIZED VIEW managed_mv_chain_a").collect()

      Seq("managed_mv_chain_a", "managed_mv_chain_b", "managed_mv_chain_c").foreach { name =>
        spark.catalog.tableExists(name) shouldBe false
        MvCatalog.lookup(spark, org.apache.spark.sql.catalyst.TableIdentifier(name)) shouldBe None
      }
    }

    it("deduplicates materialized-view fan-out and diamond descendants") {
      createDeltaSource("managed_mv_diamond_source", "id INT, value STRING")
      insertRows("managed_mv_diamond_source", "(1, 'one')")
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_mv_diamond_a AS " +
            "SELECT id, value FROM managed_mv_diamond_source"
        )
        .collect()
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_mv_diamond_b AS " +
            "SELECT id, value FROM managed_mv_diamond_a"
        )
        .collect()
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_mv_diamond_c AS " +
            "SELECT id, value FROM managed_mv_diamond_a"
        )
        .collect()
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_mv_diamond_d AS " +
            "SELECT b.id, b.value FROM managed_mv_diamond_b b " +
            "JOIN managed_mv_diamond_c c ON b.id = c.id"
        )
        .collect()

      spark.sql("DROP MATERIALIZED VIEW managed_mv_diamond_a").collect()

      Seq(
        "managed_mv_diamond_a",
        "managed_mv_diamond_b",
        "managed_mv_diamond_c",
        "managed_mv_diamond_d"
      ).foreach(name => spark.catalog.tableExists(name) shouldBe false)
    }

    it("drops a materialized and streaming mixed chain before rebuilding its streaming root") {
      createDeltaSource("managed_mixed_rebuild_source", "id INT, value STRING")
      insertRows("managed_mixed_rebuild_source", "(1, 'one')")
      val root = createStreamingSql(
        """CREATE STREAMING TABLE managed_mixed_rebuild_a
          |AS SELECT id, value FROM STREAM managed_mixed_rebuild_source""".stripMargin
      )
      process(root)
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_mixed_rebuild_b AS " +
            "SELECT id, value FROM managed_mixed_rebuild_a"
        )
        .collect()
      val leaf = createStreamingSql(
        """CREATE STREAMING TABLE managed_mixed_rebuild_c
          |AS SELECT id, value FROM STREAM managed_mixed_rebuild_b""".stripMargin
      )
      process(leaf)

      val rebuilt = createStreamingSql(
        """CREATE STREAMING TABLE managed_mixed_rebuild_a
          |OPTIONS ('onQueryChange' = 'rebuild')
          |AS SELECT id, value FROM STREAM managed_mixed_rebuild_source WHERE id = 1""".stripMargin
      )
      process(rebuilt)

      spark.catalog.tableExists("managed_mixed_rebuild_a") shouldBe true
      spark.catalog.tableExists("managed_mixed_rebuild_b") shouldBe false
      spark.catalog.tableExists("managed_mixed_rebuild_c") shouldBe false
    }

    it("drops a streaming child before its materialized-view parent") {
      createDeltaSource("managed_mv_stream_source", "id INT, value STRING")
      insertRows("managed_mv_stream_source", "(1, 'one')")
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_mv_stream_parent AS " +
            "SELECT id, value FROM managed_mv_stream_source"
        )
        .collect()
      val child = createStreamingSql(
        """CREATE STREAMING TABLE managed_mv_stream_child
          |AS SELECT id, value FROM STREAM managed_mv_stream_parent""".stripMargin
      )
      process(child)

      spark.sql("DROP MATERIALIZED VIEW managed_mv_stream_parent").collect()

      spark.catalog.tableExists("managed_mv_stream_parent") shouldBe false
      spark.catalog.tableExists("managed_mv_stream_child") shouldBe false
    }

    it("drops materialized descendants when explicitly dropping a streaming parent") {
      createDeltaSource("managed_stream_drop_source", "id INT, value STRING")
      insertRows("managed_stream_drop_source", "(1, 'one')")
      val parent = createStreamingSql(
        """CREATE STREAMING TABLE managed_stream_drop_parent
          |AS SELECT id, value FROM STREAM managed_stream_drop_source""".stripMargin
      )
      process(parent)
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_stream_drop_child AS " +
            "SELECT id, value FROM managed_stream_drop_parent"
        )
        .collect()

      spark.sql("DROP STREAMING TABLE managed_stream_drop_parent").collect()

      spark.catalog.tableExists("managed_stream_drop_parent") shouldBe false
      spark.catalog.tableExists("managed_stream_drop_child") shouldBe false
    }

    it("fails a materialized-view drop before mutation when a streaming child record is corrupt") {
      createDeltaSource("managed_mv_corrupt_source", "id INT, value STRING")
      insertRows("managed_mv_corrupt_source", "(1, 'one')")
      spark
        .sql(
          "CREATE MATERIALIZED VIEW managed_mv_corrupt_parent AS " +
            "SELECT id, value FROM managed_mv_corrupt_source"
        )
        .collect()
      val child = createStreamingSql(
        """CREATE STREAMING TABLE managed_mv_corrupt_child
          |AS SELECT id, value FROM STREAM managed_mv_corrupt_parent""".stripMargin
      )
      process(child)
      val childIdentity =
        StreamingTableMetadata.canonicalIdentity(spark, Seq("managed_mv_corrupt_child"))
      StreamingDependencyCatalog.removeTargetRecordForTesting(spark, childIdentity)

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        spark.sql("DROP MATERIALIZED VIEW managed_mv_corrupt_parent").collect()
      }

      spark.catalog.tableExists("managed_mv_corrupt_parent") shouldBe true
      spark.catalog.tableExists("managed_mv_corrupt_child") shouldBe true
      Option(spark.streams.get(child.queryId)).exists(_.isActive) shouldBe true
    }
  }
}
