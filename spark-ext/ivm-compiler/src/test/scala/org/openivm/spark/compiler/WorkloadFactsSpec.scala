package org.openivm.spark.compiler

import org.openivm.spark.common.{
  DeltaShape,
  ForeignKeyRelation,
  UniqueKey,
  WorkloadColumnStats,
  WorkloadDeltaStats,
  WorkloadFacts,
  WorkloadTableStats
}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class WorkloadFactsSpec extends AnyFunSpec with Matchers {
  describe("WorkloadFacts") {
    it("serializes empty registry facts by default") {
      WorkloadFacts().toJson shouldBe
        """{"schema_version":2,"target_dialect":"spark","compile_only":true,"force_view_delta_cascade":true,"assume_insert_only":false,"running_window_incremental":false,"delta_shape":{},"fk_relations":[],"unique_keys":[],"table_stats":{},"column_stats":{},"delta_stats":{}}"""
    }

    it("serializes per-source delta_shape facts deterministically") {
      val facts = WorkloadFacts(
        deltaShape = Map(
          "default.dim_customer" -> DeltaShape.Unchanged,
          "default.fact_sales"   -> DeltaShape.InsertOnly,
          "default.dim_product"  -> DeltaShape.General
        )
      )

      facts.toJson should include(
        """"delta_shape":{"default.dim_customer":"UNCHANGED","default.dim_product":"GENERAL","default.fact_sales":"INSERT_ONLY"}"""
      )
    }

    it("serializes fk_relations and unique_keys for openivm compile facts") {
      val facts = WorkloadFacts(
        fkRelations = Seq(ForeignKeyRelation("orders", Seq("customer_id"), "customers", Seq("id"))),
        uniqueKeys = Seq(UniqueKey("customers", Seq("id")))
      )

      facts.toJson should include(
        """"fk_relations":[{"child_table":"orders","child_columns":["customer_id"],"parent_table":"customers","parent_columns":["id"],"rely":true}]"""
      )
      facts.toJson should include(
        """"unique_keys":[{"table":"customers","columns":["id"],"rely":true}]"""
      )
    }

    it("serializes table, column, and delta stats for openivm compile facts") {
      val facts = WorkloadFacts(
        tableStats = Map(
          "sales.orders" -> WorkloadTableStats(
            rowCount = Some(100L),
            numFiles = Some(2L),
            sizeBytes = Some(4096L),
            partitionColumns = Seq("dt")
          )
        ),
        columnStats = Map(
          "sales.orders.customer_id" -> WorkloadColumnStats(
            ndv = Some(10L),
            min = Some("1"),
            max = Some("99"),
            nulls = Some(0L),
            rowCount = Some(100L)
          )
        ),
        deltaStats = Map(
          "sales.orders" -> WorkloadDeltaStats(
            rowCount = Some(5L),
            numFiles = Some(1L),
            min = Map("customer_id" -> "7"),
            max = Map("customer_id" -> "9"),
            nulls = Map("customer_id" -> 0L)
          )
        )
      )

      facts.toJson should include(
        """"table_stats":{"sales.orders":{"row_count":100,"num_files":2,"size_bytes":4096,"partition_columns":["dt"]}}"""
      )
      facts.toJson should include(
        """"column_stats":{"sales.orders.customer_id":{"ndv":10,"min":"1","max":"99","nulls":0,"row_count":100}}"""
      )
      facts.toJson should include(
        """"delta_stats":{"sales.orders":{"row_count":5,"num_files":1,"min":{"customer_id":"7"},"max":{"customer_id":"9"},"nulls":{"customer_id":0}}}"""
      )
    }

    it("does not serialize declare_rely_fk into openivm compile facts") {
      WorkloadFacts(declareRelyFk = true).toJson should not include "declare_rely_fk"
    }
  }
}
