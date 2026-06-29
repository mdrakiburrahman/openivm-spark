package org.openivm.spark.compiler

import org.openivm.spark.common.{DeltaShape, ForeignKeyRelation, UniqueKey}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class WorkloadFactsSpec extends AnyFunSpec with Matchers {
  describe("WorkloadFacts") {
    it("serializes empty registry facts by default") {
      WorkloadFacts().toJson shouldBe
        """{"schema_version":2,"target_dialect":"spark","compile_only":true,"force_view_delta_cascade":true,"assume_insert_only":false,"delta_shape":{},"fk_relations":[],"unique_keys":[]}"""
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
  }
}
