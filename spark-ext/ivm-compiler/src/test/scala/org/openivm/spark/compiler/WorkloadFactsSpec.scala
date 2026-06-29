package org.openivm.spark.compiler

import org.openivm.spark.common.{ForeignKeyRelation, UniqueKey}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class WorkloadFactsSpec extends AnyFunSpec with Matchers {
  describe("WorkloadFacts") {
    it("serializes empty registry facts by default") {
      WorkloadFacts().toJson shouldBe
        """{"schema_version":2,"target_dialect":"spark","compile_only":true,"force_view_delta_cascade":true,"assume_insert_only":false,"fk_relations":[],"unique_keys":[]}"""
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
