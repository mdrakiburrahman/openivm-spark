package org.openivm.spark.common

import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SourceVersionDeltaSpec extends AnyFunSpec with Matchers {

  describe("buildBagDiffSql") {
    it("emits a duplicate-sensitive signed old/new snapshot difference") {
      val batch = SourceVersionChangeBatch("db.orders", 11L, 12L)
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("value", StringType, nullable = true)
        )
      )

      val sql = SourceVersionDelta.buildBagDiffSql(batch, schema)
      sql should include("`db`.`orders` VERSION AS OF 11")
      sql should include("`db`.`orders` VERSION AS OF 12")
      sql.split("EXCEPT ALL", -1) should have length 3
      sql should include("CAST(-1 AS INT) AS `openivm_multiplicity`")
      sql should include("CAST(1 AS INT) AS `openivm_multiplicity`")
      sql should include("UNION ALL")
    }
  }

  describe("supportsBagDiff") {
    it("accepts nested arrays and rejects MapType at any depth") {
      SourceVersionDelta.supportsBagDiff(
        StructType(Seq(StructField("ids", ArrayType(IntegerType), nullable = true)))
      ) shouldBe true
      SourceVersionDelta.supportsBagDiff(
        StructType(
          Seq(
            StructField(
              "nested",
              StructType(Seq(StructField("attrs", MapType(StringType, StringType), nullable = true))),
              nullable = true
            )
          )
        )
      ) shouldBe false
    }
  }
}
