package org.openivm.spark.streaming

import org.scalatest.funspec.AnyFunSpec

class StreamingTableNamespaceSpec extends AnyFunSpec with StreamingTableTestFixture {

  describe("catalog-resolved streaming-table identities") {
    it("keeps same short table names distinct across namespaces after USE changes") {
      val leftDb  = "strt_ns_left"
      val rightDb = "strt_ns_right"
      val source  = "source"
      val target  = "same_target"
      spark.sql(s"CREATE DATABASE `$leftDb`").collect()
      spark.sql(s"CREATE DATABASE `$rightDb`").collect()

      spark.catalog.setCurrentDatabase(leftDb)
      createSource(source)
      val left = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $leftDb.$source"
      )

      spark.catalog.setCurrentDatabase(rightDb)
      createSource(source)
      val right = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $rightDb.$source"
      )

      left.tableName should not be right.tableName
      StreamingTableManager.show(spark, Some(Seq(leftDb))).map(_.tableName) should contain(left.tableName)
      StreamingTableManager.show(spark, Some(Seq(rightDb))).map(_.tableName) should contain(right.tableName)

      StreamingTableManager.drop(spark, Seq(leftDb, target), ifExists = false)
      StreamingTableManager.drop(spark, Seq(rightDb, target), ifExists = false)
      spark.catalog.setCurrentDatabase("default")
      spark.sql(s"DROP DATABASE `$leftDb` CASCADE").collect()
      spark.sql(s"DROP DATABASE `$rightDb` CASCADE").collect()
    }
  }
}
