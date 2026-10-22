package org.openivm.spark.streaming

import org.scalatest.funspec.AnyFunSpec

class StreamingViewResolverSpec extends AnyFunSpec with StreamingTableTestFixture {

  describe("STREAM boundary table detection") {
    it("recognizes catalog tables so their boundaries skip analysis") {
      val source = "svr_table_source"
      createSource(source)
      val database = spark.catalog.currentDatabase

      StreamingViewResolver.isSessionCatalogTable(spark, Seq(source)) shouldBe true
      StreamingViewResolver.isSessionCatalogTable(spark, Seq(database, source)) shouldBe true
      StreamingViewResolver.isSessionCatalogTable(spark, Seq("spark_catalog", database, source)) shouldBe true
    }

    it("leaves views, temporary views and unknown names to full resolution") {
      val source = "svr_view_source"
      createSource(source)
      spark.sql(s"CREATE OR REPLACE VIEW svr_view AS SELECT * FROM $source").collect()
      spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW svr_temp_view AS SELECT * FROM $source").collect()

      StreamingViewResolver.isSessionCatalogTable(spark, Seq("svr_view")) shouldBe false
      StreamingViewResolver.isSessionCatalogTable(spark, Seq("svr_temp_view")) shouldBe false
      StreamingViewResolver.isSessionCatalogTable(spark, Seq("svr_missing_table")) shouldBe false
      StreamingViewResolver.isSessionCatalogTable(spark, Seq("other_catalog", "default", source)) shouldBe false
    }
  }
}
