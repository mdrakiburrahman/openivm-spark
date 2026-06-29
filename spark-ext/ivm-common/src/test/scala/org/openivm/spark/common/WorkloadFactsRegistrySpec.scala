package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructField}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class WorkloadFactsRegistrySpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _

  private val suffix = UUID.randomUUID().toString.replace("-", "").take(8)
  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-workload-facts-$suffix")
    d.mkdirs()
    d.getAbsolutePath
  }
  private val parentTable = s"wf_parent_$suffix"
  private val childTable  = s"wf_child_$suffix"

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-WorkloadFactsRegistrySpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.sql(s"DROP TABLE IF EXISTS $childTable")
        spark.sql(s"DROP TABLE IF EXISTS $parentTable")
        spark.stop()
      }
    } finally {
      deleteDir(new File(warehouseDir))
    }
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  describe("WorkloadFactsRegistry") {
    it("keeps defaults empty when no declarations exist") {
      val facts = WorkloadFactsRegistry.forRefresh().discover(spark, Seq.empty)

      facts.fkRelations shouldBe empty
      facts.uniqueKeys shouldBe empty
    }

    it("discovers FK and unique declarations from Delta table properties") {
      spark.sql(s"""
        CREATE TABLE $parentTable (
          id BIGINT,
          code STRING
        )
        USING delta
        TBLPROPERTIES ('spark.openivm.unique_key.pk' = 'id')
      """)
      spark.sql(s"""
        CREATE TABLE $childTable (
          id BIGINT,
          parent_id BIGINT,
          amount BIGINT
        )
        USING delta
        TBLPROPERTIES (
          'spark.openivm.fk.parent_id' = '$parentTable.id',
          'spark.openivm.unique_key' = 'id'
        )
      """)
      spark.sql(s"ALTER TABLE $childTable ADD CONSTRAINT positive_child_id CHECK (id > 0)")

      val facts = WorkloadFactsRegistry.forRefresh().discover(spark, Seq(childTable, parentTable))

      facts.fkRelations should contain(
        ForeignKeyRelation(childTable, Seq("parent_id"), parentTable, Seq("id"))
      )
      facts.uniqueKeys should contain(UniqueKey(childTable, Seq("id")))
      facts.uniqueKeys should contain(UniqueKey(parentTable, Seq("id")))
      facts.deltaConstraints.map(c => c.table -> c.name) should contain(childTable -> "positive_child_id")
    }

    it("recognizes Delta generated-column schema metadata when present") {
      val metadata = new MetadataBuilder()
        .putString("delta.generationExpression", "amount * 2")
        .build()

      WorkloadFactsRegistry.generatedColumn(
        "generated_table",
        StructField("amount_twice", LongType, true, metadata)
      ) should
        contain(GeneratedColumn("generated_table", "amount_twice", "amount * 2"))
    }

    it("accepts explicit WorkloadFacts config facts alongside discovered declarations") {
      val configuredFk = ForeignKeyRelation("lineitem", Seq("order_id"), "orders", Seq("id"))
      val configuredUk = UniqueKey("orders", Seq("id"))

      val facts = WorkloadFactsRegistry
        .forRefresh()
        .discover(spark, Seq.empty, Seq(configuredFk), Seq(configuredUk))

      facts.fkRelations shouldBe Seq(configuredFk)
      facts.uniqueKeys shouldBe Seq(configuredUk)
    }
  }
}
