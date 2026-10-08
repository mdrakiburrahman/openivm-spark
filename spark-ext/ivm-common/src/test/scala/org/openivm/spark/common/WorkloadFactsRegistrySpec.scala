package org.openivm.spark.common

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructField, StructType}
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

    it("preserves identity and generated-column facts from an analyzed public schema") {
      val source = s"wf_generated_$suffix"
      val schema = StructType(
        Seq(
          StructField("id", LongType, false, new MetadataBuilder().putLong("delta.identity.start", 1L).build()),
          StructField(
            "amount_twice",
            LongType,
            true,
            new MetadataBuilder().putString("delta.generationExpression", "id * 2").build()
          )
        )
      )
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1L, 2L))), schema).createOrReplaceTempView(source)
      try {
        val resolved = spark.table(source).schema
        val expected = WorkloadFactsRegistry.forRefresh().discover(spark, Seq(source))
        val facts = WorkloadFactsRegistry
          .forRefresh()
          .discover(spark, Seq(source), resolvedSourceSchemas = Map(source -> resolved))

        facts shouldBe expected
        facts.uniqueKeys shouldBe Seq(UniqueKey(source, Seq("id")))
        facts.generatedColumns should contain allOf (
          GeneratedColumn(source, "id", "IDENTITY"),
          GeneratedColumn(source, "amount_twice", "id * 2")
        )
      } finally spark.catalog.dropTempView(source)
    }

    it("reads fresh properties and falls back for sources without a supplied schema") {
      val source = s"wf_schema_reuse_$suffix"
      spark.sql(s"CREATE TABLE $source (id BIGINT, amount BIGINT) USING delta")
      try {
        val schema  = spark.table(source).schema
        val missing = s"wf_missing_$suffix"
        val schemas = Map(source -> schema)
        val before = WorkloadFactsRegistry
          .forRefresh()
          .discover(spark, Seq(source, missing), resolvedSourceSchemas = schemas)
        before.uniqueKeys shouldBe empty

        spark.sql(s"ALTER TABLE $source SET TBLPROPERTIES ('spark.openivm.unique_key' = 'id')")
        spark.sql(s"ALTER TABLE $source ADD CONSTRAINT positive_amount CHECK (amount > 0)")
        val after = WorkloadFactsRegistry
          .forRefresh()
          .discover(spark, Seq(source, missing), resolvedSourceSchemas = schemas)
        after shouldBe WorkloadFactsRegistry.forRefresh().discover(spark, Seq(source, missing))
        after.uniqueKeys shouldBe Seq(UniqueKey(source, Seq("id")))
        after.deltaConstraints.map(c => c.name -> c.expression) shouldBe Seq("positive_amount" -> "amount > 0")
      } finally spark.sql(s"DROP TABLE $source")
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

    it("discovers FK declarations from Spark conf and serializes them for compile facts") {
      val key = "spark.openivm.fk.fact_trade"
      spark.conf.set(key, "sk_account_id->dim_account(sk_account_id);sk_security_id=dim_security.sk_security_id")
      try {
        val facts = WorkloadFactsRegistry
          .forRefresh()
          .discover(spark, Seq("default.fact_trade", "default.dim_account", "default.dim_security"))

        facts.fkRelations should contain(
          ForeignKeyRelation("fact_trade", Seq("sk_account_id"), "dim_account", Seq("sk_account_id"))
        )
        facts.fkRelations should contain(
          ForeignKeyRelation("fact_trade", Seq("sk_security_id"), "dim_security", Seq("sk_security_id"))
        )
        WorkloadFacts(fkRelations = facts.fkRelations).toJson should include(
          """"fk_relations":[{"child_table":"fact_trade","child_columns":["sk_account_id"],"parent_table":"dim_account","parent_columns":["sk_account_id"],"rely":true},{"child_table":"fact_trade","child_columns":["sk_security_id"],"parent_table":"dim_security","parent_columns":["sk_security_id"],"rely":true}]"""
        )
      } finally {
        spark.conf.unset(key)
      }
    }
  }
}
