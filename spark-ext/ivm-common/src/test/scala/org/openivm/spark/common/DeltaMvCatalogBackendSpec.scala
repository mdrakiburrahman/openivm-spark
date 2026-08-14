package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class DeltaMvCatalogBackendSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _
  private val root = {
    val dir = new File(s"target/test-delta-mv-catalog-${UUID.randomUUID().toString.take(8)}")
    dir.mkdirs()
    dir.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("openivm-spark-DeltaMvCatalogBackendSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", s"$root/warehouse")
      .config(FeatureGate.CatalogBackendKey, "delta")
      .config(FeatureGate.CatalogPathKey, s"$root/catalog")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally deleteDir(new File(root))
  }

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  private def metadata(name: String, source: String): MvMetadata =
    MvMetadata(
      name = CatalystSqlParser.parseTableIdentifier(name),
      querySql = s"SELECT count(*) FROM $source",
      refreshType = 0,
      refreshTypeName = "AGGREGATE_GROUP",
      lastVersion = 1L,
      sourceTables = Seq(source),
      sourceSchemaFingerprint = MvCatalog.schemaFingerprint(
        Map(source -> StructType(Seq(StructField("id", IntegerType))))
      ),
      location = s"$root/data/$name",
      createdAt = new Timestamp(1700000000000L),
      properties = Map("owner" -> "openivm")
    )

  describe("Delta MV catalog backend") {
    it("round-trips, indexes dependencies, advances, updates, and removes metadata") {
      val orders   = metadata("db.mv_orders", "db.orders")
      val customer = metadata("db.mv_customer", "db.customer")

      MvCatalog.ensureTables(spark)
      MvCatalog.upsert(spark, orders)
      MvCatalog.upsert(spark, customer)

      MvCatalog.lookup(spark, orders.name) shouldBe Some(orders)
      MvCatalog.list(spark).map(_.name).toSet shouldBe Set(orders.name, customer.name)
      MvCatalog.viewsForSource(spark, "db.orders").map(_.name) shouldBe Seq(orders.name)

      MvCatalog.advance(spark, orders.name, 9L)
      MvCatalog.updateProperties(spark, orders.name, Map("owner" -> "delta"))
      val updated = MvCatalog.lookup(spark, orders.name).get
      updated.lastVersion shouldBe 9L
      updated.properties shouldBe Map("owner" -> "delta")

      MvCatalog.remove(spark, orders.name)
      MvCatalog.lookup(spark, orders.name) shouldBe None
      MvCatalog.lookup(spark, customer.name) shouldBe Some(customer)
    }

    it("stores monotonic CDF watermarks in the shared Delta catalog") {
      CdfWatermarkCatalog.putAll(spark, "db.mv_orders", Map("db.orders" -> 4L, "db.lineitem" -> 7L))
      CdfWatermarkCatalog.put(spark, "db.mv_orders", "db.orders", 3L)

      CdfWatermarkCatalog.get(spark, "db.mv_orders", "db.orders") shouldBe Some(4L)
      CdfWatermarkCatalog.get(spark, "db.mv_orders", "db.lineitem") shouldBe Some(7L)

      CdfWatermarkCatalog.removeForBaseTable(spark, "db.lineitem")
      CdfWatermarkCatalog.get(spark, "db.mv_orders", "db.lineitem") shouldBe None
      CdfWatermarkCatalog.removeForView(spark, "db.mv_orders")
      CdfWatermarkCatalog.get(spark, "db.mv_orders", "db.orders") shouldBe None
    }

    it("commits independent MV metadata upserts concurrently") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val entries = (1 to 8).map(index => metadata(s"db.mv_parallel_$index", s"db.source_$index"))

      Await.result(
        Future.traverse(entries)(entry => Future(MvCatalog.upsert(spark.newSession(), entry))),
        2.minutes
      )

      entries.foreach(entry => MvCatalog.lookup(spark, entry.name) shouldBe Some(entry))
    }

    it("does not regress lastVersion while properties are updated concurrently") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val entry                                       = metadata("db.mv_property_race", "db.source_property_race")
      MvCatalog.upsert(spark, entry)

      val advances = Future {
        (2L to 20L).foreach(version => MvCatalog.advance(spark.newSession(), entry.name, version))
      }
      val propertyUpdates = Future {
        (1 to 20).foreach(index =>
          MvCatalog.updateProperties(spark.newSession(), entry.name, Map("revision" -> index.toString))
        )
      }
      Await.result(Future.sequence(Seq(advances, propertyUpdates)), 2.minutes)

      val updated = MvCatalog.lookup(spark, entry.name).get
      updated.lastVersion shouldBe 20L
      updated.properties.keySet shouldBe Set("revision")
    }

    it("partitions each Delta catalog by its independent write key") {
      def partitionColumns(relativePath: String): Seq[String] =
        DeltaTable
          .forPath(spark, s"$root/catalog/$relativePath")
          .detail()
          .select("partitionColumns")
          .head()
          .getSeq[String](0)

      partitionColumns("mv_metadata") shouldBe Seq("name")
      partitionColumns("cdf_watermarks") shouldBe Seq("view_name")
    }
  }
}
