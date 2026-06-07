package org.openivm.spark.parity

import org.apache.spark.sql.SparkSession
import org.openivm.spark.commands.{
  CreateMaterializedViewCommand,
  DropMaterializedViewCommand,
  RefreshMaterializedViewCommand
}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.openivm.spark.parser.IvmParser
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Slice of `ParserSpec` covering direct `IvmParser.parsePlan` dispatch:
  * CREATE / REFRESH / DROP MATERIALIZED VIEW each route to the correct
  * command, while non-MV SQL falls through to Spark's wrapped parser
  * unchanged.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
class ParserPassthroughSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-parser-pt-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ParserPassthroughSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
    }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (15) Direct IvmParser.parsePlan exercises the injected parser without
  //     going through Spark's session SQL machinery. We instantiate the
  //     parser exactly as OpenIvmSparkExtensions does.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(15) IvmParser directly: parsePlan dispatch") {

    it("dispatches CREATE MATERIALIZED VIEW to CreateMaterializedViewCommand") {
      val parser = new IvmParser(spark, spark.sessionState.sqlParser)
      val plan   = parser.parsePlan("CREATE MATERIALIZED VIEW v15a AS SELECT 1 AS x")
      plan shouldBe a[CreateMaterializedViewCommand]
    }

    it("dispatches REFRESH MATERIALIZED VIEW to RefreshMaterializedViewCommand") {
      val parser = new IvmParser(spark, spark.sessionState.sqlParser)
      val plan   = parser.parsePlan("REFRESH MATERIALIZED VIEW v15b")
      plan shouldBe a[RefreshMaterializedViewCommand]
    }

    it("dispatches DROP MATERIALIZED VIEW to DropMaterializedViewCommand") {
      val parser = new IvmParser(spark, spark.sessionState.sqlParser)
      val plan   = parser.parsePlan("DROP MATERIALIZED VIEW v15c")
      plan shouldBe a[DropMaterializedViewCommand]
    }

    it("delegates non-MV SQL (SELECT 1) to the wrapped Spark parser unchanged") {
      val parser = new IvmParser(spark, spark.sessionState.sqlParser)
      // Should not throw — and should resolve to a Spark logical plan,
      // not to one of our MV commands.
      val plan = parser.parsePlan("SELECT 1 AS x")
      plan shouldNot be(a[CreateMaterializedViewCommand])
      plan shouldNot be(a[RefreshMaterializedViewCommand])
      plan shouldNot be(a[DropMaterializedViewCommand])
    }
  }
}
