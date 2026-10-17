package org.openivm.spark.analyzer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog, StagingDelta}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import java.util.UUID
import scala.jdk.CollectionConverters._

/** Base-table commits that no intercepted write recorded reach dependents exactly once. */
class SourceChangeLedgerSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _
  private val warehouse           = new File(s"target/test-warehouse-ledger-${UUID.randomUUID().toString.take(8)}")

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-SourceChangeLedgerSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.openivm.changeFeed.mode", "intercept")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (warehouse.exists()) {
      val paths = Files.walk(warehouse.toPath)
      try paths.iterator().asScala.toSeq.reverse.foreach(Files.delete)
      finally paths.close()
    }
  }

  /** A source with a projection and an aggregate over it. */
  private def fixture(table: String): Unit = {
    spark.sql(s"CREATE TABLE $table (id INT, name STRING) USING delta")
    spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'a')")
    spark.sql(s"CREATE MATERIALIZED VIEW ${table}_proj AS SELECT id, name FROM $table")
    spark.sql(s"CREATE MATERIALIZED VIEW ${table}_agg AS SELECT name, count(*) AS n FROM $table GROUP BY name")
  }

  /** Run `sql` the way a writer without the OpenIVM extension would. */
  private def uninterceptedWrite(sql: String): Unit = {
    IvmDmlInterceptorRule.bypass.set(true)
    try spark.sql(sql).collect()
    finally IvmDmlInterceptorRule.bypass.set(false)
  }

  private def captured(table: String): Seq[StagingDelta] = {
    SourceChangeLedger.reconcile(spark, Seq(s"default.$table"))
    StagingCatalog.collectFor(spark, "__probe__", Seq(s"default.$table"))
  }

  private def assertBagEqual(actual: DataFrame, expected: DataFrame): Unit = {
    val missing = expected.exceptAll(actual).collect().mkString(", ")
    val extra   = actual.exceptAll(expected).collect().mkString(", ")
    withClue(s"missing=[$missing] extra=[$extra]") {
      missing shouldBe ""
      extra shouldBe ""
    }
  }

  private def assertDependentsMatch(table: String): Unit = {
    spark.sql(s"REFRESH MATERIALIZED VIEW ${table}_proj")
    spark.sql(s"REFRESH MATERIALIZED VIEW ${table}_agg")
    assertBagEqual(spark.table(s"${table}_proj").select("id", "name"), spark.sql(s"SELECT id, name FROM $table"))
    assertBagEqual(
      spark.table(s"${table}_agg").select("name", "n"),
      spark.sql(s"SELECT name, count(*) AS n FROM $table GROUP BY name")
    )
  }

  describe("SourceChangeLedger") {
    it("captures rows appended without interception as inserts") {
      fixture("ledger_append")
      uninterceptedWrite("INSERT INTO ledger_append VALUES (4, 'c'), (5, 'a')")
      uninterceptedWrite("INSERT INTO ledger_append VALUES (6, 'c')")
      captured("ledger_append").map(_.opType).distinct shouldBe Seq(StagingDelta.OpTypes.Insert)
      assertDependentsMatch("ledger_append")
    }

    it("captures deletes and updates committed without interception as an exact change") {
      fixture("ledger_mutate")
      uninterceptedWrite("DELETE FROM ledger_mutate WHERE id = 2")
      uninterceptedWrite("UPDATE ledger_mutate SET name = 'z' WHERE id = 3")
      captured("ledger_mutate").map(_.opType).toSet shouldBe Set(
        StagingDelta.OpTypes.Delete,
        StagingDelta.OpTypes.Insert
      )
      assertDependentsMatch("ledger_mutate")
    }

    it("captures a change set that both an intercepted and an unintercepted write contribute to") {
      fixture("ledger_mixed")
      uninterceptedWrite("DELETE FROM ledger_mixed WHERE id = 1")
      spark.sql("INSERT INTO ledger_mixed VALUES (7, 'b')")
      captured("ledger_mixed").map(_.opType) should not contain StagingDelta.OpTypes.Overwrite
      assertDependentsMatch("ledger_mixed")
    }

    it("records nothing for commits that change no rows") {
      fixture("ledger_optimize")
      uninterceptedWrite("INSERT INTO ledger_optimize VALUES (8, 'd')")
      captured("ledger_optimize") should not be empty
      assertDependentsMatch("ledger_optimize")
      spark.sql("OPTIMIZE ledger_optimize")
      captured("ledger_optimize") shouldBe empty
      assertDependentsMatch("ledger_optimize")
    }

    it("replaces dependents when the schema changes outside OpenIVM") {
      fixture("ledger_schema")
      spark.sql("ALTER TABLE ledger_schema ADD COLUMNS (extra INT)")
      captured("ledger_schema").map(_.opType) shouldBe Seq(StagingDelta.OpTypes.Overwrite)
    }

    it("recomputes existing dependents once when a table has no recorded position") {
      fixture("ledger_adopt")
      uninterceptedWrite("INSERT INTO ledger_adopt VALUES (9, 'e')")
      StagingCatalog.clearLedgerForTesting(spark, "default.ledger_adopt")
      spark.sql("INSERT INTO ledger_adopt VALUES (10, 'a')")
      captured("ledger_adopt").map(_.opType) should contain(StagingDelta.OpTypes.Overwrite)
      assertDependentsMatch("ledger_adopt")
      spark.sql("INSERT INTO ledger_adopt VALUES (11, 'e')")
      captured("ledger_adopt").map(_.opType) shouldBe Seq(StagingDelta.OpTypes.Insert)
      assertDependentsMatch("ledger_adopt")
    }
  }
}
