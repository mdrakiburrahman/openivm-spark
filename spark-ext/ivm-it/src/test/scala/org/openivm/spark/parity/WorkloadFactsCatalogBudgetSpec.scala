package org.openivm.spark.parity

import java.io.File
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.{Configurator, Property}
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.WorkloadFactsRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Pins the Hive-metastore call budget of source-fact discovery.
 *
 * Every metastore read runs inside Spark's globally synchronized Hive client
 * (`HiveExternalCatalog.withClient`), so a redundant `getTableMetadata` is a
 * serialized section paid by every concurrent CREATE/REFRESH. Fact discovery
 * needs exactly one `CatalogTable` per source: the Delta metadata is read from
 * the resolved location and the catalog properties come from the same object.
 *
 * The budget is measured against a real (Derby-backed) Hive metastore rather
 * than an in-memory catalog because only the Hive path emits the audit records
 * that make the call count observable, and only the Hive path is what the
 * benchmark actually pays for.
 */
class WorkloadFactsCatalogBudgetSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val runId               = UUID.randomUUID().toString.take(8)
  private val warehouseDir        = new File(s"target/facts-hms-$runId").getAbsolutePath
  private var spark: SparkSession = _

  /**
   * One `get_table` for the shared `CatalogTable` plus the three Spark spends
   * resolving the source relation for its schema (`DeltaCatalog.loadTable`).
   * Before the shared lookup this was 6: `deltaProperties` and
   * `catalogProperties` each resolved the same table independently.
   */
  private val GetTableBudget = 4

  /** `requireDbExists` for the shared lookup plus the schema resolution (was 3). */
  private val GetDatabaseBudget = 2

  private final class AuditAppender(name: String)
      extends AbstractAppender(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY) {
    private val commands = ArrayBuffer.empty[String]
    override def append(event: LogEvent): Unit = {
      val message = event.getMessage.getFormattedMessage
      if (message.contains("cmd=")) {
        commands.synchronized {
          commands += message.substring(message.indexOf("cmd=") + 4).split("\\s+").head
        }
      }
    }
    def counts: Map[String, Int] = commands.synchronized(commands.toVector.groupBy(identity).map { case (k, v) =>
      k -> v.size
    })
  }

  private def metastoreCalls[A](body: => A): (A, Map[String, Int]) = {
    val appender = new AuditAppender(s"facts-hms-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try {
      val result = body
      (result, appender.counts)
    } finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Configurator.setLevel("org.apache.hadoop.hive.metastore.HiveMetaStore.audit", Level.INFO)
    new File(warehouseDir).mkdirs()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(s"facts-hms-$runId")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:memory:factshms$runId;create=true")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS facts_db").collect()
    spark
      .sql(
        "CREATE TABLE facts_db.orders (order_id BIGINT, region STRING, amount INT) USING DELTA " +
          "TBLPROPERTIES ('delta.constraints.amount_positive' = 'amount > 0', " +
          "'spark.openivm.unique_key' = 'order_id')"
      )
      .collect()
    spark.sql("INSERT INTO facts_db.orders VALUES (1, 'east', 10), (2, 'west', 20)").collect()
  }

  override def afterAll(): Unit =
    try if (spark != null) spark.stop()
    finally super.afterAll()

  describe("WorkloadFactsRegistry.discover") {
    it("resolves each source table with a single catalog lookup") {
      // Warm the Hive client / Delta log caches so the measured call is the
      // steady-state cost rather than first-touch initialisation.
      WorkloadFactsRegistry.forRefresh().discover(spark, Seq("facts_db.orders"))

      val (facts, calls) = metastoreCalls {
        WorkloadFactsRegistry.forRefresh().discover(spark, Seq("facts_db.orders"))
      }

      withClue(s"metastore calls: ${calls.toSeq.sorted.mkString(", ")}: ") {
        calls.getOrElse("get_table", 0) should be <= GetTableBudget
        calls.getOrElse("get_database", 0) should be <= GetDatabaseBudget
      }

      facts.deltaConstraints.map(c => c.name -> c.expression) shouldBe Seq("amount_positive" -> "amount > 0")
      facts.uniqueKeys.map(_.columns) shouldBe Seq(Seq("order_id"))
    }

    it("keeps discovering facts for tables that are not resolvable through the catalog") {
      val facts = WorkloadFactsRegistry.forRefresh().discover(spark, Seq("facts_db.missing_table"))
      facts shouldBe org.openivm.spark.common.WorkloadConstraintFacts()
    }
  }
}
