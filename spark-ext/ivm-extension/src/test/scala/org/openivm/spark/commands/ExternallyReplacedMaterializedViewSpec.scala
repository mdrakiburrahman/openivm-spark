package org.openivm.spark.commands

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.openivm.spark.common.{MvCatalog, MvMetadata, StagingCatalog}
import org.openivm.spark.streaming.{StreamingTableManager, StreamingTableSpec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** A materialized view whose relation is replaced or dropped outside OpenIVM, as a dbt `table`
  * materialization does with `CREATE OR REPLACE TABLE` (after `DROP VIEW` for a view-shaped MV),
  * is deregistered without touching the live relation, while owned views are still dropped.
  */
class ExternallyReplacedMaterializedViewSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-external-mv-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("openivm-spark-ExternallyReplacedMaterializedViewSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.openivm.changeFeed.mode", "cdf")
      .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
      .config("spark.openivm.streaming.checkpointArchive.uri", s"$warehouseDir/_archive")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) {
        spark.streams.active.foreach(_.stop())
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    } finally super.afterAll()

  private def sql(text: String): Unit = {
    spark.sql(text).collect()
    ()
  }

  private def registration(name: String): Option[MvMetadata] =
    MvCatalog.lookup(spark, TableIdentifier(name, Some("default")))

  private def pathExists(location: String): Boolean = {
    val path = new Path(location)
    path.getFileSystem(spark.sessionState.newHadoopConf()).exists(path)
  }

  private def rows(table: String): Seq[String] =
    spark.table(s"default.$table").collect().map(_.toString).toSeq.sorted

  private def createSource(name: String): Unit = {
    sql(s"CREATE TABLE default.$name (id INT, v INT) USING DELTA")
    sql(s"INSERT INTO default.$name VALUES (1, 10), (2, 20), (1, 7)")
  }

  /** The SQL dbt-fabricspark's `table` materialization emits over an existing relation. */
  private def dbtTable(name: String, select: String): Unit = {
    val existing = spark.sessionState.catalog.getTableMetadata(TableIdentifier(name, Some("default")))
    if (existing.tableType == CatalogTableType.VIEW) sql(s"drop view if exists default.$name")
    sql(s"create or replace table default.$name using delta as $select")
  }

  private def createStream(name: String, source: String, where: Option[String]): Unit = {
    val raw   = spark.readStream.format("delta").table(s"default.$source")
    val query = where.fold(raw)(predicate => raw.where(predicate)).select("id", "v")
    val status = StreamingTableManager.create(
      spark,
      StreamingTableSpec(
        name = Seq(name),
        queryText = s"SELECT id, v FROM STREAM default.$source" + where.fold("")(predicate => s" WHERE $predicate"),
        query = query.queryExecution.logical,
        options = Map("onQueryChange" -> "rebuild")
      )
    )
    status.queryId.flatMap(id => Option(spark.streams.get(id))).foreach(_.processAllAvailable())
  }

  describe("a materialized view replaced or dropped outside OpenIVM") {

    it("DROP MATERIALIZED VIEW deregisters a replaced table-shaped view and keeps the user's table") {
      createSource("ext_t_src")
      sql("CREATE MATERIALIZED VIEW default.ext_t_mv AS SELECT id, v FROM default.ext_t_src")
      val registered = registration("ext_t_mv").get
      registered.usesBackingDataTable shouldBe false
      dbtTable("ext_t_mv", "SELECT id, v * 100 AS v FROM default.ext_t_src")
      val userRows = rows("ext_t_mv")

      sql("DROP MATERIALIZED VIEW default.ext_t_mv")

      registration("ext_t_mv") shouldBe empty
      MvCatalog.viewsForSource(spark, "default.ext_t_src") shouldBe empty
      rows("ext_t_mv") shouldBe userRows
      pathExists(registered.location) shouldBe true
    }

    it("DROP MATERIALIZED VIEW IF EXISTS deregisters a replaced view-shaped view and drops only its backing table") {
      createSource("ext_v_src")
      val query = "SELECT id, SUM(v) AS s, COUNT(*) AS c FROM default.ext_v_src GROUP BY id"
      sql(s"CREATE MATERIALIZED VIEW default.ext_v_mv AS $query")
      val registered = registration("ext_v_mv").get
      registered.usesBackingDataTable shouldBe true
      dbtTable("ext_v_mv", query)
      val userRows = rows("ext_v_mv")

      sql("DROP MATERIALIZED VIEW IF EXISTS default.ext_v_mv")

      registration("ext_v_mv") shouldBe empty
      rows("ext_v_mv") shouldBe userRows
      spark.catalog.tableExists("default.ext_v_mv__ivm_data") shouldBe false
      pathExists(registered.location) shouldBe false
    }

    it("DROP MATERIALIZED VIEW IF EXISTS deregisters a view whose table and data were dropped outside OpenIVM") {
      createSource("ext_d_src")
      sql("CREATE MATERIALIZED VIEW default.ext_d_mv AS SELECT id, v FROM default.ext_d_src")
      val registered = registration("ext_d_mv").get
      sql("DROP TABLE default.ext_d_mv")
      val path = new Path(registered.location)
      path.getFileSystem(spark.sessionState.newHadoopConf()).delete(path, true)

      sql("DROP MATERIALIZED VIEW IF EXISTS default.ext_d_mv")

      registration("ext_d_mv") shouldBe empty
      spark.catalog.tableExists("default.ext_d_mv") shouldBe false
    }

    it("a streaming-table rebuild deregisters replaced dependents, keeps their tables and drops owned ones") {
      createSource("ext_s_raw")
      createStream("ext_s_st", "ext_s_raw", None)
      val queryA = "SELECT id, v FROM default.ext_s_st"
      val queryB = "SELECT id, SUM(v) AS s, COUNT(*) AS c FROM default.ext_s_a GROUP BY id"
      sql(s"CREATE MATERIALIZED VIEW default.ext_s_a AS $queryA")
      sql(s"CREATE MATERIALIZED VIEW default.ext_s_b AS $queryB")
      sql("CREATE MATERIALIZED VIEW default.ext_s_owned AS SELECT id, v FROM default.ext_s_st")
      sql(
        "CREATE MATERIALIZED VIEW default.ext_s_owned_agg AS SELECT id, SUM(v) AS s FROM default.ext_s_st GROUP BY id"
      )
      val replacedBacking = registration("ext_s_b").get
      val owned           = registration("ext_s_owned").get
      val ownedAgg        = registration("ext_s_owned_agg").get
      replacedBacking.usesBackingDataTable shouldBe true
      ownedAgg.usesBackingDataTable shouldBe true
      dbtTable("ext_s_a", queryA)
      dbtTable("ext_s_b", queryB)
      val rowsA = rows("ext_s_a")
      val rowsB = rows("ext_s_b")

      createStream("ext_s_st", "ext_s_raw", Some("id > 1"))
      createStream("ext_s_st", "ext_s_raw", Some("id > 1"))

      Seq("ext_s_a", "ext_s_b", "ext_s_owned", "ext_s_owned_agg").foreach(name => registration(name) shouldBe empty)
      rows("ext_s_a") shouldBe rowsA
      rows("ext_s_b") shouldBe rowsB
      spark.catalog.tableExists("default.ext_s_b__ivm_data") shouldBe false
      pathExists(replacedBacking.location) shouldBe false
      spark.catalog.tableExists("default.ext_s_owned") shouldBe false
      pathExists(owned.location) shouldBe false
      spark.catalog.tableExists("default.ext_s_owned_agg") shouldBe false
      spark.catalog.tableExists("default.ext_s_owned_agg__ivm_data") shouldBe false
      pathExists(ownedAgg.location) shouldBe false
      rows("ext_s_st") shouldBe Seq("[2,20]")
    }

    it("DROP MATERIALIZED VIEW still drops an owned table-shaped view with its data") {
      createSource("ext_o_src")
      sql("CREATE MATERIALIZED VIEW default.ext_o_mv AS SELECT id, v FROM default.ext_o_src")
      val registered = registration("ext_o_mv").get

      sql("DROP MATERIALIZED VIEW default.ext_o_mv")

      registration("ext_o_mv") shouldBe empty
      spark.catalog.tableExists("default.ext_o_mv") shouldBe false
      pathExists(registered.location) shouldBe false
    }
  }
}
