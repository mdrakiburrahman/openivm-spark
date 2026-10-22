package org.openivm.spark.analyzer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
import org.openivm.spark.common.{DeltaTableVersion, MvCatalog, StagingCatalog, StagingDelta}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Try

/** CTAS / RTAS / REPLACE TABLE on materialized-view sources publish exact change sets after commit. */
class StagedTableReplacementSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _
  private val warehouse           = new File(s"target/test-warehouse-replace-${UUID.randomUUID().toString.take(8)}")

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-StagedTableReplacementSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
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

  private def published(table: String)(action: => Unit): Seq[StagingDelta] = {
    def staged = StagingCatalog.collectFor(spark, "__probe__", Seq(s"default.$table"))
    val before = staged.map(_.stagingPath).toSet
    action
    staged.filterNot(delta => before.contains(delta.stagingPath))
  }

  private def stagedRows(deltas: Seq[StagingDelta], opType: String): Seq[(Int, String)] =
    deltas
      .filter(_.opType == opType)
      .flatMap(delta => spark.read.format("delta").load(delta.stagingPath).collect().toSeq)
      .map(row => (row.getInt(0), row.getString(1)))

  private def rows(table: String): Seq[(Int, String)] =
    spark.table(table).select("id", "name").collect().toSeq.map(row => (row.getInt(0), row.getString(1)))

  private def assertRows(actual: DataFrame, expectedSql: String): Unit = {
    val expected = spark.sql(expectedSql)
    actual.exceptAll(expected).count() shouldBe 0L
    expected.exceptAll(actual).count() shouldBe 0L
  }

  private def assertDependentsMatch(table: String, projection: String, aggregate: String): Unit = {
    spark.sql(s"REFRESH MATERIALIZED VIEW $projection")
    spark.sql(s"REFRESH MATERIALIZED VIEW $aggregate")
    assertRows(spark.table(projection).select("id", "name"), s"SELECT id, name FROM $table")
    assertRows(
      spark.table(aggregate).select("name", "n"),
      s"SELECT name, COUNT(*) AS n FROM $table GROUP BY name"
    )
  }

  private def createSourceWithDependents(table: String, rowsSql: String): (String, String) = {
    val projection = s"mv_${table}_projection"
    val aggregate  = s"mv_${table}_aggregate"
    spark.sql(s"CREATE TABLE $table USING DELTA AS $rowsSql")
    spark.sql(s"CREATE MATERIALIZED VIEW $projection AS SELECT id, name FROM $table")
    spark.sql(s"CREATE MATERIALIZED VIEW $aggregate AS SELECT name, COUNT(*) AS n FROM $table GROUP BY name")
    (projection, aggregate)
  }

  describe("table replacements of materialized-view sources") {
    it("publish only the net row change and keep dependents incremental across repeated replacements") {
      val table = "replace_net"
      val (projection, aggregate) =
        createSourceWithDependents(table, "SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'")
      Seq(
        "SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'B' UNION ALL SELECT 4, 'd'",
        "SELECT 1 AS id, 'a' AS name UNION ALL SELECT 4, 'd' UNION ALL SELECT 5, 'e'"
      ).foreach { replacement =>
        val previous = rows(table)
        val deltas   = published(table)(spark.sql(s"CREATE OR REPLACE TABLE $table USING DELTA AS $replacement"))
        val current  = rows(table)

        deltas.map(_.opType) should contain theSameElementsAs Seq(
          StagingDelta.OpTypes.Delete,
          StagingDelta.OpTypes.Insert
        )
        stagedRows(deltas, StagingDelta.OpTypes.Delete) should contain theSameElementsAs previous.diff(current)
        stagedRows(deltas, StagingDelta.OpTypes.Insert) should contain theSameElementsAs current.diff(previous)
        assertDependentsMatch(table, projection, aggregate)
      }
    }

    it("publish nothing when a replacement rebuilds identical rows") {
      val table                   = "replace_identical"
      val rowsSql                 = "SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'"
      val (projection, aggregate) = createSourceWithDependents(table, rowsSql)
      val version                 = DeltaTableVersion.latest(spark, s"default.$table")

      published(table)(spark.sql(s"CREATE OR REPLACE TABLE $table USING DELTA AS $rowsSql")) shouldBe empty
      DeltaTableVersion.latest(spark, s"default.$table") shouldBe version + 1
      assertDependentsMatch(table, projection, aggregate)
    }

    it("retract duplicate rows by multiplicity") {
      val table = "replace_duplicates"
      val (projection, aggregate) =
        createSourceWithDependents(table, "SELECT 1 AS id, 'a' AS name UNION ALL SELECT 1, 'a' UNION ALL SELECT 2, 'b'")

      val deltas = published(table)(
        spark.sql(s"CREATE OR REPLACE TABLE $table USING DELTA AS SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'")
      )

      deltas.map(_.opType) shouldBe Seq(StagingDelta.OpTypes.Delete)
      stagedRows(deltas, StagingDelta.OpTypes.Delete) shouldBe Seq((1, "a"))
      assertDependentsMatch(table, projection, aggregate)
    }

    it("stage exactly the committed rows of a non-deterministic replacement") {
      val table                   = "replace_nondeterministic"
      val (projection, aggregate) = createSourceWithDependents(table, "SELECT 1 AS id, 'a' AS name")

      val deltas = published(table)(
        spark.sql(
          s"CREATE OR REPLACE TABLE $table USING DELTA AS SELECT id, uuid() AS name FROM VALUES (1), (2) AS v(id)"
        )
      )

      stagedRows(deltas, StagingDelta.OpTypes.Delete) shouldBe Seq((1, "a"))
      stagedRows(deltas, StagingDelta.OpTypes.Insert) should contain theSameElementsAs rows(table)
      assertDependentsMatch(table, projection, aggregate)
    }

    it("publish nothing and keep the source when the replacement fails") {
      val table                   = "replace_failed"
      val (projection, aggregate) = createSourceWithDependents(table, "SELECT 1 AS id, 'a' AS name")
      val version                 = DeltaTableVersion.latest(spark, s"default.$table")

      val deltas = published(table) {
        an[Exception] should be thrownBy spark.sql(
          s"CREATE OR REPLACE TABLE $table USING DELTA AS " +
            "SELECT id, CAST(raise_error(CONCAT('replacement failed for ', id)) AS STRING) AS name " +
            "FROM VALUES (9) AS v(id)"
        )
      }

      deltas shouldBe empty
      DeltaTableVersion.latest(spark, s"default.$table") shouldBe version
      rows(table) shouldBe Seq((1, "a"))
      assertDependentsMatch(table, projection, aggregate)
    }

    it("retract every row when REPLACE TABLE declares the same schema without a query") {
      val table      = "replace_without_query"
      val projection = s"mv_${table}_projection"
      val aggregate  = s"mv_${table}_aggregate"
      spark.sql(s"CREATE TABLE $table (id INT, name STRING) USING DELTA")
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      spark.sql(s"CREATE MATERIALIZED VIEW $projection AS SELECT id, name FROM $table")
      spark.sql(s"CREATE MATERIALIZED VIEW $aggregate AS SELECT name, COUNT(*) AS n FROM $table GROUP BY name")

      val deltas = published(table)(spark.sql(s"CREATE OR REPLACE TABLE $table (id INT, name STRING) USING DELTA"))

      deltas.map(_.opType) shouldBe Seq(StagingDelta.OpTypes.Delete)
      stagedRows(deltas, StagingDelta.OpTypes.Delete) should contain theSameElementsAs Seq((1, "a"), (2, "b"))
      assertDependentsMatch(table, projection, aggregate)
      spark.table(projection).count() shouldBe 0L
    }

    it("publish a full replacement when the schema changes and never refresh dependents silently") {
      val table                   = "replace_schema_change"
      val (projection, aggregate) = createSourceWithDependents(table, "SELECT 1 AS id, 'a' AS name")

      val deltas = published(table)(
        spark.sql(s"CREATE OR REPLACE TABLE $table USING DELTA AS SELECT 2 AS id, 'b' AS name, 10 AS extra")
      )

      deltas.map(_.opType) shouldBe Seq(StagingDelta.OpTypes.Delete, StagingDelta.OpTypes.Overwrite)
      stagedRows(deltas, StagingDelta.OpTypes.Delete) shouldBe Seq((1, "a"))
      stagedRows(deltas, StagingDelta.OpTypes.Overwrite) shouldBe Seq((2, "b"))
      if (Try(spark.sql(s"REFRESH MATERIALIZED VIEW $projection")).isSuccess)
        assertRows(spark.table(projection).select("id", "name"), s"SELECT id, name FROM $table")
      if (Try(spark.sql(s"REFRESH MATERIALIZED VIEW $aggregate")).isSuccess)
        assertRows(
          spark.table(aggregate).select("name", "n"),
          s"SELECT name, COUNT(*) AS n FROM $table GROUP BY name"
        )
    }

    it("treat DataFrameWriterV2 createOrReplace as the same exact replacement") {
      val table = "replace_writer_v2"
      val (projection, aggregate) =
        createSourceWithDependents(table, "SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'")

      val deltas = published(table)(
        spark
          .sql("SELECT 2 AS id, 'b' AS name UNION ALL SELECT 3, 'c'")
          .writeTo(table)
          .using("delta")
          .createOrReplace()
      )

      deltas.map(_.opType) should contain theSameElementsAs Seq(
        StagingDelta.OpTypes.Delete,
        StagingDelta.OpTypes.Insert
      )
      stagedRows(deltas, StagingDelta.OpTypes.Delete) shouldBe Seq((1, "a"))
      stagedRows(deltas, StagingDelta.OpTypes.Insert) shouldBe Seq((3, "c"))
      assertDependentsMatch(table, projection, aggregate)
    }

    it("refuse to capture a write into an uncommitted replacement table") {
      val table = "replace_guard"
      createSourceWithDependents(table, "SELECT 1 AS id, 'a' AS name")
      val tableSchema = spark.table(table).schema
      val uncommitted = new StagedTable {
        override def name(): String       = table
        override def schema(): StructType = tableSchema
        override def capabilities(): java.util.Set[TableCapability] =
          java.util.EnumSet.of(TableCapability.V1_BATCH_WRITE)
        override def commitStagedChanges(): Unit = ()
        override def abortStagedChanges(): Unit  = ()
      }
      val target = DataSourceV2Relation.create(uncommitted, None, Some(Identifier.of(Array("default"), table)))
      val write  = OverwriteByExpression.byPosition(target, spark.table(table).queryExecution.analyzed, Literal(true))

      val error = intercept[IllegalStateException](new IvmDmlInterceptorRule(spark).apply(write))
      error.getMessage should include("uncommitted replacement of default.replace_guard")
    }
  }
}
