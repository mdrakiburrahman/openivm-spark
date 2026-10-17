package org.openivm.spark.analyzer

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, SubqueryAlias}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.DeltaRelation
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import java.util.UUID
import scala.jdk.CollectionConverters._

/** Regression coverage for staging reads copied from retained Delta V2 write targets. */
class StagedDmlBatchReadSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _
  private val warehouse           = new File(s"target/test-warehouse-dml-batch-${UUID.randomUUID().toString.take(8)}")

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-StagedDmlBatchReadSpec")
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

  private def retainedTarget(
      tableName: String,
      options: Map[String, String] = Map.empty
  ): DataSourceV2Relation = {
    val identifier = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    val metadata   = spark.sessionState.catalog.getTableMetadata(identifier)
    val table      = DeltaTableV2(spark, new Path(metadata.location), catalogTable = Some(metadata))
    val relation = DataSourceV2Relation.create(
      table,
      None,
      Some(Identifier.of(Array(metadata.identifier.database.get), metadata.identifier.table)),
      new CaseInsensitiveStringMap(options.asJava)
    )
    relation.setTagValue(DeltaRelation.KEEP_AS_V2_RELATION_TAG, ())
    relation
  }

  private def assertRows(actual: DataFrame, expectedSql: String): Unit = {
    val expected = spark.sql(expectedSql)
    actual.exceptAll(expected).count() shouldBe 0L
    expected.exceptAll(actual).count() shouldBe 0L
  }

  private def stageOverwrite(tableName: String, replacementSql: String, iteration: Int): Unit = {
    val target     = retainedTarget(tableName)
    val beforePath = new File(warehouse, s"before-$iteration").getAbsolutePath
    val afterPath  = new File(warehouse, s"after-$iteration").getAbsolutePath
    val command = StagedDmlNode(
      spark.sessionState.sqlParser.parsePlan(s"INSERT OVERWRITE TABLE $tableName $replacementSql"),
      Seq(
        (Filter(Literal(true), target), beforePath, "DELETE"),
        (spark.sql(replacementSql).queryExecution.analyzed, afterPath, "OVERWRITE")
      ),
      s"default.$tableName"
    )
    val previousSql =
      if (iteration == 1)
        "SELECT 1 AS id, 'old' AS name UNION ALL SELECT 2, 'two'"
      else "SELECT 3 AS id, 'new' AS name UNION ALL SELECT 4, 'four'"

    command.run(spark)
    assertRows(spark.read.format("delta").load(beforePath), previousSql)
    assertRows(spark.read.format("delta").load(afterPath), replacementSql)
    assertRows(spark.table(tableName), replacementSql)
  }

  describe("retained Delta V2 staging reads") {
    it("captures both signed row images and refreshes a dependent MV on repeated overwrites") {
      val tableName = "dml_batch_repeated_base"
      val mvName    = "mv_dml_batch_repeated"
      spark.sql(s"CREATE TABLE $tableName (id INT, name STRING) USING DELTA")
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'old'), (2, 'two')")
      spark.sql(s"CREATE MATERIALIZED VIEW $mvName AS SELECT id, name FROM $tableName")

      val replacements = Seq(
        "SELECT 3 AS id, 'new' AS name UNION ALL SELECT 4, 'four'",
        "SELECT 5 AS id, 'last' AS name"
      )
      replacements.zipWithIndex.foreach { case (sql, index) =>
        stageOverwrite(tableName, sql, index + 1)
        spark.sql(s"REFRESH MATERIALIZED VIEW $mvName")
        assertRows(spark.table(mvName).select("id", "name"), sql)
      }
    }

    it("preserves target attribute bindings and predicate-scoped old rows") {
      val tableName = "dml_batch_predicate_base"
      val mvName    = "mv_dml_batch_predicate"
      spark.sql(s"CREATE TABLE $tableName (id INT, name STRING) USING DELTA")
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'keep'), (2, 'remove')")
      spark.sql(s"CREATE MATERIALIZED VIEW $mvName AS SELECT id, name FROM $tableName")
      val target      = retainedTarget(tableName)
      val condition   = EqualTo(target.output.find(_.name == "id").get, Literal(2))
      val stagingPath = new File(warehouse, "predicate-before").getAbsolutePath
      StagedDmlNode(
        spark.sessionState.sqlParser.parsePlan(s"DELETE FROM $tableName WHERE id = 2"),
        Seq((Filter(condition, SubqueryAlias("before_image", target)), stagingPath, "DELETE")),
        s"default.$tableName"
      ).run(spark)

      assertRows(spark.read.format("delta").load(stagingPath), "SELECT 2 AS id, 'remove' AS name")
      spark.sql(s"REFRESH MATERIALIZED VIEW $mvName")
      assertRows(spark.table(tableName), "SELECT 1 AS id, 'keep' AS name")
      assertRows(spark.table(mvName).select("id", "name"), "SELECT 1 AS id, 'keep' AS name")
    }

    it("retains relation read options when converting the staging scan") {
      val tableName = "dml_batch_options_base"
      spark.sql(s"CREATE TABLE $tableName (id INT, name STRING) USING DELTA")
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'original')")
      spark.sql(s"UPDATE $tableName SET name = 'changed'")
      val target      = retainedTarget(tableName, Map("versionAsOf" -> "1"))
      val stagingPath = new File(warehouse, "options-before").getAbsolutePath
      StagedDmlNode(
        spark.sessionState.sqlParser.parsePlan(s"UPDATE $tableName SET name = 'final'"),
        Seq((Filter(Literal(true), target), stagingPath, "DELETE")),
        s"default.$tableName"
      ).run(spark)

      assertRows(spark.read.format("delta").load(stagingPath), "SELECT 1 AS id, 'original' AS name")
      assertRows(spark.table(tableName), "SELECT 1 AS id, 'final' AS name")
    }
  }
}
