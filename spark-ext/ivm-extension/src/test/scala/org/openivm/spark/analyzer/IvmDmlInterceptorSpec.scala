package org.openivm.spark.analyzer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.openivm.spark.common.{FeatureGate, MvCatalog, MvMetadata, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.UUID

/**
 * Integration tests for [[IvmDmlInterceptorRule]] / [[StagedDmlNode]] /
 * [[WithDeltaStaging]] / [[org.openivm.spark.executor.DeltaStagingExec]].
 *
 * Session config matches the spec exactly:
 * {{{
 *   spark.sql.extensions =
 *     io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions
 *   spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog
 *   spark.openivm.enabled = true
 * }}}
 *
 * Delta's DeltaAnalysis fires before our rule in each resolution pass:
 *   - DELETE / UPDATE → ReplaceData before our rule sees them
 *   - MERGE           → WriteDelta  before our rule sees them
 *   - INSERT / OVERWRITE → AppendData / OverwriteByExpression (not lowered by Delta)
 */
class IvmDmlInterceptorSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-dml-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-IvmDmlInterceptorSpec")
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

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    deleteDir(new File(warehouseDir))
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Returns the qualified table name as the DML interceptor sees it.
   *  Mirrors the logic in [[org.openivm.spark.commands.MvCommandHelper.collectSourceSchemas]]:
   *  Delta tables appear as `LogicalRelation` (V1 parquet) for SELECT plans and as
   *  `DataSourceV2Relation` for write-plan targets; we handle both. */
  private def qualifiedName(tableName: String): String = {
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
    import org.apache.spark.sql.execution.datasources.LogicalRelation
    try {
      val analyzed = spark.sql(s"SELECT * FROM $tableName").queryExecution.analyzed
      analyzed
        .collectFirst {
          case r: LogicalRelation if r.catalogTable.isDefined =>
            val id = r.catalogTable.get.identifier
            id.database.fold(id.table)(db => s"$db.${id.table}")
          case r: DataSourceV2Relation if r.identifier.isDefined =>
            val id = r.identifier.get
            val ns = id.namespace()
            if (ns.nonEmpty) (ns :+ id.name()).mkString(".") else id.name()
        }
        .getOrElse(tableName)
    } catch { case _: Exception => tableName }
  }

  /** Creates a Delta base table and registers a fake MV that depends on it.
   *  Returns the qualified table name used in [[MvCatalog]]. */
  private def createBaseWithMv(
      tableName: String,
      ddlSchema: String = "(id INT, name STRING)"
  ): String = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(s"CREATE TABLE $tableName $ddlSchema USING DELTA")
    val qn = qualifiedName(tableName)
    val meta = MvMetadata(
      name = CatalystSqlParser.parseTableIdentifier(s"mv_$tableName"),
      querySql = s"SELECT * FROM $tableName",
      refreshType = 0,
      refreshTypeName = "SIMPLE_PROJECTION",
      lastVersion = 0L,
      sourceTables = Seq(qn),
      sourceSchemaFingerprint = MvCatalog.schemaFingerprint(
        Map(qn -> spark.table(tableName).schema)
      ),
      queryPlanFingerprint = None,
      location = s"$warehouseDir/mv_$tableName",
      createdAt = new Timestamp(System.currentTimeMillis()),
      properties = Map.empty
    )
    MvCatalog.upsert(spark, meta)
    qn
  }

  /** Returns all staging catalog rows for [[baseTable]] ordered by txn_ts. */
  private def stagingRows(baseTable: String) =
    StagingCatalog.collectFor(spark, "__probe__", Seq(baseTable))

  // -------------------------------------------------------------------------
  // Test 1 — plan-shape probe
  // -------------------------------------------------------------------------
  describe("Test 1: plan-shape probe") {
    it("records plan shapes for each DML type (informational)") {
      val tbl = "probe_shapes"
      createBaseWithMv(tbl)
      spark.sql(s"INSERT INTO $tbl VALUES (1, 'a'), (2, 'b')")

      val deleteAnalyzed =
        spark.sql(s"SELECT 1").queryExecution.analyzed.getClass.getSimpleName
      info(s"Probe baseline plan type: $deleteAnalyzed")

      // Each of these executes a DML whose plan shape we can observe via
      // the staging catalog entries produced.
      spark.sql(s"DELETE FROM $tbl WHERE id = 1")
      spark.sql(s"UPDATE $tbl SET name = 'b2' WHERE id = 2")
      spark.sql(s"INSERT INTO $tbl VALUES (3, 'c')")

      val qn   = qualifiedName(tbl)
      val rows = stagingRows(qn)
      info(
        s"Staging entries after DELETE + UPDATE + INSERT: ${rows.map(r => r.opType -> r.stagingPath).mkString(", ")}"
      )
      rows.size should be >= 3
    }
  }

  // -------------------------------------------------------------------------
  // Test 2 — INSERT with dependent MV
  // -------------------------------------------------------------------------
  describe("Test 2: INSERT with dependent MV") {
    it("writes a staging INSERT entry and inserts the row into the base table") {
      val tbl = "insert_base"
      val qn  = createBaseWithMv(tbl)

      spark.sql(s"INSERT INTO $tbl VALUES (10, 'hello')")

      spark.sql(s"SELECT * FROM $tbl").collect().map(_.getAs[Int]("id")) should contain(10)

      val staged = stagingRows(qn)
      staged should have size 1
      staged.head.opType shouldBe "INSERT"
      staged.head.baseTable shouldBe qn
    }
  }

  // -------------------------------------------------------------------------
  // Test 3 — OVERWRITE with dependent MV
  // -------------------------------------------------------------------------
  describe("Test 3: OVERWRITE with dependent MV") {
    it("writes a staging OVERWRITE entry and replaces the base table contents") {
      val tbl = "overwrite_base"
      val qn  = createBaseWithMv(tbl)
      spark.sql(s"INSERT INTO $tbl VALUES (1, 'old')")

      spark.sql(s"INSERT OVERWRITE $tbl VALUES (2, 'new')")

      val rows = spark.sql(s"SELECT * FROM $tbl").collect()
      rows.map(_.getAs[Int]("id")) should contain(2)

      val staged = stagingRows(qn)
      // One OVERWRITE entry (from the INSERT OVERWRITE); the initial INSERT also
      // produced a staging entry so filter by OVERWRITE.
      staged.filter(_.opType == "OVERWRITE") should have size 1
    }
  }

  // -------------------------------------------------------------------------
  // Test 4 — DELETE with dependent MV
  // -------------------------------------------------------------------------
  describe("Test 4: DELETE with dependent MV") {
    it("captures the deleted rows in staging and removes them from the base table") {
      val tbl = "delete_base"
      val qn  = createBaseWithMv(tbl)
      spark.sql(s"INSERT INTO $tbl VALUES (100, 'to-delete'), (200, 'keep')")

      spark.sql(s"DELETE FROM $tbl WHERE id = 100")

      val remaining = spark.sql(s"SELECT id FROM $tbl").collect().map(_.getAs[Int]("id"))
      remaining should not contain 100
      remaining should contain(200)

      val staged        = stagingRows(qn)
      val deleteEntries = staged.filter(_.opType == "DELETE")
      deleteEntries should not be empty
      // Verify staging Delta table contains the deleted row
      val stagingData = spark.read.format("delta").load(deleteEntries.head.stagingPath).collect()
      stagingData.map(_.getAs[Int]("id")) should contain(100)
    }
  }

  // -------------------------------------------------------------------------
  // Test 5 — UPDATE with dependent MV
  // -------------------------------------------------------------------------
  describe("Test 5: UPDATE with dependent MV") {
    it("captures UPDATE_BEFORE and UPDATE_AFTER in staging; base table reflects update") {
      val tbl = "update_base"
      val qn  = createBaseWithMv(tbl)
      spark.sql(s"INSERT INTO $tbl VALUES (1, 'old-name')")

      spark.sql(s"UPDATE $tbl SET name = 'new-name' WHERE id = 1")

      val afterRows = spark.sql(s"SELECT name FROM $tbl WHERE id = 1").collect()
      afterRows should have size 1
      afterRows.head.getAs[String]("name") shouldBe "new-name"

      val staged = stagingRows(qn)
      // After the INSERT we get an INSERT entry; after UPDATE we expect
      // UPDATE_BEFORE and UPDATE_AFTER entries.
      staged.filter(_.opType == "UPDATE_BEFORE") should not be empty
      staged.filter(_.opType == "UPDATE_AFTER") should not be empty

      val beforeData = spark.read
        .format("delta")
        .load(staged.filter(_.opType == "UPDATE_BEFORE").head.stagingPath)
        .collect()
      beforeData.map(_.getAs[String]("name")) should contain("old-name")

      val afterData = spark.read
        .format("delta")
        .load(staged.filter(_.opType == "UPDATE_AFTER").head.stagingPath)
        .collect()
      afterData.map(_.getAs[String]("name")) should contain("new-name")
    }
  }

  // -------------------------------------------------------------------------
  // Test 6 — MERGE with dependent MV
  // -------------------------------------------------------------------------
  describe("Test 6: MERGE with dependent MV") {
    it("captures MERGE_SRC rows in staging and applies the merge to the base table") {
      val tbl = "merge_base"
      val qn  = createBaseWithMv(tbl)
      spark.sql(s"INSERT INTO $tbl VALUES (1, 'original')")

      // Create a source table (no MV dependency needed)
      spark.sql("DROP TABLE IF EXISTS merge_src")
      spark.sql("CREATE TABLE merge_src (id INT, name STRING) USING DELTA")
      spark.sql("INSERT INTO merge_src VALUES (1, 'updated'), (2, 'inserted')")

      spark.sql(s"""
        MERGE INTO $tbl t
        USING merge_src s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.name = s.name
        WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
      """)

      val rows = spark.sql(s"SELECT id, name FROM $tbl ORDER BY id").collect()
      rows.map(_.getAs[Int]("id")) shouldBe Seq(1, 2)
      rows.head.getAs[String]("name") shouldBe "updated"

      val staged = stagingRows(qn)
      staged.filter(_.opType == "MERGE_SRC") should not be empty
    }
  }

  // -------------------------------------------------------------------------
  // Test 7 — no-op when no dependent MV
  // -------------------------------------------------------------------------
  describe("Test 7: no-op when no dependent MV") {
    it("does NOT write a staging entry when the table has no registered MVs") {
      val tbl = "no_mv_table"
      spark.sql(s"DROP TABLE IF EXISTS $tbl")
      spark.sql(s"CREATE TABLE $tbl (id INT, name STRING) USING DELTA")
      // Do NOT register any MV for this table.

      spark.sql(s"INSERT INTO $tbl VALUES (1, 'x')")
      spark.sql(s"DELETE FROM $tbl WHERE id = 1")

      val qn     = qualifiedName(tbl)
      val staged = stagingRows(qn)
      staged shouldBe empty
    }
  }

  // -------------------------------------------------------------------------
  // Test 8 — feature gate integration
  // -------------------------------------------------------------------------
  describe("Test 8: feature gate integration") {
    it("gate is enabled for this session; IvmDmlInterceptorRule.bypass flag is thread-local") {
      // Verify the gate is set correctly for the test session.
      FeatureGate.enabled(spark) shouldBe true
      FeatureGate.EnabledKey shouldBe "spark.openivm.enabled"

      // Verify bypass starts false, can be toggled, and is cleaned up.
      IvmDmlInterceptorRule.bypass.get() shouldBe false
      IvmDmlInterceptorRule.bypass.set(true)
      IvmDmlInterceptorRule.bypass.get() shouldBe true
      IvmDmlInterceptorRule.bypass.set(false)
      IvmDmlInterceptorRule.bypass.get() shouldBe false

      // With bypass=true the rule should return plans unmodified.
      val tbl = "gate_bypass_table"
      val qn  = createBaseWithMv(tbl)
      IvmDmlInterceptorRule.bypass.set(true)
      try {
        spark.sql(s"INSERT INTO $tbl VALUES (1, 'a')")
      } finally {
        IvmDmlInterceptorRule.bypass.set(false)
      }
      // Rule was bypassed → no staging entry
      stagingRows(qn) shouldBe empty
    }
  }

  // -------------------------------------------------------------------------
  // Test 9 — idempotency guard (plan not double-wrapped)
  // -------------------------------------------------------------------------
  describe("Test 9: idempotency guard") {
    it("produces exactly one staging entry per DML statement (no double-wrapping)") {
      val tbl = "idem_base"
      val qn  = createBaseWithMv(tbl)

      spark.sql(s"INSERT INTO $tbl VALUES (1, 'a')")

      // Confirm exactly one INSERT staging entry was written
      val staged = StagingCatalog
        .collectFor(spark, "__idem_probe__", Seq(qn))
        .filter(_.opType == "INSERT")
      staged should have size 1
    }
  }

  // -------------------------------------------------------------------------
  // Test 10 — bypass flag prevents re-interception
  // -------------------------------------------------------------------------
  describe("Test 10: bypass flag prevents infinite re-interception") {
    it("completes DELETE without stack overflow or duplicate staging entries") {
      val tbl = "bypass_base"
      val qn  = createBaseWithMv(tbl)
      spark.sql(s"INSERT INTO $tbl VALUES (1, 'a'), (2, 'b')")

      // If bypass doesn't work, StagedDmlNode.run() would re-trigger the rule
      // when executing the original DML → infinite recursion → StackOverflow.
      noException should be thrownBy {
        spark.sql(s"DELETE FROM $tbl WHERE id = 1")
      }

      val remaining = spark.sql(s"SELECT id FROM $tbl").collect().map(_.getAs[Int]("id"))
      remaining should not contain 1
      remaining should contain(2)

      // Exactly one DELETE staging entry (not duplicated)
      val staged = stagingRows(qn).filter(_.opType == "DELETE")
      staged should have size 1
    }
  }

  // -------------------------------------------------------------------------
  // Test 11 — transactional safety: staging write failure aborts DML
  // -------------------------------------------------------------------------
  describe("Test 11: transactional safety") {
    it("aborts the DML when the staging write fails, leaving the base table unchanged") {
      val tbl = "txn_safe_base"
      val qn  = createBaseWithMv(tbl)

      // Block the staging path by pre-creating a FILE where the directory
      // would be created.  The qualified name "default.txn_safe_base" is
      // sanitised to "default_txn_safe_base" by the rule's stagingPath helper.
      val safeQn      = qn.replace(".", "_")
      val blockTarget = new File(s"$warehouseDir/_ivm/staging/$safeQn")
      blockTarget.getParentFile.mkdirs()
      // Create a FILE at that path so Delta can't make a subdirectory there.
      blockTarget.createNewFile()

      an[Exception] should be thrownBy {
        spark.sql(s"INSERT INTO $tbl VALUES (99, 'should-not-appear')").collect()
      }

      // Base table must still be empty — the write was aborted.
      val rows = spark.sql(s"SELECT * FROM $tbl").collect()
      rows shouldBe empty
    }
  }
}
