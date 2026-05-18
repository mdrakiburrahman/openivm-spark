package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Slice of `InsertRuleSpec` covering every DELETE shape: single-row,
  * multi-AND, unconditional, plus the complex predicate forms
  * (OR / IN / BETWEEN / LIKE / IS NULL).
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
class InsertRuleDeleteSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-ins-delete-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-InsertRuleDeleteSpec")
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

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    withClue(s"$mvName EXCEPT ALL <expected>: ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName: ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  /** Qualified-name of `tbl` as the DML interceptor sees it. */
  private def qualName(tbl: String): String = {
    import org.apache.spark.sql.execution.datasources.LogicalRelation
    val a = spark.sql(s"SELECT * FROM $tbl").queryExecution.analyzed
    a.collectFirst {
      case r: LogicalRelation if r.catalogTable.isDefined =>
        val id = r.catalogTable.get.identifier
        id.database.fold(id.table)(db => s"$db.${id.table}")
    }.getOrElse(tbl)
  }

  private def stagingRows(tbl: String) =
    StagingCatalog.collectFor(spark, s"__probe_${UUID.randomUUID().toString.take(6)}__", Seq(qualName(tbl)))

  private def setupStaff(): String = {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_staff_ir")
    spark.sql("DROP TABLE IF EXISTS staff_ir")
    spark.sql(
      "CREATE TABLE staff_ir(id INT, name STRING, dept STRING, salary INT) USING DELTA"
    )
    val qn        = qualName("staff_ir")
    val staleRows = StagingCatalog.collectFor(spark, "__cleanup_probe__", Seq(qn))
    if (staleRows.nonEmpty) {
      StagingCatalog.markConsumed(spark, "__cleanup_mv__", staleRows.map(_.stagingPath))
      StagingCatalog.pruneFullyConsumed(spark, Map(qn -> Seq("__cleanup_mv__")))
    }

    spark.sql("INSERT INTO staff_ir VALUES (1,'Alice','eng',100),(2,'Bob','sales',200)")
    spark.sql("CREATE MATERIALIZED VIEW mv_staff_ir AS SELECT id, name, dept, salary FROM staff_ir")
    val seedStaging = StagingCatalog.collectFor(spark, "mv_staff_ir", Seq(qn))
    if (seedStaging.nonEmpty) {
      StagingCatalog.markConsumed(spark, "mv_staff_ir", seedStaging.map(_.stagingPath))
    }
    qn
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (5) DELETE — single row, multi-AND, and unconditional
  // ──────────────────────────────────────────────────────────────────────────
  describe("(5) DELETE — single row, multi-AND, and unconditional") {

    it("single-row DELETE produces a DELETE staging row and shrinks the MV after refresh") {
      val qn = setupStaff()
      spark.sql("INSERT INTO staff_ir VALUES (3, 'Carol', 'eng', 150)")
      refreshMv("mv_staff_ir")

      spark.sql("DELETE FROM staff_ir WHERE id = 3")
      val staged = stagingRows("staff_ir").filter(_.opType == "DELETE")
      staged should not be empty
      val deleted = spark.read.format("delta").load(staged.head.stagingPath).collect()
      deleted.map(_.getAs[Int]("id")) should contain(3)

      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
      val _ =
        qn // sbt -Xfatal-warnings: keep `qn` referenced to silence locals warning when this build setting is widened.
    }

    it("DELETE with AND condition removes only the matching subset") {
      setupStaff()
      spark.sql("DELETE FROM staff_ir WHERE dept = 'sales' AND salary >= 200")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }

    it("DELETE FROM with no WHERE clears the MV") {
      setupStaff()
      spark.sql("DELETE FROM staff_ir")
      refreshMv("mv_staff_ir")
      spark.table("mv_staff_ir").count() shouldBe 0L
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (13) Complex DELETE predicate shapes
  //      (insert_rule.test Complex 1-5: OR, IN, BETWEEN, LIKE, IS NULL)
  // ──────────────────────────────────────────────────────────────────────────
  describe("(13) Complex DELETE predicates") {

    it("DELETE with OR removes both matching rows") {
      setupStaff()
      spark.sql(
        "INSERT INTO staff_ir VALUES (90,'OrTest1','eng',10),(91,'OrTest2','sales',20),(92,'OrTest3','hr',30)"
      )
      refreshMv("mv_staff_ir")
      spark.sql("DELETE FROM staff_ir WHERE id = 90 OR id = 92")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
      // Only id=91 should remain from the 3 inserted.
      spark
        .sql("SELECT count(*) FROM mv_staff_ir WHERE id IN (90, 91, 92)")
        .collect()
        .head
        .getAs[Long](0) shouldBe 1L
    }

    it("DELETE with IN-list removes only the listed ids") {
      setupStaff()
      spark.sql(
        "INSERT INTO staff_ir VALUES (93,'In1','eng',10),(94,'In2','eng',20),(95,'In3','eng',30)"
      )
      refreshMv("mv_staff_ir")
      spark.sql("DELETE FROM staff_ir WHERE id IN (93, 95)")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }

    it("DELETE with BETWEEN drops the inclusive range") {
      setupStaff()
      spark.sql(
        "INSERT INTO staff_ir VALUES (100,'B1','eng',10),(101,'B2','eng',20),(102,'B3','eng',30),(103,'B4','eng',40)"
      )
      refreshMv("mv_staff_ir")
      spark.sql("DELETE FROM staff_ir WHERE id BETWEEN 101 AND 102")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }

    it("DELETE with LIKE drops by string prefix") {
      setupStaff()
      spark.sql(
        "INSERT INTO staff_ir VALUES (110,'LikeAlpha','eng',1),(111,'LikeBeta','eng',2),(112,'NoMatch','eng',3)"
      )
      refreshMv("mv_staff_ir")
      spark.sql("DELETE FROM staff_ir WHERE name LIKE 'Like%'")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }

    it("DELETE with IS NULL drops the null-named row") {
      setupStaff()
      spark.sql(
        "INSERT INTO staff_ir VALUES (120,NULL,'eng',1),(121,'NotNull','eng',2)"
      )
      refreshMv("mv_staff_ir")
      spark.sql("DELETE FROM staff_ir WHERE name IS NULL")
      refreshMv("mv_staff_ir")
      assertMvCorrect("mv_staff_ir", "SELECT id, name, dept, salary FROM staff_ir")
    }
  }
}
