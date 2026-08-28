package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.CommitInfo
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class DeltaCommitClassifierSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  import BatchVerdict._

  private var spark: SparkSession = _

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-classifier-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DeltaCommitClassifierSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      deleteDir(new File(warehouseDir))
    }
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def uniqueTable(prefix: String): String =
    s"dccls_${prefix}_${UUID.randomUUID().toString.replace("-", "").take(8)}"

  private def createTable(prefix: String): String = {
    val table = uniqueTable(prefix)
    spark.sql(s"CREATE TABLE $table (id INT, name STRING) USING delta")
    table
  }

  private def latest(table: String): Long =
    DeltaCommitClassifier.latestVersion(spark, table)

  private def classify(table: String, lastConsumedVersion: Long): BatchVerdict =
    DeltaCommitClassifier.classify(spark, table, lastConsumedVersion)

  describe("DeltaCommitClassifier") {
    it("classifies an append-only insert as InsertOnly") {
      val table  = createTable("insert")
      val before = latest(table)

      spark.sql(s"INSERT INTO $table VALUES (1, 'a')")

      classify(table, before) shouldBe InsertOnly
    }

    it("classifies delete commits as Mutating") {
      val table = createTable("delete")
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      val before = latest(table)

      spark.sql(s"DELETE FROM $table WHERE id = 1")

      classify(table, before) shouldBe Mutating
    }

    it("classifies update commits as Mutating") {
      val table = createTable("update")
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      val before = latest(table)

      spark.sql(s"UPDATE $table SET name = 'z' WHERE id = 1")

      classify(table, before) shouldBe Mutating
    }

    it("classifies merge commits as Mutating") {
      val table = createTable("merge")
      val view  = uniqueTable("merge_src")
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      spark.sql(
        s"CREATE OR REPLACE TEMP VIEW $view AS SELECT 1 AS id, 'z' AS name UNION ALL SELECT 3 AS id, 'c' AS name"
      )
      val before = latest(table)

      spark.sql(
        s"""
           |MERGE INTO $table t
           |USING $view s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET name = s.name
           |WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
           |""".stripMargin
      )

      classify(table, before) shouldBe Mutating
    }

    it("keeps an insert-only MERGE conservative when the writer omits row metrics") {
      val table = createTable("merge_insert_only")
      val view  = uniqueTable("merge_insert_only_src")
      spark.sql(s"INSERT INTO $table VALUES (1, 'a')")
      spark.sql(s"CREATE OR REPLACE TEMP VIEW $view AS SELECT 2 AS id, 'b' AS name")
      val before = latest(table)

      spark.sql(
        s"""
           |MERGE INTO $table t
           |USING $view s
           |ON t.id = s.id
           |WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
           |""".stripMargin
      )

      classify(table, before) shouldBe Mutating
    }

    it("uses MERGE row metrics when the writer records a complete insert-only proof") {
      val mergeInfo = CommitInfo
        .empty(None)
        .copy(
          operation = "MERGE",
          operationParameters = Map.empty,
          operationMetrics = Some(
            Map(
              "numTargetRowsInserted" -> "2",
              "numTargetRowsUpdated"  -> "0",
              "numTargetRowsDeleted"  -> "0"
            )
          )
        )

      DeltaCommitClassifier.classifyCommit(Seq(mergeInfo)) shouldBe InsertOnly
    }

    it("classifies insert overwrite commits as Replace") {
      val table = createTable("overwrite")
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      val before = latest(table)

      spark.sql(s"INSERT OVERWRITE TABLE $table VALUES (9, 'z')")

      classify(table, before) shouldBe Replace
    }

    it("classifies predicate-scoped replaceWhere commits as Mutating") {
      val table = createTable("replacewhere")
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      val before = latest(table)

      spark.sql(s"INSERT INTO $table REPLACE WHERE id = 1 SELECT 1 AS id, 'z' AS name")

      classify(table, before) shouldBe Mutating
    }

    it("classifies truncate commit metadata as Replace") {
      val truncateInfo = CommitInfo.empty(None).copy(operation = "TRUNCATE", operationParameters = Map.empty)

      DeltaCommitClassifier.classifyCommit(Seq(truncateInfo)) shouldBe Replace
    }

    it("classifies optimize commits with dataChange=false actions as Noop") {
      val table = createTable("optimize")
      spark.sql(s"INSERT INTO $table VALUES (1, 'a')")
      spark.sql(s"INSERT INTO $table VALUES (2, 'b')")
      val before = latest(table)

      spark.sql(s"OPTIMIZE $table")

      latest(table) should be > before
      classify(table, before) shouldBe Noop
    }

    it("classifies an empty commit range as Noop") {
      val table  = createTable("empty")
      val before = latest(table)

      classify(table, before) shouldBe Noop
    }

    it("classifies a mixed append plus delete range as Mutating") {
      val table = createTable("mixed")
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      val before = latest(table)

      spark.sql(s"INSERT INTO $table VALUES (3, 'c')")
      spark.sql(s"DELETE FROM $table WHERE id = 1")

      classify(table, before) shouldBe Mutating
    }

    it("maps batch verdicts to compile-fact delta shapes") {
      DeltaCommitClassifier.shapeOf(Noop) shouldBe DeltaShape.Unchanged
      DeltaCommitClassifier.shapeOf(InsertOnly) shouldBe DeltaShape.InsertOnly
      DeltaCommitClassifier.shapeOf(Mutating) shouldBe DeltaShape.General
      DeltaCommitClassifier.shapeOf(Replace) shouldBe DeltaShape.General
    }
  }
}
