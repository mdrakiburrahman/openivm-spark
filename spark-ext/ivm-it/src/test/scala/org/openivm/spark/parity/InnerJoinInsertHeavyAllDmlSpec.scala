package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy carve-out of `InnerJoinInsertSpec.scala`'s named_payments 11-cycle
  * DML walk (~7m).  Lives in its own forked JVM so the rest of the parity
  * suite is not blocked by this monster test.
  *
  * Table / MV names are prefixed `iji_heavy_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
class InnerJoinInsertHeavyAllDmlSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-iji-heavy-alldml-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-InnerJoinInsertHeavyAllDmlSpec")
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

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  // ============================================================================
  // (1) named_payments — 2-way INNER JOIN projection.
  //     Mirrors openivm tests 1–11 (lines 7–235).
  // ============================================================================

  describe("(1) named_payments — 2-way INNER JOIN projection (11 sequential DML cycles)") {
    ignore(
      "incrementally maintains the join across INSERTs, DELETEs, batches, no-ops, and duplicates"
    ) /* TODO: SIMPLE_PROJECTION over byte-identical duplicate source rows is not fully supported. */ {
      // --- Base tables ---
      spark.sql("CREATE TABLE IF NOT EXISTS iji_heavy_gods (uid INT, user_name STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS iji_heavy_payments (from_uid INT, to_uid INT, amount INT) USING DELTA")
      spark.sql(
        "INSERT INTO iji_heavy_gods VALUES (1, 'Apollo'), (2, 'Artemis'), (3, 'Dionysus'), (4, 'Poseidon'), (5, 'Zeus')"
      )
      spark.sql(
        "INSERT INTO iji_heavy_payments VALUES " +
          "(1, 2, 1722), (2, 3, 53), (2, 5, 360), (3, 1, 80), " +
          "(3, 2, 137), (3, 5, 83), (5, 1, 42), (1, 2, 222)"
      )

      // --- Simple join projection ---
      spark.sql(
        "CREATE MATERIALIZED VIEW iji_heavy_named_payments AS " +
          "SELECT g.user_name, p.from_uid, p.to_uid, p.amount " +
          "FROM iji_heavy_gods AS g INNER JOIN iji_heavy_payments AS p ON p.to_uid = g.uid"
      )

      val viewBody =
        "SELECT g.user_name, p.from_uid, p.to_uid, p.amount " +
          "FROM iji_heavy_gods AS g INNER JOIN iji_heavy_payments AS p ON p.to_uid = g.uid"

      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 1: Insert on RIGHT side (payments)
      spark.sql("INSERT INTO iji_heavy_payments VALUES (3, 1, 30)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 2: Delete via join
      spark.sql("DELETE FROM iji_heavy_payments WHERE from_uid = 5 AND to_uid = 1 AND amount = 42")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 3: Insert on LEFT side (gods table) — no payments to uid 6 yet
      spark.sql("INSERT INTO iji_heavy_gods VALUES (6, 'Hera')")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)
      // Now add a payment TO the new god
      spark.sql("INSERT INTO iji_heavy_payments VALUES (1, 6, 99)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 4: Insert on BOTH sides simultaneously
      spark.sql("INSERT INTO iji_heavy_gods VALUES (7, 'Athena')")
      spark.sql("INSERT INTO iji_heavy_payments VALUES (2, 7, 500)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 5: Delete from LEFT side
      spark.sql("DELETE FROM iji_heavy_gods WHERE uid = 6")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 6: Simultaneous delete on BOTH sides
      spark.sql("DELETE FROM iji_heavy_gods WHERE uid = 7")
      spark.sql("DELETE FROM iji_heavy_payments WHERE to_uid = 7")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 7: Batch insert — multiple rows at once
      spark.sql("INSERT INTO iji_heavy_payments VALUES (1, 2, 10), (1, 2, 20), (1, 2, 30)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 8: No-op refresh (no changes since last)
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 9: Duplicate rows (same values inserted twice)
      spark.sql("INSERT INTO iji_heavy_payments VALUES (4, 5, 777)")
      spark.sql("INSERT INTO iji_heavy_payments VALUES (4, 5, 777)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 10: Mixed insert + delete in same refresh cycle
      spark.sql("DELETE FROM iji_heavy_payments WHERE from_uid = 4 AND to_uid = 5 AND amount = 777")
      spark.sql("INSERT INTO iji_heavy_payments VALUES (1, 3, 999)")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)

      // Test 11: Insert on both sides + delete simultaneously
      spark.sql("INSERT INTO iji_heavy_gods VALUES (8, 'Ares')")
      spark.sql("INSERT INTO iji_heavy_payments VALUES (1, 8, 50)")
      spark.sql("DELETE FROM iji_heavy_payments WHERE from_uid = 1 AND to_uid = 3 AND amount = 999")
      refreshMv("iji_heavy_named_payments")
      assertMvCorrect("iji_heavy_named_payments", viewBody)
    }
  }
}
