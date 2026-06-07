package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy carve-out of `ChainedDmlSpec.scala` §(E) — the chm_events fan-out
  * depth-2 chain (events → events_by_user → user_count and
  * events → events_by_type → type_count) convergence walk (~3m40).  Lives in
  * its own forked JVM so the rest of the parity suite is not blocked by this
  * monster test.
  *
  * Note: the two `it` blocks inside describe(E) share state (the second
  * exercises DELETE on the same base table / MVs created by the first), so
  * they are extracted together as one cohesive scenario.  Table / MV names
  * are prefixed `chmheavy_` to guarantee no Delta-path collision with the
  * host spec or any other parallel forked JVM.
  */
class ChainedDmlHeavyConvergenceSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-chain-dm-heavy-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ChainedDmlHeavyConvergenceSpec")
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

  /** Bidirectional `EXCEPT ALL` equivalence between the MV and the recomputed
    * view body, projecting the MV onto the expected column list to drop any
    * `openivm_*` bookkeeping columns. */
  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    withClue(s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  /** Issue refreshes in dependency order: every name in `mvs` is refreshed
    * after the ones before it. */
  private def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => spark.sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  // ──────────────────────────────────────────────────────────────────────────
  // (E) Fan-out: a single base table feeds two independent depth-2 chains
  //     (chained.test L326–L450).
  //     events → events_by_user → user_count
  //     events → events_by_type → type_count
  // ──────────────────────────────────────────────────────────────────────────

  describe("(E) Fan-out depth-2 chains share the same base table") {

    it("first batch: both chains converge with the source after refresh") {
      spark.sql("CREATE TABLE IF NOT EXISTS chmheavy_events(user_id STRING, event_type STRING, value INT) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW chmheavy_events_by_user AS " +
          "SELECT user_id, SUM(value) AS total_value FROM chmheavy_events GROUP BY user_id"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW chmheavy_events_by_type AS " +
          "SELECT event_type, SUM(value) AS total_value FROM chmheavy_events GROUP BY event_type"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW chmheavy_event_user_count AS " +
          "SELECT COUNT(*) AS num_users FROM chmheavy_events_by_user"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW chmheavy_event_type_count AS " +
          "SELECT COUNT(*) AS num_types FROM chmheavy_events_by_type"
      )
      spark.sql(
        "INSERT INTO chmheavy_events VALUES " +
          "('u1','click',10),('u2','click',20),('u1','purchase',100),('u3','click',5)"
      )
      refreshChain(
        "chmheavy_events_by_user",
        "chmheavy_events_by_type",
        "chmheavy_event_user_count",
        "chmheavy_event_type_count"
      )
      assertMvCorrect(
        "chmheavy_events_by_user",
        "SELECT user_id, SUM(value) AS total_value FROM chmheavy_events GROUP BY user_id"
      )
      assertMvCorrect(
        "chmheavy_events_by_type",
        "SELECT event_type, SUM(value) AS total_value FROM chmheavy_events GROUP BY event_type"
      )
      assertMvCorrect(
        "chmheavy_event_user_count",
        "SELECT COUNT(*) AS num_users FROM (" +
          "SELECT user_id, SUM(value) AS total_value FROM chmheavy_events GROUP BY user_id) t"
      )
      assertMvCorrect(
        "chmheavy_event_type_count",
        "SELECT COUNT(*) AS num_types FROM (" +
          "SELECT event_type, SUM(value) AS total_value FROM chmheavy_events GROUP BY event_type) t"
      )
    }

    it("second batch with DELETE propagates through both chains") {
      spark.sql("INSERT INTO chmheavy_events VALUES ('u4','purchase',50),('u1','click',15)")
      spark.sql("DELETE FROM chmheavy_events WHERE user_id = 'u3'")
      refreshChain(
        "chmheavy_events_by_user",
        "chmheavy_events_by_type",
        "chmheavy_event_user_count",
        "chmheavy_event_type_count"
      )
      assertMvCorrect(
        "chmheavy_events_by_user",
        "SELECT user_id, SUM(value) AS total_value FROM chmheavy_events GROUP BY user_id"
      )
      assertMvCorrect(
        "chmheavy_events_by_type",
        "SELECT event_type, SUM(value) AS total_value FROM chmheavy_events GROUP BY event_type"
      )
      assertMvCorrect(
        "chmheavy_event_user_count",
        "SELECT COUNT(*) AS num_users FROM (" +
          "SELECT user_id, SUM(value) AS total_value FROM chmheavy_events GROUP BY user_id) t"
      )
      assertMvCorrect(
        "chmheavy_event_type_count",
        "SELECT COUNT(*) AS num_types FROM (" +
          "SELECT event_type, SUM(value) AS total_value FROM chmheavy_events GROUP BY event_type) t"
      )
    }
  }
}
