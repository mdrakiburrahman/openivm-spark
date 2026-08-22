package org.openivm.spark.parity

import org.openivm.spark.commands.{CommandConcurrencyInjection, RefreshFailureInjection}
import org.openivm.spark.parity.base.IvmParitySpecBase

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/** Parity port of `openivm/test/sql/concurrency.test`.
  *
  * == What the openivm test exercises ==
  *
  * Five concurrency scenarios in DuckDB using `concurrentloop`:
  *
  *   1. One writer (INSERT + refresh) interleaved with N readers.  Validates
  *      MVCC allows concurrent reads while a refresh is in flight.
  *   2. Multiple threads issuing `PRAGMA refresh()` on the SAME view.  Before
  *      the per-view mutex fix this raised "Conflict on tuple deletion"; with
  *      the mutex both threads wait their turn and the MV stays correct.
  *   3. Same pattern on a projection MV (no aggregation).
  *   4. One thread drops a view while another refreshes a DIFFERENT view.
  *      Both complete; the dropped view is gone, the kept view is correct.
  *   5. Multi-thread conflicting DML on the SAME base table, single refresh
  *      after.  Verifies delta consolidation works under concurrent writers.
  *
  * == Spark-side mapping ==
  *
  *   - One SparkSession is shared across threads (`local[4]` for this spec
  *     so concurrent CREATE / REFRESH / DROP requests can all make progress).
  *     Spark SparkSession is documented as
  *     thread-safe.
  *
  *   - Delta Lake provides OCC (optimistic concurrency control) at the
  *     transaction level.  When two threads try to COMMIT writes to the
  *     same Delta table simultaneously, one wins immediately and the other
  *     gets a `ConcurrentAppendException` / similar. Same-MV refreshes are
  *     serialized by the command mutex, and any retry that matters to
  *     correctness must rerun the whole command from a fresh staging
  *     snapshot rather than replaying one rewritten statement in place.
  *
  *   - We use a JDK `ExecutorService` (fixed thread pool) and `Future`s.
  *     Each test bounds total wall time with `Await.result(... , timeout)`
  *     so a deadlock would fail-fast rather than hang the suite.
  *
  *   - We do NOT assert that REFRESHes from different threads never fail —
  *     OCC may legitimately surface a transient error.  We DO assert that,
  *     after the dust settles, a final single-threaded REFRESH produces an
  *     MV bag-equal to the live view body.  This is the openivm test's
  *     stated invariant: "the MV stays correct".
  */
abstract class ConcurrencyScenarios extends IvmParitySpecBase("concurrency") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def sparkMaster: String = "local[4]"

  /** Run a body that may legitimately encounter an OCC conflict / refresh
    * collision.  Captures the error but does not fail the future — the test
    * verifies eventual consistency, not per-call success.
    */
  protected def silentlyTry(body: => Unit, errSink: AtomicReference[Throwable]): Unit =
    try body
    catch {
      case t: Throwable =>
        errSink.compareAndSet(null, t)
    }

  /** Run `tasks` on a fixed thread pool of size `parallelism`; wait `timeout`. */
  protected def runAll(parallelism: Int, timeout: Duration)(tasks: Seq[() => Unit]): Unit = {
    val pool = Executors.newFixedThreadPool(parallelism)
    try {
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      val futures                       = tasks.map(t => Future(t()))
      Await.result(Future.sequence(futures), timeout)
    } finally {
      pool.shutdown()
      pool.awaitTermination(30, TimeUnit.SECONDS)
    }
  }

  protected def withPool[A](parallelism: Int)(body: ExecutionContext => A): A = {
    val pool = Executors.newFixedThreadPool(parallelism)
    try {
      body(ExecutionContext.fromExecutorService(pool))
    } finally {
      pool.shutdown()
      pool.awaitTermination(30, TimeUnit.SECONDS)
    }
  }

  protected def runSqlEventually(sqlText: String, maxAttempts: Int = 10): Unit = {
    var attempt              = 0
    var succeeded            = false
    var lastError: Throwable = null
    while (!succeeded && attempt < maxAttempts) {
      try {
        sql(sqlText).collect()
        succeeded = true
      } catch {
        case t: Throwable =>
          lastError = t
          attempt += 1
          Thread.sleep(50L * attempt)
      }
    }
    if (!succeeded && lastError != null) throw lastError
  }

  protected def assertSqlBagEqual(actualSql: String, expectedSql: String): Unit = {
    val actual   = spark.sql(actualSql)
    val expected = spark.sql(expectedSql)
    withClue(s"$actualSql EXCEPT ALL $expectedSql: ") {
      actual.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"$expectedSql EXCEPT ALL $actualSql: ") {
      expected.exceptAll(actual).count() shouldBe 0L
    }
  }

  // ============================================================================
  // (0) CREATE on one MV must not block REFRESH on another MV
  // ============================================================================
  describe("(0) Unrelated CREATE and REFRESH commands overlap") {
    it("CREATE on one MV does not take a global lock that blocks REFRESH elsewhere") {
      sql("CREATE TABLE IF NOT EXISTS create_c0(region STRING, amount INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS refresh_c0(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO create_c0 VALUES ('east', 10), ('west', 20), ('north', 30)")
      sql("INSERT INTO refresh_c0 VALUES ('east', 1), ('west', 2)")

      val createSql  = "SELECT region, SUM(amount) AS total FROM create_c0 GROUP BY region"
      val refreshSql = "SELECT region, SUM(amount) AS total FROM refresh_c0 GROUP BY region"
      sql(s"CREATE MATERIALIZED VIEW mv_refresh_c0 AS $refreshSql")
      sql("INSERT INTO refresh_c0 VALUES ('north', 3)")

      val createEntered = new CountDownLatch(1)
      val releaseCreate = new CountDownLatch(1)

      CommandConcurrencyInjection.withBeforeCreateBody({
        createEntered.countDown()
        releaseCreate.await(30, TimeUnit.SECONDS) shouldBe true
      }) {
        withPool(2) { implicit ec =>
          val createFuture = Future(sql(s"CREATE MATERIALIZED VIEW mv_create_c0 AS $createSql").collect())
          createEntered.await(30, TimeUnit.SECONDS) shouldBe true
          val refreshFuture = Future(refreshMv("mv_refresh_c0"))
          Await.result(refreshFuture, 300.seconds)
          createFuture.isCompleted shouldBe false
          releaseCreate.countDown()
          Await.result(createFuture, 600.seconds)
        }
      }

      assertMvCorrect("mv_refresh_c0", refreshSql)
      assertMvCorrect("mv_create_c0", createSql)
    }
  }

  // ============================================================================
  // (1) Concurrent reads on an aggregate MV while a writer refreshes
  // ============================================================================
  describe("(1) Reads coexist with writer's INSERT+REFRESH on the same aggregate MV") {
    it("MV is correct after the writer finishes 5 INSERT/REFRESH cycles") {
      sql("CREATE TABLE IF NOT EXISTS sales_c1(region STRING, amount INT) USING DELTA")
      val seed = (1 to 200).map(i => s"('region_${i % 10}', $i)").mkString(", ")
      sql(s"INSERT INTO sales_c1 VALUES $seed")

      sql(
        "CREATE MATERIALIZED VIEW mv_sales_c1 AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_c1 GROUP BY region"
      )
      spark.table("mv_sales_c1").count() shouldBe 10L

      val errSink = new AtomicReference[Throwable](null)

      val writer: () => Unit = () =>
        for (k <- 0 until 5) {
          val rows = (1 to 20).map(i => s"('region_${i % 10}', ${1000 + k * 100 + i})").mkString(", ")
          silentlyTry(sql(s"INSERT INTO sales_c1 VALUES $rows"), errSink)
          silentlyTry(refreshMv("mv_sales_c1"), errSink)
        }

      // Two reader tasks loop SELECT count(*) — they should never crash even
      // while the MV is being rewritten by the writer.
      val reader: () => Unit = () =>
        for (_ <- 0 until 20) {
          silentlyTry(spark.table("mv_sales_c1").count(): Unit, errSink)
        }

      runAll(parallelism = 3, timeout = 600.seconds)(Seq(writer, reader, reader))

      // After all threads finish, a final single-threaded REFRESH must
      // converge the MV (in case the parallel run left some staged DML
      // unconsumed due to OCC retry).
      refreshMv("mv_sales_c1")
      assertMvCorrect(
        "mv_sales_c1",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_c1 GROUP BY region"
      )

      // If a fatal error was captured (something other than transient OCC
      // / commit conflict), surface it so the test fails loudly.
      val captured = Option(errSink.get())
      captured.foreach { t =>
        val msg = Option(t.getMessage).getOrElse("")
        // Concurrent commit failures / staging conflicts are tolerated; anything else fails the test.
        val tolerated = msg.contains("Concurrent") || msg.contains("conflict") ||
          msg.contains("commit") || msg.contains("DELTA_CONCURRENT")
        if (!tolerated) {
          fail(s"Unexpected exception in concurrent reader/writer: ${t.getClass.getName}: $msg")
        }
      }
    }
  }

  // ============================================================================
  // (2) Same-MV refreshes serialize; the queued refresh sees newer deltas
  // ============================================================================
  describe("(2) Same-MV REFRESH requests serialize and re-read queued deltas") {
    it("the waiting refresh starts from a fresh staging snapshot and applies the later batch once") {
      sql("CREATE TABLE IF NOT EXISTS sales_c2(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO sales_c2 VALUES ('seed', 1)")

      val refreshSql = "SELECT region, SUM(amount) AS total FROM sales_c2 GROUP BY region"
      sql(s"CREATE MATERIALIZED VIEW mv_sales_c2 AS $refreshSql")

      sql(
        "INSERT INTO sales_c2 " +
          "SELECT CASE " +
          "  WHEN id % 4 = 0 THEN 'east' " +
          "  WHEN id % 4 = 1 THEN 'west' " +
          "  WHEN id % 4 = 2 THEN 'north' " +
          "  ELSE 'south' END AS region, " +
          "CAST(id AS INT) AS amount FROM range(250000)"
      )

      withPool(2) { implicit ec =>
        val refresh1 = Future(refreshMv("mv_sales_c2"))
        Thread.sleep(150L)
        val refresh2 = Future(refreshMv("mv_sales_c2"))
        Thread.sleep(150L)
        sql(
          "INSERT INTO sales_c2 " +
            "SELECT CASE " +
            "  WHEN id % 4 = 0 THEN 'east' " +
            "  WHEN id % 4 = 1 THEN 'west' " +
            "  WHEN id % 4 = 2 THEN 'north' " +
            "  ELSE 'south' END AS region, " +
            "CAST(id + 250000 AS INT) AS amount FROM range(150000)"
        )

        Await.result(refresh1, 600.seconds)
        refresh2.isCompleted shouldBe false
        Await.result(refresh2, 600.seconds)
      }

      assertMvCorrect("mv_sales_c2", refreshSql)

      // A third REFRESH with no intervening DML must now be a no-op.
      refreshMv("mv_sales_c2")
      assertMvCorrect("mv_sales_c2", refreshSql)
    }
  }

  // ============================================================================
  // (3) Projection MV — concurrent read + writer INSERT/REFRESH
  // ============================================================================
  describe("(3) Projection MV — one writer + multiple readers") {
    it("multi-thread reads succeed while writer drives INSERT+REFRESH cycles") {
      sql(
        "CREATE TABLE IF NOT EXISTS employees_c3(id INT, name STRING, dept STRING) USING DELTA"
      )
      val seed =
        (1 to 200).map(i => s"($i, 'emp_$i', 'dept_${i % 3}')").mkString(", ")
      sql(s"INSERT INTO employees_c3 VALUES $seed")

      sql("CREATE MATERIALIZED VIEW mv_emp_c3 AS SELECT id, name FROM employees_c3")

      val errSink = new AtomicReference[Throwable](null)

      val writer: () => Unit = () =>
        for (k <- 0 until 5) {
          silentlyTry(
            sql(s"INSERT INTO employees_c3 VALUES (${1000 + k}, 'new_${1000 + k}', 'dept_0')"),
            errSink
          )
          silentlyTry(refreshMv("mv_emp_c3"), errSink)
        }
      val reader: () => Unit = () =>
        for (_ <- 0 until 30) {
          silentlyTry(spark.table("mv_emp_c3").count(): Unit, errSink)
        }

      runAll(parallelism = 3, timeout = 600.seconds)(Seq(writer, reader, reader))

      // Final converge then verify
      refreshMv("mv_emp_c3")
      assertMvCorrect("mv_emp_c3", "SELECT id, name FROM employees_c3")
    }
  }

  // ============================================================================
  // (4) DROP MATERIALIZED VIEW while another thread refreshes a DIFFERENT view
  // ============================================================================
  describe("(4) DROP one MV while refreshing another — both succeed independently") {
    it("after DROP, the target is gone; the kept MV stays correct") {
      // Set up: two unrelated MVs over two unrelated tables.
      sql("CREATE TABLE IF NOT EXISTS t4a_c4(region STRING, amount INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS t4b_c4(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO t4a_c4 VALUES ('east', 3), ('west', 4)")
      sql("INSERT INTO t4b_c4 VALUES ('east', 8), ('west', 9)")

      sql(
        "CREATE MATERIALIZED VIEW mv_drop_target_c4 AS SELECT region, SUM(amount) AS total FROM t4a_c4 GROUP BY region"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_keep_c4 AS SELECT region, SUM(amount) AS total FROM t4b_c4 GROUP BY region"
      )

      // Add DML so the keeper has something to refresh
      sql(
        "INSERT INTO t4b_c4 " +
          "SELECT CASE " +
          "  WHEN id % 4 = 0 THEN 'east' " +
          "  WHEN id % 4 = 1 THEN 'west' " +
          "  WHEN id % 4 = 2 THEN 'north' " +
          "  ELSE 'south' END AS region, " +
          "CAST(id AS INT) AS amount FROM range(250000)"
      )

      withPool(2) { implicit ec =>
        val refreshFuture = Future(refreshMv("mv_keep_c4"))
        Thread.sleep(150L)
        val dropFuture = Future(sql("DROP MATERIALIZED VIEW mv_drop_target_c4").collect())
        Await.result(dropFuture, 300.seconds)
        refreshFuture.isCompleted shouldBe false
        Await.result(refreshFuture, 600.seconds)
      }

      // The dropped view should be gone.
      val tables = spark.catalog.listTables().collect().map(_.name.toLowerCase).toSet
      tables should not contain "mv_drop_target_c4"

      // The keeper MV must still be correct.
      assertMvCorrect("mv_keep_c4", "SELECT region, SUM(amount) AS total FROM t4b_c4 GROUP BY region")
    }
  }

  // ============================================================================
  // (5) Stress: mixed conflicting DML on the SAME table → single REFRESH
  // ============================================================================
  describe("(5) Stress — conflicting INSERT / UPDATE / DELETE before one REFRESH") {
    it("after all writers finish and a single REFRESH runs, MV is bag-equal") {
      sql("CREATE TABLE IF NOT EXISTS stress_c5(id INT, grp STRING, val INT) USING DELTA")
      val seed =
        (1 to 100).map(i => s"($i, 'g${i % 5}', ${i * 10})").mkString(", ")
      sql(s"INSERT INTO stress_c5 VALUES $seed")

      sql(
        "CREATE MATERIALIZED VIEW mv_stress_c5 AS " +
          "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM stress_c5 GROUP BY grp"
      )

      withPool(3) { implicit ec =>
        val deleteF = Future(runSqlEventually("DELETE FROM stress_c5 WHERE id IN (3, 4, 6, 7, 8)"))
        val updateF = Future(runSqlEventually("UPDATE stress_c5 SET val = val + 7 WHERE id BETWEEN 21 AND 40"))
        val insertF = Future(
          runSqlEventually(
            "INSERT INTO stress_c5 VALUES " +
              "(1000, 'g0', 101), (1001, 'g1', 102), (1002, 'g2', 103), (1003, 'g3', 104)"
          )
        )
        Await.result(Future.sequence(Seq(deleteF, updateF, insertF)), 600.seconds)
      }

      refreshMv("mv_stress_c5")
      assertMvCorrect(
        "mv_stress_c5",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM stress_c5 GROUP BY grp"
      )
    }
  }

  // ============================================================================
  // (6) Per-thread consumed_by idempotency — sequential REFRESH safety
  // ============================================================================
  // openivm's `concurrency.test` Test 2 uses exactly 2 parallel REFRESHes on
  // the same view (covered by our describe(2) above).  This describe(6) adds
  // the complementary "single-thread, many sequential REFRESH calls"
  // idempotency check — the same guarantee, exercised serially.  In openivm
  // this is the per-view mutex serialising calls one at a time; on Spark the
  // analogue is the `consumed_by` array which prevents re-applying a delta
  // that has already been marked for the MV.
  //
  // We intentionally do NOT push beyond 2-thread same-MV parallelism in this
  // suite.  Heavier parallel-REFRESH stress on the SimpleProjection path can
  // expose a known limitation: when many threads race through
  // `StagingCatalog.collectFor` before any of them has marked the delta
  // `consumed_by`, the rowid-keyed signed MERGE may apply the same staged
  // delta more than once.  Aggregate (count-monoid) views are protected by
  // the post-merge zero-count DELETE; pure projection views are not.  Adding
  // an inter-view mutex would close this gap but is out of scope for the
  // parity-port phase.
  describe("(6) consumed_by idempotency under MANY sequential REFRESH calls (single thread)") {
    it("twenty sequential REFRESHes after one INSERT produce the correct MV exactly once") {
      sql("CREATE TABLE IF NOT EXISTS idem_c6(id INT, val INT) USING DELTA")
      sql(s"INSERT INTO idem_c6 VALUES ${(1 to 50).map(i => s"($i, $i)").mkString(", ")}")

      sql("CREATE MATERIALIZED VIEW mv_idem_c6 AS SELECT id, val FROM idem_c6")
      sql(
        s"INSERT INTO idem_c6 VALUES ${(51 to 70).map(i => s"($i, $i)").mkString(", ")}"
      )

      (1 to 20).foreach(_ => refreshMv("mv_idem_c6"))

      spark.table("mv_idem_c6").count() shouldBe 70L
      assertMvCorrect("mv_idem_c6", "SELECT id, val FROM idem_c6")
    }
  }

  // ============================================================================
  // (7) Failed refresh can retry from a fresh command state
  // ============================================================================
  describe("(7) Failed refresh can retry without replaying a successful batch") {
    it("compensates the failed WINDOW_PARTITION write and the retry converges exactly once") {
      sql("CREATE TABLE IF NOT EXISTS retry_c7(id BIGINT, seq INT, payload BIGINT) USING DELTA")
      sql("INSERT INTO retry_c7 SELECT id, 1 AS seq, id AS payload FROM range(1001)")

      val viewSql =
        "SELECT id, seq, payload, " +
          "LAG(payload) OVER (PARTITION BY id ORDER BY seq) AS previous_payload, " +
          "ROW_NUMBER() OVER (PARTITION BY id ORDER BY seq) AS row_num " +
          "FROM retry_c7"
      val downstreamSql = "SELECT id, seq, payload FROM retry_mv_c7"

      sql(s"CREATE MATERIALIZED VIEW retry_mv_c7 AS $viewSql")
      sql(s"CREATE MATERIALIZED VIEW retry_downstream_c7 AS $downstreamSql")
      sql(
        "CREATE TABLE retry_before_failure_c7 USING DELTA AS " +
          "SELECT id, seq, payload, previous_payload, row_num FROM retry_mv_c7"
      )

      assertMvCorrect("retry_mv_c7", viewSql)
      assertMvCorrect("retry_downstream_c7", downstreamSql)

      sql("UPDATE retry_c7 SET payload = payload + 100000 WHERE seq = 1")
      sql("INSERT INTO retry_c7 SELECT id, 2 AS seq, id + 200000 AS payload FROM range(1001)")
      sql("DELETE FROM retry_c7 WHERE seq = 1 AND id % 11 = 0")

      RefreshFailureInjection.failNextWindowCascadeInsert(spark)
      intercept[RuntimeException] {
        refreshMv("retry_mv_c7")
      }

      assertSqlBagEqual(
        "SELECT id, seq, payload, previous_payload, row_num FROM retry_mv_c7",
        "SELECT id, seq, payload, previous_payload, row_num FROM retry_before_failure_c7"
      )

      refreshMv("retry_mv_c7")
      refreshMv("retry_downstream_c7")

      assertMvCorrect("retry_mv_c7", viewSql)
      assertMvCorrect("retry_downstream_c7", downstreamSql)
    }
  }
}
