package org.openivm.spark.parity.base

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{CdfWatermarkCatalog, MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Shared base for every parity spec.
  *
  * Provides:
  *  - a SparkSession wired with Delta + openivm-spark + per-mode change
  *    feed mode,
  *  - a UUID-suffixed warehouse directory,
  *  - a `sql(...)` helper that transparently injects
  *    `TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')` into every
  *    `CREATE TABLE … USING DELTA` under CDF mode, so existing spec bodies
  *    require no per-statement modification to gain CDF coverage,
  *  - `assertMvCorrect`, `refreshMv`, `deleteDir` helpers identical in
  *    semantics to the originals.
  *
  * Subclasses MUST mix in either [[InterceptMode]] or [[CdfMode]] to
  * provide the change-feed-mode configuration.
  *
  * @param specSlug short kebab-case identifier (e.g. "sa-sum") used to
  *                 build the unique warehouse-dir prefix.  Must be unique
  *                 across the whole repo to prevent two specs from
  *                 colliding on shared on-disk RocksDB or catalog state if
  *                 they were ever to run in the same fork.
  */
abstract class IvmParitySpecBase(val specSlug: String) extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  self: IvmParityMode =>

  protected val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-$specSlug-$modeLabel-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  protected var spark: SparkSession = _

  /** Subclasses can override to add SparkConf entries that must be set at
    * session-build time (e.g. `spark.openivm.profile.refresh = true`).  The
    * intercept/cdf-mode key is injected automatically by [[startSpark]].
    */
  protected def extraSparkConf: Map[String, String] = Map.empty

  /** Test-local Spark master. Most specs are single-threaded and stay on
    * `local[1]`; concurrency-focused specs override this to admit parallel
    * driver work.
    */
  protected def sparkMaster: String = "local[1]"

  override def beforeAll(): Unit = {
    super.beforeAll()
    startSpark(extraSparkConf)
  }

  override def afterAll(): Unit =
    try {
      stopSpark()
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
    }

  /** Spin up a fresh SparkSession wired with Delta + openivm-spark + the
    * per-mode change-feed config.  Specs that need to flip a SparkConf that
    * is only consulted at session-build time (e.g.
    * `spark.openivm.delta.enableDeletionVectors`) can call
    * [[restartSpark]] / [[stopSpark]] / [[startSpark]] mid-test.
    */
  protected def startSpark(extraConf: Map[String, String] = Map.empty): Unit = {
    val builder = SparkSession
      .builder()
      .master(sparkMaster)
      .appName(s"openivm-spark-$specSlug-$modeLabel")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.openivm.changeFeed.mode", changeFeedMode.value)
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
    extraConf.foreach { case (k, v) => builder.config(k, v) }
    spark = builder.getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
    CdfWatermarkCatalog.ensureTables(spark)
  }

  protected def stopSpark(): Unit =
    if (spark != null) {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      spark = null
    }

  protected def restartSpark(extraConf: Map[String, String] = Map.empty): Unit = {
    stopSpark()
    startSpark(extraConf)
  }

  // ── helpers ─────────────────────────────────────────────────────────────

  /** SQL entry point used by spec bodies.  Rewrites `CREATE TABLE … USING DELTA`
    * to inject the mode-specific TBLPROPERTIES (CDF mode requires
    * `delta.enableChangeDataFeed = true` on every source so the
    * [[org.openivm.spark.common.CdfChangePropagation]] can read its change
    * rows).  All other SQL passes through unchanged.
    *
    * Returns the underlying [[DataFrame]] for consistency with `spark.sql`.
    */
  protected def sql(query: String): DataFrame =
    spark.sql(IvmParitySpecBase.rewriteForMode(query, cdfTblProps))

  protected def assertMvCorrect(
      mvName: String,
      expectedSql: String,
      arrayCols: Set[String] = Set.empty
  ): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq

    def project(df: DataFrame): DataFrame = {
      if (arrayCols.isEmpty) df.select(userCols.head, userCols.tail: _*)
      else {
        val exprs = userCols.map { c =>
          if (arrayCols.contains(c)) s"to_json(`$c`) AS `$c`"
          else s"`$c`"
        }
        df.selectExpr(exprs: _*)
      }
    }

    val expectedProj = project(expected)
    val mv           = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    val mvProj       = project(mv)

    withClue(s"$mvName EXCEPT ALL <expected>: ") {
      mvProj.exceptAll(expectedProj).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName: ") {
      expectedProj.exceptAll(mvProj).count() shouldBe 0L
    }
  }

  protected def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  /** Register a test case that only makes sense under intercept mode (e.g.
    * one that asserts directly on `StagingCatalog` contents or simulates a
    * staging-row corruption).  Under CDF mode the test is registered as
    * `ignore(...)` so the full describe/it tree stays visible in the
    * scalatest output but the body is not executed.
    *
    * Usage in scenarios files:
    *   itIntercept("does X") { ... }
    *
    * Prefer this over guarding the test body with an `if` — keeping the
    * test visible (but ignored) makes parity gaps explicit in the
    * scalatest output.
    */
  protected def itIntercept(text: String)(test: => Any): Unit = {
    if (changeFeedMode == org.openivm.spark.common.ChangeFeedMode.Intercept)
      it(text)(test)
    else
      ignore(text)(test)
  }

  /** Register a test that only makes sense under CDF mode.  Under intercept
    * mode the test is registered as `ignore(text)` so the parity gap stays
    * visible without executing the (CDF-specific) body. The inverse of
    * [[itIntercept]]. */
  protected def itCdf(text: String)(test: => Any): Unit = {
    if (changeFeedMode == org.openivm.spark.common.ChangeFeedMode.Cdf)
      it(text)(test)
    else
      ignore(text)(test)
  }

  /** Register an entire `describe` block that only makes sense under
    * intercept mode.  Under CDF mode the block is registered as a single
    * `ignore(text)` so the suite still reports the gap explicitly without
    * executing the (intercept-specific) body.
    */
  protected def describeIntercept(text: String)(body: => Unit): Unit = {
    if (changeFeedMode == org.openivm.spark.common.ChangeFeedMode.Intercept)
      describe(text)(body)
    else
      ignore(s"[$text] is intercept-mode-only — skipped under CDF mode")(())
  }

  protected def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }
}

object IvmParitySpecBase {

  /** Inject `delta.enableChangeDataFeed = true` into every
    * `CREATE TABLE … USING DELTA` statement when `cdfTblProps` is
    * non-empty.  Existing TBLPROPERTIES clauses are extended in-place
    * (idempotent if the property is already present).
    */
  def rewriteForMode(query: String, cdfTblProps: String): String = {
    if (cdfTblProps.isEmpty) return query
    if (!query.toUpperCase.contains("USING DELTA")) return query

    val pattern =
      """(?is)\b(USING\s+DELTA)((?:\s+OPTIONS\s*\([^)]*\))?(?:\s+PARTITIONED\s+BY\s*\([^)]*\))?(?:\s+LOCATION\s+'[^']*')?)(\s+TBLPROPERTIES\s*\(\s*([^)]*?)\s*\))?""".r

    pattern.replaceAllIn(
      query,
      m => {
        val delta         = m.group(1)
        val trailing      = Option(m.group(2)).getOrElse("")
        val existingInner = Option(m.group(4)).getOrElse("").trim
        val mergedInner =
          if (existingInner.isEmpty) cdfTblProps
          else if (existingInner.toLowerCase.contains("delta.enablechangedatafeed")) existingInner
          else s"$existingInner, $cdfTblProps"
        scala.util.matching.Regex.quoteReplacement(s"$delta$trailing TBLPROPERTIES ($mergedInner)")
      }
    )
  }
}
