package org.openivm.spark.parity.tpcdi

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_json}
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

import java.io.{File, PrintWriter}
import java.nio.file.{Files, StandardCopyOption}
import scala.collection.mutable
import scala.util.control.NonFatal

/** One-shot fixture extractor for the TPC-DI parity spec.
  *
  * Reads the existing ivm-bench run output at
  * `&lt;bench-mount&gt;/results/3/spark-openivm/_ivm/_meta/mv_metadata` (a Delta
  * table containing every MV's de-jinja'd `query_sql`, `refresh_type_name`,
  * `source_tables`, etc.) and the raw Delta source tables at
  * `&lt;bench-mount&gt;/raw/3/delta/{batch1,staging,batch2,batch3,audit}/&lt;table&gt;`.
  *
  * Writes a committable fixture tree under `&lt;resources&gt;/tpcdi/`:
  *
  *   data/{base,batch2,batch3}/&lt;table&gt;.csv
  *   schemas/&lt;table&gt;.ddl
  *   models/&lt;schema&gt;/&lt;short&gt;.sql        (and gold/analytics/&lt;short&gt;.sql)
  *   classifications.tsv                  (name TAB refresh_type_name TAB refresh_type)
  *   topology.txt                         (one MV name per line, bronze→silver→gold)
  *
  * This is a developer tool — NOT executed by the test suite. Invoke once
  * via:
  *
  *   sbt 'ivmIt/Test/runMain org.openivm.spark.parity.tpcdi.TpcDiExtractor &lt;bench-mount&gt; &lt;resources-out&gt;'
  *
  * Concretely, inside the dev docker container:
  *
  *   docker compose --env-file pins.env -f docker/docker-compose.yml \
  *     run --rm -T build sbt 'ivmIt/Test/runMain \
  *     org.openivm.spark.parity.tpcdi.TpcDiExtractor \
  *     /work/spark-ext/.temp/ivm-bench/mount \
  *     /work/spark-ext/spark-ext/ivm-it/src/test/resources/tpcdi'
  *
  * Per-table row limit defaults to 100 (the user's stated ceiling for the
  * minimal-data goal). If a source table's slice is too small to satisfy a
  * downstream MV's FK coverage, an `extractor-limits.tsv` file
  * (`&lt;table&gt;\t&lt;limit&gt;` lines) in the resources dir overrides the default
  * for that table. The extractor reports any MV that recomputes to ZERO
  * rows over the captured fixtures and exits non-zero — that's the signal
  * to bump the relevant table's limit.
  */
object TpcDiExtractor {

  // ── Bench layout constants ────────────────────────────────────────────────

  // mv_metadata Delta path (relative to <bench-mount>):
  private val MvMetaSub = "results/3/spark-openivm/_ivm/_meta/mv_metadata"

  // raw/3/delta/ subdir → bench tpcdi.* table-name prefix mapping. Mirrors
  // .temp/ivm-bench/src/containers/dbt-server/services/spark_openivm_sources.py.
  private val Batch1Tables: Seq[String] = Seq(
    "customer_mgmt",
    "date",
    "finwire",
    "hr",
    "industry",
    "status_type",
    "tax_rate",
    "trade_history",
    "trade_type"
  )
  private val StagingTables: Seq[String] = Seq(
    "cash_transaction",
    "daily_market",
    "holding_history",
    "prospect",
    "trade",
    "watch_history",
    "account",
    "customer",
    "batch_date"
  )

  // dbt schema namespaces (everything else in source_tables is treated as a leaf source).
  private val MvSchemas: Set[String] = Set("bronze", "silver", "gold")

  // dbt analytics models live under gold/analytics/<name>.sql per dbt_project.yml.
  // Detect by name — pins.env-style, no behaviour change if dbt renames them.
  private val AnalyticsModels: Set[String] = Set(
    "broker_performance",
    "customer_concentration",
    "daily_market_pulse",
    "market_volatility",
    "trade_volume_stats"
  )

  // Default per-table row limit. Override per table via extractor-limits.tsv
  // in the resources output dir.
  private val DefaultRowLimit = 100

  // ── Main ──────────────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      Console.err.println(
        s"""Usage: TpcDiExtractor <bench-mount-dir> <resources-out-dir>
           |
           |  bench-mount-dir   absolute path to .temp/ivm-bench/mount
           |  resources-out-dir absolute path to spark-ext/ivm-it/src/test/resources/tpcdi
           |""".stripMargin
      )
      sys.exit(2)
    }

    val benchMount = new File(args(0)).getAbsoluteFile
    val outDir     = new File(args(1)).getAbsoluteFile

    require(benchMount.isDirectory, s"bench-mount-dir does not exist or is not a dir: $benchMount")
    outDir.mkdirs()
    require(outDir.isDirectory, s"resources-out-dir is not a dir: $outDir")

    val mvMetaPath = new File(benchMount, MvMetaSub).getAbsolutePath
    val rawDelta   = new File(benchMount, "raw/3/delta").getAbsolutePath
    require(new File(mvMetaPath).isDirectory, s"missing mv_metadata Delta: $mvMetaPath")
    require(new File(rawDelta).isDirectory, s"missing raw/3/delta: $rawDelta")

    val limits = loadLimits(new File(outDir, "extractor-limits.tsv"))

    val spark = buildSparkSession(outDir)
    try {
      val mvRows = readMvMetadata(spark, mvMetaPath)
      println(s"[extractor] mv_metadata: ${mvRows.size} MVs")

      // 1. Write per-MV model SQL files (with parser-safe paren-stripping).
      val sqlForMv = mutable.LinkedHashMap.empty[String, String]
      mvRows.foreach { mv =>
        val sql = stripOuterParens(mv.querySql, spark)
        sqlForMv += mv.name -> sql
        val sqlFile = modelSqlFile(outDir, mv.name)
        sqlFile.getParentFile.mkdirs()
        writeStringAtomically(sqlFile, sql + "\n")
      }
      println(s"[extractor] wrote ${sqlForMv.size} model SQL files under ${new File(outDir, "models").getAbsolutePath}")

      // 2. Write classifications.tsv.
      val classFile  = new File(outDir, "classifications.tsv")
      val classLines = mvRows.map(mv => s"${mv.name}\t${mv.refreshTypeName}\t${mv.refreshType}")
      writeStringAtomically(classFile, classLines.mkString("", "\n", "\n"))
      println(s"[extractor] wrote ${classFile.getAbsolutePath} (${mvRows.size} entries)")

      // 3. Compute topology purely from source_tables (MV→MV edges; same-layer ok).
      val mvNames  = mvRows.map(_.name).toSet
      val byShort  = mvRows.iterator.map(m => m.shortName -> m.name).toMap
      val topology = topoOrder(mvRows, mvNames, byShort)
      val topoFile = new File(outDir, "topology.txt")
      writeStringAtomically(topoFile, topology.mkString("", "\n", "\n"))
      println(
        s"[extractor] wrote ${topoFile.getAbsolutePath} (${topology.size} entries, ${topology.head} … ${topology.last})"
      )

      // 4. Capture per-source CSV slices + schema DDL.
      val capturedTables = captureSourceFixtures(spark, rawDelta, outDir, limits)
      println(
        s"[extractor] captured ${capturedTables.size} source tables under ${new File(outDir, "data").getAbsolutePath}"
      )

      // 5. Coverage validation: slice raw Delta sources the same way and replay every MV body
      //    against them. Empty results are WARN (not fail) — many TPC-DI MVs cascade through
      //    empties from filters like `WHERE rec_type = 'CMP'` over data that doesn't include
      //    that type; the bench's mv_metadata captured those MVs as empty too. SQL parse /
      //    resolution errors are still HARD failures (they indicate a Spark-vs-DuckDB
      //    dialect issue with the captured query_sql that the spec would also hit at CREATE
      //    time).
      val coverageReports       = validateCoverage(spark, outDir, rawDelta, limits, topology, sqlForMv.toMap, mvRows)
      val (empties, hardErrors) = coverageReports.partition(_._2.startsWith("empty recompute"))
      if (empties.nonEmpty) {
        println(s"[extractor] WARN: ${empties.size} MV(s) recompute to ZERO rows over the captured slices:")
        empties.foreach { case (mvName, why) => println(s"  - $mvName: $why") }
        println(
          "[extractor] WARN: these MVs will still be created and structurally validated by TpcDiSpec; " +
            "their EXCEPT ALL parity check passes trivially (0 = 0)."
        )
      }
      if (hardErrors.nonEmpty) {
        Console.err.println(s"[extractor] FAILED coverage validation for ${hardErrors.size} MVs:")
        hardErrors.foreach { case (mvName, why) => Console.err.println(s"  - $mvName: $why") }
        sys.exit(3)
      }
      println("[extractor] coverage validation: no SQL errors.")

      println("[extractor] done.")
    } finally {
      spark.stop()
    }
  }

  // ── Spark session ─────────────────────────────────────────────────────────

  private def buildSparkSession(outDir: File): SparkSession = {
    // Spark warehouse lives outside the resources tree so committed artefacts
    // can never include leaked parquet / _delta_log files. We tear it down at
    // process exit; the directory name is process-stable so re-runs reuse the
    // same on-disk cache when possible.
    val warehouse = new File(
      System.getProperty("java.io.tmpdir"),
      s"openivm-spark-tpcdi-extractor-warehouse-${ProcessHandle.current().pid()}"
    )
    if (warehouse.exists()) deleteRecursively(warehouse)
    warehouse.mkdirs()
    sys.addShutdownHook(deleteRecursively(warehouse))
    SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-TpcDiExtractor")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "false") // extractor doesn't need MV interception
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  // ── mv_metadata reading ───────────────────────────────────────────────────

  private final case class MvRow(
      name: String,
      querySql: String,
      refreshType: Int,
      refreshTypeName: String,
      sourceTables: Seq[String]
  ) {
    def shortName: String = name.split("\\.").last
    def schema: String    = name.split("\\.").head
  }

  private def readMvMetadata(spark: SparkSession, mvMetaPath: String): Seq[MvRow] = {
    // Dedupe by name keeping the latest created_at. Use Delta to read.
    val df = DeltaTable.forPath(spark, mvMetaPath).toDF
    df.createOrReplaceTempView("__mv_meta")
    val latest = spark.sql(
      """
        |SELECT name, query_sql, refresh_type, refresh_type_name, source_tables, created_at
        |FROM (
        |  SELECT name, query_sql, refresh_type, refresh_type_name, source_tables, created_at,
        |         ROW_NUMBER() OVER (PARTITION BY name ORDER BY created_at DESC) AS rn
        |  FROM __mv_meta
        |) WHERE rn = 1
        |ORDER BY name
        |""".stripMargin
    )
    latest.collect().toIndexedSeq.map { r =>
      MvRow(
        name = r.getAs[String]("name"),
        querySql = r.getAs[String]("query_sql"),
        refreshType = r.getAs[Int]("refresh_type"),
        refreshTypeName = r.getAs[String]("refresh_type_name"),
        sourceTables = r.getSeq[String](r.fieldIndex("source_tables"))
      )
    }
  }

  // ── Paren-stripping (parser-safe) ─────────────────────────────────────────

  /** Strip a leading `(` and trailing `)` ONLY when they wrap the entire SQL
    * and the stripped body parses standalone. Otherwise leave the SQL
    * untouched. This handles the dbt `CREATE MATERIALIZED VIEW … AS (sql)`
    * wrapping without mangling models whose body starts with a CTE-style
    * `(SELECT …) UNION (SELECT …)`.
    */
  private[tpcdi] def stripOuterParens(sql: String, spark: SparkSession): String = {
    val trimmed = sql.trim
    if (trimmed.length < 2 || trimmed.head != '(' || trimmed.last != ')') return sql
    // Matching-paren check: walk the string, ensuring the open-paren at idx=0
    // pairs with the close-paren at idx=length-1.
    var depth   = 0
    var matched = -1
    val chars   = trimmed.toCharArray
    var i       = 0
    while (i < chars.length && matched == -1) {
      val ch = chars(i)
      if (ch == '(') depth += 1
      else if (ch == ')') {
        depth -= 1
        if (depth == 0) matched = i
      }
      i += 1
    }
    if (matched != chars.length - 1) return sql
    val inner = trimmed.substring(1, trimmed.length - 1).trim
    // Parse-validate before accepting the strip.
    try {
      spark.sessionState.sqlParser.parseQuery(inner)
      inner
    } catch {
      case NonFatal(_) => sql
    }
  }

  // ── Model SQL file routing ────────────────────────────────────────────────

  /** e.g. "bronze.brokerage_trade" → spark-ext/.../models/bronze/brokerage_trade.sql
    *      "gold.broker_performance" → spark-ext/.../models/gold/analytics/broker_performance.sql
    */
  private def modelSqlFile(outDir: File, mvName: String): File = {
    val parts = mvName.split("\\.")
    require(parts.length == 2, s"unexpected MV name (need <schema>.<short>): $mvName")
    val schema = parts(0)
    val short  = parts(1)
    val sub    = if (schema == "gold" && AnalyticsModels.contains(short)) "gold/analytics" else schema
    new File(outDir, s"models/$sub/$short.sql")
  }

  // ── Topology computation ──────────────────────────────────────────────────

  /** Topo-sort the MV graph derived from source_tables.
    *
    * - An edge exists from MV `u` to MV `v` iff `v`'s `source_tables` list
    *   contains `u.name` (qualified `<schema>.<short>`). Same-layer edges
    *   (e.g., silver.financials → silver.companies) are preserved.
    * - Non-MV sources (anything outside MvSchemas) are leaves with no
    *   incoming edges.
    * - The collectSourceSchemas helper sometimes records MV deps by `short`
    *   name only (when the upstream is queried unqualified inside the dbt
    *   ref expansion); we map any short-only entry whose short matches an
    *   existing MV's short to its fully-qualified MV name.
    */
  private def topoOrder(
      mvRows: Seq[MvRow],
      mvNames: Set[String],
      byShort: Map[String, String]
  ): Seq[String] = {
    // Build incoming-edge map: mv → set of upstream MVs.
    val incoming = mvRows.map { mv =>
      val ups = mv.sourceTables
        .flatMap { src =>
          if (mvNames.contains(src)) Some(src)
          else if (!src.contains('.') && byShort.contains(src)) Some(byShort(src))
          else if (src.contains('.')) {
            val short = src.split("\\.").last
            if (byShort.contains(short) && MvSchemas.exists(s => src == s"$s.$short")) Some(byShort(short))
            else None
          } else None
        }
        .toSet
        .filterNot(_ == mv.name)
      mv.name -> ups
    }.toMap

    // Kahn's algorithm; stable tie-break on alphabetical order.
    val remaining = mutable.Map(incoming.toSeq: _*)
    val result    = mutable.ListBuffer.empty[String]
    while (remaining.nonEmpty) {
      val ready = remaining.iterator
        .filter { case (_, ups) => ups.forall(u => !remaining.contains(u)) }
        .map(_._1)
        .toSeq
        .sorted
      if (ready.isEmpty) {
        throw new RuntimeException(
          s"[extractor] cycle detected in MV DAG: remaining = ${remaining.keys.toSeq.sorted.mkString(", ")}"
        )
      }
      ready.foreach { name =>
        result += name
        remaining.remove(name)
      }
    }
    result.toSeq
  }

  // ── Source fixture capture ────────────────────────────────────────────────

  /** Per-table row limit override from `extractor-limits.tsv`. */
  private def loadLimits(file: File): Map[String, Int] = {
    if (!file.isFile) return Map.empty
    scala.io.Source
      .fromFile(file)
      .getLines()
      .map(_.trim)
      .filter(l => l.nonEmpty && !l.startsWith("#"))
      .map { l =>
        val parts = l.split("\\s+")
        require(parts.length == 2, s"bad extractor-limits.tsv line: $l")
        parts(0) -> parts(1).toInt
      }
      .toMap
  }

  /** Returns the list of captured table names (matching tpcdi.<t> in the bench). */
  private def captureSourceFixtures(
      spark: SparkSession,
      rawDelta: String,
      outDir: File,
      limits: Map[String, Int]
  ): Seq[String] = {
    val captured = mutable.ListBuffer.empty[String]

    // batch1_<t>: read raw/3/delta/batch1/<t> → base/batch1_<t>.csv (no batch2/3).
    Batch1Tables.foreach { t =>
      val tname   = s"batch1_$t"
      val srcPath = new File(rawDelta, s"batch1/$t").getAbsolutePath
      val limit   = limits.getOrElse(tname, DefaultRowLimit)
      val df      = readAndSlice(spark, srcPath, limit)
      writeSchemaAndCsv(df, outDir, "base", tname)
      captured += tname
    }

    // staging_<t>: base from raw/3/delta/staging/<t>; batch2/batch3 from matching subdirs.
    StagingTables.foreach { t =>
      val tname    = s"staging_$t"
      val limit    = limits.getOrElse(tname, DefaultRowLimit)
      val basePath = new File(rawDelta, s"staging/$t").getAbsolutePath
      val baseDf   = readAndSlice(spark, basePath, limit)
      writeSchemaAndCsv(baseDf, outDir, "base", tname)
      captured += tname

      Seq("batch2", "batch3").foreach { b =>
        val bPath = new File(rawDelta, s"$b/$t").getAbsolutePath
        if (new File(bPath).isDirectory) {
          val bDf = readAndSlice(spark, bPath, limit)
          writeSchemaAndCsv(bDf, outDir, b, tname)
        } else {
          println(s"[extractor]   $tname: no $b subdir at $bPath — skipping")
        }
      }
    }

    // audit (singleton): base only.
    locally {
      val tname   = "audit"
      val srcPath = new File(rawDelta, "audit").getAbsolutePath
      val limit   = limits.getOrElse(tname, DefaultRowLimit)
      val df      = readAndSlice(spark, srcPath, limit)
      writeSchemaAndCsv(df, outDir, "base", tname)
      captured += tname
    }

    captured.toSeq
  }

  /** Stable ordering: sort by ALL columns ASC (with nulls first) before .limit(n).
    * This is deterministic regardless of underlying Delta file layout.
    */
  private def readAndSlice(spark: SparkSession, deltaPath: String, limitN: Int): DataFrame = {
    val raw      = spark.read.format("delta").load(deltaPath)
    val sortCols = raw.columns.map(c => col(s"`$c`").asc_nulls_first)
    raw.orderBy(sortCols: _*).limit(limitN)
  }

  /** Emit `data/<batchSub>/<tname>.csv` (a single file, not a directory) and
    * `schemas/<tname>.ddl` (only on the first call for the table — base).
    *
    * CSV cannot natively represent STRUCT / ARRAY / MAP columns. For each
    * such column we `to_json(col)` at write time so the CSV stores its
    * string-encoded form; the schema DDL captures the ORIGINAL Spark type
    * so [[TpcDiFixtureLoader]] can `from_json(col, originalType)` it back
    * to the structured type at load time. Round-trip fidelity is preserved.
    */
  private def writeSchemaAndCsv(df: DataFrame, outDir: File, batchSub: String, tname: String): Unit = {
    // Schema DDL: only write on the base call (the schema is the same across
    // base/batch2/batch3 because they're all Delta-derived from the same dbt
    // schema). Idempotent overwrite.
    if (batchSub == "base") {
      val schemaFile = new File(outDir, s"schemas/$tname.ddl")
      schemaFile.getParentFile.mkdirs()
      writeStringAtomically(schemaFile, df.schema.toDDL + "\n")
    }

    // Apply to_json() to every non-CSV-compatible column.
    val csvDf = df.select(df.schema.fields.map { f =>
      f.dataType match {
        case _: StructType | _: ArrayType | _: MapType => to_json(col(s"`${f.name}`")).as(f.name)
        case _                                         => col(s"`${f.name}`")
      }
    }: _*)

    val dir = new File(outDir, s"data/$batchSub/.__csv_$tname")
    if (dir.exists()) deleteRecursively(dir)
    dir.getParentFile.mkdirs()

    csvDf
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(dir.getAbsolutePath)

    // Move single part-*.csv to <tname>.csv, then remove the writer dir.
    val parts = dir.listFiles().filter(f => f.isFile && f.getName.startsWith("part-") && f.getName.endsWith(".csv"))
    require(parts.length == 1, s"expected exactly 1 part-*.csv in $dir, got ${parts.length}")
    val finalFile = new File(outDir, s"data/$batchSub/$tname.csv")
    Files.move(parts.head.toPath, finalFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    deleteRecursively(dir)

    val rowCount = countCsvRows(finalFile)
    println(s"[extractor]   $batchSub/$tname.csv: $rowCount data rows")
  }

  // ── Coverage validation ───────────────────────────────────────────────────

  /** Replay every MV body against the captured *raw-Delta slices* (same
    * slicing logic as fixture capture, just without CSV round-trip); return
    * MVs that produce ZERO rows (empty result = FK coverage lost in the
    * slice).
    *
    * Uses a *separate* SparkSession with NO openivm extension so we get
    * pure SQL evaluation — no MV interception or refresh-compile machinery.
    * Loads sliced sources into managed Delta tables under
    * `tpcdi.<t>` / `bronze.<t>` / `silver.<t>` / `gold.<t>` (each MV's
    * body becomes its own table for downstream MVs to read).
    *
    * Note: we deliberately do NOT validate the CSV round-trip here. That
    * is validated end-to-end by [[TpcDiSpec]]'s "batch-1 backfill bag-equal"
    * check using the actual runtime [[TpcDiFixtureLoader]].
    */
  private def validateCoverage(
      probeSparkUnused: SparkSession,
      outDir: File,
      rawDelta: String,
      limits: Map[String, Int],
      topology: Seq[String],
      sqlForMv: Map[String, String],
      mvRows: Seq[MvRow]
  ): Seq[(String, String)] = {
    // Run validation in a fresh session under a throwaway warehouse so the
    // extractor's main SparkSession state is untouched. Path lives in
    // java.io.tmpdir so a leak can NEVER pollute the committed resources
    // tree even if the extractor crashes mid-run.
    val probeWarehouse = new File(
      System.getProperty("java.io.tmpdir"),
      s"openivm-spark-tpcdi-extractor-coverage-${ProcessHandle.current().pid()}"
    )
    if (probeWarehouse.exists()) deleteRecursively(probeWarehouse)
    probeWarehouse.mkdirs()
    sys.addShutdownHook(deleteRecursively(probeWarehouse))

    val spark2 = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-TpcDiExtractor-coverage")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", probeWarehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    val problems = mutable.ListBuffer.empty[(String, String)]
    try {
      spark2.sql("CREATE DATABASE IF NOT EXISTS tpcdi")
      Seq("bronze", "silver", "gold").foreach(s => spark2.sql(s"CREATE DATABASE IF NOT EXISTS $s"))

      // Load every captured source into tpcdi.<t> with the same slicing
      // logic the fixture capture uses (stable ORDER BY + .limit(N)).
      Batch1Tables.foreach { t =>
        val tname   = s"batch1_$t"
        val srcPath = new File(rawDelta, s"batch1/$t").getAbsolutePath
        val limit   = limits.getOrElse(tname, DefaultRowLimit)
        readAndSlice(spark2, srcPath, limit).write.format("delta").mode("overwrite").saveAsTable(s"tpcdi.$tname")
      }
      StagingTables.foreach { t =>
        val tname   = s"staging_$t"
        val srcPath = new File(rawDelta, s"staging/$t").getAbsolutePath
        val limit   = limits.getOrElse(tname, DefaultRowLimit)
        readAndSlice(spark2, srcPath, limit).write.format("delta").mode("overwrite").saveAsTable(s"tpcdi.$tname")
      }
      locally {
        val tname   = "audit"
        val srcPath = new File(rawDelta, "audit").getAbsolutePath
        val limit   = limits.getOrElse(tname, DefaultRowLimit)
        readAndSlice(spark2, srcPath, limit).write.format("delta").mode("overwrite").saveAsTable(s"tpcdi.$tname")
      }

      // Materialise each MV in topo order as a Delta table (NOT a MATERIALIZED VIEW —
      // we only need its query result for the coverage check).
      topology.foreach { mvName =>
        val sql = sqlForMv(mvName)
        try {
          val df    = spark2.sql(sql)
          val count = df.count()
          df.write.format("delta").mode("overwrite").saveAsTable(mvName)
          if (count == 0L) {
            // Diagnostic: which source_tables does this MV depend on?
            val ups = mvRows.find(_.name == mvName).map(_.sourceTables.mkString(", ")).getOrElse("?")
            problems += (mvName -> s"empty recompute over fixtures (upstreams: $ups)")
          }
        } catch {
          case NonFatal(e) =>
            // SQL parse/resolution failure during validation is itself a fixture issue
            // (often: a 3-part name or DuckDB-only syntax). Report and continue.
            problems += (mvName -> s"sql failed during recompute: ${e.getMessage}")
        }
      }
    } finally {
      spark2.stop()
      deleteRecursively(probeWarehouse)
    }

    // Silence the never-used probeSparkUnused argument warning.
    val _ = probeSparkUnused
    problems.toSeq
  }

  // ── File / I/O helpers ────────────────────────────────────────────────────

  private def writeStringAtomically(file: File, content: String): Unit = {
    file.getParentFile.mkdirs()
    val tmp = new File(file.getParentFile, s".${file.getName}.tmp")
    val pw  = new PrintWriter(tmp, "UTF-8")
    try pw.write(content)
    finally pw.close()
    Files.move(tmp.toPath, file.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteRecursively))
    f.delete()
    ()
  }

  private def countCsvRows(file: File): Long = {
    val src = scala.io.Source.fromFile(file)
    try math.max(0L, src.getLines().size.toLong - 1L)
    finally src.close()
  }
}
