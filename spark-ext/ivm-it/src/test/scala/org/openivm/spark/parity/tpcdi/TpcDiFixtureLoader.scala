package org.openivm.spark.parity.tpcdi

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType}
import org.openivm.spark.analyzer.IvmDmlInterceptorRule

import java.io.{File, InputStream}

/** Runtime CSV → typed Delta loader for the TPC-DI parity spec.
  *
  * Reads the CSV fixtures + sidecar schema DDLs produced by
  * [[TpcDiExtractor]] from `src/test/resources/tpcdi/{data,schemas}/`,
  * casts each CSV file into the original Spark schema (parsing any
  * JSON-encoded STRUCT / ARRAY / MAP columns back to the structured
  * type), and writes the result into a managed `tpcdi.<table>` Delta
  * table under the SparkSession's `spark.sql.warehouse.dir`.
  *
  * `loadBase` flips [[IvmDmlInterceptorRule.bypass]] on for the duration
  * of the cold backfill — at backfill time no MVs exist yet so there is
  * nothing for the interceptor to tee, and skipping the staging path
  * saves wall-clock.
  *
  * `appendBatch` does NOT bypass — this is what makes the spec's
  * batch-2/batch-3 the actual incremental-refresh path: the SQL DML
  * `INSERT INTO tpcdi.staging_<t> SELECT * FROM <csv>` is what
  * [[IvmDmlInterceptorRule]] tees to the per-MV staging Delta.
  */
object TpcDiFixtureLoader {

  private val ResourceRoot = "/tpcdi"

  /** Cold backfill of every `tpcdi.<t>` source table. Bypasses the IVM
    * DML interceptor (no MVs exist yet — nothing to stage). Creates the
    * `tpcdi` database if absent.
    *
    * Pass `enableCdf = true` to set `delta.enableChangeDataFeed = true` on
    * every materialised source so the CDF-mode change-propagation path can
    * resolve them (no-op for intercept mode).
    */
  def loadBase(spark: SparkSession, enableCdf: Boolean = false): Unit = {
    spark.sql("CREATE DATABASE IF NOT EXISTS tpcdi")
    IvmDmlInterceptorRule.bypass.set(true)
    try {
      val tables = listFixtureTables("base")
      tables.foreach { tname =>
        val df     = readFixture(spark, "base", tname)
        val writer = df.write.format("delta").mode("overwrite")
        val withCdf =
          if (enableCdf) writer.option("delta.enableChangeDataFeed", "true")
          else writer
        withCdf.saveAsTable(s"tpcdi.$tname")
        if (enableCdf) {
          spark
            .sql(s"ALTER TABLE tpcdi.$tname SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
            .collect()
        }
      }
    } finally {
      IvmDmlInterceptorRule.bypass.set(false)
    }
  }

  /** Apply the batch-N CDC append: for each `data/batch<n>/<t>.csv`, issue
    * `INSERT INTO tpcdi.<t> SELECT * FROM <csv-as-temp-view>`. Does NOT
    * bypass the interceptor — this is the call that produces the staging
    * deltas the spec's REFRESH step consumes.
    */
  def appendBatch(spark: SparkSession, batchName: String): Unit = {
    require(batchName == "batch2" || batchName == "batch3", s"invalid batch: $batchName")
    val tables = listFixtureTables(batchName)
    tables.foreach { tname =>
      val df      = readFixture(spark, batchName, tname)
      val tmpView = s"__tpcdi_${batchName}_$tname"
      df.createOrReplaceTempView(tmpView)
      // Explicit column list to defend against any column-order drift
      // between the captured fixture and the Delta table that batch-1
      // created. The DDL schema is the source of truth in both writes.
      val cols = df.columns.map(c => s"`$c`").mkString(", ")
      spark.sql(s"INSERT INTO tpcdi.$tname ($cols) SELECT $cols FROM $tmpView")
      spark.catalog.dropTempView(tmpView)
    }
  }

  // ── Internals ─────────────────────────────────────────────────────────────

  /** Discover the captured table names by listing `data/&lt;sub&gt;/&lt;table&gt;.csv`.
    * Uses classloader resource enumeration so the spec works from both
    * sbt and a packaged JAR.
    */
  private def listFixtureTables(sub: String): Seq[String] = {
    // Resources don't expose a directory listing through the classloader,
    // so we read a generated manifest file. We don't emit one — instead,
    // we lean on the well-known set of TPC-DI source tables hard-coded
    // in [[TpcDiExtractor]] and filter to only those whose CSV exists.
    val all = ExpectedSources(sub)
    all.filter { tname =>
      val res = s"$ResourceRoot/data/$sub/$tname.csv"
      Option(getClass.getResourceAsStream(res)).map { is => is.close(); true }.getOrElse(false)
    }
  }

  /** Read one fixture CSV into a typed DataFrame matching the original
    * Delta schema. Columns whose original type is STRUCT / ARRAY / MAP are
    * read as STRING (their JSON-encoded form) then `from_json`-decoded
    * back to the original type, preserving round-trip fidelity.
    */
  private def readFixture(spark: SparkSession, sub: String, tname: String): DataFrame = {
    val schema  = loadSchema(tname)
    val csvPath = copyResourceToTemp(s"$ResourceRoot/data/$sub/$tname.csv", s"$sub-$tname.csv")
    // CSV-side schema: replace nested types with STRING (the JSON-encoded form).
    val csvSchema = StructType(schema.fields.map { f =>
      f.dataType match {
        case _: StructType | _: ArrayType | _: MapType => StructField(f.name, StringType, f.nullable, f.metadata)
        case _                                         => f
      }
    })
    val raw = spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(csvSchema)
      .csv(csvPath.getAbsolutePath)

    // Cast JSON-encoded columns back to their structured types.
    val select = schema.fields.map { f =>
      f.dataType match {
        case _: StructType | _: ArrayType | _: MapType =>
          from_json(col(s"`${f.name}`"), f.dataType).as(f.name)
        case _ =>
          col(s"`${f.name}`")
      }
    }
    raw.select(select: _*)
  }

  /** Load `<resources>/tpcdi/schemas/<tname>.ddl` and parse to a StructType. */
  private def loadSchema(tname: String): StructType = {
    val resource = s"$ResourceRoot/schemas/$tname.ddl"
    val is = Option(getClass.getResourceAsStream(resource))
      .getOrElse(throw new IllegalStateException(s"missing fixture schema: $resource"))
    val ddl =
      try slurp(is).trim
      finally is.close()
    StructType.fromDDL(ddl)
  }

  /** Spark's CSV reader needs a filesystem path, not a JVM classpath
    * resource. Copy each resource to a per-process temp dir on first use
    * and return the file. Idempotent — repeated calls return the same
    * file.
    */
  private def copyResourceToTemp(resource: String, fileName: String): File = {
    val tempDir =
      new File(System.getProperty("java.io.tmpdir"), s"openivm-spark-tpcdi-${ProcessHandle.current().pid()}")
    tempDir.mkdirs()
    val out = new File(tempDir, fileName)
    if (out.exists() && out.length() > 0L) return out
    val is = Option(getClass.getResourceAsStream(resource))
      .getOrElse(throw new IllegalStateException(s"missing fixture CSV: $resource"))
    try {
      java.nio.file.Files.copy(is, out.toPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    } finally is.close()
    out
  }

  private def slurp(is: InputStream): String = {
    val src = scala.io.Source.fromInputStream(is, "UTF-8")
    try src.getLines().mkString("\n")
    finally src.close()
  }

  /** Source-table inventory by batch — mirrors [[TpcDiExtractor.Batch1Tables]]
    * + [[TpcDiExtractor.StagingTables]] + audit. Hard-coded so the loader
    * doesn't need filesystem-style classpath enumeration (which is unreliable
    * inside packaged JARs).
    */
  private val Batch1Tables: Seq[String] = Seq(
    "batch1_customer_mgmt",
    "batch1_date",
    "batch1_finwire",
    "batch1_hr",
    "batch1_industry",
    "batch1_status_type",
    "batch1_tax_rate",
    "batch1_trade_history",
    "batch1_trade_type"
  )
  private val StagingTables: Seq[String] = Seq(
    "staging_cash_transaction",
    "staging_daily_market",
    "staging_holding_history",
    "staging_prospect",
    "staging_trade",
    "staging_watch_history",
    "staging_account",
    "staging_customer",
    "staging_batch_date"
  )

  private def ExpectedSources(sub: String): Seq[String] = sub match {
    case "base"              => (Batch1Tables ++ StagingTables :+ "audit").sorted
    case "batch2" | "batch3" => StagingTables.sorted
    case other               => throw new IllegalArgumentException(s"unknown fixture sub: $other")
  }
}
