package org.openivm.spark.parity.tpcdi

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{MvCatalog, StagingCatalog}

import java.io.{File, PrintWriter}
import java.util.UUID

/** Isolated reproducer for the silver.accounts / silver.customers parity
  * divergence surfaced by [[org.openivm.spark.parity.TpcDiSpec]].
  *
  * Loads ONLY the source tables silver.accounts depends on (transitively),
  * creates the MV via CREATE MATERIALIZED VIEW, then dumps:
  *
  *   - the full openivm-emitted INSERT SQL (no log truncation)
  *   - the MV's stored content as CSV
  *   - the body-recompute as CSV
  *   - per-column diff (rows in MV not in expected, by column)
  *
  * Invoke via:
  *   sbt 'ivmIt/Test/runMain org.openivm.spark.parity.tpcdi.TpcDiAccountsRepro <out-dir>'
  */
object TpcDiAccountsRepro {

  def main(args: Array[String]): Unit = {
    val outDir = new File(if (args.nonEmpty) args(0) else "/tmp/tpcdi-accounts-repro")
    outDir.mkdirs()
    val warehouseDir = new File(outDir, s"warehouse-${UUID.randomUUID().toString.take(8)}").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("TpcDiAccountsRepro")
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
    try {
      MvCatalog.ensureTables(spark)
      StagingCatalog.ensureTables(spark)

      Seq("tpcdi", "bronze", "silver").foreach(s => spark.sql(s"CREATE DATABASE IF NOT EXISTS $s"))

      // Load just the sources we need for silver.accounts:
      //   tpcdi.batch1_customer_mgmt, tpcdi.staging_customer, tpcdi.staging_account,
      //   tpcdi.batch1_tax_rate
      TpcDiFixtureLoader.loadBase(spark)

      // Materialise the bronze upstreams that silver.accounts joins
      // (bronze.crm_customer_mgmt + bronze.reference_tax_rate).
      val bronzeCrm = readResourceSql("/tpcdi/models/bronze/crm_customer_mgmt.sql")
      val bronzeTax = readResourceSql("/tpcdi/models/bronze/reference_tax_rate.sql")
      spark.sql(s"CREATE MATERIALIZED VIEW bronze.crm_customer_mgmt AS $bronzeCrm")
      spark.sql(s"CREATE MATERIALIZED VIEW bronze.reference_tax_rate AS $bronzeTax")

      val silverAccounts = readResourceSql("/tpcdi/models/silver/accounts.sql")
      spark.sql(s"CREATE MATERIALIZED VIEW silver.accounts AS $silverAccounts")

      // Dump MV content and body recompute, then diff them.
      val mv       = spark.table("silver.accounts")
      val expected = spark.sql(silverAccounts)
      val userCols = expected.columns.toSeq

      println(s"[repro] silver.accounts MV row count = ${mv.count()}")
      println(s"[repro] body recompute  row count = ${expected.count()}")

      writeCsv(mv.selectExpr(userCols.map(c => s"`$c`"): _*), new File(outDir, "mv.csv"))
      writeCsv(expected, new File(outDir, "expected.csv"))

      val mvExtra = mv
        .selectExpr(userCols.map(c => s"`$c`"): _*)
        .exceptAll(expected)
      val recExtra = expected.exceptAll(mv.selectExpr(userCols.map(c => s"`$c`"): _*))
      writeCsv(mvExtra.limit(20), new File(outDir, "mv_extra.csv"))
      writeCsv(recExtra.limit(20), new File(outDir, "expected_extra.csv"))
      println(s"[repro] mv exceptAll expected = ${mvExtra.count()}")
      println(s"[repro] expected exceptAll mv = ${recExtra.count()}")

      // Print MV's Delta history (operation + parameters).
      val meta = MvCatalog.list(spark).find(_.name.identifier == "accounts").get
      val hist = DeltaTable.forPath(spark, meta.location).history().orderBy("version")
      println(s"[repro] silver.accounts Delta history (location=${meta.location}):")
      hist.show(20, truncate = false)
      writeJson(hist, new File(outDir, "history.json"))
    } finally {
      spark.stop()
    }
  }

  private def readResourceSql(resource: String): String = {
    val is  = getClass.getResourceAsStream(resource)
    val src = scala.io.Source.fromInputStream(is, "UTF-8")
    try src.getLines().mkString("\n")
    finally { src.close(); is.close() }
  }

  private def writeCsv(df: org.apache.spark.sql.DataFrame, out: File): Unit = {
    val tmp = new File(out.getParentFile, s".${out.getName}.dir")
    if (tmp.exists()) deleteRecursively(tmp)
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(tmp.getAbsolutePath)
    val part = tmp.listFiles().find(f => f.getName.startsWith("part-") && f.getName.endsWith(".csv")).get
    java.nio.file.Files.move(part.toPath, out.toPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    deleteRecursively(tmp)
  }

  private def writeJson(df: org.apache.spark.sql.DataFrame, out: File): Unit = {
    val tmp = new File(out.getParentFile, s".${out.getName}.dir")
    if (tmp.exists()) deleteRecursively(tmp)
    df.coalesce(1).write.mode("overwrite").json(tmp.getAbsolutePath)
    val part = tmp.listFiles().find(_.getName.startsWith("part-")).getOrElse(tmp.listFiles().head)
    java.nio.file.Files.move(part.toPath, out.toPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    deleteRecursively(tmp)
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteRecursively))
    f.delete()
    ()
  }

  // Silence the unused-import warning if any tooling complains.
  @inline private def _refPrintWriter: PrintWriter = null
}
