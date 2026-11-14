package org.openivm.spark.compiler

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType
import org.openivm.spark.common.SparkRefreshRewriter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkOpaqueMarkerRefreshSpec extends AnyFlatSpec with Matchers {
  "Opaque Spark markers" should "preserve projection results across mixed incremental DML" in {
    assume(sys.env.get("OPENIVM_MARKER_TEST").contains("1"))
    val cli       = sys.env("OPENIVM_CLI_PATH")
    val extension = sys.env("OPENIVM_EXTENSION_PATH")
    val wrapper   = Files.createTempFile("openivm-opaque-cli-", ".sh")
    Files.write(
      wrapper,
      s"""#!/bin/sh
         |(echo "LOAD '${extension.replace(
          "'",
          "''"
        )}'; SET GLOBAL openivm_spark_expression_markers=true;"; cat) | '${cli.replace("'", "'\\''")}' "$$@"
         |""".stripMargin.getBytes(StandardCharsets.UTF_8)
    )
    require(wrapper.toFile.setExecutable(true))
    val warehouse = new File(s"target/test-warehouse-marker-${UUID.randomUUID().toString.take(8)}").getAbsolutePath
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkOpaqueMarkerRefreshSpec")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val compiler = OpenIvmCompiler.build(cliPath = wrapper.toString)
    try {
      spark.conf.set("spark.sql.legacy.sizeOfNull", "true")
      Seq(false, true).foreach { ansi =>
        spark.conf.set("spark.sql.ansi.enabled", ansi.toString)
        val source = s"marker_src_$ansi"
        val mv     = s"marker_mv_$ansi"
        val path   = s"$warehouse/$mv"
        val expected = s"""SELECT id, txt RLIKE '(?i)ä' AS re, size(split(txt, '/')) AS n,
          |element_at(split(txt, '/'), -1) AS leaf,
          |sha2(to_json(named_struct('O\\'Brien', txt, 'Odd Name', txt, 'nested', named_struct('Mixed', id)),
          |map('ignoreNullFields', 'false', 'timeZone', 'UTC')), 256) AS digest FROM $source
          |WHERE txt IS NULL OR CAST(txt AS STRING) RLIKE '(?i)ä' OR txt LIKE '%/%'""".stripMargin
        val prepared = SparkExpressionPrepass.prepare(expected, caseSensitive = false)
        val bridge   = prepared.markers
        val request = CompileRequest(
          mv,
          prepared.sql,
          Map(source -> StructType.fromDDL("id INT, txt STRING"))
        )
        val compiled = compiler.compile(request)
        compiled.refreshTypeName shouldBe "SIMPLE_PROJECTION"
        val initial = bridge.restore(compiled.initialLoadSql)
        bridge.requireAllSeen()
        bridge.resetSeen()
        spark.sql(s"CREATE TABLE $source (id INT, txt STRING) USING DELTA")
        spark.sql(s"INSERT INTO $source VALUES (1, NULL), (2, 'Ä'), (3, 'a/b')")
        spark.sql(s"CREATE TABLE $mv USING DELTA LOCATION '$path' AS $initial")
        def verify(): Unit = {
          val actual  = spark.table(mv).select("id", "re", "n", "leaf", "digest")
          val correct = spark.sql(expected)
          actual.exceptAll(correct).count() shouldBe 0L
          correct.exceptAll(actual).count() shouldBe 0L
        }
        verify()
        spark.sql(s"DELETE FROM $source WHERE id = 1")
        spark.sql(s"UPDATE $source SET txt = 'x/y' WHERE id = 2")
        spark.sql(s"INSERT INTO $source VALUES (4, 'ä')")
        spark
          .sql(s"""SELECT id, txt, openivm_multiplicity, TIMESTAMP('2100-01-01') AS openivm_timestamp
          |FROM VALUES (1, CAST(NULL AS STRING), -1), (2, 'Ä', -1), (2, 'x/y', 1), (4, 'ä', 1)
          |AS changes(id, txt, openivm_multiplicity)""".stripMargin)
          .createOrReplaceTempView(s"openivm_delta_$source")
        val rewritten = SparkRefreshRewriter.rewrite(
          compiledSql = compiled.sql,
          mvName = TableIdentifier(mv),
          mvLocation = path,
          viewLogicalName = mv,
          sourceTempViews = Map(source -> s"openivm_delta_$source"),
          viewDeltaPath = s"$warehouse/${mv}_delta",
          postProcess = sql => bridge.restore(LptsSparkDialect.translate(sql)),
          sourceSchemas = Map(source -> Seq("id", "txt"))
        )
        bridge.requireAllSeen()
        rewritten.statements.foreach(stmt => spark.sql(stmt))
        verify()
      }
    } finally {
      compiler.close()
      spark.stop()
      Files.deleteIfExists(wrapper)
    }
  }
}
