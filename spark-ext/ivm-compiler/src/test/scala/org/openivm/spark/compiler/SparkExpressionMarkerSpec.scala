package org.openivm.spark.compiler

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkExpressionMarkerSpec extends AnyFlatSpec with Matchers {
  import SparkExpressionMarker._

  "Spark expression markers" should "restore nested arguments without losing casts or nulls" in {
    val bridge  = new SparkExpressionMarker
    val split   = bridge.encode(Seq("CAST(id AS VARCHAR)", "','"), TextArray)(args => s"split(${args.mkString(", ")})")
    val size    = bridge.encode(Seq(split), Integer)(args => s"size(${args.head})")
    val emitted = size.replace(" AS VARCHAR", " AS STRING")
    bridge.restore(emitted) shouldBe "CAST(size(split(CAST(id AS STRING), ',')) AS INTEGER)"
    bridge.requireAllSeen()
  }

  it should "preserve constants, nested structs, and arbitrary field names" in {
    val bridge = new SparkExpressionMarker
    val json = bridge.encode(
      Seq("named_struct('Odd Name', NULL, 'nested', named_struct('Mixed', id))", "map('x', 'y')"),
      Text
    )(args => s"to_json(${args.mkString(", ")})")
    val hash = bridge.encode(Seq(json, "256"), Text)(args => s"sha2(${args.mkString(", ")})")
    bridge.restore(hash) shouldBe
      "sha2(to_json(named_struct('Odd Name', NULL, 'nested', named_struct('Mixed', id)), map('x', 'y')), 256)"
    bridge.requireAllSeen()
  }

  it should "restore date generator bounds and struct markers in the same program" in {
    val bridge       = new SparkExpressionMarker(opaque = true)
    val start        = bridge.encode(Seq("start_date"), DateBound)(_.head)
    val end          = bridge.encode(Seq("end_date"), DateBound)(_.head)
    val seed         = "named_struct('k', CAST(NULL AS INT))"
    val struct       = bridge.encode(Seq("id"), Struct(seed))(args => s"named_struct('k', ${args.head})")
    val nativeStruct = struct.replace(seed, "{'k': NULL, 'nested': {'x': NULL}}")
    val sql = s"SELECT unnest(sequence(CAST($start AS TIMESTAMP), CAST($end AS TIMESTAMP), " +
      s"INTERVAL '1 DAY')), $nativeStruct"
    bridge.restore(sql) shouldBe
      "SELECT explode(sequence(start_date, end_date, INTERVAL '1 DAY')), named_struct('k', id)"
    bridge.requireAllSeen()
  }

  it should "reject missing or malformed markers" in {
    val bridge = new SparkExpressionMarker
    val marker = bridge.encode(Seq("'Ä'", "'(?i)ä'"), Boolean)(args => s"regexp_like(${args.mkString(", ")})")
    intercept[IllegalStateException](bridge.restore(marker.replace("row(", "other(")))
    intercept[IllegalStateException](bridge.requireAllSeen())
    bridge.restore(marker)
    bridge.requireAllSeen()
    bridge.resetSeen()
    intercept[IllegalStateException](bridge.requireAllSeen())
    val opaque = new SparkExpressionMarker(opaque = true)
    intercept[IllegalArgumentException] {
      opaque.encode(Seq("""struct_pack("Odd Name" := id)"""), Text)(_.head)
    }
  }

  it should "survive native compilation of nested projection expressions" in {
    val cli        = sys.env.getOrElse("OPENIVM_CLI_PATH", "/opt/openivm/duckdb")
    val escapedCli = cli.replace("'", "'\\''")
    val wrapper    = Files.createTempFile("openivm-marker-cli-", ".sh")
    val extension  = sys.env.getOrElse("OPENIVM_EXTENSION_PATH", "/opt/openivm/openivm.duckdb_extension")
    val settings   = s"LOAD '${extension.replace("'", "''")}'; SET GLOBAL openivm_spark_expression_markers=true;"
    Files.write(
      wrapper,
      s"""#!/bin/sh
         |(echo "$settings"; cat) | '$escapedCli' "$$@"
         |""".stripMargin.getBytes(StandardCharsets.UTF_8)
    )
    require(wrapper.toFile.setExecutable(true), "Could not make native CLI wrapper executable")
    val compiler = OpenIvmCompiler.build(cliPath = wrapper.toString)
    try {
      val bridge = new SparkExpressionMarker(opaque = true)
      val regex  = bridge.encode(Seq("'Ä'", "'(?i)ä'"), Boolean)(args => s"regexp_like(${args.mkString(", ")})")
      val split  = bridge.encode(Seq("txt", "','"), TextArray)(args => s"split(${args.mkString(", ")})")
      val size   = bridge.encode(Seq(split), Integer)(args => s"size(${args.head})")
      val json = bridge.encode(Seq("txt", "id"), Text)(args =>
        s"to_json(named_struct('Odd Name', ${args.head}, 'nested', named_struct('other', ${args(1)})))"
      )
      val hash = bridge.encode(Seq(json, "256"), Text)(args => s"sha2(${args.mkString(", ")})")
      val quoted = bridge.encode(Seq("txt"), Text)(args =>
        s"to_json(named_struct(concat('O', chr(39), 'Brien', chr(92), 'Name'), ${args.head}))"
      )
      val flattened = bridge.encode(Seq("txt", "id"), Text)(args =>
        s"to_json(named_struct('dup', ${args.head}, 'dup', named_struct('nested', ${args(1)})), " +
          "map('ignoreNullFields', 'false'))"
      )
      val compiled = compiler.compile(
        CompileRequest(
          "marker_probe",
          s"SELECT id, $regex AS re, $size AS n, $hash AS digest, $quoted AS quoted, $flattened AS flattened FROM src",
          Map("src" -> StructType.fromDDL("id INT, txt STRING"))
        )
      )
      compiled.refreshTypeName shouldBe "SIMPLE_PROJECTION"
      val sql = bridge.restore(compiled.sql)
      bridge.requireAllSeen()
      bridge.resetSeen()
      compiled.initialLoadSql should not be empty
      val initial = bridge.restore(compiled.initialLoadSql)
      bridge.requireAllSeen()
      sql should include("regexp_like('Ä', '(?i)ä')")
      sql should include("size(split(")
      sql should include("sha2(to_json(named_struct('Odd Name'")
      val spark = SparkSession.builder().master("local[1]").appName("SparkExpressionMarkerNativeSpec").getOrCreate()
      try {
        spark.conf.set("spark.sql.legacy.sizeOfNull", "true")
        spark
          .sql("SELECT * FROM VALUES (1, CAST(NULL AS STRING)), (2, 'a,b') AS src(id, txt)")
          .createOrReplaceTempView("src")
        val expected = """SELECT id, regexp_like('Ä', '(?i)ä') AS re, size(split(txt, ',')) AS n,
          |sha2(to_json(named_struct('Odd Name', txt, 'nested', named_struct('other', id))), 256) AS digest,
          |to_json(named_struct(concat('O', chr(39), 'Brien', chr(92), 'Name'), txt)) AS quoted,
          |to_json(named_struct('dup', txt, 'dup', named_struct('nested', id)),
          |map('ignoreNullFields', 'false')) AS flattened
          |FROM src""".stripMargin
        Seq(false, true).foreach { ansi =>
          spark.conf.set("spark.sql.ansi.enabled", ansi.toString)
          val actual  = spark.sql(initial).select("id", "re", "n", "digest", "quoted", "flattened")
          val correct = spark.sql(expected)
          actual.exceptAll(correct).count() shouldBe 0L
          correct.exceptAll(actual).count() shouldBe 0L
        }
      } finally spark.stop()
    } finally {
      compiler.close()
      Files.deleteIfExists(wrapper)
    }
  }

  it should "preserve Spark results for constants, nulls and nested hashes" in {
    val bridge = new SparkExpressionMarker
    val regex  = bridge.encode(Seq("'Ä'", "'(?i)ä'"), Boolean)(args => s"regexp_like(${args.mkString(", ")})")
    val split  = bridge.encode(Seq("txt", "','"), TextArray)(args => s"split(${args.mkString(", ")})")
    val size   = bridge.encode(Seq(split), Integer)(args => s"size(${args.head})")
    val hash = bridge.encode(
      Seq(
        "to_json(named_struct('Odd Name', txt, 'nested', named_struct('Mixed', txt)), map('ignoreNullFields', 'false'))",
        "256"
      ),
      Text
    )(args => s"sha2(${args.mkString(", ")})")
    val restored = bridge.restore(s"SELECT txt, $regex AS re, $size AS n, $hash AS digest FROM marker_src")
    bridge.requireAllSeen()
    val expected =
      """SELECT txt, regexp_like('Ä', '(?i)ä') AS re, size(split(txt, ',')) AS n,
        |sha2(to_json(named_struct('Odd Name', txt, 'nested', named_struct('Mixed', txt)),
        |map('ignoreNullFields', 'false')), 256) AS digest FROM marker_src""".stripMargin
    val spark = SparkSession.builder().master("local[1]").appName("SparkExpressionMarkerSpec").getOrCreate()
    try {
      spark.conf.set("spark.sql.legacy.sizeOfNull", "true")
      spark
        .sql("SELECT * FROM VALUES (CAST(NULL AS STRING)), ('a,b') AS marker_src(txt)")
        .createOrReplaceTempView("marker_src")
      Seq(false, true).foreach { ansi =>
        spark.conf.set("spark.sql.ansi.enabled", ansi.toString)
        val actual  = spark.sql(restored)
        val correct = spark.sql(expected)
        actual.exceptAll(correct).count() shouldBe 0L
        correct.exceptAll(actual).count() shouldBe 0L
        val nullSize = actual.where("txt IS NULL").select("n").head()
        if (ansi) nullSize.isNullAt(0) shouldBe true
        else nullSize.getInt(0) shouldBe -1
      }
      spark.sql("SELECT regexp_like('Ä', '(?i)ä')").head().getBoolean(0) shouldBe false
    } finally spark.stop()
  }
}
