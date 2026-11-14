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

  it should "reject missing or malformed markers" in {
    val bridge = new SparkExpressionMarker
    val marker = bridge.encode(Seq("'Ä'", "'(?i)ä'"), Boolean)(args => s"regexp_like(${args.mkString(", ")})")
    intercept[IllegalStateException](bridge.restore(marker.replace("row(", "other(")))
    intercept[IllegalStateException](bridge.requireAllSeen())
  }

  it should "survive native compilation of nested projection expressions" in {
    val cli        = sys.env.getOrElse("OPENIVM_CLI_PATH", "/opt/openivm/duckdb")
    val escapedCli = cli.replace("'", "'\\''")
    val wrapper    = Files.createTempFile("openivm-marker-cli-", ".sh")
    Files.write(
      wrapper,
      s"""#!/bin/sh
         |(echo "SET disabled_optimizers='expression_rewriter,common_subexpressions';"; cat) | '$escapedCli' "$$@"
         |""".stripMargin.getBytes(StandardCharsets.UTF_8)
    )
    require(wrapper.toFile.setExecutable(true), "Could not make native CLI wrapper executable")
    val compiler = OpenIvmCompiler.build(cliPath = wrapper.toString)
    try {
      val bridge = new SparkExpressionMarker
      val regex  = bridge.encode(Seq("'Ä'", "'(?i)ä'"), Boolean)(args => s"regexp_like(${args.mkString(", ")})")
      val split  = bridge.encode(Seq("txt", "','"), TextArray)(args => s"split(${args.mkString(", ")})")
      val size   = bridge.encode(Seq(split), Integer)(args => s"size(${args.head})")
      val json =
        bridge.encode(Seq("struct_pack(\"Odd Name\" := txt, nested := struct_pack(other := id))"), Text)(args =>
          s"to_json(${args.head})"
        )
      val hash   = bridge.encode(Seq(json, "256"), Text)(args => s"sha2(${args.mkString(", ")})")
      val quoted = bridge.encode(Seq("""struct_pack("O'Brien\Name" := txt)"""), Text)(args => s"to_json(${args.head})")
      val compiled = compiler.compile(
        CompileRequest(
          "marker_probe",
          s"SELECT id, $regex AS re, $size AS n, $hash AS digest, $quoted AS quoted FROM src",
          Map("src" -> StructType.fromDDL("id INT, txt STRING"))
        )
      )
      compiled.refreshTypeName shouldBe "SIMPLE_PROJECTION"
      val sql = bridge.restore(compiled.sql)
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
          |to_json(named_struct(concat('O', chr(39), 'Brien', chr(92), 'Name'), txt)) AS quoted
          |FROM src""".stripMargin
        Seq(false, true).foreach { ansi =>
          spark.conf.set("spark.sql.ansi.enabled", ansi.toString)
          val actual  = spark.sql(initial).select("id", "re", "n", "digest", "quoted")
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
