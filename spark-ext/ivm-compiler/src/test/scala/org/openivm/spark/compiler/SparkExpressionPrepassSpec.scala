package org.openivm.spark.compiler

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkExpressionPrepassSpec extends AnyFlatSpec with Matchers {
  "Spark expression prepass" should "reject nondeterministic and unsupported marker expressions" in {
    intercept[IllegalArgumentException] {
      SparkExpressionPrepass.prepare("SELECT sha2(rand(), 256) FROM src", caseSensitive = false)
    }
    intercept[IllegalArgumentException] {
      SparkExpressionPrepass.prepare("SELECT sha2(custom_fn(s), 256) FROM src", caseSensitive = false)
    }
    intercept[IllegalArgumentException] {
      SparkExpressionPrepass.prepare("SELECT (s) RLIKE 'p' FROM src", caseSensitive = false)
    }
  }

  it should "preserve monitoring expression shapes through native initial-load compilation" in {
    assume(sys.env.get("OPENIVM_MARKER_TEST").contains("1"))
    val cli       = sys.env("OPENIVM_CLI_PATH")
    val extension = sys.env("OPENIVM_EXTENSION_PATH")
    val wrapper   = Files.createTempFile("openivm-prepass-cli-", ".sh")
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
    val compiler = OpenIvmCompiler.build(cliPath = wrapper.toString)
    val spark    = SparkSession.builder().master("local[1]").appName("SparkExpressionPrepassSpec").getOrCreate()
    try {
      spark.conf.set("spark.sql.legacy.sizeOfNull", "true")
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      spark
        .sql("""SELECT * FROM VALUES
        |(CAST(NULL AS STRING), 'a', TIMESTAMP '2024-01-01 01:02:03', 1L, false),
        |('Ä', 'b', TIMESTAMP '2024-02-01 01:02:03', 2L, true),
        |('A/B', 'c', CAST(NULL AS TIMESTAMP), CAST(NULL AS BIGINT), CAST(NULL AS BOOLEAN))
        |AS src(s, id, t, n, deleted)""".stripMargin)
        .createOrReplaceTempView("src")
      val original = """SELECT id, CAST(s AS STRING) RLIKE '(?i)ä' AS re,
        |CASE WHEN LOWER(TRIM(s)) RLIKE '^[a-z0-9]+$' THEN 'Good' ELSE 'Bad' END AS status,
        |NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(s AS STRING))), '/+', '/'), '/$', ''), '') AS canonical,
        |REGEXP_EXTRACT(TRIM(CAST(s AS STRING)), '[A-Z](/.*)', 1) AS capture,
        |ELEMENT_AT(SPLIT(TRIM(s), '/'), -1) AS leaf, SIZE(SPLIT(s, '/')) AS n_parts,
        |SHA2(TO_JSON(STRUCT(id,t,deleted,n), MAP('ignoreNullFields','false','timeZone','UTC')),256) AS h,
        |SHA2(TO_JSON(NAMED_STRUCT('Odd Name',s,'nested',NAMED_STRUCT('Mixed',id)),
        |MAP('ignoreNullFields','false')),256) AS nested_hash
        |FROM src""".stripMargin
      val prepared = SparkExpressionPrepass.prepare(original, caseSensitive = false)
      val compiled = compiler.compile(
        CompileRequest(
          "prepass_probe",
          prepared.sql,
          Map("src" -> StructType.fromDDL("s STRING, id STRING, t TIMESTAMP, n BIGINT, deleted BOOLEAN"))
        )
      )
      compiled.refreshTypeName shouldBe "SIMPLE_PROJECTION"
      val initial = prepared.markers.restore(compiled.initialLoadSql)
      prepared.markers.requireAllSeen()
      prepared.markers.resetSeen()
      prepared.markers.restore(compiled.sql)
      prepared.markers.requireAllSeen()
      Seq(false, true).foreach { ansi =>
        spark.conf.set("spark.sql.ansi.enabled", ansi.toString)
        val actual =
          spark.sql(initial).select("id", "re", "status", "canonical", "capture", "leaf", "n_parts", "h", "nested_hash")
        val expected = spark.sql(original)
        actual.exceptAll(expected).count() shouldBe 0L
        expected.exceptAll(actual).count() shouldBe 0L
      }
      Seq(
        """WITH filtered AS (SELECT id, s FROM src WHERE s IS NOT NULL)
          |SELECT f.id, LOWER(f.s) AS lowered FROM filtered f""".stripMargin,
        """SELECT id, TO_JSON(STRUCT(id AS `Camel Name`, LOWER(s) AS value),
          |MAP('ignoreNullFields','false')) AS body FROM src""".stripMargin
      ).foreach { query =>
        val probe = SparkExpressionPrepass.prepare(query, caseSensitive = false)
        val result = compiler.compile(
          CompileRequest(
            "prepass_scope",
            probe.sql,
            Map("src" -> StructType.fromDDL("s STRING, id STRING, t TIMESTAMP, n BIGINT, deleted BOOLEAN"))
          )
        )
        val restored = probe.markers.restore(result.initialLoadSql)
        probe.markers.requireAllSeen()
        val actual   = spark.sql(restored)
        val expected = spark.sql(query)
        actual.exceptAll(expected).count() shouldBe 0L
        expected.exceptAll(actual).count() shouldBe 0L
      }
      spark.sql("SELECT id, named_struct('MiXeD Field', s) AS profile FROM src").createOrReplaceTempView("nested_src")
      val nestedQuery = "SELECT n.id, LOWER(n.profile.`MiXeD Field`) AS lowered FROM nested_src n"
      val nestedProbe = SparkExpressionPrepass.prepare(nestedQuery, caseSensitive = false)
      val nestedResult = compiler.compile(
        CompileRequest(
          "prepass_nested",
          nestedProbe.sql,
          Map("nested_src" -> StructType.fromDDL("id STRING, profile STRUCT<`MiXeD Field`: STRING>"))
        )
      )
      val nestedInitial = nestedProbe.markers.restore(nestedResult.initialLoadSql)
      val nestedActual  = spark.sql(nestedInitial)
      nestedProbe.markers.requireAllSeen()
      nestedProbe.markers.resetSeen()
      nestedProbe.markers.restore(nestedResult.sql)
      nestedProbe.markers.requireAllSeen()
      nestedResult.refreshTypeName shouldBe "SIMPLE_PROJECTION"
      val nestedExpected = spark.sql(nestedQuery)
      nestedActual.exceptAll(nestedExpected).count() shouldBe 0L
      nestedExpected.exceptAll(nestedActual).count() shouldBe 0L
    } finally {
      spark.stop()
      compiler.close()
      Files.deleteIfExists(wrapper)
    }
  }

  it should "reject native changes to case-sensitive nested field access" in {
    assume(sys.env.get("OPENIVM_MARKER_TEST").contains("1"))
    val cli       = sys.env("OPENIVM_CLI_PATH")
    val extension = sys.env("OPENIVM_EXTENSION_PATH")
    val wrapper   = Files.createTempFile("openivm-nested-cli-", ".sh")
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
    val compiler = OpenIvmCompiler.build(cliPath = wrapper.toString)
    val spark    = SparkSession.builder().master("local[1]").appName("SparkNestedFieldSpec").getOrCreate()
    try {
      spark.conf.set("spark.sql.caseSensitive", "true")
      spark.sql("SELECT 'id' AS id, named_struct('MiXeD Field', 'S') AS profile").createOrReplaceTempView("nested_src")
      val query = "SELECT n.id, LOWER(n.profile.`MiXeD Field`) AS lowered FROM nested_src n"
      val probe = SparkExpressionPrepass.prepare(query, caseSensitive = true)
      val result = compiler.compile(
        CompileRequest(
          "prepass_nested_case",
          probe.sql,
          Map("nested_src" -> StructType.fromDDL("id STRING, profile STRUCT<`MiXeD Field`: STRING>"))
        )
      )
      val error = intercept[IllegalStateException] {
        probe.markers.restore(result.initialLoadSql)
      }
      error.getMessage should include("case-sensitive Spark attribute")
    } finally {
      spark.stop()
      compiler.close()
      Files.deleteIfExists(wrapper)
    }
  }
}
