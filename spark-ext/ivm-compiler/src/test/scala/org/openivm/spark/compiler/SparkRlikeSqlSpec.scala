package org.openivm.spark.compiler

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SparkRlikeSqlSpec extends AnyFunSpec with Matchers {
  private val fractionOperand =
    "SUBSTRING(REGEXP_EXTRACT(TRIM(CAST(event_time_raw AS STRING)), " +
      "'[T ,][0-9]{2}[:.][0-9]{2}[:.][0-9]{2}[.]([0-9]+)', 1), 8)"
  private val timestampPattern =
    "'^[0-9]{8},[ ]*([0-9]{2}:[0-9]{2}:[0-9]{2}|[0-9]{2}[.][0-9]{2}[.][0-9]{2})" +
      "([.][0-9]{1,7})?(Z|[+-][0-9]{2}:?[0-9]{2})?$'"
  private val monitoringBody =
    s"""SELECT id, CASE WHEN $fractionOperand RLIKE '[1-9]' THEN 1 ELSE 0 END AS fraction,
       |CASE WHEN TRIM(CAST(s.status_message_timestamputc_matched AS STRING))
       | RLIKE $timestampPattern THEN 1 ELSE 0 END AS valid
       |FROM rlike_src s""".stripMargin

  describe("Spark RLIKE normalization") {
    it("only rewrites predicates, preserving literals, identifiers, comments and Unicode offsets") {
      val sql = """SELECT '😀 rlike \'text\'', `rlike`, "rlike", x /* RLIKE */ RLIKE '[1-9]'
                  |FROM src -- rlike
                  |WHERE `has rlike` NOT /* rlike */ RLIKE 'can''t'""".stripMargin
      val expected = """SELECT '😀 rlike \'text\'', `rlike`, "rlike", (regexp_like(x /* RLIKE */ , '[1-9]'))
                       |FROM src -- rlike
                       |WHERE (NOT regexp_like(`has rlike`  /* rlike */ , 'can''t'))""".stripMargin
      SparkRlikeSql.normalize(sql) shouldBe expected
      SparkRlikeSql.normalize(expected) shouldBe expected
    }

    it("preserves nested operands, precedence and the REGEXP synonym") {
      val sql = "SELECT CASE WHEN (CASE WHEN a RLIKE b THEN c ELSE d END) NOT RLIKE " +
        "concat('x', coalesce(p, 'y')) OR e REGEXP 'z' THEN 1 ELSE 0 END FROM src"
      val normalized = SparkRlikeSql.normalize(sql)
      normalized should include("(regexp_like(a , b))")
      normalized should include("NOT regexp_like((CASE WHEN")
      normalized should include("(regexp_like(e , 'z'))")
      noException should be thrownBy CatalystSqlParser.parsePlan(normalized)
    }

    it("handles nested predicates in the pattern and prefix NOT") {
      val sql = "SELECT NOT (x RLIKE (CASE WHEN p RLIKE 'q' THEN 'a' ELSE 'b' END)) FROM src"
      val expected = "SELECT NOT ((regexp_like(x , (CASE WHEN (regexp_like(p , 'q')) " +
        "THEN 'a' ELSE 'b' END)))) FROM src"
      SparkRlikeSql.normalize(sql) shouldBe expected
      noException should be thrownBy CatalystSqlParser.parsePlan(expected)
    }

    it("retains both monitoring expressions in multiline CASE arms") {
      val normalized = SparkRlikeSql.normalize(monitoringBody)
      normalized should include(s"(regexp_like($fractionOperand , '[1-9]'))")
      normalized should include("(regexp_like(TRIM(CAST(s.status_message_timestamputc_matched AS STRING))")
      normalized should include(timestampPattern)
      noException should be thrownBy CatalystSqlParser.parsePlan(normalized)
    }

    it("rejects malformed input rather than rewriting a recovered partial parse") {
      intercept[Exception] {
        SparkRlikeSql.normalize("SELECT CASE WHEN x RLIKE THEN 1 END FROM src")
      }
      intercept[Exception] {
        SparkRlikeSql.normalize("SELECT x RLIKE 'a' /* unclosed")
      }
    }

    it("restores emitted function names without rewriting unrelated SQL text") {
      val sql = """SELECT regexp_matches(a, '[1-9]'), REGEXP_MATCHES(b, p),
                  | 'regexp_matches(', `regexp_matches`, "regexp_matches", xregexp_matches(a),
                  | other.regexp_matches(a) -- regexp_matches(
                  | /* regexp_matches( outer /* inner */ still comment */ FROM src""".stripMargin
      val expected = sql
        .replace("regexp_matches(a, '[1-9]')", "regexp_like(a, '[1-9]')")
        .replace("REGEXP_MATCHES(b, p)", "regexp_like(b, p)")
      SparkRlikeSql.restore(sql) shouldBe expected
      SparkRlikeSql.restore(expected) shouldBe expected
    }

    it("preserves search, null, dynamic-pattern and monitoring CASE semantics in Spark") {
      val directory = new File(s"target/test-rlike-sql-${UUID.randomUUID()}")
      directory.mkdirs()
      val spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("rlike-sql")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", directory.getAbsolutePath)
        .getOrCreate()
      try {
        val input = """WITH src AS (SELECT * FROM VALUES ('abc123xyz'), ('abc'), (CAST(NULL AS STRING)) AS t(v))
                      |SELECT v, v RLIKE '[1-9]' AS positive, v NOT RLIKE '[1-9]' AS negative,
                      |CASE WHEN substring(v, 2) RLIKE concat('[', '1-9]') THEN 'yes' ELSE 'no' END AS result
                      |FROM src""".stripMargin
        val original  = spark.sql(input)
        val rewritten = spark.sql(SparkRlikeSql.normalize(input))
        rewritten.exceptAll(original).count() shouldBe 0L
        original.exceptAll(rewritten).count() shouldBe 0L
        rewritten.collect().find(_.getString(0) == "abc123xyz").get.getBoolean(1) shouldBe true
        spark
          .sql(SparkRlikeSql.restore("SELECT regexp_matches('abc123xyz', '[1-9]')"))
          .head()
          .getBoolean(0) shouldBe true

        val withSource = """WITH rlike_src AS (SELECT * FROM VALUES
                           | (1, '2026-11-13T12:34:56.12345678', '20261113, 12:34:56.123Z'),
                           | (2, '2026-11-13T12:34:56.12345670', 'invalid'),
                           | (3, NULL, NULL)
                           | AS t(id, event_time_raw, status_message_timestamputc_matched))
                           |""".stripMargin + monitoringBody
        val monitoringOriginal   = spark.sql(withSource)
        val monitoringNormalized = spark.sql(SparkRlikeSql.normalize(withSource))
        monitoringNormalized.exceptAll(monitoringOriginal).count() shouldBe 0L
        monitoringOriginal.exceptAll(monitoringNormalized).count() shouldBe 0L
        val hit = monitoringNormalized.collect().find(_.getInt(0) == 1).get
        hit.getInt(1) shouldBe 1
        hit.getInt(2) shouldBe 1
      } finally {
        spark.stop()
        def delete(file: File): Unit = {
          if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(delete))
          Files.deleteIfExists(file.toPath)
        }
        delete(directory)
      }
    }

    it("keeps positive, negated and monitoring queries incremental and emits Spark initial-load SQL") {
      val extension = sys.env.getOrElse("OPENIVM_EXTENSION_PATH", "/opt/openivm/openivm.duckdb_extension")
      val compiler  = OpenIvmCompiler.build(extension)
      try {
        val bodies = Seq(
          "SELECT id, txt FROM rlike_src WHERE txt RLIKE '[1-9]'",
          "SELECT id, txt FROM rlike_src WHERE txt NOT RLIKE '[1-9]'",
          monitoringBody
        )
        bodies.zipWithIndex.foreach { case (body, i) =>
          val result = compiler.compile(
            CompileRequest(
              viewName = s"rlike_compile_mv_$i",
              viewSql = body,
              sources = Map(
                "rlike_src" -> StructType.fromDDL(
                  "id INT, txt STRING, event_time_raw STRING, status_message_timestamputc_matched STRING"
                )
              )
            )
          )
          result.refreshTypeName should not equal "FULL_REFRESH"
          result.initialLoadSql should include("regexp_like")
          noException should be thrownBy CatalystSqlParser.parsePlan(result.initialLoadSql)
          val translated = LptsSparkDialect.translate(result.sql)
          translated should include("regexp_like")
          translated should not include "regexp_matches"
        }
      } finally compiler.close()
    }
  }
}
