package org.openivm.spark.compiler

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OpenIvmCompilerPrePassSpec extends AnyFunSpec with Matchers {

  describe("renameSparkFunctionShimCalls") {
    it("rewrites only the Spark spellings DuckDB cannot parse or bind directly") {
      val sql =
        "SELECT to_date(raw), to_timestamp(raw), date_format(ts), to_date(raw, 'yyyyMMdd'), " +
          "to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss'), date_format(ts, 'yyyyMMdd'), " +
          "last_value(raw) OVER (PARTITION BY grp ORDER BY ts), " +
          "last_value(raw, true) OVER (PARTITION BY grp ORDER BY ts) FROM src"

      OpenIvmCompiler.renameSparkFunctionShimCalls(sql) shouldBe
        "SELECT __sparkfn_to_date_1arg(raw), __sparkfn_to_timestamp_1arg(raw), date_format(ts), __sparkfn_to_date(raw, 'yyyyMMdd'), " +
        "__sparkfn_to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss'), __sparkfn_date_format(ts, 'yyyyMMdd'), " +
        "last_value(raw) OVER (PARTITION BY grp ORDER BY ts), " +
        "last_value(raw IGNORE NULLS) OVER (PARTITION BY grp ORDER BY ts) FROM src"
    }

    it("counts only top-level commas inside the matched call") {
      val sql =
        "SELECT to_date(coalesce(a, b)), to_date(coalesce(a, b), 'yyyyMMdd'), " +
          "coalesce(to_timestamp(raw), ts), coalesce(to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss'), ts), " +
          "last_value(coalesce(a, b), true) OVER (PARTITION BY grp ORDER BY ts), " +
          "date_format(coalesce(ts1, ts2), 'yyyyMMdd') FROM src"

      OpenIvmCompiler.renameSparkFunctionShimCalls(sql) shouldBe
        "SELECT __sparkfn_to_date_1arg(coalesce(a, b)), __sparkfn_to_date(coalesce(a, b), 'yyyyMMdd'), " +
        "coalesce(__sparkfn_to_timestamp_1arg(raw), ts), coalesce(__sparkfn_to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss'), ts), " +
        "last_value(coalesce(a, b) IGNORE NULLS) OVER (PARTITION BY grp ORDER BY ts), " +
        "__sparkfn_date_format(coalesce(ts1, ts2), 'yyyyMMdd') FROM src"
    }

    it("ignores commas that appear inside string literals") {
      val sql =
        "SELECT to_date(concat(raw, ',suffix')), to_date(concat(raw, ',suffix'), 'yyyyMMdd'), " +
          "to_timestamp(raw), to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss,SSS'), date_format(ts, 'yyyy,MM,dd') FROM src"

      OpenIvmCompiler.renameSparkFunctionShimCalls(sql) shouldBe
        "SELECT __sparkfn_to_date_1arg(concat(raw, ',suffix')), __sparkfn_to_date(concat(raw, ',suffix'), 'yyyyMMdd'), " +
        "__sparkfn_to_timestamp_1arg(raw), __sparkfn_to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss,SSS'), __sparkfn_date_format(ts, 'yyyy,MM,dd') FROM src"
    }

    it("does not rewrite names inside string literals, quoted identifiers, or comments") {
      val sql =
        """SELECT 'to_date(raw)' AS txt,
          |       "to_timestamp" AS quoted_name,
          |       -- last_value(raw, true) OVER (PARTITION BY grp ORDER BY ts)
          |       to_date(raw) AS parsed,
          |       last_value(raw, true) OVER (PARTITION BY grp ORDER BY ts) AS filled
          |FROM src /* to_timestamp(raw), to_date(raw, 'yyyyMMdd') */""".stripMargin

      OpenIvmCompiler.renameSparkFunctionShimCalls(sql) shouldBe
        """SELECT 'to_date(raw)' AS txt,
          |       "to_timestamp" AS quoted_name,
          |       -- last_value(raw, true) OVER (PARTITION BY grp ORDER BY ts)
          |       __sparkfn_to_date_1arg(raw) AS parsed,
          |       last_value(raw IGNORE NULLS) OVER (PARTITION BY grp ORDER BY ts) AS filled
          |FROM src /* to_timestamp(raw), to_date(raw, 'yyyyMMdd') */""".stripMargin
    }

    it("handles escaped single quotes in unrelated literals while rewriting the real call") {
      val sql =
        "SELECT 'it''s still text: date_format(ts, ''yyyyMMdd'')' AS note, date_format(ts, 'yyyyMMdd') FROM src"

      OpenIvmCompiler.renameSparkFunctionShimCalls(sql) shouldBe
        "SELECT 'it''s still text: date_format(ts, ''yyyyMMdd'')' AS note, __sparkfn_date_format(ts, 'yyyyMMdd') FROM src"
    }

    it("rewrites first_value(expr, true) exactly as last_value(expr, true), mirroring the existing shim") {
      val sql =
        "SELECT first_value(raw) OVER (PARTITION BY grp ORDER BY ts), " +
          "first_value(raw, true) OVER (PARTITION BY grp ORDER BY ts), " +
          "first_value(coalesce(a, b), false) OVER (PARTITION BY grp ORDER BY ts) FROM src"

      OpenIvmCompiler.renameSparkFunctionShimCalls(sql) shouldBe
        "SELECT first_value(raw) OVER (PARTITION BY grp ORDER BY ts), " +
        "first_value(raw IGNORE NULLS) OVER (PARTITION BY grp ORDER BY ts), " +
        "first_value(coalesce(a, b)) OVER (PARTITION BY grp ORDER BY ts) FROM src"
    }

    it("rewrites 0-arg current_date() and current_timestamp() to collision-free shim names") {
      val sql = "SELECT id, current_date() AS today, current_timestamp() AS now_ts FROM src"

      OpenIvmCompiler.renameSparkFunctionShimCalls(sql) shouldBe
        "SELECT id, __sparkfn_current_date() AS today, __sparkfn_current_timestamp() AS now_ts FROM src"
    }

    it("does not rewrite current_date/current_timestamp inside string literals or comments") {
      val sql =
        """SELECT 'current_date()' AS txt,
          |       -- current_timestamp()
          |       current_date() AS today
          |FROM src""".stripMargin

      OpenIvmCompiler.renameSparkFunctionShimCalls(sql) shouldBe
        """SELECT 'current_date()' AS txt,
          |       -- current_timestamp()
          |       __sparkfn_current_date() AS today
          |FROM src""".stripMargin
    }
  }

  describe("translateSparkStringLiteralEscapes") {
    it(
      "decodes Spark's backslash escapes in the normalize_os_name REPLACE fragment to DuckDB's doubled-quote convention"
    ) {
      // Verbatim (modulo Jinja `{{ expr }}` -> `os_name` substitution) from
      // dbt-server's codegen/macros/spark/os_helpers.sql normalize_os_name
      // macro -- the exact fragment named in the task.
      val sparkFragment =
        """TRIM('_' FROM
          |        REPLACE(REPLACE(REPLACE(REPLACE(
          |            LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(os_name,
          |                ' ', '_'), '-', '_'), '.', '_'), '/', '_'), '\\', '_'), '(', '_'), ')', '_'), ',', '_'), ':', '_'), '\'', '_'))
          |        , '____', '_'), '___', '_'), '__', '_'), '__', '_')
          |    )""".stripMargin

      val expectedDuckdbFragment =
        """TRIM('_' FROM
          |        REPLACE(REPLACE(REPLACE(REPLACE(
          |            LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(os_name,
          |                ' ', '_'), '-', '_'), '.', '_'), '/', '_'), '\', '_'), '(', '_'), ')', '_'), ',', '_'), ':', '_'), '''', '_'))
          |        , '____', '_'), '___', '_'), '__', '_'), '__', '_')
          |    )""".stripMargin

      SparkFunctionShimSql.translateSparkStringLiteralEscapes(sparkFragment) shouldBe expectedDuckdbFragment
    }

    it("decodes each documented Spark backslash escape sequence") {
      val sql  = raw"SELECT '\0', '\b', '\n', '\r', '\t', '\Z', '\%', '\_', '\q' FROM src"
      val nul  = 0.toChar.toString
      val ctlZ = 26.toChar.toString
      SparkFunctionShimSql.translateSparkStringLiteralEscapes(sql) shouldBe
        s"SELECT '$nul', '\b', '\n', '\r', '\t', '$ctlZ', '\\%', '\\_', 'q' FROM src"
    }

    it("passes through literals with no backslash escapes unchanged") {
      val sql = "SELECT REPLACE(s, ' ', '_'), REPLACE(s, '-', '_') FROM src"
      SparkFunctionShimSql.translateSparkStringLiteralEscapes(sql) shouldBe sql
    }

    it("passes through an already doubled-quote-escaped literal unchanged") {
      val sql = "SELECT 'it''s fine' AS note FROM src"
      SparkFunctionShimSql.translateSparkStringLiteralEscapes(sql) shouldBe sql
    }

    it("does not rewrite backslashes inside double-quoted identifiers or comments") {
      val sql =
        raw"""SELECT "weird\name" AS c1 -- literal backslash \' not a string: '\\'
             |FROM src""".stripMargin
      SparkFunctionShimSql.translateSparkStringLiteralEscapes(sql) shouldBe sql
    }
  }
}
