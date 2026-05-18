package org.openivm.spark.compiler

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Unit tests for [[LptsSparkDialect]].
  *
  * Each test focuses on one rewrite rule; combined and idempotency tests
  * at the end verify the full [[LptsSparkDialect.translate]] pipeline.
  */
class LptsSparkDialectSpec extends AnyFunSpec with Matchers {

  // ── 1. generate_series (2-arg) ───────────────────────────────────────────────
  describe("rewriteGenerateSeries") {
    it("rewrites generate_series(1, 10) to sequence(1, 10)") {
      LptsSparkDialect.rewriteGenerateSeries("generate_series(1, 10)") shouldBe "sequence(1, 10)"
    }

    it("rewrites generate_series(1, 10, 2) to sequence(1, 10, 2)") {
      LptsSparkDialect.rewriteGenerateSeries("generate_series(1, 10, 2)") shouldBe "sequence(1, 10, 2)"
    }

    it("is case-insensitive") {
      LptsSparkDialect.rewriteGenerateSeries("GENERATE_SERIES(0, n)") shouldBe "sequence(0, n)"
    }

    it("does not replace a partial match like my_generate_series(") {
      val sql = "SELECT my_generate_series(1, 5) AS x"
      LptsSparkDialect.rewriteGenerateSeries(sql) shouldBe sql
    }

    it("is idempotent") {
      val input = "SELECT generate_series(1, 10) AS pos"
      val once  = LptsSparkDialect.rewriteGenerateSeries(input)
      val twice = LptsSparkDialect.rewriteGenerateSeries(once)
      once shouldBe twice
    }
  }

  // ── 2. now()::timestamp ──────────────────────────────────────────────────────
  describe("rewriteNowTimestamp") {
    it("rewrites now()::timestamp to current_timestamp()") {
      LptsSparkDialect.rewriteNowTimestamp("now()::timestamp") shouldBe "current_timestamp()"
    }

    it("is case-insensitive for NOW and TIMESTAMP") {
      LptsSparkDialect.rewriteNowTimestamp("NOW()::TIMESTAMP") shouldBe "current_timestamp()"
    }

    it("tolerates whitespace between now(), ::, and timestamp") {
      LptsSparkDialect.rewriteNowTimestamp("now() :: timestamp") shouldBe "current_timestamp()"
    }

    it("is idempotent") {
      val once  = LptsSparkDialect.rewriteNowTimestamp("now()::timestamp")
      val twice = LptsSparkDialect.rewriteNowTimestamp(once)
      once shouldBe twice
    }
  }

  // ── 3. Postfix casts ─────────────────────────────────────────────────────────
  describe("rewritePostfixCasts") {
    it("rewrites '2024-01-01'::TIMESTAMP to CAST('2024-01-01' AS TIMESTAMP)") {
      LptsSparkDialect.rewritePostfixCasts("'2024-01-01'::TIMESTAMP") shouldBe
        "CAST('2024-01-01' AS TIMESTAMP)"
    }

    it("rewrites mv.openivm_timestamp::DATE to CAST(mv.openivm_timestamp AS DATE)") {
      LptsSparkDialect.rewritePostfixCasts("mv.openivm_timestamp::DATE") shouldBe
        "CAST(mv.openivm_timestamp AS DATE)"
    }

    it("rewrites 123::BIGINT to CAST(123 AS BIGINT)") {
      LptsSparkDialect.rewritePostfixCasts("123::BIGINT") shouldBe "CAST(123 AS BIGINT)"
    }

    it("rewrites DECIMAL type with precision/scale: col::DECIMAL(10,2)") {
      LptsSparkDialect.rewritePostfixCasts("amount::DECIMAL(10,2)") shouldBe
        "CAST(amount AS DECIMAL(10,2))"
    }

    it("does NOT rewrite ::TYPE that appears inside a single-quoted string literal") {
      val sql = "SELECT 'foo::bar'"
      LptsSparkDialect.rewritePostfixCasts(sql) shouldBe sql
    }

    it("does NOT rewrite ::TIMESTAMP inside a single-quoted string literal") {
      val sql = "SELECT 'foo::TIMESTAMP'"
      LptsSparkDialect.rewritePostfixCasts(sql) shouldBe sql
    }

    it("rewrites ::TIMESTAMP that follows a string literal (outside quotes)") {
      LptsSparkDialect.rewritePostfixCasts("SELECT '2024-01-01'::TIMESTAMP") shouldBe
        "SELECT CAST('2024-01-01' AS TIMESTAMP)"
    }

    it("handles a string literal with escaped '' inside it") {
      // 'it''s ok'::VARCHAR — the '' inside is an escaped single quote; the
      // ::VARCHAR is outside and must be rewritten; VARCHAR is normalised to STRING.
      LptsSparkDialect.rewritePostfixCasts("'it''s ok'::VARCHAR") shouldBe
        "CAST('it''s ok' AS STRING)"
    }

    it("leaves existing CAST(...) unchanged") {
      val sql = "SELECT CAST('2024-01-01' AS TIMESTAMP)"
      LptsSparkDialect.rewritePostfixCasts(sql) shouldBe sql
    }

    it("is idempotent") {
      val input = "SELECT '2024-01-01'::TIMESTAMP, mv.openivm_timestamp::DATE, 123::BIGINT"
      val once  = LptsSparkDialect.rewritePostfixCasts(input)
      val twice = LptsSparkDialect.rewritePostfixCasts(once)
      once shouldBe twice
    }

    it("normalises NULL::VARCHAR to CAST(NULL AS STRING)") {
      LptsSparkDialect.rewritePostfixCasts("NULL::VARCHAR") shouldBe "CAST(NULL AS STRING)"
    }

    it("normalises NULL::CHAR to CAST(NULL AS STRING)") {
      LptsSparkDialect.rewritePostfixCasts("NULL::CHAR") shouldBe "CAST(NULL AS STRING)"
    }

    it("normalises NULL::TEXT to CAST(NULL AS STRING)") {
      LptsSparkDialect.rewritePostfixCasts("NULL::TEXT") shouldBe "CAST(NULL AS STRING)"
    }

    it("preserves CAST(NULL AS INTEGER) untouched (non-string type)") {
      LptsSparkDialect.rewritePostfixCasts("NULL::INTEGER") shouldBe "CAST(NULL AS INTEGER)"
    }
  }

  // ── 3b. Parenthesised postfix casts (func(...)::TYPE) ───────────────────────
  describe("rewriteParenthesisedCasts") {
    it("rewrites COALESCE(a, b)::DOUBLE to CAST(COALESCE(a, b) AS DOUBLE)") {
      LptsSparkDialect.rewriteParenthesisedCasts("COALESCE(a, b)::DOUBLE") shouldBe
        "CAST(COALESCE(a, b) AS DOUBLE)"
    }

    it("rewrites NULLIF(x, 0)::BIGINT to CAST(NULLIF(x, 0) AS BIGINT)") {
      LptsSparkDialect.rewriteParenthesisedCasts("NULLIF(x, 0)::BIGINT") shouldBe
        "CAST(NULLIF(x, 0) AS BIGINT)"
    }

    it("rewrites AVG-style pattern from openivm MERGE: COALESCE(v.sum + d.sum, v.sum, d.sum)::DOUBLE") {
      val input =
        "COALESCE(v.openivm_sum_avg_x + d.openivm_sum_avg_x, v.openivm_sum_avg_x, d.openivm_sum_avg_x)::DOUBLE"
      val expected =
        "CAST(COALESCE(v.openivm_sum_avg_x + d.openivm_sum_avg_x, v.openivm_sum_avg_x, d.openivm_sum_avg_x) AS DOUBLE)"
      LptsSparkDialect.rewriteParenthesisedCasts(input) shouldBe expected
    }

    it("rewrites GREATEST(expr, 0)::DOUBLE to CAST(GREATEST(expr, 0) AS DOUBLE)") {
      LptsSparkDialect.rewriteParenthesisedCasts("GREATEST(x - y, 0)::DOUBLE") shouldBe
        "CAST(GREATEST(x - y, 0) AS DOUBLE)"
    }

    it("handles nested function calls: SQRT(GREATEST(sum_sq - sum * sum, 0))::DOUBLE") {
      val input    = "SQRT(GREATEST(sum_sq - sum * sum, 0))::DOUBLE"
      val expected = "CAST(SQRT(GREATEST(sum_sq - sum * sum, 0)) AS DOUBLE)"
      LptsSparkDialect.rewriteParenthesisedCasts(input) shouldBe expected
    }

    it("normalises VARCHAR to STRING: func(x)::VARCHAR") {
      LptsSparkDialect.rewriteParenthesisedCasts("TRIM(x)::VARCHAR") shouldBe
        "CAST(TRIM(x) AS STRING)"
    }

    it("rewrites multiple func(...)::TYPE occurrences in one pass") {
      val input  = "COALESCE(a, b)::DOUBLE / NULLIF(COALESCE(c, d), 0)::DOUBLE"
      val result = LptsSparkDialect.rewriteParenthesisedCasts(input)
      result should include("CAST(COALESCE(a, b) AS DOUBLE)")
      result should include("CAST(NULLIF(COALESCE(c, d), 0) AS DOUBLE)")
    }

    it("does not modify expressions without ::TYPE after closing paren") {
      val sql = "COALESCE(a, b) + NULLIF(c, 0)"
      LptsSparkDialect.rewriteParenthesisedCasts(sql) shouldBe sql
    }

    it("is idempotent") {
      val input = "COALESCE(v.sum + d.sum, v.sum, d.sum)::DOUBLE"
      val once  = LptsSparkDialect.rewriteParenthesisedCasts(input)
      val twice = LptsSparkDialect.rewriteParenthesisedCasts(once)
      once shouldBe twice
    }
  }

  // In the postfix casts section, also verify the parenthesised-cast path via rewritePostfixCasts:
  describe("rewritePostfixCasts (parenthesised expressions)") {
    it("rewrites COALESCE(a, b)::DOUBLE via rewritePostfixCasts") {
      LptsSparkDialect.rewritePostfixCasts("COALESCE(a, b)::DOUBLE") shouldBe
        "CAST(COALESCE(a, b) AS DOUBLE)"
    }

    it("rewrites a realistic AVG MERGE SET clause from openivm") {
      val input =
        "avg_x = COALESCE(v.openivm_sum_avg_x + d.openivm_sum_avg_x, v.openivm_sum_avg_x, d.openivm_sum_avg_x)::DOUBLE / NULLIF(COALESCE(v.openivm_count_avg_x + d.openivm_count_avg_x, v.openivm_count_avg_x, d.openivm_count_avg_x), 0)"
      val result = LptsSparkDialect.rewritePostfixCasts(input)
      result should include("CAST(COALESCE(")
      result should not include "::DOUBLE"
    }

    it("does not rewrite ::DOUBLE inside a single-quoted string even with paren handling active") {
      val sql = "SELECT 'func()::DOUBLE'"
      LptsSparkDialect.rewritePostfixCasts(sql) shouldBe sql
    }
  }

  // ── 4. Interval literals ─────────────────────────────────────────────────────
  describe("rewriteIntervalLiterals") {
    it("rewrites INTERVAL '1 microsecond' to INTERVAL 1 MICROSECOND") {
      LptsSparkDialect.rewriteIntervalLiterals("INTERVAL '1 microsecond'") shouldBe
        "INTERVAL 1 MICROSECOND"
    }

    it("rewrites INTERVAL '1 hour' to INTERVAL 1 HOUR") {
      LptsSparkDialect.rewriteIntervalLiterals("INTERVAL '1 hour'") shouldBe "INTERVAL 1 HOUR"
    }

    it("rewrites INTERVAL '30 days' to INTERVAL 30 DAYS") {
      LptsSparkDialect.rewriteIntervalLiterals("INTERVAL '30 days'") shouldBe "INTERVAL 30 DAYS"
    }

    it("is case-insensitive on the INTERVAL keyword") {
      LptsSparkDialect.rewriteIntervalLiterals("interval '5 minutes'") shouldBe "INTERVAL 5 MINUTES"
    }

    it("leaves already-rewritten INTERVAL N UNIT unchanged") {
      val sql = "INTERVAL 1 HOUR"
      LptsSparkDialect.rewriteIntervalLiterals(sql) shouldBe sql
    }

    it("is idempotent") {
      val input = "WHERE ts > now() - INTERVAL '1 hour'"
      val once  = LptsSparkDialect.rewriteIntervalLiterals(input)
      val twice = LptsSparkDialect.rewriteIntervalLiterals(once)
      once shouldBe twice
    }
  }

  // ── 4b. count_star() ─────────────────────────────────────────────────────────
  describe("rewriteCountStar") {
    it("rewrites count_star() to COUNT(*)") {
      LptsSparkDialect.rewriteCountStar("count_star()") shouldBe "COUNT(*)"
    }

    it("is case-insensitive") {
      LptsSparkDialect.rewriteCountStar("COUNT_STAR()") shouldBe "COUNT(*)"
    }

    it("tolerates whitespace inside the parentheses") {
      LptsSparkDialect.rewriteCountStar("count_star( )") shouldBe "COUNT(*)"
    }

    it("does not match my_count_star()") {
      val sql = "SELECT my_count_star() FROM t"
      LptsSparkDialect.rewriteCountStar(sql) shouldBe sql
    }

    it("is idempotent") {
      val once  = LptsSparkDialect.rewriteCountStar("count_star()")
      val twice = LptsSparkDialect.rewriteCountStar(once)
      once shouldBe twice
    }
  }

  // ── 4b. struct_extract → dot-notation field access ────────────────────────
  describe("rewriteStructExtract") {
    it("rewrites single-level struct_extract(s, 'k') to s.k") {
      LptsSparkDialect.rewriteStructExtract("struct_extract(s, 'a')") shouldBe "s.a"
    }

    it("does NOT backtick fields with a leading underscore (Spark allows them as identifiers)") {
      LptsSparkDialect.rewriteStructExtract("struct_extract(t4_Customer, '_c_id')") shouldBe
        "t4_Customer._c_id"
    }

    it("backticks fields with special characters") {
      LptsSparkDialect.rewriteStructExtract("struct_extract(s, 'a b')") shouldBe
        "s.`a b`"
    }

    it("rewrites nested struct_extract bottom-up") {
      LptsSparkDialect.rewriteStructExtract(
        "struct_extract(struct_extract(t4_Customer, 'name'), 'c_l_name')"
      ) shouldBe "t4_Customer.name.c_l_name"
    }

    it("rewrites triple-nested struct_extract") {
      LptsSparkDialect.rewriteStructExtract(
        "struct_extract(struct_extract(struct_extract(c, 'contactinfo'), 'c_phone_1'), 'c_local')"
      ) shouldBe "c.contactinfo.c_phone_1.c_local"
    }

    it("rewrites struct_extract embedded inside a CAST expression") {
      LptsSparkDialect.rewriteStructExtract(
        "CAST(struct_extract(s, 'k') AS STRING)"
      ) shouldBe "CAST(s.k AS STRING)"
    }

    it("is case-insensitive on the function name") {
      LptsSparkDialect.rewriteStructExtract("STRUCT_EXTRACT(s, 'k')") shouldBe "s.k"
    }

    it("is idempotent") {
      val once  = LptsSparkDialect.rewriteStructExtract("struct_extract(s, 'k')")
      val twice = LptsSparkDialect.rewriteStructExtract(once)
      once shouldBe twice
    }

    it("does not touch SQL that lacks struct_extract") {
      val sql = "SELECT s.k FROM t"
      LptsSparkDialect.rewriteStructExtract(sql) shouldBe sql
    }
  }

  // ── 4c. TIMESTAMP WITH TIME ZONE → TIMESTAMP ──────────────────────────────
  describe("rewriteTimestampWithTimeZone") {
    it("rewrites WITH TIME ZONE form") {
      LptsSparkDialect.rewriteTimestampWithTimeZone(
        "CAST(x AS TIMESTAMP WITH TIME ZONE)"
      ) shouldBe "CAST(x AS TIMESTAMP)"
    }

    it("rewrites WITHOUT TIME ZONE form") {
      LptsSparkDialect.rewriteTimestampWithTimeZone(
        "CAST(x AS TIMESTAMP WITHOUT TIME ZONE)"
      ) shouldBe "CAST(x AS TIMESTAMP)"
    }

    it("is case-insensitive and tolerates extra whitespace") {
      LptsSparkDialect.rewriteTimestampWithTimeZone(
        "CAST(x AS timestamp   with   time   zone)"
      ) shouldBe "CAST(x AS TIMESTAMP)"
    }

    it("leaves bare TIMESTAMP unchanged") {
      LptsSparkDialect.rewriteTimestampWithTimeZone(
        "CAST(x AS TIMESTAMP)"
      ) shouldBe "CAST(x AS TIMESTAMP)"
    }

    it("is idempotent") {
      val sql  = "CAST(x AS TIMESTAMP WITH TIME ZONE)"
      val once = LptsSparkDialect.rewriteTimestampWithTimeZone(sql)
      LptsSparkDialect.rewriteTimestampWithTimeZone(once) shouldBe once
    }
  }

  // ── 5. translate pipeline ────────────────────────────────────────────────────
  describe("translate") {
    it("passes through SQL with no DuckDB-isms unchanged") {
      val sql =
        """MERGE INTO mv_region AS t
          |USING delta_src AS s ON t.region = s.region
          |WHEN MATCHED THEN UPDATE SET t.total = t.total + s.delta
          |WHEN NOT MATCHED THEN INSERT (region, total) VALUES (s.region, s.delta)""".stripMargin
      LptsSparkDialect.translate(sql) shouldBe sql
    }

    it("rewrites multiple DuckDB-isms in a single SQL (test 10)") {
      val input =
        "INSERT INTO t SELECT generate_series(1,10), 'x'::TIMESTAMP, INTERVAL '1 hour'"
      val expected =
        "INSERT INTO t SELECT sequence(1,10), CAST('x' AS TIMESTAMP), INTERVAL 1 HOUR"
      LptsSparkDialect.translate(input) shouldBe expected
    }

    it("translates now()::timestamp before rewriting generic ::TYPE casts (test 3)") {
      LptsSparkDialect.translate("now()::timestamp") shouldBe "current_timestamp()"
    }

    it("rewrites count_star() to COUNT(*) as part of the pipeline") {
      LptsSparkDialect.translate("SELECT count_star() FROM t") shouldBe
        "SELECT COUNT(*) FROM t"
    }

    it("leaves IS NOT DISTINCT FROM unchanged") {
      val sql = "SELECT a, b FROM t WHERE a IS NOT DISTINCT FROM b"
      LptsSparkDialect.translate(sql) shouldBe sql
    }

    it("leaves existing sequence() unchanged (test 12 — Spark syntax passthrough)") {
      val sql = "SELECT sequence(1, 10) AS pos"
      LptsSparkDialect.translate(sql) shouldBe sql
    }

    it("leaves existing CAST(...) unchanged (test 12 — Spark syntax passthrough)") {
      val sql = "SELECT CAST('2024-01-01' AS TIMESTAMP) AS ts"
      LptsSparkDialect.translate(sql) shouldBe sql
    }

    // ── Idempotency (test 9) ─────────────────────────────────────────────────
    it("is idempotent: translate(translate(s)) == translate(s) for a simple cast") {
      val s = "'2024-01-01'::TIMESTAMP"
      LptsSparkDialect.translate(LptsSparkDialect.translate(s)) shouldBe
        LptsSparkDialect.translate(s)
    }

    it("is idempotent for a combined DuckDB SQL fragment") {
      val s =
        """WITH d AS (SELECT generate_series(1, cnt) AS pos FROM delta)
          |SELECT pos, now()::timestamp AS ts, amount::BIGINT AS amt,
          |       INTERVAL '1 microsecond' AS gap
          |FROM d""".stripMargin
      LptsSparkDialect.translate(LptsSparkDialect.translate(s)) shouldBe
        LptsSparkDialect.translate(s)
    }

    it("is idempotent for a literal-heavy SQL") {
      val s = "SELECT '2024-01-01'::TIMESTAMP, 'foo::bar' AS raw, generate_series(0, 5)"
      LptsSparkDialect.translate(LptsSparkDialect.translate(s)) shouldBe
        LptsSparkDialect.translate(s)
    }

    // ── Real-world refresh SQL shape (test 13) ───────────────────────────────
    // Represents the shape of SQL emitted by openivm for an AGGREGATE_GROUP
    // (RefreshType 0) view.  The raw SQL contains DuckDB-specific tokens that
    // the lpts pipeline did NOT translate (assembled as raw strings in
    // refresh_compiler.cpp):  ::TIMESTAMP casts, generate_series, and
    // INTERVAL literals.  After translate() the result must be valid Spark SQL.
    it("translates a realistic openivm AGGREGATE_GROUP refresh SQL to Spark") {
      val raw =
        """WITH delta_insert AS (
          |  SELECT region, amount, 1 AS openivm_sign,
          |         now()::timestamp AS openivm_ts
          |  FROM memory.main.openivm_delta_insert_mv_region
          |),
          |delta_delete AS (
          |  SELECT region, amount, -1 AS openivm_sign,
          |         now()::timestamp AS openivm_ts
          |  FROM memory.main.openivm_delta_delete_mv_region
          |),
          |delta_all AS (
          |  SELECT * FROM delta_insert
          |  UNION ALL
          |  SELECT * FROM delta_delete
          |),
          |seq_guard AS (
          |  SELECT generate_series(1, 10) AS pos
          |),
          |delta_agg AS (
          |  SELECT region,
          |         SUM(amount * openivm_sign) AS total,
          |         SUM(openivm_sign)          AS cnt
          |  FROM delta_all
          |  GROUP BY region
          |)
          |MERGE INTO memory.main.mv_region AS target
          |USING delta_agg AS source
          |  ON target.region = source.region
          |WHEN MATCHED AND source.cnt + target.cnt = 0 THEN DELETE
          |WHEN MATCHED THEN UPDATE SET
          |  target.total = target.total + source.total,
          |  target.cnt   = target.cnt   + source.cnt
          |WHEN NOT MATCHED THEN INSERT (region, total, cnt)
          |  VALUES (source.region, source.total, source.cnt);
          |DELETE FROM memory.main.openivm_delta_insert_mv_region
          |  WHERE openivm_ts < '2024-01-01'::TIMESTAMP
          |     OR openivm_ts < INTERVAL '1 microsecond'""".stripMargin

      val translated = LptsSparkDialect.translate(raw)

      // DuckDB-specific tokens must be gone
      translated should not include "generate_series"
      translated should not include "::TIMESTAMP"
      translated should not include "::timestamp"
      translated should not include "now()::"
      translated should not include "INTERVAL '"

      // Spark replacements must be present
      translated should include("sequence(1, 10)")
      translated should include("current_timestamp()")
      translated should include("CAST('2024-01-01' AS TIMESTAMP)")
      translated should include("INTERVAL 1 MICROSECOND")

      // MERGE and DELETE syntax must be preserved (Spark + Delta support)
      translated should include("MERGE INTO")
      translated should include("WHEN MATCHED")
      translated should include("WHEN NOT MATCHED")
      translated should include("DELETE FROM")
    }
  }
}
