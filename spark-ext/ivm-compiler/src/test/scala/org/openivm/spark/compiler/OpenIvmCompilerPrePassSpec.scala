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
        "__sparkfn_last_value(raw, true) OVER (PARTITION BY grp ORDER BY ts) FROM src"
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
        "__sparkfn_last_value(coalesce(a, b), true) OVER (PARTITION BY grp ORDER BY ts), " +
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
          |       __sparkfn_last_value(raw, true) OVER (PARTITION BY grp ORDER BY ts) AS filled
          |FROM src /* to_timestamp(raw), to_date(raw, 'yyyyMMdd') */""".stripMargin
    }

    it("handles escaped single quotes in unrelated literals while rewriting the real call") {
      val sql =
        "SELECT 'it''s still text: date_format(ts, ''yyyyMMdd'')' AS note, date_format(ts, 'yyyyMMdd') FROM src"

      OpenIvmCompiler.renameSparkFunctionShimCalls(sql) shouldBe
        "SELECT 'it''s still text: date_format(ts, ''yyyyMMdd'')' AS note, __sparkfn_date_format(ts, 'yyyyMMdd') FROM src"
    }
  }
}
