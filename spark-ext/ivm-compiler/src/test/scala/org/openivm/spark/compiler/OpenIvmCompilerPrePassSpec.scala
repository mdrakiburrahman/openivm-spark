package org.openivm.spark.compiler

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OpenIvmCompilerPrePassSpec extends AnyFunSpec with Matchers {

  describe("renameTwoArgDateFns") {
    it("rewrites only the 2-arg Spark date/time spellings") {
      val sql =
        "SELECT to_date(raw), to_timestamp(raw), date_format(ts), to_date(raw, 'yyyyMMdd'), " +
          "to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss'), date_format(ts, 'yyyyMMdd') FROM src"

      OpenIvmCompiler.renameTwoArgDateFns(sql) shouldBe
        "SELECT to_date(raw), to_timestamp(raw), date_format(ts), __sparkfn_to_date(raw, 'yyyyMMdd'), " +
        "__sparkfn_to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss'), __sparkfn_date_format(ts, 'yyyyMMdd') FROM src"
    }

    it("counts only top-level commas inside the matched call") {
      val sql =
        "SELECT to_date(coalesce(a, b), 'yyyyMMdd'), coalesce(to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss'), ts), " +
          "date_format(coalesce(ts1, ts2), 'yyyyMMdd') FROM src"

      OpenIvmCompiler.renameTwoArgDateFns(sql) shouldBe
        "SELECT __sparkfn_to_date(coalesce(a, b), 'yyyyMMdd'), coalesce(__sparkfn_to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss'), ts), " +
        "__sparkfn_date_format(coalesce(ts1, ts2), 'yyyyMMdd') FROM src"
    }

    it("ignores commas that appear inside string literals") {
      val sql =
        "SELECT to_date(concat(raw, ',suffix'), 'yyyyMMdd'), to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss,SSS'), " +
          "date_format(ts, 'yyyy,MM,dd') FROM src"

      OpenIvmCompiler.renameTwoArgDateFns(sql) shouldBe
        "SELECT __sparkfn_to_date(concat(raw, ',suffix'), 'yyyyMMdd'), __sparkfn_to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss,SSS'), " +
        "__sparkfn_date_format(ts, 'yyyy,MM,dd') FROM src"
    }

    it("does not rewrite names inside string literals, quoted identifiers, or comments") {
      val sql =
        """SELECT 'to_date(raw, ''yyyyMMdd'')' AS txt,
          |       "to_timestamp" AS quoted_name,
          |       -- date_format(ts, 'yyyyMMdd')
          |       to_date(raw, 'yyyyMMdd') AS parsed
          |FROM src /* to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss') */""".stripMargin

      OpenIvmCompiler.renameTwoArgDateFns(sql) shouldBe
        """SELECT 'to_date(raw, ''yyyyMMdd'')' AS txt,
          |       "to_timestamp" AS quoted_name,
          |       -- date_format(ts, 'yyyyMMdd')
          |       __sparkfn_to_date(raw, 'yyyyMMdd') AS parsed
          |FROM src /* to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss') */""".stripMargin
    }

    it("handles escaped single quotes in unrelated literals while rewriting the real call") {
      val sql =
        "SELECT 'it''s still text: date_format(ts, ''yyyyMMdd'')' AS note, date_format(ts, 'yyyyMMdd') FROM src"

      OpenIvmCompiler.renameTwoArgDateFns(sql) shouldBe
        "SELECT 'it''s still text: date_format(ts, ''yyyyMMdd'')' AS note, __sparkfn_date_format(ts, 'yyyyMMdd') FROM src"
    }
  }
}
