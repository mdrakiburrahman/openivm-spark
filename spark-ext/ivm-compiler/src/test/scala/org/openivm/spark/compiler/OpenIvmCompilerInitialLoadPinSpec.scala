package org.openivm.spark.compiler

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType
import org.openivm.spark.common.PinnedSourcePathMissingException
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Unit tests for the compiler's initial-load emit surface
  * ([[OpenIvmCompiler.reattachInitialLoadSourceReads]]), the pure transform
  * [[OpenIvmCompiler.parseInitialLoadSql]] runs on the CTAS body openivm emits.
  *
  * These exercise the ACTUAL emitted initial-load SQL (not just a helper): a
  * snapshot-pinned source read must bind to its VERIFIED physical Delta path
  * (``delta.`<path>` <clause>``) — never the logical/friendly name — closing the
  * alias-rebind TOCTOU, while an unpinned source keeps its friendly qualified
  * name. No DuckDB subprocess is required.
  */
class OpenIvmCompilerInitialLoadPinSpec extends AnyFunSpec with Matchers {

  private def path(short: String): String =
    s"abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/$short"

  private def req(
      viewSql: String,
      viewName: String = "mv_stg",
      qualified: Map[String, String] = Map.empty,
      pinnedPaths: Map[String, String] = Map.empty
  ): CompileRequest =
    CompileRequest(
      viewName = viewName,
      viewSql = viewSql,
      sources = Map.empty[String, StructType],
      sourceQualifiedNames = qualified,
      sourceSnapshotPinnedPaths = pinnedPaths
    )

  private def countOf(needle: String, haystack: String): Int =
    java.util.regex.Pattern.quote(needle).r.findAllMatchIn(haystack).size

  describe("reattachInitialLoadSourceReads") {
    it("path-binds a pinned initial-load read to its verified Delta path (VERSION)") {
      val out = OpenIvmCompiler.reattachInitialLoadSourceReads(
        "SELECT arm_collection_id FROM memory.main.arm_collection_dim",
        req(
          viewSql = "SELECT arm_collection_id FROM `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094",
          qualified = Map("arm_collection_dim" -> "arc_sql_db_bi.arm_collection_dim"),
          pinnedPaths = Map("arm_collection_dim" -> path("arm_collection_dim"))
        )
      )
      out shouldBe s"SELECT arm_collection_id FROM delta.`${path("arm_collection_dim")}` VERSION AS OF 46094"
      out should not include "memory.main"
      out should not include "arc_sql_db_bi"
      noException should be thrownBy CatalystSqlParser.parsePlan(out)
    }

    it("preserves a TIMESTAMP AS OF clause verbatim when path-binding") {
      val ts = "TIMESTAMP AS OF '2026-09-04 05:00:00'"
      val out = OpenIvmCompiler.reattachInitialLoadSourceReads(
        "SELECT id FROM memory.main.accounts",
        req(
          viewSql = s"SELECT id FROM `db`.`accounts` $ts",
          qualified = Map("accounts" -> "db.accounts"),
          pinnedPaths = Map("accounts" -> path("accounts"))
        )
      )
      out shouldBe s"SELECT id FROM delta.`${path("accounts")}` $ts"
      out.toUpperCase should not include "VERSION AS OF"
    }

    it("path-binds every repeated occurrence of a self-joined pinned source") {
      val out = OpenIvmCompiler.reattachInitialLoadSourceReads(
        "SELECT a.id FROM memory.main.accounts a INNER JOIN memory.main.accounts b ON a.id = b.id",
        req(
          viewSql =
            "SELECT a.id FROM `db`.`accounts` VERSION AS OF 5 a INNER JOIN `db`.`accounts` VERSION AS OF 5 b ON a.id = b.id",
          pinnedPaths = Map("accounts" -> path("accounts"))
        )
      )
      countOf(s"delta.`${path("accounts")}` VERSION AS OF 5", out) shouldBe 2
      out should not include "memory.main"
      noException should be thrownBy CatalystSqlParser.parsePlan(out)
    }

    it("escapes backticks in the verified path") {
      val weird = "s3://bucket/weird`name"
      val out = OpenIvmCompiler.reattachInitialLoadSourceReads(
        "SELECT id FROM memory.main.accounts",
        req(
          viewSql = "SELECT id FROM `db`.`accounts` VERSION AS OF 9",
          pinnedPaths = Map("accounts" -> weird)
        )
      )
      out shouldBe "SELECT id FROM delta.`s3://bucket/weird``name` VERSION AS OF 9"
    }

    it("path-binds only pinned reads and keeps the friendly qualified name for unpinned reads") {
      val out = OpenIvmCompiler.reattachInitialLoadSourceReads(
        "SELECT a.id, b.region FROM memory.main.arm_collection_dim a JOIN memory.main.region_dim b ON a.region_id = b.id",
        req(
          viewSql = "SELECT a.id, b.region FROM `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094 a " +
            "JOIN `arc_sql_db_bi`.`region_dim` b ON a.region_id = b.id",
          qualified = Map(
            "arm_collection_dim" -> "arc_sql_db_bi.arm_collection_dim",
            "region_dim"         -> "arc_sql_db_bi.region_dim"
          ),
          pinnedPaths = Map("arm_collection_dim" -> path("arm_collection_dim"))
        )
      )
      out should include(s"delta.`${path("arm_collection_dim")}` VERSION AS OF 46094 a")
      // The unpinned source keeps the user-friendly qualified name overlay.
      out should include("arc_sql_db_bi.region_dim b")
      out should not include "delta.`arc_sql_db_bi"
    }

    it("keeps prefix-colliding pinned sources bound to their own paths") {
      val out = OpenIvmCompiler.reattachInitialLoadSourceReads(
        "SELECT c.id FROM memory.main.customer c JOIN memory.main.customer_address ca ON c.id = ca.id",
        req(
          viewSql =
            "SELECT c.id FROM `db`.`customer` VERSION AS OF 3 c JOIN `db`.`customer_address` VERSION AS OF 9 ca ON c.id = ca.id",
          pinnedPaths = Map("customer" -> path("customer"), "customer_address" -> path("customer_address"))
        )
      )
      out should include(s"delta.`${path("customer")}` VERSION AS OF 3 c")
      out should include(s"delta.`${path("customer_address")}` VERSION AS OF 9 ca")
      out should not include "customer VERSION AS OF 3_address"
    }

    it("hard-fails a pinned source with no verified path instead of a logical-name fallback") {
      val ex = the[PinnedSourcePathMissingException] thrownBy
        OpenIvmCompiler.reattachInitialLoadSourceReads(
          "SELECT arm_collection_id FROM memory.main.arm_collection_dim",
          req(
            viewSql = "SELECT arm_collection_id FROM `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094",
            viewName = "mv_stg_arm",
            qualified = Map("arm_collection_dim" -> "arc_sql_db_bi.arm_collection_dim"),
            pinnedPaths = Map.empty
          )
        )
      ex.getMessage should include("mv_stg_arm")
      ex.getMessage should include("arm_collection_dim")
      ex.getMessage.toUpperCase should not include "FULL_REFRESH"
    }

    it("leaves an unpinned view untouched (no pins, no paths required)") {
      val out = OpenIvmCompiler.reattachInitialLoadSourceReads(
        "SELECT id FROM memory.main.accounts",
        req(
          viewSql = "SELECT id FROM `db`.`accounts`",
          qualified = Map("accounts" -> "db.accounts")
        )
      )
      out shouldBe "SELECT id FROM db.accounts"
      out.toUpperCase should not include "VERSION AS OF"
    }
  }
}
