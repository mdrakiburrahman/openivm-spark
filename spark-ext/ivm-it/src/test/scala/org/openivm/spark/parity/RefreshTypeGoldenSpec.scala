package org.openivm.spark.parity

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale

import org.apache.spark.sql.types.StructType
import org.openivm.spark.compiler.{CompileRequest, OpenIvmCompiler}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RefreshTypeGoldenSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val extensionPath: String =
    sys.env.getOrElse("OPENIVM_EXTENSION_PATH", "/opt/openivm/openivm.duckdb_extension")

  private var sharedCompiler: OpenIvmCompiler = _

  private val goldenHashes: Map[String, String] = Map(
    "aggregate-group"   -> "bd91dd2c51fb6979d9378b19adabe4b2b26f5d44081c49aea834fa7119fd38a6",
    "simple-aggregate"  -> "aadb5bbd47d2809c30bfb93990755aaeccd4c2d4be1462f8f506cf9b6766ac77",
    "simple-projection" -> "18d935257c938c1ec37e170ca74007edbb66c3dd4cbeb8674be1e7f342b42baa",
    "window-partition"  -> "3418a93acb2dee0a5aee4ddb7f746a4fa44697d0b6e9c118d4caf4935bd473fd",
    "group-recompute"   -> "84004ce70205de1de41c709acd9a01ea36a588ffbd668d44c755b356041a2520"
  )

  private val cases: Seq[GoldenCase] = Seq(
    GoldenCase(
      caseName = "aggregate-group",
      viewName = "mv_rtg_aggregate_group",
      viewSql = "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM rtg_sales_agg GROUP BY region",
      sources = Map("rtg_sales_agg" -> StructType.fromDDL("region STRING, amount INT")),
      expectedRefreshTypeName = "AGGREGATE_GROUP"
    ),
    GoldenCase(
      caseName = "simple-aggregate",
      viewName = "mv_rtg_simple_aggregate",
      viewSql = "SELECT SUM(amount) AS total FROM rtg_sales_simple_agg",
      sources = Map("rtg_sales_simple_agg" -> StructType.fromDDL("id INT, amount INT")),
      expectedRefreshTypeName = "SIMPLE_AGGREGATE"
    ),
    GoldenCase(
      caseName = "simple-projection",
      viewName = "mv_rtg_simple_projection",
      viewSql = "SELECT id, value FROM rtg_proj_src WHERE value > 0",
      sources = Map("rtg_proj_src" -> StructType.fromDDL("id INT, value INT")),
      expectedRefreshTypeName = "SIMPLE_PROJECTION"
    ),
    GoldenCase(
      caseName = "window-partition",
      viewName = "mv_rtg_window_partition",
      viewSql =
        "SELECT id, region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) AS rn FROM rtg_sales_wp",
      sources = Map("rtg_sales_wp" -> StructType.fromDDL("id INT, region STRING, amount INT")),
      expectedRefreshTypeName = "WINDOW_PARTITION"
    ),
    GoldenCase(
      caseName = "group-recompute",
      viewName = "mv_rtg_group_recompute",
      viewSql = "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM rtg_sales_gr GROUP BY region",
      sources = Map("rtg_sales_gr" -> StructType.fromDDL("id INT, region STRING, amount INT")),
      expectedRefreshTypeName = "GROUP_RECOMPUTE"
    )
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    sharedCompiler = OpenIvmCompiler.build(extensionPath)
  }

  override def afterAll(): Unit =
    try {
      if (sharedCompiler != null) sharedCompiler.close()
    } finally {
      super.afterAll()
    }

  describe("RefreshType golden SQL regression net") {
    cases.foreach { c =>
      it(s"freezes ${c.caseName}") {
        val result = sharedCompiler.compile(
          CompileRequest(
            viewName = c.viewName,
            viewSql = c.viewSql,
            sources = c.sources
          )
        )
        val hash = sha256Hex(normalizeSql(result.sql))

        result.refreshTypeName shouldBe c.expectedRefreshTypeName
        result.refreshTypeName should not be "FULL_REFRESH"

        // Regenerate with: ./spark-ext/dev/dev.sh test -Dopenivm.golden.print=true 'testOnly ...RefreshTypeGoldenSpec'
        if (sys.props.get("openivm.golden.print").contains("true")) {
          println(s"${c.caseName} ${result.refreshTypeName} $hash")
          val evidence = java.nio.file.Files.createDirectories(java.nio.file.Paths.get("target", "golden-sql"))
          java.nio.file.Files.write(evidence.resolve(c.caseName + ".sql"), result.sql.getBytes(StandardCharsets.UTF_8))
        } else {
          hash shouldBe goldenHashes(c.caseName)
        }
      }
    }
  }

  // Matches the datetime literal itself, independent of the surrounding cast
  // syntax, so both `'…'::TIMESTAMP` and `CAST('…' AS TIMESTAMP)` normalize.
  private val TimestampLiteral =
    """'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?'""".r

  private def normalizeSql(sql: String): String =
    TimestampLiteral
      .replaceAllIn(sql, "'<timestamp>'")
      .trim
      .replaceAll("\\s+", " ")
      .toLowerCase(Locale.ROOT)

  private def sha256Hex(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map("%02x".format(_))
      .mkString
}

private final case class GoldenCase(
    caseName: String,
    viewName: String,
    viewSql: String,
    sources: Map[String, StructType],
    expectedRefreshTypeName: String
)
