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
    "aggregate-group"   -> "39008d641677a1c82c306574e0ba80ce2de6647ef8b7f4afd4f0ce7d77f89934",
    "simple-aggregate"  -> "afef8de6843efa9786213fd9ee3545bb4e586301003f62a64e7d80fbda849e41",
    "simple-projection" -> "1a749f0a8c4e9462352bbe9b1bc5f5602d5cdeb899558bf6f9bdbecd2302365f",
    "window-partition"  -> "6eae946d75120d6cd1dbd75925412056db051df57ac12e60ee5f812722acf985",
    "group-recompute"   -> "1f74c90d2f5babf2d108fe37360d5f2f91369f7b7c52a1229e974102649a0305"
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
