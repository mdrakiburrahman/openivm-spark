package org.openivm.spark.compiler

import java.io.File
import java.nio.file.Files
import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class LptsSparkLikeDialectSpec extends AnyFunSpec with Matchers {
  describe("generated LIKE dialect") {
    it("normalizes positive negative and case-insensitive pattern operators idempotently") {
      val sql        = "SELECT a~~'x%', b!~~'%y', c~~*'z_', d!~~*'q%'"
      val translated = LptsSparkDialect.translate(sql)
      translated shouldBe "SELECT a LIKE 'x%', b NOT LIKE '%y', c ILIKE 'z_', d NOT ILIKE 'q%'"
      LptsSparkDialect.translate(translated) shouldBe translated
    }

    it("preserves pattern literals quoted identifiers and comments") {
      val sql = """SELECT '~~ !~~*', `a~~b`, "~~", 'can''t ~~', x~~'%Portal%'
                  |-- ~~ !~~
                  |/* !~~* outer /* ~~ inner */ still ~~ */
                  |""".stripMargin
      val translated = LptsSparkDialect.rewriteLikeOperators(sql)
      translated shouldBe sql.replace("x~~'%Portal%'", "x LIKE '%Portal%'")
    }

    it("evaluates generated CASE GROUP BY UNION DISTINCT LIKE SQL with Spark semantics") {
      val directory = new File(s"target/test-like-dialect-${UUID.randomUUID()}")
      directory.mkdirs()
      val spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("like-dialect")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", directory.getAbsolutePath)
        .getOrCreate()
      try {
        val sql = """
          |WITH scans AS (
          | SELECT * FROM VALUES ('PortalClient'), ('portalclient'), ('CLI'), (NULL) AS source(user_agent)
          | UNION SELECT * FROM VALUES ('PortalClient') AS other(user_agent)
          |), distinct_scans AS (SELECT DISTINCT user_agent FROM scans)
          |SELECT CASE WHEN (user_agent ~~ '%Portal%') THEN 'Portal' ELSE 'Other' END AS source,
          | COUNT(*) AS events
          |FROM distinct_scans
          |GROUP BY CASE WHEN (user_agent ~~ '%Portal%') THEN 'Portal' ELSE 'Other' END
          |""".stripMargin
        val translated = LptsSparkDialect.translate(sql)
        val actual     = spark.sql(translated).collect().map(row => row.getString(0) -> row.getLong(1)).toMap
        actual shouldBe Map("Portal" -> 1L, "Other" -> 3L)
        val row = spark
          .sql(
            LptsSparkDialect.translate(
              "SELECT 'portal' ~~* '%Portal%' AS a, 'CLI' !~~ '%Portal%' AS b, " +
                "'PORTAL' !~~* '%Portal%' AS c, CAST(NULL AS STRING) ~~ '%Portal%' AS d"
            )
          )
          .collect()
          .head
        row.getBoolean(0) shouldBe true
        row.getBoolean(1) shouldBe true
        row.getBoolean(2) shouldBe false
        row.isNullAt(3) shouldBe true
      } finally {
        spark.stop()
        def delete(file: File): Unit = {
          if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(delete))
          Files.deleteIfExists(file.toPath)
        }
        delete(directory)
      }
    }
  }
}
