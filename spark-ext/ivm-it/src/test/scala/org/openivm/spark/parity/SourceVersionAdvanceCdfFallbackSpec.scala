package org.openivm.spark.parity

import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class SourceVersionAdvanceCdfFallbackSpec extends IvmParitySpecBase("source-version-cdf-fallback") with CdfMode {

  private def latestVersion(table: String): Long =
    spark.sql(s"DESCRIBE HISTORY $table").selectExpr("max(version)").head().getLong(0)

  describe("ADVANCE SOURCE VERSIONS bounded-CDF fallback") {
    it("falls back to an exact snapshot bag diff when CDF was enabled after the requested range began") {
      spark.sql("CREATE TABLE svacf_src(id INT, grp STRING, amount INT) USING DELTA").collect()
      spark.sql("INSERT INTO svacf_src VALUES (1, 'a', 10), (2, 'b', 20)").collect()
      val pinnedVersion = latestVersion("svacf_src")

      spark.sql("INSERT INTO svacf_src VALUES (3, 'a', 5)").collect()
      spark
        .sql(
          "ALTER TABLE svacf_src SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        )
        .collect()
      spark.sql("INSERT INTO svacf_src VALUES (4, 'c', 30)").collect()
      val targetVersion = latestVersion("svacf_src")

      an[Exception] should be thrownBy {
        spark.read
          .format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", pinnedVersion + 1L)
          .option("endingVersion", targetVersion)
          .table("svacf_src")
          .count()
      }

      sql(
        s"CREATE MATERIALIZED VIEW svacf_mv AS " +
          s"SELECT grp, SUM(amount) AS total, COUNT(*) AS cnt " +
          s"FROM svacf_src VERSION AS OF $pinnedVersion GROUP BY grp"
      )
      sql(
        s"ALTER MATERIALIZED VIEW svacf_mv ADVANCE SOURCE VERSIONS " +
          s"(svacf_src = $targetVersion)"
      )

      assertMvCorrect(
        "svacf_mv",
        s"SELECT grp, SUM(amount) AS total, COUNT(*) AS cnt " +
          s"FROM svacf_src VERSION AS OF $targetVersion GROUP BY grp"
      )
    }
  }
}
