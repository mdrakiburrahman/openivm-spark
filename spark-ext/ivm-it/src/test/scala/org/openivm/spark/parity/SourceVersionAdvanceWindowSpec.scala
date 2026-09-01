package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class SourceVersionAdvanceWindowSpec extends IvmParitySpecBase("source-version-window") with InterceptMode {

  private def latestVersion(table: String): Long =
    spark.sql(s"DESCRIBE HISTORY $table").selectExpr("max(version)").head().getLong(0)

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found")).refreshType
  }

  describe("WindowPartition ADVANCE SOURCE VERSIONS") {
    it("publishes a signed downstream cascade and suppresses it when the net window delta is empty") {
      spark
        .sql(
          """CREATE TABLE svaw_src(id INT, region STRING, amount INT) USING DELTA
            |TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""".stripMargin
        )
        .collect()
      sql("INSERT INTO svaw_src VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 5)")
      val pinnedVersion = latestVersion("svaw_src")
      val windowSql =
        s"""SELECT id, region, amount,
           |ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) AS rn
           |FROM svaw_src VERSION AS OF $pinnedVersion""".stripMargin

      sql(s"CREATE MATERIALIZED VIEW svaw_window AS $windowSql")
      mvRefreshType("svaw_window") shouldBe RefreshTypeCode.WindowPartition
      sql(
        "CREATE MATERIALIZED VIEW svaw_downstream AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM svaw_window GROUP BY region"
      )

      sql("INSERT INTO svaw_src VALUES (4, 'east', 15)")
      val changedVersion = latestVersion("svaw_src")
      spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", pinnedVersion + 1L)
        .option("endingVersion", changedVersion)
        .table("svaw_src")
        .count() shouldBe 1L
      sql(
        s"ALTER MATERIALIZED VIEW svaw_window ADVANCE SOURCE VERSIONS " +
          s"(svaw_src = $changedVersion)"
      )
      val changedExpected =
        s"""SELECT id, region, amount,
           |ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) AS rn
           |FROM svaw_src VERSION AS OF $changedVersion""".stripMargin
      assertMvCorrect("svaw_window", changedExpected)

      val downstreamBeforeChange = mvDataVersion("svaw_downstream")
      refreshMv("svaw_downstream")
      mvDataVersion("svaw_downstream") should be > downstreamBeforeChange
      assertMvCorrect(
        "svaw_downstream",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM svaw_window GROUP BY region"
      )

      val upstreamBeforeNoop   = mvDataVersion("svaw_window")
      val downstreamBeforeNoop = mvDataVersion("svaw_downstream")
      sql("UPDATE svaw_src SET amount = amount + 100 WHERE id = 1")
      sql("UPDATE svaw_src SET amount = amount - 100 WHERE id = 1")
      val netZeroVersion = latestVersion("svaw_src")

      sql(
        s"ALTER MATERIALIZED VIEW svaw_window ADVANCE SOURCE VERSIONS " +
          s"(svaw_src = $netZeroVersion)"
      )
      mvDataVersion("svaw_window") shouldBe upstreamBeforeNoop
      val upstreamId = spark.sessionState.sqlParser.parseTableIdentifier("svaw_window")
      MvCatalog
        .lookup(spark, upstreamId)
        .getOrElse(fail("MV svaw_window not found"))
        .querySql should include(s"VERSION AS OF $netZeroVersion")
      refreshMv("svaw_downstream")
      mvDataVersion("svaw_downstream") shouldBe downstreamBeforeNoop
      assertMvCorrect(
        "svaw_downstream",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM svaw_window GROUP BY region"
      )
    }
  }
}
