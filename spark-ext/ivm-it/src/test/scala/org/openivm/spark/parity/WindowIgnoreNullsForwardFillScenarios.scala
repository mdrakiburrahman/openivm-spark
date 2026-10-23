package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}

abstract class WindowIgnoreNullsForwardFillScenarios extends IvmParitySpecBase("window-ignore-nulls-forward-fill") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.QueryLogEnabledKey -> "true")

  protected def mvRefreshType(name: String): Int =
    MvCatalog
      .list(spark)
      .find(_.name.table == name)
      .getOrElse(fail(s"MV $name not found in catalog"))
      .refreshType

  describe("last_value(expr, true) forward-fill over nullable rows") {
    it("prunes composite enrichment joins through mixed changes and keeps downstream bags exact") {
      sql(
        "CREATE TABLE winnf_people (customer_id INT, ts INT, first_name STRING, last_name STRING, addr STRING, addr2 STRING) USING DELTA"
      )
      sql(
        "CREATE TABLE winnf_enrichment (first_name STRING, last_name STRING, addr STRING, addr2 STRING, value STRING) USING DELTA"
      )
      sql(
        "INSERT INTO winnf_people VALUES (1,1,'Ann','Bee','Main',NULL), (1,2,'Ann','Bee','Main',NULL), (2,1,'Ann','Other','Main',NULL), (3,1,'Other','Bee','Main',NULL), (4,1,'Old','Key','Side',''), (NULL,1,'Ann','Bee','Main',NULL)"
      )
      sql("INSERT INTO winnf_enrichment VALUES ('Ann','Bee','Main','', 'old'), ('Old','Key','Side',NULL,'prior')")
      val peopleQuery =
        "SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ts) AS ordinal FROM winnf_people"
      sql(s"CREATE MATERIALIZED VIEW winnf_people_mv AS $peopleQuery")
      val query = """WITH joined AS (
                    | SELECT c.*, p.value FROM winnf_people_mv c LEFT JOIN winnf_enrichment p
                    | ON c.first_name = p.first_name AND c.last_name = p.last_name
                    | AND c.addr = p.addr AND COALESCE(c.addr2, '') = COALESCE(p.addr2, '')
                    |) SELECT customer_id, ts, value,
                    |LAST_VALUE(value, true) OVER (PARTITION BY customer_id ORDER BY ts) AS carried
                    |FROM joined""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW winnf_joined AS $query")
      val downstream =
        "SELECT customer_id, COUNT(*) AS n, MAX(carried) AS carried FROM winnf_joined GROUP BY customer_id"
      sql(s"CREATE MATERIALIZED VIEW winnf_joined_child AS $downstream")
      assertMvCorrect("winnf_joined", query)
      sql("UPDATE winnf_enrichment SET value = 'new' WHERE first_name = 'Ann'")
      sql("DELETE FROM winnf_people WHERE customer_id = 4")
      refreshMv("winnf_people_mv")
      assertMvCorrect("winnf_people_mv", peopleQuery)
      refreshMv("winnf_joined")
      refreshMv("winnf_joined_child")
      assertMvCorrect("winnf_joined", query)
      assertMvCorrect("winnf_joined_child", downstream)
      val affectedSql = sql("SHOW OPENIVM QUERY LOG")
        .collect()
        .toSeq
        .filter(row => row.getString(1).split("\\.").last == "winnf_joined")
        .filter(_.getString(6) == "rewritten_stmt")
        .map(_.getString(9))
        .find(_.contains("LEFT SEMI JOIN"))
        .getOrElse(fail("missing exact joined-key pruning"))
      affectedSql should include("coalesce")
      sql("DELETE FROM winnf_enrichment WHERE first_name = 'Ann'")
      sql(
        "INSERT INTO winnf_enrichment VALUES ('Other','Bee','Main',NULL,'added'), ('Other','Bee','Main',NULL,'added')"
      )
      sql("UPDATE winnf_people SET first_name = 'Other', last_name = 'Bee' WHERE customer_id = 1")
      refreshMv("winnf_people_mv")
      assertMvCorrect("winnf_people_mv", peopleQuery)
      refreshMv("winnf_joined")
      refreshMv("winnf_joined_child")
      assertMvCorrect("winnf_joined", query)
      assertMvCorrect("winnf_joined_child", downstream)
    }

    it("stays WindowPartition and bag-equal after initial load and refresh") {
      sql("CREATE TABLE winnf_customer_src (customer_id INT, effective_ts TIMESTAMP, status STRING) USING DELTA")
      sql(
        "INSERT INTO winnf_customer_src VALUES " +
          "(1, TIMESTAMP'2024-01-01 00:00:00', 'bronze'), " +
          "(1, TIMESTAMP'2024-02-01 00:00:00', NULL), " +
          "(1, TIMESTAMP'2024-03-01 00:00:00', 'silver'), " +
          "(2, TIMESTAMP'2024-01-15 00:00:00', NULL), " +
          "(2, TIMESTAMP'2024-02-15 00:00:00', 'starter')"
      )

      val mvName = "winnf_mv_customer"
      val viewSql =
        "SELECT customer_id, effective_ts, status, " +
          "last_value(status, true) OVER (PARTITION BY customer_id ORDER BY effective_ts) AS carried_status, " +
          "coalesce(status, last_value(status, true) OVER (PARTITION BY customer_id ORDER BY effective_ts)) AS filled_status " +
          "FROM winnf_customer_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewSql")

      mvRefreshType(mvName) shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect(mvName, viewSql)

      sql(
        "INSERT INTO winnf_customer_src VALUES " +
          "(1, TIMESTAMP'2024-04-01 00:00:00', NULL), " +
          "(2, TIMESTAMP'2024-03-15 00:00:00', NULL), " +
          "(3, TIMESTAMP'2024-01-20 00:00:00', 'new')"
      )
      refreshMv(mvName)

      mvRefreshType(mvName) shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect(mvName, viewSql)
    }
  }
}
