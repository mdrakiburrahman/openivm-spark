package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, MvMetadata, RefreshProfileCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class NamedStructAggregateScenarios extends IvmParitySpecBase("named-struct-aggregate") {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.openivm.refresh.windowRunningIncremental.enabled" -> "true",
    "spark.openivm.refresh.groupedWindowState.enabled" -> "true",
    "spark.openivm.compile.classificationCache.enabled" -> "true",
    FeatureGate.ProfileRefreshKey -> "true",
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed" -> "true",
    "spark.sql.session.timeZone" -> "UTC",
    "spark.sql.caseSensitive" -> "false",
    "spark.sql.ansi.enabled" -> "false",
    "spark.sql.legacy.sizeOfNull" -> "true"
  )

  it("maintains named struct values with scalar and nullable nested ordering keys") {
    sql("CREATE TABLE nsagg_source (grp STRING, id INT, name STRING, ts TIMESTAMP) USING DELTA")
    sql("""INSERT INTO nsagg_source VALUES
      |('a', 1, 'dated', TIMESTAMP '2024-01-01 00:00:00'),
      |('a', 2, 'null-key', NULL),
      |('b', 3, NULL, NULL)""".stripMargin)
    val value = "named_struct('arbitrary_field', name, 'nested', named_struct('the_id', id))"
    val queries = Seq(
      "nsagg_scalar_mv" -> s"SELECT grp, max_by($value, id) AS picked FROM nsagg_source GROUP BY grp",
      "nsagg_nested_mv" -> s"""SELECT grp, max_by($value,
        |named_struct('rank', 1, 'sort key', named_struct('timestamp', ts))) AS picked
        |FROM nsagg_source GROUP BY grp""".stripMargin,
      "nsagg_key_mv" -> s"""SELECT grp, max_by($value, ordering_key) AS picked,
        |max_by($value, named_struct('rank', 1, 'key', ordering_key)) AS nested_picked FROM
        |(SELECT grp, id, name, ts, named_struct('when', CAST(ts AS TIMESTAMP),
        |'when2', CAST(ts AS TIMESTAMP), 'id', coalesce(id, 0)) AS ordering_key FROM nsagg_source) s GROUP BY grp""".stripMargin,
      "nsagg_bare_mv" -> "SELECT grp, max_by(struct(id, name, ts), struct(ts)) AS picked FROM nsagg_source GROUP BY grp"
    )
    queries.foreach { case (name, query) =>
      sql(s"CREATE MATERIALIZED VIEW $name AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.AggregateGroup
      assertMvCorrect(name, query)
    }
    val cteQuery = s"""WITH base AS (SELECT grp, id, name, ts FROM nsagg_source),
      |keyed AS (SELECT *, named_struct('first', CAST(ts AS TIMESTAMP),
      |  'second', CAST(ts AS TIMESTAMP), 'text', coalesce(name, ''),
      |  'number1', coalesce(CAST(id AS DECIMAL(38,0)), CAST(0 AS DECIMAL(38,0))),
      |  'number2', coalesce(CAST(id AS DECIMAL(38,0)), CAST(0 AS DECIMAL(38,0))),
      |  'last', coalesce(name, '')) AS ordering_key FROM base),
      |p1 AS (SELECT grp, id, name, ordering_key FROM keyed),
      |p2 AS (SELECT grp, id, name, ordering_key, CASE WHEN id > 1 THEN 1 ELSE 0 END AS rank FROM keyed),
      |p3 AS (SELECT grp, id, name, ordering_key FROM keyed WHERE id IS NOT NULL),
      |p4 AS (SELECT grp, id, name, ordering_key FROM keyed WHERE name IS NOT NULL),
      |s1 AS (SELECT grp, max_by($value, ordering_key) AS chosen FROM p1 GROUP BY grp),
      |s2 AS (SELECT grp, max_by($value, named_struct('rank', rank, 'key', ordering_key)) AS chosen FROM p2 GROUP BY grp),
      |s3 AS (SELECT grp, max_by($value, ordering_key) AS chosen FROM p3 GROUP BY grp),
      |s4 AS (SELECT grp, max_by($value, ordering_key) AS chosen FROM p4 GROUP BY grp)
      |SELECT a.grp, a.chosen.arbitrary_field AS v1, b.chosen.arbitrary_field AS v2,
      |  c.chosen.arbitrary_field AS v3, d.chosen.arbitrary_field AS v4
      |FROM s1 a LEFT JOIN s2 b ON a.grp = b.grp LEFT JOIN s3 c ON a.grp = c.grp
      |  LEFT JOIN s4 d ON a.grp = d.grp""".stripMargin
    sql(s"CREATE MATERIALIZED VIEW nsagg_cte_mv AS $cteQuery")
    val cteId   = spark.sessionState.sqlParser.parseTableIdentifier("nsagg_cte_mv")
    val cteMeta = MvCatalog.lookup(spark, cteId).get
    cteMeta.properties(MvMetadata.CompileRefreshTypeKey) should not be "COMPILE_FAILED"
    cteMeta.refreshType should not be RefreshTypeCode.FullRefresh
    assertMvCorrect("nsagg_cte_mv", cteQuery)
    sql("UPDATE nsagg_source SET ts = TIMESTAMP '2024-03-01 00:00:00' WHERE id = 2")
    sql("DELETE FROM nsagg_source WHERE id = 1")
    sql("INSERT INTO nsagg_source VALUES ('a', 4, 'later', TIMESTAMP '2024-04-01 00:00:00'), ('c', 5, NULL, NULL)")
    queries.foreach { case (name, query) =>
      refreshMv(name)
      assertMvCorrect(name, query)
    }
    RefreshProfileCatalog.removeAll(spark)
    refreshMv("nsagg_cte_mv")
    RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
    assertMvCorrect("nsagg_cte_mv", cteQuery)
  }

}

class NamedStructAggregateCdfIntegrationSpec extends NamedStructAggregateScenarios with CdfMode
class NamedStructAggregateInterceptIntegrationSpec extends NamedStructAggregateScenarios with InterceptMode
