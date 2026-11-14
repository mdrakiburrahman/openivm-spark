package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class NamedStructAggregateScenarios extends IvmParitySpecBase("named-struct-aggregate") {
  self: IvmParityMode =>

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
    sql("UPDATE nsagg_source SET ts = TIMESTAMP '2024-03-01 00:00:00' WHERE id = 2")
    sql("DELETE FROM nsagg_source WHERE id = 1")
    sql("INSERT INTO nsagg_source VALUES ('a', 4, 'later', TIMESTAMP '2024-04-01 00:00:00'), ('c', 5, NULL, NULL)")
    queries.foreach { case (name, query) =>
      refreshMv(name)
      assertMvCorrect(name, query)
    }
  }

}

class NamedStructAggregateCdfIntegrationSpec       extends NamedStructAggregateScenarios with CdfMode
class NamedStructAggregateInterceptIntegrationSpec extends NamedStructAggregateScenarios with InterceptMode
