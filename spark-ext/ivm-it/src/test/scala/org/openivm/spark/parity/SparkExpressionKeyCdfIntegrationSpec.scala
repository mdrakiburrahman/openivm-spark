package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class SparkExpressionKeyCdfIntegrationSpec extends IvmParitySpecBase("expression-keys-cdf") with CdfMode {
  describe("Spark expression bridge in relational keys") {
    def checkIncremental(mv: String): Unit = {
      val id = spark.sessionState.sqlParser.parseTableIdentifier(mv)
      MvCatalog.lookup(spark, id).get.refreshType should not be RefreshTypeCode.FullRefresh
    }

    it("maintains a grouped expression") {
      sql("CREATE TABLE expr_key_group (id INT, txt STRING) USING DELTA")
      sql("INSERT INTO expr_key_group VALUES (1, 'a//b'), (2, 'a/b'), (3, 'c')")
      val query =
        """SELECT nullif(regexp_replace(regexp_replace(lower(trim(cast(txt AS STRING))), '/+', '/'), '/$', ''), '') AS k,
        |count(*) AS n FROM expr_key_group
        |GROUP BY nullif(regexp_replace(regexp_replace(lower(trim(cast(txt AS STRING))), '/+', '/'), '/$', ''), '')""".stripMargin
      val explained =
        sql(s"EXPLAIN CREATE MATERIALIZED VIEW expr_key_group_explain AS $query").collect().head.getString(0)
      explained should include(""""compile_refresh_type_name":"AGGREGATE_GROUP"""")
      val dryProgram =
        sql(s"SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW expr_key_group_show AS $query").collect().head.getString(0)
      dryProgram.toLowerCase should include("regexp_replace")
      dryProgram should not include "__openivm_spark_expression"
      sql(s"CREATE MATERIALIZED VIEW expr_key_group_mv AS $query")
      checkIncremental("expr_key_group_mv")
      assertMvCorrect("expr_key_group_mv", query)
      sql("UPDATE expr_key_group SET txt = 'c//' WHERE id = 1")
      sql("DELETE FROM expr_key_group WHERE id = 2")
      sql("INSERT INTO expr_key_group VALUES (4, 'c/')")
      refreshMv("expr_key_group_mv")
      assertMvCorrect("expr_key_group_mv", query)
    }

    it("maintains expressions in join conditions") {
      sql("CREATE TABLE expr_key_left (id INT, txt STRING) USING DELTA")
      sql("CREATE TABLE expr_key_right (id INT, txt STRING) USING DELTA")
      sql("INSERT INTO expr_key_left VALUES (1, 'a//b'), (2, 'c')")
      sql("INSERT INTO expr_key_right VALUES (10, 'a/b'), (20, 'c')")
      val query = """SELECT l.id AS lid, r.id AS rid FROM expr_key_left l JOIN expr_key_right r
        |ON regexp_replace(l.txt, '/+', '/') = regexp_replace(r.txt, '/+', '/')""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW expr_key_join_mv AS $query")
      checkIncremental("expr_key_join_mv")
      assertMvCorrect("expr_key_join_mv", query)
      sql("UPDATE expr_key_left SET txt = 'c//' WHERE id = 1")
      sql("INSERT INTO expr_key_right VALUES (30, 'c/')")
      refreshMv("expr_key_join_mv")
      assertMvCorrect("expr_key_join_mv", query)
    }

    it("maintains expressions in window partition keys") {
      sql("CREATE TABLE expr_key_window (id INT, grp INT, txt STRING) USING DELTA")
      sql("INSERT INTO expr_key_window VALUES (1, 1, 'a//b'), (2, 1, 'a/b'), (3, 1, 'c')")
      val query = """SELECT id, row_number() OVER (
        |PARTITION BY regexp_replace(txt, '/+', '/') ORDER BY id) AS rn
        |FROM expr_key_window""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW expr_key_window_mv AS $query")
      checkIncremental("expr_key_window_mv")
      assertMvCorrect("expr_key_window_mv", query)
      sql("UPDATE expr_key_window SET txt = 'c//' WHERE id = 1")
      sql("INSERT INTO expr_key_window VALUES (4, 1, 'c/')")
      refreshMv("expr_key_window_mv")
      assertMvCorrect("expr_key_window_mv", query)
    }

    it("maintains expressions in window order keys") {
      sql("CREATE TABLE expr_key_order (id INT, grp INT, txt STRING) USING DELTA")
      sql("INSERT INTO expr_key_order VALUES (1, 1, 'a//b'), (2, 1, 'a/b'), (3, 1, 'c')")
      val query = """SELECT id, row_number() OVER (
        |PARTITION BY grp ORDER BY regexp_replace(txt, '/+', '/'), id) AS rn
        |FROM expr_key_order""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW expr_key_order_mv AS $query")
      checkIncremental("expr_key_order_mv")
      assertMvCorrect("expr_key_order_mv", query)
      sql("UPDATE expr_key_order SET txt = 'c//' WHERE id = 1")
      sql("INSERT INTO expr_key_order VALUES (4, 1, 'c/')")
      refreshMv("expr_key_order_mv")
      assertMvCorrect("expr_key_order_mv", query)
    }
  }
}
