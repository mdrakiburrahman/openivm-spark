package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

abstract class SimpleProjectionOuterJoinScenarios extends IvmParitySpecBase("simple-projection-outer-join") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def assertSimpleProjection(name: String): Unit = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found")).refreshType shouldBe
      RefreshTypeCode.SimpleProjection
  }

  describe("LEFT JOIN SIMPLE_PROJECTION") {
    it("uses incremental partial recompute for unmatched and matched side transitions") {
      sql("CREATE TABLE IF NOT EXISTS spoj_l_users(id INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS spoj_l_orders(id INT, user_id INT, product STRING) USING DELTA")
      sql("INSERT INTO spoj_l_users VALUES (1, 'Alice'), (2, 'Bob')")
      sql("INSERT INTO spoj_l_orders VALUES (10, 1, 'Book')")

      val viewBody =
        "SELECT u.id AS uid, u.name, o.id AS oid, o.product " +
          "FROM spoj_l_users u LEFT JOIN spoj_l_orders o ON u.id = o.user_id"
      sql(s"CREATE MATERIALIZED VIEW spoj_l_mv AS $viewBody")
      assertSimpleProjection("spoj_l_mv")
      assertMvCorrect("spoj_l_mv", viewBody)

      sql("INSERT INTO spoj_l_users VALUES (3, 'Carol')")
      refreshMv("spoj_l_mv")
      assertMvCorrect("spoj_l_mv", viewBody)
      assertSimpleProjection("spoj_l_mv")

      sql("INSERT INTO spoj_l_orders VALUES (20, 3, 'Pencil')")
      refreshMv("spoj_l_mv")
      assertMvCorrect("spoj_l_mv", viewBody)
      assertSimpleProjection("spoj_l_mv")

      sql("DELETE FROM spoj_l_orders WHERE id = 20")
      refreshMv("spoj_l_mv")
      assertMvCorrect("spoj_l_mv", viewBody)
      assertSimpleProjection("spoj_l_mv")

      sql("DELETE FROM spoj_l_users WHERE id = 3")
      refreshMv("spoj_l_mv")
      assertMvCorrect("spoj_l_mv", viewBody)
      assertSimpleProjection("spoj_l_mv")
    }
  }

  describe("nested LEFT JOIN with a widened nullable lookup key") {
    it("preserves null-padded duplicates through mixed changes") {
      sql("CREATE TABLE spoj_n_events(id INT) USING DELTA")
      sql("CREATE TABLE spoj_n_profiles(id INT, value INT) USING DELTA")
      sql("CREATE TABLE spoj_n_dimensions(value INT, dimension_key INT) USING DELTA")
      sql("INSERT INTO spoj_n_events VALUES (1)")
      sql("INSERT INTO spoj_n_profiles VALUES (1, 10)")
      sql("INSERT INTO spoj_n_dimensions VALUES (10, 2)")

      // The BIGINT cast also arises implicitly for an INT key coalesced with
      // -2147483648. The first lookup's nullable key must survive that cast.
      val viewBody =
        "SELECT e.id, COALESCE(d.dimension_key, 1) AS dimension_key " +
          "FROM spoj_n_events e " +
          "LEFT JOIN spoj_n_profiles p ON e.id = p.id " +
          "LEFT JOIN spoj_n_dimensions d ON COALESCE(CAST(p.value AS BIGINT), -1) = d.value"
      sql(s"CREATE MATERIALIZED VIEW spoj_n_mv AS $viewBody")
      assertSimpleProjection("spoj_n_mv")
      assertMvCorrect("spoj_n_mv", viewBody)

      sql("INSERT INTO spoj_n_events VALUES (2), (2)")
      refreshMv("spoj_n_mv")
      assertMvCorrect("spoj_n_mv", viewBody)
      assertSimpleProjection("spoj_n_mv")

      sql("INSERT INTO spoj_n_profiles VALUES (2, 20)")
      sql("INSERT INTO spoj_n_dimensions VALUES (20, 3)")
      sql("UPDATE spoj_n_profiles SET value = 20 WHERE id = 1")
      refreshMv("spoj_n_mv")
      assertMvCorrect("spoj_n_mv", viewBody)
      assertSimpleProjection("spoj_n_mv")

      sql("DELETE FROM spoj_n_profiles WHERE id = 2")
      sql("DELETE FROM spoj_n_dimensions WHERE value = 20")
      refreshMv("spoj_n_mv")
      assertMvCorrect("spoj_n_mv", viewBody)
      assertSimpleProjection("spoj_n_mv")
    }
  }

  describe("affected-key CTE scope") {
    it("keeps affected-key CTEs in executable delete MERGEs") {
      sql("CREATE TABLE spoj_cte_target(openivm_left_key INT, label STRING) USING DELTA")
      val target = spark.sessionState.sqlParser.parseTableIdentifier("spoj_cte_target")
      Seq(false, true).foreach { inlineSource =>
        sql("DELETE FROM spoj_cte_target")
        sql(
          "INSERT INTO spoj_cte_target VALUES (NULL, 'drop-null'), (1, 'drop-a'), (1, 'drop-b'), (2, 'keep'), (2, 'keep')"
        )
        val source =
          if (inlineSource)
            "(SELECT openivm_left_key FROM openivm_affected WHERE openivm_left_key IS NULL OR openivm_left_key = 1)"
          else "openivm_affected"
        val compiled =
          s"""WITH affected_seed AS (
             |  SELECT CAST(NULL AS INT) AS openivm_left_key UNION ALL SELECT 1 UNION ALL SELECT 1
             |), openivm_affected AS (SELECT DISTINCT openivm_left_key FROM affected_seed)
             |DELETE FROM openivm_data_spoj_cte_target AS tgt
             |WHERE EXISTS (SELECT 1 FROM $source AS aff
             |WHERE aff.openivm_left_key IS NOT DISTINCT FROM tgt.openivm_left_key);
             |""".stripMargin
        val rewritten = org.openivm.spark.common.SparkRefreshRewriter.rewrite(
          compiledSql = compiled,
          mvName = target,
          mvLocation = warehouseDir,
          viewLogicalName = "spoj_cte_target",
          sourceTempViews = Map.empty,
          viewDeltaPath = s"$warehouseDir/unused-view-delta"
        )
        rewritten.statements should have size 1
        rewritten.statements.foreach(statement => sql(statement).collect())
        assertMvCorrect(
          "spoj_cte_target",
          "SELECT CAST(2 AS INT) AS openivm_left_key, 'keep' AS label UNION ALL SELECT 2, 'keep'"
        )
      }
    }
  }

  describe("RIGHT JOIN SIMPLE_PROJECTION") {
    it("uses incremental partial recompute for preserved-right transitions") {
      sql("CREATE TABLE IF NOT EXISTS spoj_r_sales(id INT, product_id INT, qty INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS spoj_r_products(id INT, name STRING) USING DELTA")
      sql("INSERT INTO spoj_r_sales VALUES (10, 1, 5)")
      sql("INSERT INTO spoj_r_products VALUES (1, 'Book'), (2, 'Pencil')")

      val viewBody =
        "SELECT s.id AS sid, s.qty, p.id AS pid, p.name " +
          "FROM spoj_r_sales s RIGHT JOIN spoj_r_products p ON s.product_id = p.id"
      sql(s"CREATE MATERIALIZED VIEW spoj_r_mv AS $viewBody")
      assertSimpleProjection("spoj_r_mv")
      assertMvCorrect("spoj_r_mv", viewBody)

      sql("INSERT INTO spoj_r_products VALUES (3, 'Pen')")
      refreshMv("spoj_r_mv")
      assertMvCorrect("spoj_r_mv", viewBody)
      assertSimpleProjection("spoj_r_mv")

      sql("INSERT INTO spoj_r_sales VALUES (20, 3, 7)")
      refreshMv("spoj_r_mv")
      assertMvCorrect("spoj_r_mv", viewBody)
      assertSimpleProjection("spoj_r_mv")

      sql("DELETE FROM spoj_r_sales WHERE id = 20")
      refreshMv("spoj_r_mv")
      assertMvCorrect("spoj_r_mv", viewBody)
      assertSimpleProjection("spoj_r_mv")

      sql("DELETE FROM spoj_r_products WHERE id = 3")
      refreshMv("spoj_r_mv")
      assertMvCorrect("spoj_r_mv", viewBody)
      assertSimpleProjection("spoj_r_mv")
    }
  }

  describe("FULL OUTER JOIN SIMPLE_PROJECTION") {
    it("uses incremental bidirectional partial recompute for both null-padded sides") {
      sql("CREATE TABLE IF NOT EXISTS spoj_f_left(id INT, lname STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS spoj_f_right(id INT, lid INT, rval STRING) USING DELTA")
      sql("INSERT INTO spoj_f_left VALUES (1, 'Alice'), (2, 'Bob')")
      sql("INSERT INTO spoj_f_right VALUES (10, 1, 'A'), (20, 4, 'Orphan')")

      val viewBody =
        "SELECT l.id AS lid, l.lname, r.id AS rid, r.rval " +
          "FROM spoj_f_left l FULL OUTER JOIN spoj_f_right r ON l.id = r.lid"
      sql(s"CREATE MATERIALIZED VIEW spoj_f_mv AS $viewBody")
      assertSimpleProjection("spoj_f_mv")
      assertMvCorrect("spoj_f_mv", viewBody)

      sql("INSERT INTO spoj_f_left VALUES (3, 'Carol')")
      refreshMv("spoj_f_mv")
      assertMvCorrect("spoj_f_mv", viewBody)
      assertSimpleProjection("spoj_f_mv")

      sql("INSERT INTO spoj_f_right VALUES (30, 3, 'C')")
      refreshMv("spoj_f_mv")
      assertMvCorrect("spoj_f_mv", viewBody)
      assertSimpleProjection("spoj_f_mv")

      sql("DELETE FROM spoj_f_right WHERE id = 30")
      refreshMv("spoj_f_mv")
      assertMvCorrect("spoj_f_mv", viewBody)
      assertSimpleProjection("spoj_f_mv")

      sql("INSERT INTO spoj_f_left VALUES (4, 'Dora')")
      refreshMv("spoj_f_mv")
      assertMvCorrect("spoj_f_mv", viewBody)
      assertSimpleProjection("spoj_f_mv")

      sql("DELETE FROM spoj_f_left WHERE id = 4")
      refreshMv("spoj_f_mv")
      assertMvCorrect("spoj_f_mv", viewBody)
      assertSimpleProjection("spoj_f_mv")
    }
  }
}
