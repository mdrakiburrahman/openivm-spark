package org.openivm.spark.parity

import org.openivm.spark.common.{CdfWatermarkCatalog, FeatureGate, MvCatalog, MvProjectionSource}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class CountedDistinctCascadeCdfSpec extends IvmParitySpecBase("counted-distinct-cascade") with CdfMode {
  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.QueryLogEnabledKey -> "true")

  it("feeds a grouped child from a counted DISTINCT parent's physical CDF") {
    val base   = "cdcw_base"
    val parent = "default.cdcw_parent"
    val child  = "default.cdcw_child"
    spark.conf.set("spark.openivm.catalogPreservesColumnCase", "true")
    spark.conf.set("spark.openivm.managedTablesRoot", s"$warehouseDir/Tables")
    try {
      sql(s"CREATE TABLE $base(id INT, site STRING, pool INT) USING DELTA")
      sql(s"INSERT INTO $base VALUES (1,'anchor',5), (2,'east',2)")
      val parentQuery = s"SELECT DISTINCT site, pool FROM $base"
      val childQuery  = s"SELECT site, COUNT(*) AS n FROM $parent GROUP BY site"
      val expected    = s"SELECT site, COUNT(*) AS n FROM ($parentQuery) p GROUP BY site"
      sql(s"CREATE MATERIALIZED VIEW $parent AS $parentQuery")
      sql(s"CREATE MATERIALIZED VIEW $child AS $childQuery")
      MvProjectionSource.isProjection(spark, parent) shouldBe true
      val childId = spark.sessionState.sqlParser.parseTableIdentifier(child)
      val source  = MvCatalog.lookup(spark, childId).get.sourceTables.head
      source shouldBe parent
      val path    = mvDataLocation(parent).replace("`", "``")
      val backing = "SELECT site, pool, openivm_distinct_count FROM delta.`" + path + "`"
      val steps = Seq(
        (s"INSERT INTO $base VALUES (3,'east',NULL),(4,'east',NULL),(5,'east',NULL)", Some(3L), 1L),
        (s"DELETE FROM $base WHERE id IN (3,4)", Some(1L), 0L),
        (s"DELETE FROM $base WHERE id = 5", None, -1L),
        (s"INSERT INTO $base VALUES (6,'east',NULL),(7,'east',NULL)", Some(2L), 1L)
      )
      for ((mutation, stored, publicNet) <- steps) {
        val before = mvDataVersion(parent)
        sql(mutation)
        refreshMv(parent)
        assertMvCorrect(parent, parentQuery)
        sql(backing).filter("site = 'east' AND pool IS NULL").collect().map(_.getLong(2)).toList shouldBe stored.toList
        val after = mvDataVersion(parent)
        after should be > before
        val cdf = sql(
          s"SELECT _change_type FROM table_changes_by_path('$path', ${before + 1}, $after) " +
            "WHERE site = 'east' AND pool IS NULL"
        ).collect().map(_.getString(0))
        cdf.map {
          case "insert" | "update_postimage" => 1L
          case "delete" | "update_preimage"  => -1L
          case other                         => fail(s"unexpected change type $other")
        }.sum shouldBe publicNet
        refreshMv(child)
        assertMvCorrect(child, expected)
        CdfWatermarkCatalog.get(spark, child, source) shouldBe Some(after)
        val replay = mvDataVersion(child)
        refreshMv(child)
        mvDataVersion(child) shouldBe replay
        CdfWatermarkCatalog.get(spark, child, source) shouldBe Some(after)
      }
    } finally {
      spark.conf.unset("spark.openivm.catalogPreservesColumnCase")
      spark.conf.unset("spark.openivm.managedTablesRoot")
    }
  }
}
