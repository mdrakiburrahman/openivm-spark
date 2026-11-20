package org.openivm.spark.parity

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.openivm.spark.common.{CdfWatermarkCatalog, FeatureGate, MvCatalog, MvProjectionSource}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class CountedDistinctCascadeCdfSpec extends IvmParitySpecBase("counted-distinct-cascade") with CdfMode {
  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.QueryLogEnabledKey -> "true",
      FeatureGate.CountedDistinctStageFoldEnabledKey -> sys.env.contains("COUNTED_DISTINCT_TEST_FOLD").toString
    )

  it("feeds a grouped child from a counted DISTINCT parent's physical CDF") {
    sys.env.get("COUNTED_DISTINCT_REPLAY_PHASE") match {
      case Some(phase @ ("prepare" | "restore")) =>
        val root  = Paths.get(sys.env("COUNTED_DISTINCT_REPLAY_ROOT"))
        val state = root.resolve(s"state-$phase")
        Files.exists(state) shouldBe false
        restartSpark(
          extraSparkConf ++ Map(
            "spark.sql.catalogImplementation"             -> "hive",
            "spark.hadoop.javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=$root/metastore;create=true",
            "spark.sql.warehouse.dir"                     -> root.resolve("warehouse").toString,
            FeatureGate.StatePathKey                      -> state.toString,
            FeatureGate.StateSyncUriKey                   -> root.resolve("archive").toUri.toString,
            FeatureGate.StateSyncAsyncEnabledKey          -> "false"
          )
        )
        val receipt = root.resolve("prepare-receipt")
        val pid     = ProcessHandle.current().pid()
        if (phase == "prepare") {
          val (parentVersion, childVersion) = exercise()
          Files.write(receipt, s"$pid\n$parentVersion\n$childVersion\n".getBytes(UTF_8), StandardOpenOption.CREATE_NEW)
        } else {
          val values = new String(Files.readAllBytes(receipt), UTF_8).trim.split("\n").map(_.toLong)
          pid should not be values(0)
          assertMvCorrect(parent, parentQuery)
          assertMvCorrect(child, expected)
          mvDataVersion(parent) shouldBe values(1)
          mvDataVersion(child) shouldBe values(2)
          CdfWatermarkCatalog.get(spark, child, parent) shouldBe Some(values(1))
          refreshMv(child)
          mvDataVersion(child) shouldBe values(2)
          CdfWatermarkCatalog.get(spark, child, parent) shouldBe Some(values(1))
          assertMvCorrect(child, expected)
        }
      case Some(other) => fail(s"Unknown counted DISTINCT replay phase $other")
      case None        => exercise()
    }
  }

  private val base        = "cdcw_base"
  private val parent      = "default.cdcw_parent"
  private val child       = "default.cdcw_child"
  private val parentQuery = s"SELECT DISTINCT site, pool FROM $base"
  private val expected    = s"SELECT site, COUNT(*) AS n FROM ($parentQuery) p GROUP BY site"

  private def exercise(): (Long, Long) = {
    spark.conf.set("spark.openivm.catalogPreservesColumnCase", "true")
    spark.conf.set("spark.openivm.managedTablesRoot", s"${spark.conf.get("spark.sql.warehouse.dir")}/Tables")
    try {
      sql(s"CREATE TABLE $base(id INT, site STRING, pool INT) USING DELTA")
      sql(s"INSERT INTO $base VALUES (1,'anchor',5), (2,'east',2)")
      val childQuery = s"SELECT site, COUNT(*) AS n FROM $parent GROUP BY site"
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
        (s"INSERT INTO $base VALUES (6,'east',NULL),(7,'east',NULL)", Some(2L), 1L),
        (s"INSERT INTO $base VALUES (8,'east',NULL),(9,'east',NULL)", Some(4L), 0L)
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
      (mvDataVersion(parent), mvDataVersion(child))
    } finally {
      spark.conf.unset("spark.openivm.catalogPreservesColumnCase")
      spark.conf.unset("spark.openivm.managedTablesRoot")
    }
  }
}
