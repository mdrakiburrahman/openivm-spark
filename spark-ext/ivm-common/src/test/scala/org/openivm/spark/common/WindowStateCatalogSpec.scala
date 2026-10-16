package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDBRegistry, RocksDBCodec}
import org.rocksdb.{Options, RocksDB}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import scala.jdk.CollectionConverters._

/** Adapted from the rocksdb-perf-tuning prototype's catalog tests. */
class WindowStateCatalogSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _
  private val warehouse           = Files.createTempDirectory("window-state-catalog").toFile

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("WindowStateCatalogSpec")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      OpenIvmRocksDBRegistry.closeAll()
      if (spark != null) spark.stop()
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      def remove(file: File): Unit = {
        if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(remove))
        file.delete()
        ()
      }
      remove(warehouse)
    }
  }

  describe("versioned prototype state") {
    it("keeps fused grouped DISTINCT state exact and rejects SQL-settings changes before publication") {
      import org.apache.spark.sql.{DataFrame, Row}
      import org.apache.spark.sql.functions._
      import org.apache.spark.sql.types._
      val group = "group.key"; val value = "value ` key"; val countName = "row.count"; val distinct = "distinct.v"
      def quoted(name: String)  = "`" + name.replace("`", "``") + "`"
      val inputSchema           = new StructType().add(group, StringType).add(value, LongType).add("amount", DoubleType)
      def input(rows: Seq[Row]) = spark.createDataFrame(rows.asJava, inputSchema)
      def ordinary(rows: DataFrame) =
        rows.groupBy(col(quoted(group))).agg(count(lit(1L)).as(countName), sum("amount").as("total"))
      def summary(rows: DataFrame) =
        rows
          .groupBy(col(quoted(group)))
          .agg(count(lit(1L)).as(countName), sum("amount").as("total"), countDistinct(col(quoted(value))).as(distinct))
      def exact(a: DataFrame, b: DataFrame): Unit = {
        a.exceptAll(b).count() shouldBe 0L
        b.exceptAll(a).count() shouldBe 0L
      }
      val initial = input(Seq(Row("a", 1L, 1.5), Row("a", 1L, 1.5), Row(null, 2L, 1.0)))
      val current = input(Seq(Row("a", 1L, 1.5), Row("a", 2L, 2.0), Row(null, 2L, 1.0)))
      val delta = input(Seq(Row("a", 1L, 1.5)))
        .withColumn("openivm_multiplicity", lit(-1L))
        .unionByName(input(Seq(Row("a", 2L, 2.0))).withColumn("openivm_multiplicity", lit(1L)))
      val view = "default.fused_grouped_distinct"
      val db = OpenIvmRocksDBRegistry.getOrOpen(
        spark,
        OpenIvmStatePaths.perMvDbPath(spark, view),
        OpenIvmStatePaths.PerMvColumnFamilies
      )
      val neighbor = RocksDBCodec.utf8("grouped_distinct:v1:g;neighbor")
      db.withBatch(b => db.put(b, "default", neighbor, RocksDBCodec.utf8("keep")))
      val state = new GroupedDistinctState(
        spark,
        view,
        "unit",
        inputSchema,
        summary(initial).schema,
        Vector(group),
        countName,
        Vector(value -> distinct),
        maxCache = 1
      )
      try {
        state.prepareBootstrap(summary(initial), "0").publish(Array[Byte](0))
        state.cachedSize("0") shouldBe 0L
        val prepared = state.prepare(delta, current, "0")(ordinary)
        exact(prepared.summary, summary(current))
        intercept[IllegalStateException] {
          db.withBatch { batch =>
            prepared.stageInto(db, batch, "uncommitted"); throw new IllegalStateException("rollback")
          }
        }
        exact(state.all("0"), summary(initial))
        GroupedDistinctState.checkpoint(spark, view).map(_.toVector) shouldBe Some(Vector[Byte](0))
        prepared.publish("1", Array[Byte](1, 2, 3))
        GroupedDistinctState.checkpoint(spark, view).map(_.toVector) shouldBe Some(Vector[Byte](1, 2, 3))
        exact(state.all("1"), summary(current))
        state.cachedSize("1") should be <= 1L
        db.get("default", neighbor).map(RocksDBCodec.fromUtf8) shouldBe Some("keep")
        val adaptive      = "spark.sql.adaptive.autoBroadcastJoinThreshold"
        val priorAdaptive = spark.sessionState.conf.getAllConfs.get(adaptive)
        spark.conf.set(adaptive, "-1")
        try state.cachedSize("1") should be <= 1L
        finally
          priorAdaptive match {
            case Some(value) => spark.conf.set(adaptive, value)
            case None        => spark.conf.unset(adaptive)
          }
        val setting = "spark.sql.ansi.enabled"
        val prior   = spark.sessionState.conf.getAllConfs.get(setting)
        spark.conf.set(setting, (!spark.conf.get(setting).toBoolean).toString)
        try {
          intercept[IllegalArgumentException] { state.cachedSize("1") }.getMessage should include(
            "SQL settings changed"
          )
          intercept[IllegalArgumentException] {
            state.prepareBootstrap(summary(current), "2")
          }.getMessage should include("SQL settings changed")
        } finally
          prior match {
            case Some(value) => spark.conf.set(setting, value)
            case None        => spark.conf.unset(setting)
          }
        exact(state.all("1"), summary(current))
      } finally state.close()
    }

    it("reuses an already-open MV shard without creating a cache column family") {
      val view = "default.no_extra_family"
      val path = OpenIvmStatePaths.perMvDbPath(spark, view)
      OpenIvmRocksDBRegistry.getOrOpen(spark, path, OpenIvmStatePaths.PerMvColumnFamilies)
      def families: Set[String] = {
        val options = new Options()
        try RocksDB.listColumnFamilies(options, path).asScala.map(RocksDBCodec.fromUtf8).toSet
        finally options.close()
      }
      val before = families
      before should not contain "window_state"
      WindowStateCatalog.putAll(spark, view, "id", 0L, 1L, Seq("a"), Map("a" -> "seed"))
      WindowStateCatalog.getMany(spark, view, "id", 1L, Seq("a")) shouldBe Map("a" -> "seed")
      families shouldBe before
    }

    it("preserves other default-family keys when clearing a stale cache generation") {
      val view = "default.namespace"
      val db = OpenIvmRocksDBRegistry.getOrOpen(
        spark,
        OpenIvmStatePaths.perMvDbPath(spark, view),
        OpenIvmStatePaths.PerMvColumnFamilies
      )
      val cf        = RocksDBCodec.fromUtf8(RocksDB.DEFAULT_COLUMN_FAMILY)
      val unrelated = Seq("stamp", "row:a", "window_state:other", "window_state:row", "window_state:row;after")
      db.withBatch { batch =>
        unrelated.foreach(key => db.put(batch, cf, RocksDBCodec.utf8(key), RocksDBCodec.utf8("keep")))
      }
      WindowStateCatalog.putAll(spark, view, "id", 0L, 1L, Seq("a", "b"), Map("a" -> "old", "b" -> "old"))
      WindowStateCatalog.putAll(spark, view, "id", 4L, 5L, Seq("a"), Map("a" -> "new"))
      WindowStateCatalog.getMany(spark, view, "id", 5L, Seq("a", "b")) shouldBe Map("a" -> "new")
      unrelated.foreach(key => db.get(cf, RocksDBCodec.utf8(key)).map(RocksDBCodec.fromUtf8) shouldBe Some("keep"))
    }

    it("treats a legacy cache family as a miss and rebuilds in the existing default family") {
      val view = "default.legacy"
      val db = OpenIvmRocksDBRegistry.getOrOpen(
        spark,
        OpenIvmStatePaths.perMvDbPath(spark, view),
        OpenIvmStatePaths.PerMvColumnFamilies :+ "window_state"
      )
      db.withBatch { batch =>
        db.put(batch, "window_state", RocksDBCodec.utf8("stamp"), RocksDBCodec.utf8("1\nid"))
        db.put(batch, "window_state", RocksDBCodec.utf8("row:a"), RocksDBCodec.utf8("legacy"))
      }
      OpenIvmRocksDBRegistry.closeAll()
      WindowStateCatalog.getMany(spark, view, "id", 1L, Seq("a")) shouldBe empty
      WindowStateCatalog.putAll(spark, view, "id", 1L, 2L, Seq("a"), Map("a" -> "rebuilt"))
      WindowStateCatalog.getMany(spark, view, "id", 2L, Seq("a")) shouldBe Map("a" -> "rebuilt")
    }

    it("reads only requested keys at the matching Delta version and survives reopen") {
      WindowStateCatalog.putAll(
        spark,
        "default.mv",
        "table/program",
        0L,
        1L,
        Seq("p1", "p2"),
        Map("p1" -> "{\"v\":42}", "p2" -> "{\"v\":null}")
      )
      OpenIvmRocksDBRegistry.closeAll()
      WindowStateCatalog.getMany(spark, "default.mv", "table/program", 1L, Seq("p1", "missing")) shouldBe
        Map("p1" -> "{\"v\":42}")
      WindowStateCatalog.getMany(spark, "default.mv", "table/program", 0L, Seq("p1")) shouldBe empty
      WindowStateCatalog.getMany(spark, "default.mv", "recreated-table/program", 1L, Seq("p1")) shouldBe empty
      WindowStateCatalog.getMany(spark, "default.mv_extra", "table/program", 1L, Seq("p1")) shouldBe empty
    }

    it("advances unaffected keys and removes recomputed partitions in the same batch") {
      val view = "default.partial"
      WindowStateCatalog.putAll(
        spark,
        view,
        "id",
        0L,
        1L,
        Seq("a", "b", "c"),
        Map("a" -> "old", "b" -> "old", "c" -> "keep")
      )
      WindowStateCatalog.putAll(spark, view, "id", 1L, 2L, Seq("a", "b"), Map("a" -> "new"))
      WindowStateCatalog.getMany(spark, view, "id", 2L, Seq("a", "b", "c")) shouldBe Map("a" -> "new", "c" -> "keep")
      WindowStateCatalog.getMany(spark, view, "id", 1L, Seq("a")) shouldBe empty
    }

    it("discards every old key after a version gap or changed table/program identity") {
      val view = "default.stale"
      WindowStateCatalog.putAll(spark, view, "id", 0L, 1L, Seq("a", "b"), Map("a" -> "old", "b" -> "old"))
      WindowStateCatalog.putAll(spark, view, "id", 4L, 5L, Seq("a"), Map("a" -> "new"))
      WindowStateCatalog.getMany(spark, view, "id", 5L, Seq("a", "b")) shouldBe Map("a" -> "new")
      WindowStateCatalog.putAll(spark, view, "changed", 5L, 6L, Seq("b"), Map("b" -> "rebuilt"))
      WindowStateCatalog.getMany(spark, view, "changed", 6L, Seq("a", "b")) shouldBe Map("b" -> "rebuilt")
    }

    it("removes cached state with its owning materialized view") {
      val view = "default.dropped"
      WindowStateCatalog.putAll(spark, view, "id", 0L, 1L, Seq("a"), Map("a" -> "old"))
      MvCatalog.remove(spark, spark.sessionState.sqlParser.parseTableIdentifier(view))
      WindowStateCatalog.getMany(spark, view, "id", 1L, Seq("a")) shouldBe empty
    }
  }
}
