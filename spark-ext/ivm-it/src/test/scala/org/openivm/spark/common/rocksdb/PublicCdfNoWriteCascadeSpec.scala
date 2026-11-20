package org.openivm.spark.common.rocksdb

import org.apache.spark.sql.DataFrame
import org.openivm.spark.common.{
  BatchVerdict,
  CdfChangeBatch,
  CdfChangePropagation,
  CdfWatermarkCatalog,
  DeltaCommitClassifier,
  FeatureGate,
  MvCatalog,
  MvMetadata,
  MvProjectionSource,
  RefreshProfileCatalog,
  RefreshTypeCode
}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class PublicCdfNoWriteCascadeSpec extends IvmParitySpecBase("public-cdf-no-write-cascade") with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.ProfileRefreshKey          -> "true",
    FeatureGate.PublicCdfNoWriteEnabledKey -> "true"
  )

  private def metadata(name: String): MvMetadata =
    MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(name)).getOrElse(fail(s"Missing $name"))

  private def key(meta: MvMetadata): String = meta.name.database.fold(meta.name.table)(db => s"$db.${meta.name.table}")

  private def assertBag(actual: DataFrame, expected: DataFrame): Unit = {
    actual.exceptAll(expected).count() shouldBe 0L
    expected.exceptAll(actual).count() shouldBe 0L
  }

  private def outcome(name: String, expected: String, n: Int): Unit = {
    val seen = RefreshProfileCatalog.scanAll(spark).map(_.refreshId).toSet
    refreshMv(name)
    val totals =
      RefreshProfileCatalog.scanAll(spark).filter(row => row.stepName == "total_refresh" && !seen(row.refreshId))
    totals.map(_.detail.split(";").drop(1).mkString(";")).toSeq shouldBe Seq(s"outcome=$expected;pending_deltas=$n")
  }

  it("consumes a signed public zero without writing, then propagates a changed row through the child") {
    val producer     = "pcnw_producer"
    val middle       = "pcnw_middle"
    val child        = "pcnw_child"
    val bodyProducer = "SELECT DISTINCT k FROM pcnw_base"
    val bodyMiddle   = s"SELECT k FROM $producer"
    val bodyChild    = s"SELECT CAST(k AS INT) AS k FROM $middle"
    sql("CREATE TABLE pcnw_base(k INT, payload INT) USING DELTA")
    sql("INSERT INTO pcnw_base VALUES (1, 10), (NULL, 20)")
    sql(s"CREATE MATERIALIZED VIEW $producer AS $bodyProducer")
    sql(s"CREATE MATERIALIZED VIEW $middle AS $bodyMiddle")
    sql(s"CREATE MATERIALIZED VIEW $child AS $bodyChild")
    val middleMeta = metadata(middle)
    val childMeta  = metadata(child)
    middleMeta.refreshType shouldBe RefreshTypeCode.SimpleProjection
    childMeta.refreshType shouldBe RefreshTypeCode.SimpleProjection
    val producerLink                                 = middleMeta.sourceTables.head
    val middleLink                                   = childMeta.sourceTables.head
    val middle0                                      = mvDataVersion(middle)
    val child0                                       = mvDataVersion(child)
    def offsets(meta: MvMetadata): Map[String, Long] = CdfWatermarkCatalog.getAll(spark, key(meta), meta.sourceTables)
    def pending(meta: MvMetadata) = new CdfChangePropagation()
      .collectChanges(spark, key(meta), meta.sourceTables, meta.changeWatermarks)
    offsets(middleMeta) shouldBe empty
    offsets(childMeta) shouldBe empty
    mvDataVersion(producer) shouldBe 0L

    // Two hidden-count updates produce duplicate pre/post images of a public NULL row.
    Seq(21, 22).foreach { payload =>
      sql(s"INSERT INTO pcnw_base VALUES (NULL, $payload)")
      refreshMv(producer)
    }
    mvDataVersion(producer) shouldBe 2L
    assertMvCorrect(producer, bodyProducer)
    pending(middleMeta) shouldBe Seq(CdfChangeBatch(producerLink, 0L, 2L))
    DeltaCommitClassifier.classify(spark, producerLink, 0L) shouldBe BatchVerdict.Mutating
    val opts   = Map("readChangeFeed" -> "true", "startingVersion" -> "1", "endingVersion" -> "2")
    val raw    = spark.read.format("delta").options(opts).load(mvDataLocation(producer))
    val hidden = raw.columns.filter(_.startsWith("openivm_")).toSeq
    hidden should not be empty
    val deleted  = raw.where("_change_type = 'update_preimage'")
    val inserted = raw.where("_change_type = 'update_postimage'")
    val cols     = raw.columns.filterNot(_.startsWith("_commit_")).filterNot(_ == "_change_type").toSeq
    deleted.select(cols.head, cols.tail: _*).exceptAll(inserted.select(cols.head, cols.tail: _*)).isEmpty shouldBe false
    val public         = MvProjectionSource.read(spark, producerLink, opts)
    val expectedImages = sql("SELECT CAST(NULL AS INT) AS k UNION ALL SELECT CAST(NULL AS INT) AS k")
    assertBag(public.where("_change_type = 'update_preimage'").select("k"), expectedImages)
    assertBag(public.where("_change_type = 'update_postimage'").select("k"), expectedImages)

    outcome(middle, "public_cdf_no_write", 1)
    mvDataVersion(middle) shouldBe middle0
    assertMvCorrect(middle, bodyMiddle)
    OpenIvmRocksDBRegistry.closeAll()
    offsets(middleMeta) shouldBe Map(producerLink -> 2L)
    pending(middleMeta) shouldBe empty
    outcome(middle, "no_pending_deltas", 0)
    mvDataVersion(middle) shouldBe middle0
    pending(childMeta) shouldBe empty
    outcome(child, "no_pending_deltas", 0)
    offsets(childMeta) shouldBe empty
    mvDataVersion(child) shouldBe child0
    assertMvCorrect(child, bodyChild)

    sql("INSERT INTO pcnw_base VALUES (2, 30), (NULL, 23)")
    refreshMv(producer)
    mvDataVersion(producer) shouldBe 3L
    DeltaCommitClassifier.classify(spark, producerLink, 2L) shouldBe BatchVerdict.Mutating
    outcome(middle, "incremental_executed", 1)
    val middle1 = mvDataVersion(middle)
    middle1 should be > middle0
    offsets(middleMeta) shouldBe Map(producerLink -> 3L)
    pending(childMeta) shouldBe Seq(CdfChangeBatch(middleLink, middle0, middle1))
    outcome(child, "incremental_executed", 1)
    mvDataVersion(child) should be > child0
    offsets(childMeta) shouldBe Map(middleLink -> middle1)
    assertMvCorrect(middle, bodyProducer)
    assertMvCorrect(child, bodyChild)
    sql(
      "CREATE TABLE pcnw_regular_full USING DELTA AS " +
        s"SELECT CAST(k AS INT) AS k FROM ($bodyProducer) AS source_rows"
    )
    assertMvCorrect(child, "SELECT k FROM pcnw_regular_full")
  }
}
