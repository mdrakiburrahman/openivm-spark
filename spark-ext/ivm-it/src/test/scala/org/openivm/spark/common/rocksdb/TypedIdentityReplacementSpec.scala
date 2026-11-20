package org.openivm.spark.common.rocksdb

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.openivm.spark.common.{BatchVerdict, CdfWatermarkCatalog, DeltaCommitClassifier, FeatureGate, MvCatalog, RefreshProfileCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class TypedIdentityReplacementSpec extends IvmParitySpecBase("typed-identity-replacement") with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.ProfileRefreshKey -> "true",
    FeatureGate.PublicCdfNoWriteEnabledKey -> "true",
    "spark.sql.session.timeZone" -> "UTC"
  ) ++ sys.env.get("TYPED_IDENTITY_EVIDENCE").toSeq.flatMap { directory =>
    val events = Files.createDirectories(Paths.get(directory).resolve("spark-events"))
    Seq("spark.eventLog.enabled" -> "true", "spark.eventLog.dir" -> events.toUri.toString)
  }

  private val shapes = Seq((13, 9, 4), (13, 9, 4), (12, 9, 5), (12, 9, 5))
  private def exercise(index: Int, enabled: Boolean, expectNoWrite: Boolean): Unit = {
    val tag = s"tir_${index}_${if (enabled) "on" else "off"}"
    val base = s"${tag}_base"
    val parent = s"${tag}_parent"
    val child = s"${tag}_child"
    val (strings, ints, decimals) = shapes(index)
    val types = Seq.fill(strings)("STRING") ++ Seq.fill(ints)("INT") ++ Seq.fill(decimals)("DECIMAL(18,2)") ++ Seq("TIMESTAMP")
    val columns = types.indices.map(i => s"c$i")
    val definition = columns.zip(types).map { case (name, dataType) => s"$name $dataType" }.mkString(", ")
    val projection = columns.zip(types).map { case (name, dataType) => s"CAST($name AS $dataType) AS $name" }.mkString(", ")
    val seed = (0 until 4).map { row =>
      types.zipWithIndex.map { case (dataType, i) =>
        if (row == 3 || (row == 2 && i % 7 == 0)) "NULL"
        else dataType match {
          case "STRING" => if (row == 2) "'é a '" else "'É a'"
          case "INT" => if (row == 2) "-2147483648" else "2147483647"
          case "DECIMAL(18,2)" => if (row == 2) "-9999999999999999.99" else "0.01"
          case "TIMESTAMP" => if (row == 2) "TIMESTAMP '1969-12-31 23:59:59.999999'" else "TIMESTAMP '2024-06-01 01:02:03.123456'"
        }
      }.mkString("(", ", ", ")")
    }.mkString(", ")
    spark.sparkContext.getConf.getBoolean(FeatureGate.PublicCdfNoWriteEnabledKey, false) shouldBe enabled
    sql(s"CREATE TABLE $base ($definition) USING DELTA")
    sql(s"INSERT INTO $base VALUES $seed")
    sql(s"CREATE MATERIALIZED VIEW $parent AS SELECT ${columns.mkString(", ")} FROM $base")
    val body = s"SELECT $projection FROM $parent"
    sql(s"CREATE MATERIALIZED VIEW $child AS $body")
    val childId = spark.sessionState.sqlParser.parseTableIdentifier(child)
    val meta = MvCatalog.lookup(spark, childId).getOrElse(fail(s"Missing $child"))
    meta.refreshType shouldBe RefreshTypeCode.SimpleProjection
    val link = meta.sourceTables.head
    val p0 = mvDataVersion(parent)
    val c0 = mvDataVersion(child)
    val path = mvDataLocation(parent)
    val parentBefore = spark.read.format("delta").option("versionAsOf", p0).load(path).selectExpr(columns: _*)
    sql(s"INSERT OVERWRITE TABLE $base VALUES $seed")
    refreshMv(parent)
    val p1 = mvDataVersion(parent)
    p1 should be > p0
    assertMvCorrect(parent, s"SELECT * FROM $base")
    val log = DeltaCommitClassifier.classify(spark, link, p0)
    val commits = (p0 to p1).map { v =>
      val filename = f"$v%020d.json"
      s"$filename\n" + new String(Files.readAllBytes(Paths.get(new URI(path)).resolve("_delta_log").resolve(filename)), UTF_8)
    }
    def evidence(label: String, data: String): Unit = sys.env.get("TYPED_IDENTITY_EVIDENCE").foreach { dir =>
      Files.write(Paths.get(dir).resolve(s"$tag.$label"), data.getBytes(UTF_8), StandardOpenOption.CREATE_NEW)
    }
    evidence("delta-log", commits.mkString("\n"))
    evidence("gate", s"spark=${spark.version}\nclassifier=$log\nlink=$link\nstartExclusive=$p0\nendInclusive=$p1\n${meta.properties}\n$body\n")
    log shouldBe BatchVerdict.Replace
    val raw = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", p0 + 1).option("endingVersion", p1).load(path)
    evidence("cdf.jsonl", raw.toJSON.collect().mkString("\n") + "\n")
    for (kind <- Seq("insert", "delete")) {
      val rows = raw.where(s"_change_type = '$kind'").selectExpr(columns: _*)
      rows.exceptAll(parentBefore).count() shouldBe 0L
      parentBefore.exceptAll(rows).count() shouldBe 0L
    }
    val known = RefreshProfileCatalog.scanAll(spark).map(_.refreshId).toSet
    spark.sparkContext.setJobGroup(tag, tag)
    val start = System.nanoTime()
    try refreshMv(child) finally spark.sparkContext.clearJobGroup()
    val ms = (System.nanoTime() - start) / 1000000L
    val profiles = RefreshProfileCatalog.scanAll(spark).filterNot(row => known(row.refreshId)).toSeq
    val c1 = mvDataVersion(child)
    val offsets = CdfWatermarkCatalog.getAll(spark, child, meta.sourceTables)
    evidence("refresh", s"elapsed_ms=$ms\nchild_before=$c0\nchild_after=$c1\noffsets=$offsets\n${profiles.mkString("\n")}\n")
    profiles.filter(_.stepName == "total_refresh").map(_.detail).mkString should include (
      if (expectNoWrite) "outcome=public_cdf_no_write" else "outcome=full_refresh_executed"
    )
    if (expectNoWrite) c1 shouldBe c0 else c1 should be > c0
    offsets shouldBe Map(link -> p1)
    assertMvCorrect(child, body)
  }

  it("reproduces Replace and reads both physical signs through the bounded feed") {
    shapes.indices.foreach(exercise(_, enabled = true, expectNoWrite = true))
    restartSpark(extraSparkConf - FeatureGate.PublicCdfNoWriteEnabledKey)
    shapes.indices.foreach(exercise(_, enabled = false, expectNoWrite = false))
  }
}
