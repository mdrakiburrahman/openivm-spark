package org.openivm.spark.common.rocksdb

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.openivm.spark.common.{
  BatchVerdict,
  CdfChangeAvailability,
  DeltaTableVersion,
  CdfWatermarkCatalog,
  DeltaCommitClassifier,
  FeatureGate,
  MvCatalog,
  RefreshProfileCatalog,
  RefreshTypeCode
}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class TypedIdentityReplacementSpec extends IvmParitySpecBase("typed-identity-replacement") with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.ProfileRefreshKey          -> "true",
    FeatureGate.PublicCdfNoWriteEnabledKey -> "true",
    "spark.sql.session.timeZone"           -> "UTC"
  ) ++ sys.env.get("TYPED_IDENTITY_EVIDENCE").toSeq.flatMap { directory =>
    val events = Files.createDirectories(Paths.get(directory).resolve("spark-events"))
    Seq("spark.eventLog.enabled" -> "true", "spark.eventLog.dir" -> events.toUri.toString)
  }

  private val restorePhase = sys.env.get("TYPED_IDENTITY_RESTORE_PHASE")
  private val restoreRoot  = sys.env.get("TYPED_IDENTITY_RESTORE_ROOT").map(Paths.get(_))
  private def durableConf: Map[String, String] = {
    val root  = restoreRoot.getOrElse(fail("Missing durable fixture directory"))
    val phase = restorePhase.getOrElse(fail("Missing durable fixture phase"))
    val state = root.resolve(s"state-$phase")
    Files.exists(state) shouldBe false
    extraSparkConf ++ Map(
      "spark.sql.catalogImplementation"             -> "hive",
      "spark.hadoop.javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=$root/metastore;create=true",
      "spark.sql.warehouse.dir"                     -> root.resolve("warehouse").toString,
      FeatureGate.StatePathKey                      -> state.toString,
      FeatureGate.StateSyncUriKey                   -> root.resolve("archive").toUri.toString,
      FeatureGate.StateSyncAsyncEnabledKey          -> "false"
    )
  }

  private def totalSince(known: Set[String]): String =
    RefreshProfileCatalog
      .scanAll(spark)
      .filter(row => row.stepName == "total_refresh" && !known(row.refreshId))
      .map(_.detail)
      .mkString

  private val shapes = Seq((9, 5, 3, 16), (9, 5, 3, 16), (17, 13, 7, 38), (17, 13, 7, 38))
  private val reproduceOnly = sys.env.contains("TYPED_PLAIN_REPRODUCE")
  private def exercise(index: Int, enabled: Boolean, expectNoWrite: Boolean, changedFirst: Boolean = false): Unit = {
    val tag                       = s"tir_${index}_${if (enabled) "on" else "off"}${if (changedFirst) "_changed" else ""}"
    val base                      = s"${tag}_base"
    val parent                    = s"${tag}_parent"
    val child                     = s"${tag}_child"
    val (strings, ints, decimals, width) = shapes(index)
    val types =
      Seq.fill(strings)("STRING") ++ Seq.fill(ints)("INT") ++ Seq.fill(decimals)("DECIMAL(18,2)") ++ Seq("TIMESTAMP")
    val columns    = types.indices.map(i => s"c$i")
    val definition = columns.zip(types).map { case (name, dataType) => s"$name $dataType" }.mkString(", ")
    val projection = {
      val projected = if (width == columns.size) columns else columns.drop(2)
      projected.map { name => s"CAST($name AS ${types(columns.indexOf(name))}) AS $name" }.mkString(", ")
    }
    val seed = (0 until 4)
      .map { row =>
        types.zipWithIndex
          .map { case (dataType, i) =>
            if (row == 3 || (row == 2 && i % 7 == 0)) "NULL"
            else
              dataType match {
                case "STRING"        => if (row == 2) "'é a '" else "'É a'"
                case "INT"           => if (row == 2) "-2147483648" else "2147483647"
                case "DECIMAL(18,2)" => if (row == 2) "-9999999999999999.99" else "0.01"
                case "TIMESTAMP" =>
                  if (row == 2) "TIMESTAMP '1969-12-31 23:59:59.999999'" else "TIMESTAMP '2024-06-01 01:02:03.123456'"
              }
          }
          .mkString("(", ", ", ")")
      }
      .mkString(", ")
    spark.sparkContext.getConf.getBoolean(FeatureGate.PublicCdfNoWriteEnabledKey, false) shouldBe enabled
    sql(s"CREATE TABLE $base ($definition) USING DELTA")
    sql(s"INSERT INTO $base VALUES $seed")
    sql(s"CREATE MATERIALIZED VIEW $parent AS SELECT ${columns.mkString(", ")} FROM $base")
    val body = s"SELECT $projection FROM $parent"
    sql(s"CREATE MATERIALIZED VIEW $child AS $body")
    val childId = spark.sessionState.sqlParser.parseTableIdentifier(child)
    val meta    = MvCatalog.lookup(spark, childId).getOrElse(fail(s"Missing $child"))
    meta.refreshType shouldBe RefreshTypeCode.SimpleProjection
    val link         = meta.sourceTables.head
    val p0           = mvDataVersion(parent)
    val c0           = mvDataVersion(child)
    val path         = mvDataLocation(parent)
    val parentBefore = spark.read.format("delta").option("versionAsOf", p0).load(path).selectExpr(columns: _*)
    def plainOverwrite(query: String): Unit =
      sql(query).coalesce(1).write.format("delta").mode("overwrite").save(path)
    if (changedFirst) sql(s"INSERT OVERWRITE TABLE $base VALUES ${seed.replace("'É a'", "'É a '")}")
    plainOverwrite(s"SELECT * FROM $base")
    val p1 = mvDataVersion(parent)
    p0 shouldBe 0L
    p1 shouldBe 1L
    assertMvCorrect(parent, s"SELECT * FROM $base")
    val delta = DeltaTableVersion.deltaLogFor(spark, path)
    val log = DeltaCommitClassifier.classify(spark, link, p0)
    val overwrite = delta.getChanges(p1, failOnDataLoss = true).next()._2
    val info = overwrite.collect { case c: org.apache.spark.sql.delta.actions.CommitInfo => c }
    info.map(_.operation) shouldBe Seq("WRITE")
    info.head.operationParameters.keySet shouldBe Set("mode", "partitionBy")
    val inserted = overwrite.collect { case file: org.apache.spark.sql.delta.actions.AddFile => file }
    val removed = overwrite.collect { case file: org.apache.spark.sql.delta.actions.RemoveFile => file }
    (inserted.size, removed.size) shouldBe ((1, 1))
    inserted.head.path should not be removed.head.path
    inserted.head.dataChange shouldBe true
    removed.head.dataChange shouldBe true
    overwrite.size shouldBe 3
    spark.table(child).schema.length shouldBe width
    parentBefore.schema.length shouldBe columns.size
    val commits = (p0 to p1).map { v =>
      val filename = f"$v%020d.json"
      s"$filename\n" + new String(
        Files.readAllBytes(Paths.get(new URI(path)).resolve("_delta_log").resolve(filename)),
        UTF_8
      )
    }
    def evidence(label: String, data: String): Unit = sys.env.get("TYPED_IDENTITY_EVIDENCE").foreach { dir =>
      Files.write(Paths.get(dir).resolve(s"$tag.$label"), data.getBytes(UTF_8), StandardOpenOption.CREATE_NEW)
    }
    evidence("delta-log", commits.mkString("\n"))
    evidence(
      "gate",
      s"spark=${spark.version}\nclassifier=$log\nlink=$link\nstartExclusive=$p0\nendInclusive=$p1\nconsumerWidth=$width\nphysicalWidth=${columns.size}\noldFile=${removed.head.path}\nnewFile=${inserted.head.path}\n${meta.properties}\n$body\n"
    )
    log shouldBe BatchVerdict.Replace
    val raw = spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", p0 + 1)
      .option("endingVersion", p1)
      .load(path)
    val evidenceColumns = columns.map(c => if (c == columns.last) s"unix_micros($c) AS ${c}_epoch_micros" else c)
    evidence(
      "cdf.jsonl",
      raw
        .selectExpr((evidenceColumns ++ Seq("_change_type", "_commit_version")): _*)
        .toJSON
        .collect()
        .mkString("\n") + "\n"
    )
    for (kind <- Seq("insert", "delete")) {
      val rows = raw.where(s"_change_type = '$kind'").selectExpr(columns: _*)
      if (changedFirst && kind == "insert") {
        rows.exceptAll(parentBefore).count() should be > 0L
        parentBefore.exceptAll(rows).count() should be > 0L
      } else {
        rows.exceptAll(parentBefore).count() shouldBe 0L
        parentBefore.exceptAll(rows).count() shouldBe 0L
      }
    }
    if (expectNoWrite && index == 0 && spark.version.startsWith("4.1")) {
      val sc             = spark.sparkContext
      val cancelled      = new java.util.concurrent.atomic.AtomicBoolean(false)
      val cancelledGroup = s"$tag-cancelled-proof"
      val listener = new SparkListener {
        override def onJobStart(event: SparkListenerJobStart): Unit = {
          val properties = event.properties
          if (
            properties != null && cancelledGroup == properties.getProperty("spark.jobGroup.id") &&
            "replacement" == properties.getProperty("spark.openivm.publicCdfNoWrite.proof") &&
            cancelled.compareAndSet(false, true)
          ) {
            sc.cancelJobGroup(cancelledGroup)
          }
        }
      }
      sc.addSparkListener(listener)
      sc.setJobGroup(cancelledGroup, cancelledGroup, interruptOnCancel = true)
      val failure =
        try intercept[Exception](refreshMv(child))
        finally {
          sc.clearJobGroup()
          sc.removeSparkListener(listener)
        }
      cancelled.get() shouldBe true
      mvDataVersion(child) shouldBe c0
      CdfWatermarkCatalog.get(spark, child, link) shouldBe None
      evidence("cancel", s"targetVersion=$c0\noffset=absent\n${failure.toString}\n")
    }
    val known = RefreshProfileCatalog.scanAll(spark).map(_.refreshId).toSet
    spark.sparkContext.setJobGroup(tag, tag)
    val start = System.nanoTime()
    try refreshMv(child)
    finally spark.sparkContext.clearJobGroup()
    val ms       = (System.nanoTime() - start) / 1000000L
    val profiles = RefreshProfileCatalog.scanAll(spark).filterNot(row => known(row.refreshId)).toSeq
    val c1       = mvDataVersion(child)
    val offsets  = CdfWatermarkCatalog.getAll(spark, child, meta.sourceTables)
    evidence(
      "refresh",
      s"elapsed_ms=$ms\nchild_before=$c0\nchild_after=$c1\noffsets=$offsets\n${profiles.mkString("\n")}\n"
    )
    profiles.filter(_.stepName == "total_refresh").map(_.detail).mkString should include(
      if (expectNoWrite) "outcome=public_cdf_no_write" else "outcome=full_refresh_executed"
    )
    if (expectNoWrite) c1 shouldBe c0 else c1 should be > c0
    offsets shouldBe Map(link -> p1)
    assertMvCorrect(child, body)

    if (restorePhase.contains("prepare")) {
      val root = restoreRoot.get
      Files.write(
        root.resolve("prepare-pid"),
        ProcessHandle.current().pid().toString.getBytes(UTF_8),
        StandardOpenOption.CREATE_NEW
      )
      Files.write(root.resolve("child-version"), c1.toString.getBytes(UTF_8), StandardOpenOption.CREATE_NEW)
      Files.write(root.resolve("consumed"), p1.toString.getBytes(UTF_8), StandardOpenOption.CREATE_NEW)
    } else if (expectNoWrite && index == 0) {
      def replace(values: String): Unit = {
        sql(s"INSERT OVERWRITE TABLE $base VALUES $values")
        plainOverwrite(s"SELECT * FROM $base")
      }
      def mustWrite(label: String, oldVersion: Long): Unit = {
        val seen = RefreshProfileCatalog.scanAll(spark).map(_.refreshId).toSet
        refreshMv(child)
        val outcome = totalSince(seen)
        outcome should include("outcome=full_refresh_executed")
        val after = mvDataVersion(child)
        after should be > oldVersion
        assertMvCorrect(child, body)
        evidence(label, s"child_before=$oldVersion\nchild_after=$after\n$outcome\n")
      }
      // A trailing-space-only change in the binary string key must not cancel.
      val changed = seed.replace("'É a'", "'É a '")
      replace(changed)
      mustWrite("changed", c1)

      // Remove only the obsolete file used for inferred delete images, never a live parent file.
      val c2 = mvDataVersion(child)
      val p2 = mvDataVersion(parent)
      replace(changed)
      val p3      = mvDataVersion(parent)
      val commit  = DeltaTableVersion.deltaLogFor(spark, path).getChanges(p3, failOnDataLoss = true).next()._2
      val removed = commit.collect { case remove: org.apache.spark.sql.delta.actions.RemoveFile => remove.path }
      removed should not be empty
      removed.foreach(file => Files.delete(Paths.get(new URI(path)).resolve(file)))
      val missing = CdfChangeAvailability.unavailableReason(spark, link, p2, p3)
      missing.getOrElse(fail("Missing delete image was not detected")) should include(
        "change data files no longer exist"
      )
      evidence("missing-cdf", missing.mkString + "\n")
      mustWrite("unavailable", c2)
    }
  }

  private def partialOverwrite(): Unit = {
    val parent = "tir_partial_parent"
    val child = "tir_partial_child"
    sql("CREATE TABLE tir_partial_base(k INT, payload STRING) USING DELTA")
    sql("INSERT INTO tir_partial_base VALUES (0, 'replace me'), (1, 'keep me')")
    sql(s"CREATE MATERIALIZED VIEW $parent PARTITIONED BY (k) AS SELECT k, payload FROM tir_partial_base")
    val body = s"SELECT CAST(k AS INT) k, CAST(payload AS STRING) payload FROM $parent"
    sql(s"CREATE MATERIALIZED VIEW $child AS $body")
    val path = mvDataLocation(parent)
    val delta = DeltaTableVersion.deltaLogFor(spark, path)
    val p0 = mvDataVersion(parent)
    val c0 = mvDataVersion(child)
    val initial = delta.getChanges(p0, failOnDataLoss = true).next()._2.collect {
      case add: org.apache.spark.sql.delta.actions.AddFile => add.path
    }.toSet
    initial.size shouldBe 2
    sql("SELECT k, payload FROM tir_partial_base WHERE k = 0").write
      .format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic").save(path)
    val p1 = mvDataVersion(parent)
    p1 shouldBe p0 + 1L
    val actions = delta.getChanges(p1, failOnDataLoss = true).next()._2
    val removed = actions.collect { case file: org.apache.spark.sql.delta.actions.RemoveFile => file.path }.toSet
    val surviving = initial -- removed
    removed.size shouldBe 1
    surviving.size shouldBe 1
    val link = s"default.$parent"
    val verdict = DeltaCommitClassifier.classify(spark, link, p0)
    val seen = RefreshProfileCatalog.scanAll(spark).map(_.refreshId).toSet
    refreshMv(child)
    val outcome = totalSince(seen)
    outcome should not include "outcome=public_cdf_no_write"
    if (verdict == BatchVerdict.Replace) mvDataVersion(child) should be > c0
    assertMvCorrect(child, body)
    assertMvCorrect(parent, "SELECT k, payload FROM tir_partial_base")
    sys.env.get("TYPED_IDENTITY_EVIDENCE").foreach { dir =>
      val commit = Paths.get(new URI(path)).resolve("_delta_log").resolve(f"$p1%020d.json")
      Files.copy(commit, Paths.get(dir).resolve("partial.delta-log"))
      Files.write(Paths.get(dir).resolve("partial.receipt"), s"$verdict\ninitial=$initial\nremoved=$removed\nsurviving=$surviving\n$outcome\n".getBytes(UTF_8), StandardOpenOption.CREATE_NEW)
    }
  }

  it("proves exact typed bags or retains the ordinary replacement route") {
    restorePhase match {
      case Some("prepare") =>
        restartSpark(durableConf)
        exercise(0, enabled = true, expectNoWrite = true)
      case Some("restore") =>
        val root                      = restoreRoot.get
        def prior(name: String): Long = new String(Files.readAllBytes(root.resolve(name)), UTF_8).trim.toLong
        val pid                       = ProcessHandle.current().pid()
        pid should not be prior("prepare-pid")
        restartSpark(durableConf)
        val child  = "tir_0_on_child"
        val parent = "default.tir_0_on_parent"
        OpenIvmStateSync.maybeRestore(spark)
        CdfWatermarkCatalog.get(spark, child, parent) shouldBe Some(prior("consumed"))
        mvDataVersion(child) shouldBe prior("child-version")
        val seen = RefreshProfileCatalog.scanAll(spark).map(_.refreshId).toSet
        refreshMv(child)
        val outcome = totalSince(seen)
        outcome should include("outcome=no_pending_deltas")
        mvDataVersion(child) shouldBe prior("child-version")
        val selected = spark.table(child).columns.mkString(", ")
        assertMvCorrect(child, s"SELECT $selected FROM $parent")
        Files.write(
          root.resolve("restore-receipt"),
          s"prepare_pid=${prior("prepare-pid")}\nrestore_pid=$pid\nrestored_offset=${prior("consumed")}\nchild_version=${prior("child-version")}\n$outcome\n"
            .getBytes(UTF_8),
          StandardOpenOption.CREATE_NEW
        )
      case Some(other) => fail(s"Unknown durable fixture phase $other")
      case None if reproduceOnly =>
        exercise(0, enabled = true, expectNoWrite = false)
      case None =>
        shapes.indices.foreach(exercise(_, enabled = true, expectNoWrite = true))
        exercise(0, enabled = true, expectNoWrite = false, changedFirst = true)
        partialOverwrite()
        restartSpark(extraSparkConf - FeatureGate.PublicCdfNoWriteEnabledKey)
        shapes.indices.foreach(exercise(_, enabled = false, expectNoWrite = false))
    }
  }
}
