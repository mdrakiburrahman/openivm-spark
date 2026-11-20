package org.openivm.spark.commands

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.openivm.spark.common.{CdfWatermarkCatalog, DeltaCommitClassifier, DeltaShape, FeatureGate, MvCatalog, MvMetadata, RefreshProfileCatalog, RefreshProfileRow, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}
import scala.jdk.CollectionConverters._

class NativeScopeRefreshDispatchSpec extends IvmParitySpecBase("native-scope-refresh-dispatch") with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
    FeatureGate.WindowCascadeMergeEnabledKey -> "false",
    FeatureGate.GlobalWindowCdfMergeEnabledKey -> "false",
    FeatureGate.CompileClassificationCacheEnabledKey -> "false",
    FeatureGate.ProfileRefreshKey -> "true",
    FeatureGate.QueryLogEnabledKey -> "true"
  )

  private val root = Paths.get("/workspaces/openivm-spark-scoped-pub-exp-6bb1353/.logs/native-r2-scope-pair/actual-refresh-watermark-replay-20261120-0628")
  private val invocationLog = root.resolve("cli-invocations.tsv")
  private var referencePhysicalSchema = Option.empty[String]
  private val upstreams = Vector(("nsc_b1", "base"), ("nsc_p1", "patched"), ("nsc_p2", "patched"), ("nsc_b2", "base"))
  private val zeros = (0 until 10).map(_ => "0").mkString(", ")
  private val xCols = (0 until 10).map(i => s"f.x$i").mkString(", ")
  private val fullQuery =
    "WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY island " +
      "ORDER BY moment DESC, seq ASC NULLS LAST, token ASC NULLS LAST) AS pos FROM default.obs), " +
      "lookup AS (SELECT DISTINCT NULLIF(TRIM(raw), '') AS code FROM default.tags " +
      "WHERE UPPER(TRIM(kind)) = 'ACTIVE') " +
      s"SELECT f.token, f.island, f.moment AS last_seen, true AS present, f.code, f.payload, " +
      "(d.code IS NOT NULL) AS flagged, " + xCols + " " +
      "FROM numbered f LEFT JOIN lookup d ON f.code = d.code WHERE f.pos = 1"

  private def append(file: Path, text: String): Unit = {
    Files.createDirectories(file.getParent)
    Files.write(file, text.getBytes(UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
  private def writeText(file: Path, text: String): Unit = {
    Files.createDirectories(file.getParent)
    Files.write(file, text.getBytes(UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }
  private def sameBag(a: DataFrame, b: DataFrame, cols: Seq[String]): Boolean = {
    val left = a.select(cols.map(col): _*); val right = b.select(cols.map(col): _*)
    left.exceptAll(right).limit(1).count() == 0L && right.exceptAll(left).limit(1).count() == 0L
  }
  private def version(path: String): Long = DeltaCommitClassifier.latestVersion(spark, path)
  private def tableId(path: String): String =
    spark.sql(s"DESCRIBE DETAIL delta.`$path`").head().getAs[String]("id")

  private case class PhysicalCommitEvidence(
      version: Long,
      operation: String,
      parameters: Map[String, String],
      addActions: Int,
      removeActions: Int
  ) {
    private def parameter(keys: String*): Option[String] =
      keys.iterator.flatMap(parameters.get).take(1).toSeq.headOption
        .map(_.trim.stripPrefix("\"").stripSuffix("\""))

    private val operationUpper = operation.trim.toUpperCase(java.util.Locale.ROOT)
    private val overwriteLike =
      operationUpper.contains("REPLACE") || operationUpper.contains("OVERWRITE") || operationUpper == "TRUNCATE" ||
        parameter("mode", "Mode").exists(v => Set("OVERWRITE", "REPLACE")(v.toUpperCase(java.util.Locale.ROOT)))
    val hasScopedPredicate = parameter("predicate", "replaceWhere", "partitionPredicate")
      .exists(v => v.nonEmpty && v != "[]" && !v.equalsIgnoreCase("true"))

    val unscopedOverwrite: Boolean = overwriteLike && !hasScopedPredicate

    def serialized: String = {
      val normalizedParameters = parameters.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(";")
      Seq(version, operation, normalizedParameters, addActions, removeActions, unscopedOverwrite).mkString("|")
        .replace("\t", " ").replace("\n", " ").replace("\r", " ")
    }
  }

  private def physicalCommit(path: String, commitVersion: Long): PhysicalCommitEvidence = {
    val actions = DeltaLog.forTable(spark, new org.apache.hadoop.fs.Path(path))
      .getChanges(commitVersion, failOnDataLoss = false)
      .takeWhile(_._1 <= commitVersion)
      .find(_._1 == commitVersion)
      .map(_._2.toSeq)
      .getOrElse(fail(s"missing physical Delta commit $commitVersion at $path"))
    val info = actions.collectFirst { case commit: CommitInfo => commit }
      .getOrElse(fail(s"Delta commit $commitVersion at $path has no CommitInfo action"))
    PhysicalCommitEvidence(
      commitVersion,
      info.operation,
      info.operationParameters,
      actions.count(_.isInstanceOf[AddFile]),
      actions.count(_.isInstanceOf[RemoveFile])
    )
  }

  private def logPhysicalCommit(phase: String, mv: String, evidence: PhysicalCommitEvidence): Unit =
    append(root.resolve("physical-commits.tsv"), Seq(phase, mv, evidence.serialized).mkString("\t") + "\n")
  private def profileRows(view: String): Seq[RefreshProfileRow] = {
    val matching = RefreshProfileCatalog.scanAll(spark).filter(_.viewName == view)
    val operationIds = matching.map(_.refreshId).distinct
    operationIds should have size 1
    val operationId = operationIds.head
    matching.filter(_.refreshId == operationId)
  }
  private def profileText(phase: String, mv: String, rows: Seq[RefreshProfileRow]): Unit =
    rows.foreach(r => append(root.resolve("profiles.tsv"),
      Seq(phase, mv, r.refreshId, r.viewName, r.stepOrder, r.stepName, r.durationMs, r.detail)
        .mkString("\t") + "\n"))

  private def createSources(): Unit = {
    sql("CREATE TABLE default.obs (island INT, moment TIMESTAMP, seq INT, token STRING, code STRING, payload STRING, " +
      (0 until 10).map(i => s"x$i INT").mkString(", ") + ") USING DELTA")
    sql("CREATE TABLE default.tags (raw STRING, kind STRING) USING DELTA")
    sql("INSERT INTO default.obs SELECT CAST(id DIV 10 AS INT), TIMESTAMP '2026-01-01 00:00:00', " +
      "CAST(id % 10 AS INT), CONCAT('token_', CAST(id AS STRING)), " +
      "CONCAT('C', CAST((id DIV 10) % 100 AS STRING)), 'bulk', " + zeros + " FROM range(0, 100000)")
    sql("INSERT INTO default.obs SELECT CAST(NULL AS INT), TIMESTAMP '2026-01-01 00:00:00', " +
      "CAST(id AS INT), CONCAT('null_', CAST(id AS STRING)), 'C0', 'null', " + zeros + " FROM range(0, 10)")
    sql("INSERT INTO default.obs VALUES (7777, TIMESTAMP '2026-01-01 00:00:00', 0, 'token_77770', 'C77', 'bulk', " + zeros + "), " +
      "(7777, TIMESTAMP '2026-01-01 00:00:00', 0, 'token_77770', 'C77', 'bulk', " + zeros + ")")
    sql("INSERT INTO default.tags SELECT CONCAT('C', CAST(id AS STRING)), 'ACTIVE' FROM range(0, 100)")
    sql("INSERT INTO default.tags VALUES ('C0','ACTIVE'), ('   ','ACTIVE'), (CAST(NULL AS STRING),'ACTIVE'), ('C0','INACTIVE')")
  }

  private def invocationRows: Seq[Array[String]] =
    Files.readAllLines(invocationLog, UTF_8).asScala.toSeq.drop(1).map(_.split("\\t", -1))
  private def invocationCount(view: String): Int = invocationRows.count(row => row.headOption.contains(view))
  private def assertInvocationAfter(view: String, variant: String, previous: Int): Unit = {
    val rows = invocationRows.filter(row => row.headOption.contains(view))
    rows.size should be > previous
    val added = rows.drop(previous)
    val (cliHash, extensionHash) = if (variant == "base")
      ("ea5b574e5720b6ca8d6cd3eb4e0f37adaff889c01413e70fc8343f5f7a659ffc", "086c7f5721d5d3ce36b6fdf5845108c4d70bd45827e387e272d9e9befe48e6f1")
    else ("4d700379be9b2924866200d9c98bd6dc2aff7880aa7694c2036830c82ac9a973", "c465711e5c3d17371ac24e1e87d45994a88d1bfb8b24f7559448a457fe3c518b")
    added.foreach { row =>
      row(1) shouldBe variant
      row(3) shouldBe cliHash
      row(5) shouldBe extensionHash
      row(7) shouldBe "false"
      Seq(8, 10, 12).foreach(index => Files.exists(Paths.get(row(index))) shouldBe true)
      Seq(9, 11, 13).foreach(index => row(index).matches("[0-9a-f]{64}") shouldBe true)
      row(14) shouldBe "0"
    }
  }
  private def compilerExecutionStatus(view: String, variant: String, previous: Int,
                                      rows: Seq[RefreshProfileRow], physicalCommitObserved: Boolean = false): String = {
    val calls = invocationCount(view) - previous
    val fullRefresh = rows.find(_.stepName == "full_refresh_decision")
    val compile = rows.find(_.stepName == "generate_refresh_sql.compile")
    val outcome = rows.find(_.stepName == "total_refresh").map(_.detail).getOrElse("")
    if (fullRefresh.nonEmpty) {
      if (calls > 0) assertInvocationAfter(view, variant, previous)
      s"full_refresh:${fullRefresh.get.detail}"
    } else if (compile.exists(_.detail.contains("compile_cache_hit=true"))) {
      calls shouldBe 0
      "compile_cache_hit"
    } else if (compile.nonEmpty || calls > 0) {
      assertInvocationAfter(view, variant, previous)
      "native_cli_invoked"
    } else if (outcome.contains("outcome=no_op")) {
      calls shouldBe 0
      "no_op"
    } else if (physicalCommitObserved) {
      "physical_commit_observed_without_classifier"
    } else {
      fail(s"$view had neither a compile step nor an explained full-refresh/no-op path: $outcome")
    }
  }
  private def createViews(): Unit = upstreams.foreach { case (mv, variant) =>
    val before = invocationCount(mv)
    sql(s"CREATE MATERIALIZED VIEW $mv AS $fullQuery")
    assertInvocationAfter(mv, variant, before)
    val meta = MvCatalog.lookup(spark, org.apache.spark.sql.catalyst.TableIdentifier(mv)).get
    meta.refreshType shouldBe RefreshTypeCode.WindowPartition
    val schema = spark.read.format("delta").load(meta.location).schema.json
    referencePhysicalSchema match {
      case Some(expected) => schema shouldBe expected
      case None => referencePhysicalSchema = Some(schema)
    }
    val downstream = s"${mv}_dn"
    val downstreamBefore = invocationCount(downstream)
    sql(s"CREATE MATERIALIZED VIEW $downstream AS SELECT token, island, flagged FROM $mv")
    assertInvocationAfter(downstream, variant, downstreamBefore)
    MvCatalog.lookup(spark, org.apache.spark.sql.catalyst.TableIdentifier(downstream)).get.refreshType shouldBe
      RefreshTypeCode.SimpleProjection
  }

  private def mutateFacts(): Unit =
    sql("INSERT INTO default.obs VALUES " +
      "(1,TIMESTAMP '2026-02-01 00:00:00',1,'new_1','C1','new'," + zeros + "), " +
      "(2,TIMESTAMP '2026-02-01 00:00:00',1,'new_2','C2','new'," + zeros + "), " +
      "(3,TIMESTAMP '2026-02-01 00:00:00',1,'new_3','C3','new'," + zeros + "), " +
      "(CAST(NULL AS INT),TIMESTAMP '2026-02-01 00:00:00',1,'new_null','C0','new'," + zeros + ")")

  private def mutateSeed(): Unit =
    sql("UPDATE default.tags SET raw='C200' WHERE raw='C0' AND kind='ACTIVE'")

  private def sourceVersions: Map[String, Long] = Map(
    "obs" -> version("default.obs"), "tags" -> version("default.tags"))

  private def changedPartitions(starts: Map[String, Long]): Set[Option[Int]] = {
    def cdf(table: String, start: Long): DataFrame = {
      val end = version(s"default.$table")
      if (end <= start) spark.table(s"default.$table").limit(0)
      else spark.read.format("delta").option("readChangeFeed", "true")
        .option("startingVersion", start + 1L).option("endingVersion", end).table(s"default.$table")
    }
    val factKeys = cdf("obs", starts("obs")).select("island").collect().map { r =>
      if (r.isNullAt(0)) None else Some(r.getInt(0))
    }.toSet
    val tagCdf = cdf("tags", starts("tags"))
    val codes = tagCdf.select("raw").collect().flatMap { r =>
      if (r.isNullAt(0)) None else Option(r.getString(0).trim).filter(_.nonEmpty)
    }.distinct
    val dimKeys = if (codes.isEmpty) Set.empty[Option[Int]] else
      spark.table("default.obs").filter(col("code").isin(codes.toIndexedSeq: _*)).select("island").distinct().collect().map { r =>
        if (r.isNullAt(0)) None else Some(r.getInt(0))
      }.toSet
    factKeys ++ dimKeys
  }

  private def sourceShapes(meta: MvMetadata, starts: Map[String, Long]): Map[String, DeltaShape] =
    meta.sourceTables.map { source =>
      val short = source.split("\\.").last
      source -> DeltaCommitClassifier.classifyShape(spark, source, starts(short))
    }.toMap

  private def scopedRows(df: DataFrame, keys: Set[Option[Int]], cols: Seq[String]): DataFrame = {
    val schema = StructType(Seq(StructField("island", IntegerType, nullable = true)))
    val keyDf = spark.createDataFrame(spark.sparkContext.parallelize(keys.toSeq.map(k => Row(k match {
      case Some(value) => value
      case None => null
    }))), schema).alias("k")
    df.alias("d").join(keyDf, col("d.island").eqNullSafe(col("k.island")))
      .select(cols.map(c => col(s"d.`$c`")): _*)
  }

  private def signed(df: DataFrame, cols: Seq[String], sign: Int): DataFrame =
    df.select((cols.map(col) :+ lit(sign).cast(IntegerType).as("openivm_multiplicity")): _*)

  private def downstreamNoopReplay(mv: String, expected: DataFrame, phase: String): Unit = {
    val consumer = s"${mv}_dn"
    val consumerMeta = MvCatalog.lookup(spark, org.apache.spark.sql.catalyst.TableIdentifier(consumer)).get
    val upstreamSource = consumerMeta.sourceTables.find(_.split("\\.").last == mv).get
    val sourcePath = MvCatalog.lookup(spark, org.apache.spark.sql.catalyst.TableIdentifier(mv)).get.location
    val sourceBefore = version(sourcePath)
    val consumerPath = consumerMeta.location
    val targetBefore = version(consumerPath)
    val watermarkBefore = CdfWatermarkCatalog.get(spark, consumer, upstreamSource)
    watermarkBefore shouldBe Some(sourceBefore)
    val targetBeforeRows = spark.read.format("delta").option("versionAsOf", targetBefore).load(consumerPath)
    val expectedFull = expected.select("token", "island", "flagged")
    sameBag(targetBeforeRows, expectedFull, Seq("token", "island", "flagged")) shouldBe true

    RefreshProfileCatalog.removeAll(spark)
    val cliBefore = invocationCount(consumer)
    val start = System.nanoTime()
    sql(s"REFRESH MATERIALIZED VIEW $consumer").collect()
    val refreshMs = (System.nanoTime() - start) / 1000000L
    val profile = RefreshProfileCatalog.scanAll(spark).filter(_.viewName == consumer)
    profileText(s"$phase-replay-noop", consumer, profile)
    val profileOutcome = profile.find(_.stepName == "total_refresh").map(_.detail).getOrElse("NO_TOTAL_REFRESH_ROW")
    val targetAfter = version(consumerPath)
    val sourceAfter = version(sourcePath)
    val watermarkAfter = CdfWatermarkCatalog.get(spark, consumer, upstreamSource)
    val output = spark.table(consumer).select("token", "island", "flagged")
    val outputMatchesFull = sameBag(output, expectedFull, Seq("token", "island", "flagged"))
    val cliCalls = invocationCount(consumer) - cliBefore

    targetAfter shouldBe targetBefore
    sourceAfter shouldBe sourceBefore
    watermarkAfter shouldBe watermarkBefore
    outputMatchesFull shouldBe true
    append(root.resolve("no-op-receipt.tsv"), Seq(
      phase, consumer, "patched", "downstream_replay", targetBefore, targetAfter,
      sourceBefore, sourceAfter, watermarkBefore.map(_.toString).getOrElse("NONE"),
      watermarkAfter.map(_.toString).getOrElse("NONE"), refreshMs, cliCalls,
      outputMatchesFull, profileOutcome
    ).mkString("\t") + "\n")
  }

  private def runRefresh(mv: String, variant: String, starts: Map[String, Long], keys: Set[Option[Int]],
                         expected: DataFrame, batch: String, orderIndex: Int): Unit = {
    val target = MvCatalog.lookup(spark, org.apache.spark.sql.catalyst.TableIdentifier(mv)).get
    val shapes = sourceShapes(target, starts).toSeq.sortBy(_._1).map { case (k, v) => s"$k=${v.compileFactValue}" }.mkString(",")
    val compileSource = "actual-native-cli;compile-cache-disabled"
    val targetPath = target.location
    val before = version(targetPath); val beforeId = tableId(targetPath)
    val oldRows = spark.read.format("delta").option("versionAsOf", before).load(targetPath).cache()
    oldRows.count()
    RefreshProfileCatalog.removeAll(spark)
    val producerCliBefore = invocationCount(mv)
    val producerStart = System.nanoTime()
    sql(s"REFRESH MATERIALIZED VIEW $mv").collect()
    val producerMs = (System.nanoTime() - producerStart) / 1000000L
    val producerProfile = profileRows(mv)
    profileText(s"$batch-producer", mv, producerProfile)
    val producerDetails = producerProfile.filter(_.stepName == "execute_refresh_sql_stmt").map(_.detail)
    producerDetails.exists(_.contains("window_replace_delete_skipped")) shouldBe true
    producerDetails.exists(_.contains("replace_where_overwrite")) shouldBe true
    producerDetails.exists(_.contains("window_target_cdf_cascade_skipped")) shouldBe true
    producerProfile.filter(_.stepName == "generate_refresh_sql.compile")
      .exists(_.detail.contains("compile_cache_hit=false")) shouldBe true
    val after = version(targetPath)
    after shouldBe before + 1L
    val producerPhysicalCommit = physicalCommit(targetPath, after)
    logPhysicalCommit(s"$batch-producer", mv, producerPhysicalCommit)
    producerPhysicalCommit.version shouldBe after
    if (variant == "patched") producerPhysicalCommit.unscopedOverwrite shouldBe false
    val producerCompileExecution = compilerExecutionStatus(
      mv, variant, producerCliBefore, producerProfile, physicalCommitObserved = after > before
    )
    val cdfStart = System.nanoTime()
    val cdf = spark.read.format("delta").option("readChangeFeed", "true")
      .option("startingVersion", before + 1L).option("endingVersion", after).load(targetPath)
    val physicalColumns = oldRows.columns.toSeq
    val targetCols = physicalColumns.filterNot(_.startsWith("openivm_"))
    val signedCdf = cdf.withColumn("openivm_multiplicity",
      when(col("_change_type").isin("delete", "update_preimage"), lit(-1)).otherwise(lit(1)).cast(IntegerType))
      .select((targetCols.map(col) :+ col("openivm_multiplicity")): _*)
    val expectedOld = if (variant == "base") oldRows else scopedRows(oldRows, keys, targetCols)
    val expectedNew = if (variant == "base") expected else scopedRows(expected, keys, targetCols)
    val expectedSigned = signed(expectedOld, targetCols, -1).unionByName(signed(expectedNew, targetCols, 1))
    val targetAfter = spark.read.format("delta").load(targetPath)
    targetAfter.columns.toSeq shouldBe physicalColumns
    targetAfter.schema.json shouldBe oldRows.schema.json
    val cdfRows = cdf.count()
    sameBag(signedCdf, expectedSigned, targetCols :+ "openivm_multiplicity") shouldBe true
    sameBag(targetAfter, expected, targetCols) shouldBe true
    tableId(targetPath) shouldBe beforeId
    val deleteRows = cdf.filter(col("_change_type").isin("delete", "update_preimage")).count()
    val insertRows = cdf.filter(col("_change_type").isin("insert", "update_postimage")).count()
    val cdfVerifyMs = (System.nanoTime() - cdfStart) / 1000000L
    append(root.resolve("producer-receipt.tsv"),
      Seq(batch, orderIndex, mv, variant, shapes, keys.toSeq.map(_.fold("NULL")(_.toString)).sorted.mkString(","),
        compileSource, producerCompileExecution, producerMs, before, after, after - before,
        producerPhysicalCommit.serialized, producerPhysicalCommit.unscopedOverwrite, cdfRows, deleteRows, insertRows, cdfVerifyMs,
        beforeId, physicalColumns.mkString(","), "signed_cdf_bag=pass", "target_output_bag=pass",
        "schema_and_table_identity=pass").mkString("\t") + "\n")

    val consumer = s"${mv}_dn"
    val consumerMeta = MvCatalog.lookup(spark, org.apache.spark.sql.catalyst.TableIdentifier(consumer)).get
    val upstreamSource = consumerMeta.sourceTables.find(_.split("\\.").last == mv).get
    val consumerPath = consumerMeta.location
    val consumerBefore = version(consumerPath)
    val consumerOldRows = spark.read.format("delta").option("versionAsOf", consumerBefore).load(consumerPath).cache()
    consumerOldRows.count()
    val consumerSourceVersionBefore = version(targetPath)
    val consumerWatermarkBefore = CdfWatermarkCatalog.get(spark, consumer, upstreamSource)
    RefreshProfileCatalog.removeAll(spark)
    val consumerCliBefore = invocationCount(consumer)
    val consumerStart = System.nanoTime()
    sql(s"REFRESH MATERIALIZED VIEW $consumer").collect()
    val consumerMs = (System.nanoTime() - consumerStart) / 1000000L
    val consumerProfile = profileRows(consumer)
    profileText(s"$batch-consumer", consumer, consumerProfile)
    val consumerSourceVersionAfter = version(targetPath)
    val consumerCdfStartVersion = consumerWatermarkBefore.map(_ + 1L).getOrElse(-1L)
    val upstreamIntervalVersions = cdf.select("_commit_version").distinct().collect().map(_.getLong(0)).toSeq.sorted
    val consumerCdfRows = consumerWatermarkBefore.filter(_ < consumerSourceVersionAfter).map { watermark =>
      spark.read.format("delta").option("readChangeFeed", "true")
        .option("startingVersion", watermark + 1L).option("endingVersion", consumerSourceVersionAfter)
        .load(targetPath).count()
    }.getOrElse(0L)
    val consumerWatermark = CdfWatermarkCatalog.get(spark, consumer, upstreamSource)
    val consumerAfter = version(consumerPath)
    val consumerPhysicalCommit = physicalCommit(consumerPath, consumerAfter)
    logPhysicalCommit(s"$batch-consumer", consumer, consumerPhysicalCommit)
    val consumerCompileExecution = compilerExecutionStatus(
      consumer, variant, consumerCliBefore, consumerProfile, physicalCommitObserved = consumerAfter > consumerBefore
    )
    val consumerWatermarkMatches = consumerWatermark.contains(consumerSourceVersionAfter)
    val consumerVersionAdvanced = consumerAfter > consumerBefore
    val expectedConsumer = expected.select("token", "island", "flagged")
    val consumerTargetCols = Seq("token", "island", "flagged")
    val consumerActual = spark.table(consumer).select(consumerTargetCols.map(col): _*)
    val consumerTargetCdf = spark.read.format("delta").option("readChangeFeed", "true")
      .option("startingVersion", consumerBefore + 1L).option("endingVersion", consumerAfter).load(consumerPath)
    val consumerSignedCdf = consumerTargetCdf.withColumn("openivm_multiplicity",
      when(col("_change_type").isin("delete", "update_preimage"), lit(-1)).otherwise(lit(1)).cast(IntegerType))
      .select((consumerTargetCols.map(col) :+ col("openivm_multiplicity")): _*)
    val expectedConsumerOld = if (consumerPhysicalCommit.hasScopedPredicate)
      scopedRows(consumerOldRows, keys, consumerTargetCols) else consumerOldRows
    val expectedConsumerNew = if (consumerPhysicalCommit.hasScopedPredicate)
      scopedRows(expectedConsumer, keys, consumerTargetCols) else expectedConsumer
    val expectedConsumerSigned = signed(expectedConsumerOld, consumerTargetCols, -1)
      .unionByName(signed(expectedConsumerNew, consumerTargetCols, 1))
    val consumerVerifyStart = System.nanoTime()
    val consumerCorrect = sameBag(consumerActual, expectedConsumer, consumerTargetCols)
    val consumerSignedCdfCorrect = sameBag(
      consumerSignedCdf, expectedConsumerSigned, consumerTargetCols :+ "openivm_multiplicity"
    )
    val exactNextIntervalStatus = if (variant == "patched" && batch == "seed-rekey") {
      consumerWatermarkBefore shouldBe Some(before)
      consumerCdfStartVersion shouldBe before + 1L
      consumerSourceVersionBefore shouldBe after
      consumerSourceVersionAfter shouldBe after
      consumerWatermark shouldBe Some(after)
      consumerCdfRows shouldBe cdfRows
      upstreamIntervalVersions shouldBe Seq(after)
      consumerPhysicalCommit.operation.toUpperCase(java.util.Locale.ROOT) shouldBe "MERGE"
      consumerPhysicalCommit.hasScopedPredicate shouldBe true
      val physicalPredicate = consumerPhysicalCommit.parameters.getOrElse("predicate", "").toLowerCase(java.util.Locale.ROOT)
      physicalPredicate should include ("token")
      physicalPredicate should include ("island")
      val producerSignedForConsumer = signedCdf
        .select((consumerTargetCols.map(col) :+ col("openivm_multiplicity")): _*)
      sameBag(consumerSignedCdf, producerSignedForConsumer,
        consumerTargetCols :+ "openivm_multiplicity") shouldBe true
      s"(${consumerWatermarkBefore.get},${consumerSourceVersionAfter}];versions=${upstreamIntervalVersions.mkString(",")};signed_bag=pass"
    } else "not_applicable"
    val consumerVerifyMs = (System.nanoTime() - consumerVerifyStart) / 1000000L
    val consumerTargetCdfRows = consumerTargetCdf.count()
    val consumerTargetCdfDeletes = consumerTargetCdf.filter(col("_change_type").isin("delete", "update_preimage")).count()
    val consumerTargetCdfInserts = consumerTargetCdf.filter(col("_change_type").isin("insert", "update_postimage")).count()
    val consumerDispatch = consumerProfile.find(_.stepName == "generate_refresh_sql.dispatch").map(_.detail).getOrElse("")
    val consumerSourceShape = consumerProfile.find(_.stepName == "generate_refresh_sql.eligibility").map(_.detail).getOrElse("")
    val consumerRefreshDecision = consumerProfile.find(_.stepName == "full_refresh_decision").map(_.detail).getOrElse("")
    append(root.resolve("consumer-receipt.tsv"),
      Seq(batch, orderIndex, mv, variant, consumerMs, consumerCompileExecution, consumerDispatch,
        consumerSourceShape, consumerRefreshDecision, consumerBefore, consumerAfter,
        consumerWatermarkBefore.map(_.toString).getOrElse("NONE"), consumerWatermark.map(_.toString).getOrElse("NONE"),
        consumerCdfStartVersion, consumerSourceVersionAfter, consumerSourceVersionBefore, consumerSourceVersionAfter,
        consumerCdfRows, consumerVersionAdvanced, consumerPhysicalCommit.serialized,
        consumerTargetCdfRows, consumerTargetCdfDeletes, consumerTargetCdfInserts,
        consumerSignedCdfCorrect, exactNextIntervalStatus, consumerVerifyMs, consumerCorrect).mkString("\t") + "\n")
    consumerWatermarkMatches shouldBe true
    consumerVersionAdvanced shouldBe true
    consumerSignedCdfCorrect shouldBe true
    consumerCorrect shouldBe true
    consumerOldRows.unpersist()
    oldRows.unpersist()
  }

  it("records actual producer CDF and downstream classification without forcing incremental execution") {
    Files.createDirectories(root)
    Files.write(invocationLog, "view\tvariant\tcli_path\tcli_sha256\textension_path\textension_sha256\troute_overhead_ns\tmarkers_present\tinput_path\tinput_sha256\trouted_path\trouted_sha256\tstdout_path\tstdout_sha256\texit_code\tstderr_path\n".getBytes(UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    writeText(root.resolve("physical-commits.tsv"), "phase\tmv\tversion|operation|parameters|add_actions|remove_actions|unscoped_overwrite\n")
    writeText(root.resolve("producer-receipt.tsv"), "batch\torder\tmv\tvariant\tsource_shapes\taffected_keys\tcompile_source\tproducer_compile_execution\tproducer_refresh_ms\ttarget_version_before\ttarget_version_after\tcommit_count\tphysical_commit\tproducer_write_unscoped_overwrite\tcdf_rows\tcdf_delete\tcdf_insert\ttarget_cdf_verify_ms\ttarget_delta_id\ttarget_physical_columns\tcdf_bag\ttarget_bag\tschema_identity\n")
    writeText(root.resolve("consumer-receipt.tsv"), "batch\torder\tmv\tvariant\tconsumer_refresh_ms\tconsumer_compile_execution\tconsumer_dispatch\tconsumer_source_shape\tconsumer_refresh_decision\tconsumer_version_before\tconsumer_version_after\twatermark_before\twatermark_after\tupstream_cdf_start_version\tupstream_cdf_end_version\tupstream_source_version_before\tupstream_source_version_after\tavailable_upstream_cdf_rows\tconsumer_version_advanced\tconsumer_physical_commit\tconsumer_target_cdf_rows\tconsumer_target_cdf_delete\tconsumer_target_cdf_insert\tconsumer_signed_cdf_bag_equal\tconsumer_exact_interval_and_consumed_bag\tconsumer_verify_ms\tconsumer_bag_equal_to_regular_full\n")
    writeText(root.resolve("no-op-receipt.tsv"), "phase\tmv\tvariant\tkind\ttarget_version_before\ttarget_version_after\tupstream_version_before\tupstream_version_after\twatermark_before\twatermark_after\trefresh_ms\tcli_calls\toutput_bag_equal_to_regular_full\tprofile_outcome\n")
    writeText(root.resolve("profiles.tsv"), "phase\tmv\trefresh_id\tview_name\tstep_order\tstep_name\tduration_ms\tdetail\n")
    createSources()
    createViews()
    val initial = spark.sql(fullQuery).cache()
    initial.count() shouldBe 10001L
    upstreams.foreach { case (mv, _) => spark.table(mv).count() shouldBe 10001L }
    val factStarts = sourceVersions
    mutateFacts()
    val factKeys = changedPartitions(factStarts)
    factKeys shouldBe Set(Some(1), Some(2), Some(3), None)
    val factExpected = spark.sql(fullQuery).cache(); factExpected.count()
    Seq("nsc_b1", "nsc_p1", "nsc_p2", "nsc_b2").zipWithIndex.foreach { case (mv,i) =>
      val variant = if (mv.contains("_b")) "base" else "patched"
      runRefresh(mv, variant, factStarts, factKeys, factExpected, "fact-only", i)
    }
    factExpected.unpersist()

    val seedStarts = sourceVersions
    mutateSeed()
    val seedKeys = changedPartitions(seedStarts)
    seedKeys.size shouldBe 101
    seedKeys should contain(None)
    val seedExpected = spark.sql(fullQuery).cache(); seedExpected.count()
    Seq("nsc_p1", "nsc_b1", "nsc_b2", "nsc_p2").zipWithIndex.foreach { case (mv,i) =>
      val variant = if (mv.contains("_b")) "base" else "patched"
      runRefresh(mv, variant, seedStarts, seedKeys, seedExpected, "seed-rekey", i)
      if (mv == "nsc_p1") downstreamNoopReplay(mv, seedExpected, "seed-rekey")
    }
    seedExpected.unpersist(); initial.unpersist()
    val noOpMeta = MvCatalog.lookup(spark, org.apache.spark.sql.catalyst.TableIdentifier("nsc_p1")).get
    val noOpBefore = version(noOpMeta.location)
    val noOpSourceBefore = version("default.obs")
    val noOpCliBefore = invocationCount("nsc_p1")
    val noOpStart = System.nanoTime()
    sql("REFRESH MATERIALIZED VIEW nsc_p1").collect()
    val noOpMs = (System.nanoTime() - noOpStart) / 1000000L
    val noOpAfter = version(noOpMeta.location)
    val noOpSourceAfter = version("default.obs")
    noOpAfter shouldBe noOpBefore
    noOpSourceAfter shouldBe noOpSourceBefore
    val noOpCompile = if (invocationCount("nsc_p1") > noOpCliBefore) "actual-native-cli(noop)" else "no-native-compile"
    val producerNoOpOutput = sameBag(spark.table("nsc_p1"), spark.sql(fullQuery),
      spark.table("nsc_p1").columns.toSeq.filterNot(_.startsWith("openivm_")))
    producerNoOpOutput shouldBe true
    append(root.resolve("no-op-receipt.tsv"), Seq(
      "same-session-noop", "nsc_p1", "patched", "producer", noOpBefore, noOpAfter,
      noOpSourceBefore, noOpSourceAfter, "NONE", "NONE", noOpMs,
      invocationCount("nsc_p1") - noOpCliBefore, producerNoOpOutput, noOpCompile
    ).mkString("\t") + "\n")
  }
}