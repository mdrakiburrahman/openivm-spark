package org.openivm.spark.compiler

import java.nio.charset.StandardCharsets
import java.util.UUID

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.openivm.spark.common.{TimeTravelPinReason, TimeTravelPinStatus}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class FabricPinAliasMappingSpec extends AnyFunSpec with Matchers {

  private val OperationalArmCollection =
    "__fabric_encoded__arc_sql_db_bi_7d1f.arm_collection_dim"
  private val ExactArmCollectionRef = "`arc_sql_db_bi`.`arm_collection_dim`"

  private val windowedStagingSql =
    """WITH source AS (
      |  SELECT d.arm_collection_id, d.observed_at
      |  FROM `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094 AS d
      |),
      |staging AS (
      |  SELECT arm_collection_id,
      |         ROW_NUMBER() OVER (PARTITION BY arm_collection_id ORDER BY observed_at) AS row_num
      |  FROM source
      |)
      |SELECT arm_collection_id, row_num FROM staging""".stripMargin

  private def sourceIdentity(alias: String): SparkTimeTravelSql.SourceIdentity = {
    val pathToken = alias.replaceAll("[^A-Za-z0-9]", "_")
    SparkTimeTravelSql.SourceIdentity(
      alias,
      s"abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/$pathToken",
      UUID.nameUUIDFromBytes(alias.getBytes(StandardCharsets.UTF_8)).toString
    )
  }

  private def resolvedPins(
      sql: String,
      operationalByExactTableRef: Map[String, SparkTimeTravelSql.SourceIdentity]
  ): Seq[SparkTimeTravelSql.ResolvedSnapshotPin] =
    SparkTimeTravelSql.split(sql).pins.map { pin =>
      val operational =
        operationalByExactTableRef.getOrElse(pin.tableRef, fail(s"missing operational identity for ${pin.tableRef}"))
      SparkTimeTravelSql.ResolvedSnapshotPin(
        pin,
        SparkTimeTravelSql.SourceIdentity(
          pin.tableRef,
          operational.deltaLogDataPath,
          operational.deltaTableMetadataId
        ),
        operational
      )
    }

  private def bindings(
      sources: Seq[SparkTimeTravelSql.SourceIdentity],
      pins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin],
      resolutionFailures: Seq[SparkTimeTravelSql.PinResolutionFailure] = Seq.empty
  ): SparkTimeTravelSql.ResolvedSnapshotPinBindings =
    SparkTimeTravelSql.ResolvedSnapshotPinBindings(sources.map(_.alias), sources, pins, resolutionFailures)

  private def withoutVersionValues(sql: String): String =
    sql.replaceAll("VERSION AS OF \\d+", "VERSION AS OF <version>")

  private def validate(
      operation: SparkTimeTravelSql.PinIdentityOperation,
      sources: Seq[SparkTimeTravelSql.SourceIdentity],
      pins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin],
      persistedPins: Option[Seq[SparkTimeTravelSql.ResolvedSnapshotPin]],
      resolutionFailures: Seq[SparkTimeTravelSql.PinResolutionFailure] = Seq.empty
  ): Either[String, Unit] =
    SparkTimeTravelSql.validateResolvedSnapshotPins(
      operation,
      bindings(sources, pins, resolutionFailures),
      persistedPins
    )

  private def telemetry(
      sql: String,
      sources: Seq[SparkTimeTravelSql.SourceIdentity],
      pins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin],
      persistedPins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin] = Seq.empty
  ): SparkTimeTravelSql.PinTelemetry =
    SparkTimeTravelSql.pinTelemetry(sql, bindings(sources, pins), persistedPins)

  private def assertApplied(
      sql: String,
      sources: Seq[SparkTimeTravelSql.SourceIdentity],
      pins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin],
      expectedPins: Seq[String],
      persistedPins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin] = Seq.empty
  ): SparkTimeTravelSql.PinTelemetry = {
    val observed = telemetry(sql, sources, pins, persistedPins)
    observed.status shouldBe TimeTravelPinStatus.Applied
    observed.reason shouldBe TimeTravelPinReason.PinsResolved
    observed.pins shouldBe expectedPins.sorted
    observed.detail shouldBe None
    observed
  }

  private def assertHardFailure(
      operation: SparkTimeTravelSql.PinIdentityOperation,
      sources: Seq[SparkTimeTravelSql.SourceIdentity],
      pins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin],
      persistedPins: Option[Seq[SparkTimeTravelSql.ResolvedSnapshotPin]],
      expectedDetail: String,
      resolutionFailures: Seq[SparkTimeTravelSql.PinResolutionFailure] = Seq.empty
  ): Unit = {
    val failure = validate(operation, sources, pins, persistedPins, resolutionFailures).left.toOption
      .getOrElse(fail(s"$operation unexpectedly accepted pinned source identities"))
    failure.toLowerCase should include(expectedDetail)
    failure.toUpperCase should not include "FULL_REFRESH"
  }

  private def assertPropertyReadFails(
      properties: Map[String, String],
      currentBindings: SparkTimeTravelSql.ResolvedSnapshotPinBindings,
      expectedDetail: String
  ): Unit =
    SparkTimeTravelSql
      .readPinnedSourceIdentityProperties(properties, currentBindings)
      .left
      .toOption
      .getOrElse(fail("malformed pinned source identity property unexpectedly decoded"))
      .toLowerCase should include(expectedDetail)

  private def dataPathRelation(source: SparkTimeTravelSql.SourceIdentity): String =
    s"delta.`${source.deltaLogDataPath}`"

  private def assertPathBoundRewrite(
      sql: String,
      currentBindings: SparkTimeTravelSql.ResolvedSnapshotPinBindings,
      persistedPins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin],
      expectedSql: String
  ): Unit =
    SparkTimeTravelSql
      .rewriteSnapshotPinsByDataPath(sql, currentBindings, persistedPins)
      .fold(error => fail(error), identity) shouldBe expectedSql

  private def assertSurfaceRewrite(
      surface: SparkTimeTravelSql.PinRewriteSurface,
      sql: String,
      currentBindings: SparkTimeTravelSql.ResolvedSnapshotPinBindings,
      persistedPins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin],
      expectedSql: String,
      expectedOccurrences: Int
  ): Unit = {
    val rewritten = SparkTimeTravelSql
      .rewritePinnedReadSurface(surface, sql, currentBindings, persistedPins)
      .fold(error => fail(error), identity)
    rewritten.sql shouldBe expectedSql
    rewritten.pinnedOccurrenceCount shouldBe expectedOccurrences
    noException should be thrownBy CatalystSqlParser.parsePlan(rewritten.sql)
  }

  private def assertCheckpointFailure(
      operation: SparkTimeTravelSql.PinIdentityOperation,
      checkpoint: SparkTimeTravelSql.PinBindingCheckpoint,
      currentBindings: SparkTimeTravelSql.ResolvedSnapshotPinBindings,
      persistedPins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin],
      expectedDetail: String,
      createArtifactsCleaned: Boolean,
      refreshRestored: Boolean
  ): Unit = {
    val failure = SparkTimeTravelSql
      .verifySnapshotPinBindingsAt(operation, checkpoint, currentBindings, persistedPins)
      .left
      .toOption
      .getOrElse(fail(s"$operation $checkpoint unexpectedly accepted a TOCTOU source rebind"))
    failure.detail.toLowerCase should include(expectedDetail)
    failure.detail.toUpperCase should not include "FULL_REFRESH"
    failure.createArtifactsCleaned shouldBe createArtifactsCleaned
    failure.refreshRestored shouldBe refreshRestored
    failure.watermarksUnchanged shouldBe true
    failure.consumedChangesUnchanged shouldBe true
    failure.ctasRetried shouldBe false
    failure.preVersionUnchanged shouldBe true
    failure.createPostCheckOutsideRetry shouldBe true
    failure.refreshMarkersUnchanged shouldBe true
  }

  describe("Fabric V1 snapshot-pin resolution") {
    it("resolves the exact backticked arc_sql_db_bi windowed staging pin by DeltaLog.dataPath") {
      val operational = sourceIdentity(OperationalArmCollection)
      val pins = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> operational)
      )
      pins.map(_.pin.tableRef) shouldBe Seq(ExactArmCollectionRef)
      pins.map(_.pin.segments) shouldBe Seq(Seq("arc_sql_db_bi", "arm_collection_dim"))
      pins.head.sqlVisibleSource.matchesAlias(ExactArmCollectionRef) shouldBe true
      pins.head.sqlVisibleSource.deltaLogDataPath shouldBe operational.deltaLogDataPath
      pins.head.sqlVisibleSource.deltaTableMetadataId shouldBe operational.deltaTableMetadataId
      pins.head.emitsResolved shouldBe OperationalArmCollection
      validate(SparkTimeTravelSql.PinIdentityOperation.Create, Seq(operational), pins, None) shouldBe Right(())

      assertApplied(
        windowedStagingSql,
        Seq(operational),
        pins,
        Seq(s"$OperationalArmCollection=VERSION AS OF 46094")
      )
    }

    it("uses one binding result at all six compiler and seven snapshot-pin sites") {
      val operational = sourceIdentity(OperationalArmCollection)
      val pins = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> operational)
      )
      val resolved = bindings(Seq(operational), pins)
      val sites    = SparkTimeTravelSql.BindingSite.CompilerSites ++ SparkTimeTravelSql.BindingSite.PinSites

      SparkTimeTravelSql.BindingSite.CompilerSites should have size 6
      SparkTimeTravelSql.BindingSite.PinSites should have size 7
      sites should have size 13
      sites should contain(SparkTimeTravelSql.BindingSite.RefreshFreshSchemasShortToQual)
      sites.foreach { site =>
        (SparkTimeTravelSql.bindingFor(resolved, site) eq resolved) shouldBe true
      }
    }

    it("hard-fails before compile or CTAS for every physical pinned-source resolution failure") {
      val source = sourceIdentity(OperationalArmCollection)
      val pins = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> source)
      )
      val ambiguousSource = sourceIdentity("__fabric_encoded_duplicate.arm_collection_dim").copy(
        deltaLogDataPath = source.deltaLogDataPath
      )
      val zeroId = "00000000-0000-0000-0000-000000000000"
      val cases = Seq(
        (
          Seq(source),
          pins,
          Seq(SparkTimeTravelSql.PinResolutionFailure(pins.head.pin, "physical resolution exception")),
          "resolution"
        ),
        (
          Seq(source.copy(deltaLogDataPath = "")),
          pins,
          Seq.empty[SparkTimeTravelSql.PinResolutionFailure],
          "datapath"
        ),
        (
          Seq(source.copy(deltaTableMetadataId = "")),
          pins,
          Seq.empty[SparkTimeTravelSql.PinResolutionFailure],
          "metadata.id"
        ),
        (
          Seq(source.copy(deltaTableMetadataId = zeroId)),
          pins,
          Seq.empty[SparkTimeTravelSql.PinResolutionFailure],
          "zero"
        ),
        (
          Seq(source, ambiguousSource),
          pins,
          Seq.empty[SparkTimeTravelSql.PinResolutionFailure],
          "ambiguous"
        )
      )

      cases.foreach { case (sources, resolvedPins, failures, expectedDetail) =>
        assertHardFailure(
          SparkTimeTravelSql.PinIdentityOperation.Create,
          sources,
          resolvedPins,
          None,
          expectedDetail,
          failures
        )
      }
    }

    it("persists a deterministic structured source-identity property with ABFSS paths, UUIDs, and versions") {
      val left  = sourceIdentity("__fabric_encoded_a.foo")
      val right = sourceIdentity("__fabric_encoded_b.foo")
      val sql =
        "SELECT a.id FROM `b`.`foo` VERSION AS OF 9 b JOIN `a`.`foo` VERSION AS OF 7 a ON a.id = b.id"
      val pins = resolvedPins(
        sql,
        Map("`a`.`foo`" -> left, "`b`.`foo`" -> right)
      )
      val resolved = bindings(Seq(left, right), pins)
      val first    = SparkTimeTravelSql.pinnedSourceIdentityProperties(resolved)
      val second   = SparkTimeTravelSql.pinnedSourceIdentityProperties(resolved.copy(pins = resolved.pins.reverse))
      val ordered  = pins.sortBy(_.operationalSource.alias)
      val leftPin  = ordered.head
      val rightPin = ordered.last
      val expected =
        s"""[{"alias":"${leftPin.operationalSource.alias}","deltaLogDataPath":"${leftPin.operationalSource.deltaLogDataPath}","deltaTableMetadataId":"${leftPin.operationalSource.deltaTableMetadataId}","pinRef":"${leftPin.pin.tableRef}","pinSegments":["a","foo"],"version":7},{"alias":"${rightPin.operationalSource.alias}","deltaLogDataPath":"${rightPin.operationalSource.deltaLogDataPath}","deltaTableMetadataId":"${rightPin.operationalSource.deltaTableMetadataId}","pinRef":"${rightPin.pin.tableRef}","pinSegments":["b","foo"],"version":9}]"""

      ordered.foreach { pin =>
        pin.operationalSource.deltaLogDataPath should startWith("abfss://")
        UUID
          .fromString(pin.operationalSource.deltaTableMetadataId)
          .toString shouldBe pin.operationalSource.deltaTableMetadataId
      }
      first shouldBe second
      first shouldBe Map(SparkTimeTravelSql.PinnedSourceIdentitiesPropertyKey -> expected)
      SparkTimeTravelSql.readPinnedSourceIdentityProperties(first, resolved) shouldBe Right(ordered)
    }

    it("round-trips an exact TIMESTAMP AS OF pin without converting it to a version") {
      val source = sourceIdentity(OperationalArmCollection)
      val sql =
        "SELECT d.id FROM `arc_sql_db_bi`.`arm_collection_dim` TIMESTAMP AS OF '2026-09-04 05:00:00' AS d"
      val pins     = resolvedPins(sql, Map(ExactArmCollectionRef -> source))
      val resolved = bindings(Seq(source), pins)
      val expected =
        s"""[{"alias":"${source.alias}","deltaLogDataPath":"${source.deltaLogDataPath}","deltaTableMetadataId":"${source.deltaTableMetadataId}","pinRef":"$ExactArmCollectionRef","pinSegments":["arc_sql_db_bi","arm_collection_dim"],"timestamp":"2026-09-04 05:00:00"}]"""
      val properties = SparkTimeTravelSql.pinnedSourceIdentityProperties(resolved)

      pins.map(_.pin.clause) shouldBe Seq("TIMESTAMP AS OF '2026-09-04 05:00:00'")
      properties shouldBe Map(SparkTimeTravelSql.PinnedSourceIdentitiesPropertyKey -> expected)
      SparkTimeTravelSql.readPinnedSourceIdentityProperties(properties, resolved) shouldBe Right(pins)
    }

    it("hard-fails bounded property reads for malformed, non-object, incomplete, duplicate, and stale entries") {
      val source = sourceIdentity(OperationalArmCollection)
      val pins = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> source)
      )
      val current = bindings(Seq(source), pins)
      val key     = SparkTimeTravelSql.PinnedSourceIdentitiesPropertyKey
      val valid =
        s"""[{"alias":"${source.alias}","deltaLogDataPath":"${source.deltaLogDataPath}","deltaTableMetadataId":"${source.deltaTableMetadataId}","pinRef":"$ExactArmCollectionRef","pinSegments":["arc_sql_db_bi","arm_collection_dim"],"version":46094}]"""
      val duplicateAlias =
        valid.dropRight(1) +
          s""",{"alias":"${source.alias}","deltaLogDataPath":"abfss://workspace@onelake.dfs.fabric.microsoft.com/other/Tables/dim","deltaTableMetadataId":"11111111-1111-1111-1111-111111111111","pinRef":"`other`.`dim`","pinSegments":["other","dim"],"version":3}]"""
      val duplicatePinRef =
        valid.dropRight(1) +
          s""",{"alias":"__fabric_encoded_other.dim","deltaLogDataPath":"abfss://workspace@onelake.dfs.fabric.microsoft.com/other/Tables/dim","deltaTableMetadataId":"22222222-2222-2222-2222-222222222222","pinRef":"$ExactArmCollectionRef","pinSegments":["arc_sql_db_bi","arm_collection_dim"],"version":3}]"""
      val stale =
        valid.replace(source.alias, "__fabric_encoded_stale.arm_collection_dim")

      Seq(
        Map(key -> ("x" * (SparkTimeTravelSql.MaxPinnedSourceIdentitiesPropertyBytes + 1))) -> "size",
        Map(key -> "{")                                                                     -> "malformed",
        Map(key -> "null")                                                                  -> "object",
        Map.empty[String, String]                                                           -> "missing",
        Map(key -> (valid.dropRight(1) + """,{"unexpected":true}]"""))                      -> "extra",
        Map(key -> duplicateAlias)                                                          -> "duplicate alias",
        Map(key -> duplicatePinRef)                                                         -> "duplicate pinref",
        Map(key -> stale)                                                                   -> "stale"
      ).foreach { case (properties, detail) =>
        assertPropertyReadFails(properties, current, detail)
      }
    }

    it("keeps CREATE, REFRESH, and ADVANCE SOURCE VERSIONS on independently resolved pins") {
      val leftSource  = sourceIdentity("__fabric_encoded_a.foo")
      val rightSource = sourceIdentity("__fabric_encoded_b.foo")
      val sql =
        "SELECT a.id FROM `a`.`foo` VERSION AS OF 7 a JOIN `b`.`foo` VERSION AS OF 9 b ON a.id = b.id"
      val sources = Seq(leftSource, rightSource)
      val pins = resolvedPins(
        sql,
        Map("`a`.`foo`" -> leftSource, "`b`.`foo`" -> rightSource)
      )
      val resolved = bindings(sources, pins)

      val atCreate = assertApplied(
        sql,
        sources,
        pins,
        Seq(s"${leftSource.alias}=VERSION AS OF 7", s"${rightSource.alias}=VERSION AS OF 9")
      )
      val atRefresh = assertApplied(
        sql,
        sources,
        pins,
        Seq(s"${leftSource.alias}=VERSION AS OF 7", s"${rightSource.alias}=VERSION AS OF 9"),
        pins
      )
      atRefresh shouldBe atCreate

      val advanced = SparkTimeTravelSql
        .repinVersions(
          sql,
          resolved,
          pins,
          Map("a.foo" -> 8L, "b.foo" -> 10L)
        )
        .fold(error => fail(error), identity)
      advanced.currentVersions shouldBe Map(leftSource.alias -> 7L, rightSource.alias -> 9L)
      advanced.targetVersions shouldBe Map(leftSource.alias -> 8L, rightSource.alias -> 10L)
      advanced.pins shouldBe
        Seq(s"${leftSource.alias}=VERSION AS OF 8", s"${rightSource.alias}=VERSION AS OF 10").sorted
      advanced.querySql should include("`a`.`foo` VERSION AS OF 8")
      advanced.querySql should include("`b`.`foo` VERSION AS OF 10")
      withoutVersionValues(advanced.querySql) shouldBe withoutVersionValues(sql)
      resolved.sourceIdentities shouldBe sources
    }

    it("rejects ADVANCE other_catalog.db.foo by suffix or case instead of matching persisted db.foo") {
      val persistedSource = sourceIdentity("__fabric_encoded.db.foo")
      val sql             = "SELECT d.id FROM db.foo VERSION AS OF 7 AS d"
      val pins            = resolvedPins(sql, Map("db.foo" -> persistedSource))
      val resolved        = bindings(Seq(persistedSource), pins)

      UUID.fromString(persistedSource.deltaTableMetadataId).toString shouldBe persistedSource.deltaTableMetadataId
      Seq("other_catalog.db.foo", "OTHER_CATALOG.DB.FOO").foreach { requestedSource =>
        SparkTimeTravelSql
          .repinVersions(
            sql,
            resolved,
            pins,
            Map(requestedSource -> 8L)
          )
          .left
          .toOption
          .getOrElse(fail(s"$requestedSource unexpectedly matched the persisted physical source"))
          .toLowerCase should include("advance source identifier")
      }
    }

    it("hard-fails globally duplicated pinned short names instead of falling back to FULL_REFRESH") {
      val left  = sourceIdentity("__fabric_encoded_a.foo")
      val right = sourceIdentity("__fabric_encoded_b.foo")
      val sql =
        "SELECT a.id FROM a.foo VERSION AS OF 7 a JOIN b.foo VERSION AS OF 9 b ON a.id = b.id"
      val pins = resolvedPins(sql, Map("a.foo" -> left, "b.foo" -> right))

      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.Create,
        Seq(left, right),
        pins,
        None,
        "duplicate short"
      )
    }

    it("hard-fails a pinned a.foo plus a live distinct b.foo in CREATE, dryCompile, and dryRewrite") {
      val pinnedSource = sourceIdentity("__fabric_encoded_a.foo")
      val liveSource   = sourceIdentity("__fabric_encoded_b.foo")
      val sql =
        "SELECT a.id FROM a.foo VERSION AS OF 7 a JOIN b.foo b ON a.id = b.id"
      val pins     = resolvedPins(sql, Map("a.foo" -> pinnedSource))
      val resolved = bindings(Seq(pinnedSource, liveSource), pins)

      resolved.sourceTables shouldBe Seq(pinnedSource.alias, liveSource.alias)
      Seq(
        SparkTimeTravelSql.PinIdentityOperation.Create,
        SparkTimeTravelSql.PinIdentityOperation.DryCompile,
        SparkTimeTravelSql.PinIdentityOperation.DryRewrite
      ).foreach { operation =>
        assertHardFailure(
          operation,
          resolved.sourceIdentities,
          resolved.pins,
          None,
          "duplicate short"
        )
      }
    }

    it("hard-fails same db and table names from two catalogs when their pinned compiler short names collide") {
      val left  = sourceIdentity("__fabric_encoded_catalog_one.db.foo")
      val right = sourceIdentity("__fabric_encoded_catalog_two.db.foo")
      val sql =
        "SELECT a.id FROM catalog_one.db.foo VERSION AS OF 7 a " +
          "JOIN catalog_two.db.foo VERSION AS OF 9 b ON a.id = b.id"
      val pins = resolvedPins(
        sql,
        Map("catalog_one.db.foo" -> left, "catalog_two.db.foo" -> right)
      )

      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.Create,
        Seq(left, right),
        pins,
        None,
        "duplicate short"
      )
    }

    it("allows a repeated self-join only when every pin has one physical id, path, and canonical clause") {
      val source = sourceIdentity("__fabric_encoded_self_join.foo")
      val sql =
        "SELECT left_source.id FROM db.foo VERSION AS OF 7 AS left_source " +
          "JOIN db.foo VERSION AS OF 7 AS right_source ON left_source.id = right_source.id"
      val pins = resolvedPins(sql, Map("db.foo" -> source))

      pins should have size 2
      pins.map(_.operationalSource.deltaLogDataPath).distinct shouldBe Seq(source.deltaLogDataPath)
      pins.map(_.operationalSource.deltaTableMetadataId).distinct shouldBe Seq(source.deltaTableMetadataId)
      pins.map(_.pin.clause).distinct shouldBe Seq("VERSION AS OF 7")
      validate(SparkTimeTravelSql.PinIdentityOperation.Create, Seq(source), pins, None) shouldBe Right(())
    }

    it(
      "hard-fails a same-physical source at two canonical versions, including Foo and foo in a case-insensitive catalog"
    ) {
      val source = sourceIdentity("__fabric_encoded_case_db.foo")
      val twoVersions =
        "SELECT left_source.id FROM db.foo VERSION AS OF 7 AS left_source " +
          "JOIN db.foo VERSION AS OF 8 AS right_source ON left_source.id = right_source.id"
      val caseFoldedVersions =
        "SELECT upper_source.id FROM `case_db`.`Foo` VERSION AS OF 7 AS upper_source " +
          "JOIN `case_db`.`foo` version as of 8 AS lower_source ON upper_source.id = lower_source.id"

      Seq(twoVersions, caseFoldedVersions).foreach { sql =>
        val observed = SparkTimeTravelSql.pinTelemetry(sql, Seq(source.alias))
        observed.status shouldBe TimeTravelPinStatus.CompileFailed
        observed.reason shouldBe TimeTravelPinReason.UnsupportedPinShape
      }
      val failedPin = SparkTimeTravelSql.SnapshotPin("db.foo", "VERSION AS OF 8")
      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.Create,
        Seq(source),
        Seq.empty,
        None,
        "canonical clause",
        Seq(SparkTimeTravelSql.PinResolutionFailure(failedPin, "same physical source has multiple canonical clauses"))
      )
    }

    it("uses the existing parser's CTE and subquery traversal for every repeated self-join pin") {
      val operational = sourceIdentity(OperationalArmCollection)
      val sql =
        """WITH source AS (
          |  SELECT left_source.id
          |  FROM `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094 AS left_source
          |  JOIN `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094 AS right_source
          |    ON left_source.id = right_source.id
          |)
          |SELECT source.id,
          |       (SELECT MAX(scalar_source.id)
          |        FROM (WITH nested_source AS (
          |          SELECT n.id FROM `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094 AS n
          |        ) SELECT id FROM nested_source) scalar_source) AS max_id
          |FROM source
          |WHERE EXISTS (
          |  SELECT 1 FROM `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094 AS e
          |  WHERE e.id = source.id
          |)""".stripMargin
      val pins = resolvedPins(sql, Map(ExactArmCollectionRef -> operational))

      pins should have size 4
      pins.map(_.operationalSource.deltaLogDataPath).distinct shouldBe Seq(operational.deltaLogDataPath)
      pins.map(_.pin.clause).distinct shouldBe Seq("VERSION AS OF 46094")
      validate(SparkTimeTravelSql.PinIdentityOperation.Create, Seq(operational), pins, None) shouldBe Right(())
      assertApplied(
        sql,
        Seq(operational),
        pins,
        Seq(s"$OperationalArmCollection=VERSION AS OF 46094")
      )
    }

    it("matches a quoted identifier that itself contains dots without splitting its segments") {
      val operational = sourceIdentity("__fabric_encoded_quoted.arm_collection_dim")
      val exactRef    = "`arc.sql.db`.`arm.collection.dim`"
      val sql =
        "SELECT q.id FROM `arc.sql.db`.`arm.collection.dim` VERSION AS OF 46094 AS q"
      val pins = resolvedPins(sql, Map(exactRef -> operational))

      pins.head.pin.segments shouldBe Seq("arc.sql.db", "arm.collection.dim")
      assertApplied(sql, Seq(operational), pins, Seq(s"${operational.alias}=VERSION AS OF 46094"))
    }

    it("preserves case-sensitive Foo and foo pins as separate exact references") {
      val upper = sourceIdentity("__fabric_encoded_case.Foo")
      val lower = sourceIdentity("__fabric_encoded_case.foo")
      val sql =
        "SELECT upper_source.id FROM `case_db`.`Foo` VERSION AS OF 7 upper_source " +
          "JOIN `case_db`.`foo` VERSION AS OF 9 lower_source ON upper_source.id = lower_source.id"
      val pins = resolvedPins(sql, Map("`case_db`.`Foo`" -> upper, "`case_db`.`foo`" -> lower))

      assertApplied(
        sql,
        Seq(upper, lower),
        pins,
        Seq(s"${upper.alias}=VERSION AS OF 7", s"${lower.alias}=VERSION AS OF 9")
      )
    }

    it("refuses a CTE shadow of a pinned source explicitly") {
      val operational = sourceIdentity(OperationalArmCollection)
      val sql =
        "WITH arm_collection_dim AS (SELECT 1 AS id) " +
          "SELECT p.id FROM `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094 AS p " +
          "JOIN arm_collection_dim shadow ON p.id = shadow.id"

      val observed = telemetry(sql, Seq(operational), Seq.empty)
      observed.status shouldBe TimeTravelPinStatus.CompileFailed
      observed.reason shouldBe TimeTravelPinReason.UnsupportedPinShape
      observed.pins shouldBe empty
      observed.detail.getOrElse(fail("missing CTE-shadow refusal detail")) should include(
        "pinned in one place and read live in another"
      )
    }

    it("fails a namespace rebind before REFRESH instead of suffix-matching a different source") {
      val current = sourceIdentity(OperationalArmCollection)
      val rebound = sourceIdentity("__fabric_encoded_rebound.arm_collection_dim")
      val bareSql = "SELECT id FROM arm_collection_dim VERSION AS OF 46094"
      val pins    = resolvedPins(bareSql, Map("arm_collection_dim" -> rebound))

      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.Refresh,
        Seq(current),
        pins,
        Some(pins),
        "source"
      )
    }

    it("fails before REFRESH or ADVANCE when a pinned DeltaLog.dataPath changes") {
      val operational = sourceIdentity(OperationalArmCollection)
      val persisted = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> operational)
      )
      val rebound = persisted.map { pin =>
        pin.copy(
          operationalSource = pin.operationalSource.copy(
            deltaLogDataPath =
              "abfss://workspace@onelake.dfs.fabric.microsoft.com/other-lakehouse/Tables/arm_collection_dim"
          )
        )
      }

      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.Refresh,
        Seq(operational),
        rebound,
        Some(persisted),
        "deltalog.datapath"
      )
      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.Advance,
        Seq(operational),
        rebound,
        Some(persisted),
        "deltalog.datapath"
      )
    }

    it("fails before REFRESH or ADVANCE when the same-name source has a new Delta metadata id") {
      val operational = sourceIdentity(OperationalArmCollection)
      val persisted = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> operational)
      )
      val rebound = persisted.map { pin =>
        pin.copy(
          operationalSource = pin.operationalSource.copy(deltaTableMetadataId = "replacement-delta-table-id")
        )
      }

      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.Refresh,
        Seq(operational),
        rebound,
        Some(persisted),
        "metadata.id"
      )
      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.Advance,
        Seq(operational),
        rebound,
        Some(persisted),
        "metadata.id"
      )
    }

    it("rewrites every repeated VERSION AS OF pin to its verified DeltaLog.dataPath despite an alias rebind") {
      val verified = sourceIdentity(OperationalArmCollection)
      val rebound = verified.copy(
        deltaLogDataPath =
          "abfss://workspace@onelake.dfs.fabric.microsoft.com/rebound-lakehouse/Tables/arm_collection_dim",
        deltaTableMetadataId = "11111111-1111-1111-1111-111111111111"
      )
      val sql =
        s"""SELECT left_source.arm_collection_id
           |FROM $ExactArmCollectionRef VERSION AS OF 46094 AS left_source
           |JOIN $ExactArmCollectionRef VERSION AS OF 46094 AS right_source
           |  ON left_source.arm_collection_id = right_source.arm_collection_id""".stripMargin
      val persisted = resolvedPins(sql, Map(ExactArmCollectionRef -> verified))
      val current   = resolvedPins(sql, Map(ExactArmCollectionRef -> rebound))
      val expected =
        s"""SELECT left_source.arm_collection_id
           |FROM ${dataPathRelation(verified)} VERSION AS OF 46094 AS left_source
           |JOIN ${dataPathRelation(verified)} VERSION AS OF 46094 AS right_source
           |  ON left_source.arm_collection_id = right_source.arm_collection_id""".stripMargin

      assertPathBoundRewrite(sql, bindings(Seq(rebound), current), persisted, expected)
    }

    it("rewrites every repeated TIMESTAMP AS OF pin to its verified DeltaLog.dataPath unchanged") {
      val verified = sourceIdentity(OperationalArmCollection)
      val rebound = verified.copy(
        deltaLogDataPath =
          "abfss://workspace@onelake.dfs.fabric.microsoft.com/rebound-lakehouse/Tables/arm_collection_dim",
        deltaTableMetadataId = "22222222-2222-2222-2222-222222222222"
      )
      val sql =
        s"""SELECT left_source.arm_collection_id
           |FROM $ExactArmCollectionRef TIMESTAMP AS OF '2026-09-04 05:00:00' AS left_source
           |JOIN $ExactArmCollectionRef TIMESTAMP AS OF '2026-09-04 05:00:00' AS right_source
           |  ON left_source.arm_collection_id = right_source.arm_collection_id""".stripMargin
      val persisted = resolvedPins(sql, Map(ExactArmCollectionRef -> verified))
      val current   = resolvedPins(sql, Map(ExactArmCollectionRef -> rebound))
      val expected =
        s"""SELECT left_source.arm_collection_id
           |FROM ${dataPathRelation(verified)} TIMESTAMP AS OF '2026-09-04 05:00:00' AS left_source
           |JOIN ${dataPathRelation(verified)} TIMESTAMP AS OF '2026-09-04 05:00:00' AS right_source
           |  ON left_source.arm_collection_id = right_source.arm_collection_id""".stripMargin

      assertPathBoundRewrite(sql, bindings(Seq(rebound), current), persisted, expected)
    }

    it("path-binds every repeated VERSION occurrence in user, compiler initial-load, and refresh-rewriter SQL") {
      val verified = sourceIdentity(OperationalArmCollection)
      val rebound = verified.copy(
        deltaLogDataPath =
          "abfss://workspace@onelake.dfs.fabric.microsoft.com/rebound-lakehouse/Tables/arm_collection_dim"
      )
      val userSql =
        s"""SELECT left_source.arm_collection_id
           |FROM $ExactArmCollectionRef VERSION AS OF 46094 AS left_source
           |JOIN $ExactArmCollectionRef VERSION AS OF 46094 AS right_source
           |  ON left_source.arm_collection_id = right_source.arm_collection_id""".stripMargin
      val persisted = resolvedPins(userSql, Map(ExactArmCollectionRef -> verified))
      val current   = resolvedPins(userSql, Map(ExactArmCollectionRef -> rebound))
      val source    = dataPathRelation(verified)
      val initialLoadSql =
        "SELECT left_source.arm_collection_id FROM memory.main.arm_collection_dim AS left_source " +
          "JOIN memory.main.arm_collection_dim AS right_source ON left_source.arm_collection_id = right_source.arm_collection_id"
      val rewrittenInitialLoad =
        s"SELECT left_source.arm_collection_id FROM $source VERSION AS OF 46094 AS left_source " +
          s"JOIN $source VERSION AS OF 46094 AS right_source ON left_source.arm_collection_id = right_source.arm_collection_id"
      val emittedSql =
        "INSERT INTO mv SELECT arm_collection_id FROM memory.main.arm_collection_dim " +
          "UNION ALL SELECT arm_collection_id FROM memory.main.arm_collection_dim"
      val rewrittenEmitted =
        s"INSERT INTO mv SELECT arm_collection_id FROM $source VERSION AS OF 46094 " +
          s"UNION ALL SELECT arm_collection_id FROM $source VERSION AS OF 46094"
      val currentBindings = bindings(Seq(rebound), current)

      assertSurfaceRewrite(
        SparkTimeTravelSql.PinRewriteSurface.UserFullQuery,
        userSql,
        currentBindings,
        persisted,
        userSql.replace(ExactArmCollectionRef, source),
        2
      )
      assertSurfaceRewrite(
        SparkTimeTravelSql.PinRewriteSurface.CompilerInitialLoad,
        initialLoadSql,
        currentBindings,
        persisted,
        rewrittenInitialLoad,
        2
      )
      assertSurfaceRewrite(
        SparkTimeTravelSql.PinRewriteSurface.SparkRefreshRewriterEmitted,
        emittedSql,
        currentBindings,
        persisted,
        rewrittenEmitted,
        2
      )
    }

    it("path-binds every repeated TIMESTAMP occurrence in user, compiler initial-load, and refresh-rewriter SQL") {
      val verified = sourceIdentity(OperationalArmCollection)
      val rebound = verified.copy(
        deltaLogDataPath =
          "abfss://workspace@onelake.dfs.fabric.microsoft.com/rebound-lakehouse/Tables/arm_collection_dim"
      )
      val timestamp = "'2026-09-04 05:00:00'"
      val userSql =
        s"""SELECT left_source.arm_collection_id
           |FROM $ExactArmCollectionRef TIMESTAMP AS OF $timestamp AS left_source
           |JOIN $ExactArmCollectionRef TIMESTAMP AS OF $timestamp AS right_source
           |  ON left_source.arm_collection_id = right_source.arm_collection_id""".stripMargin
      val persisted = resolvedPins(userSql, Map(ExactArmCollectionRef -> verified))
      val current   = resolvedPins(userSql, Map(ExactArmCollectionRef -> rebound))
      val source    = dataPathRelation(verified)
      val initialLoadSql =
        "SELECT left_source.arm_collection_id FROM memory.main.arm_collection_dim AS left_source " +
          "JOIN memory.main.arm_collection_dim AS right_source ON left_source.arm_collection_id = right_source.arm_collection_id"
      val rewrittenInitialLoad =
        s"SELECT left_source.arm_collection_id FROM $source TIMESTAMP AS OF $timestamp AS left_source " +
          s"JOIN $source TIMESTAMP AS OF $timestamp AS right_source ON left_source.arm_collection_id = right_source.arm_collection_id"
      val emittedSql =
        "INSERT INTO mv SELECT arm_collection_id FROM memory.main.arm_collection_dim " +
          "UNION ALL SELECT arm_collection_id FROM memory.main.arm_collection_dim"
      val rewrittenEmitted =
        s"INSERT INTO mv SELECT arm_collection_id FROM $source TIMESTAMP AS OF $timestamp " +
          s"UNION ALL SELECT arm_collection_id FROM $source TIMESTAMP AS OF $timestamp"
      val currentBindings = bindings(Seq(rebound), current)

      assertSurfaceRewrite(
        SparkTimeTravelSql.PinRewriteSurface.UserFullQuery,
        userSql,
        currentBindings,
        persisted,
        userSql.replace(ExactArmCollectionRef, source),
        2
      )
      assertSurfaceRewrite(
        SparkTimeTravelSql.PinRewriteSurface.CompilerInitialLoad,
        initialLoadSql,
        currentBindings,
        persisted,
        rewrittenInitialLoad,
        2
      )
      assertSurfaceRewrite(
        SparkTimeTravelSql.PinRewriteSurface.SparkRefreshRewriterEmitted,
        emittedSql,
        currentBindings,
        persisted,
        rewrittenEmitted,
        2
      )
    }

    it("rewrites every final incremental emitted SQL occurrence by verified path for VERSION and TIMESTAMP pins") {
      val verified = sourceIdentity(OperationalArmCollection)
      val versionSql =
        s"SELECT d.arm_collection_id FROM $ExactArmCollectionRef VERSION AS OF 46094 AS d"
      val versionPins = resolvedPins(versionSql, Map(ExactArmCollectionRef -> verified))
      val versionProgram = Seq(
        "CREATE OR REPLACE TEMP VIEW openivm_old_mv AS SELECT arm_collection_id FROM memory.main.arm_collection_dim",
        "MERGE INTO mv USING (SELECT arm_collection_id FROM memory.main.arm_collection_dim " +
          "UNION ALL SELECT arm_collection_id FROM memory.main.arm_collection_dim) delta ON mv.arm_collection_id = delta.arm_collection_id"
      )
      val versionExpected = Seq(
        s"CREATE OR REPLACE TEMP VIEW openivm_old_mv AS SELECT arm_collection_id FROM ${dataPathRelation(verified)} VERSION AS OF 46094",
        s"MERGE INTO mv USING (SELECT arm_collection_id FROM ${dataPathRelation(verified)} VERSION AS OF 46094 " +
          s"UNION ALL SELECT arm_collection_id FROM ${dataPathRelation(verified)} VERSION AS OF 46094) delta ON mv.arm_collection_id = delta.arm_collection_id"
      )

      SparkTimeTravelSql
        .rewriteEmittedSnapshotPinsByDataPath(versionProgram, bindings(Seq(verified), versionPins), versionPins)
        .fold(error => fail(error), identity) shouldBe versionExpected

      val timestampSql =
        s"SELECT d.arm_collection_id FROM $ExactArmCollectionRef TIMESTAMP AS OF '2026-09-04 05:00:00' AS d"
      val timestampPins = resolvedPins(timestampSql, Map(ExactArmCollectionRef -> verified))
      val timestampProgram =
        Seq("INSERT INTO mv SELECT arm_collection_id FROM memory.main.arm_collection_dim")
      val timestampExpected =
        Seq(
          s"INSERT INTO mv SELECT arm_collection_id FROM ${dataPathRelation(verified)} TIMESTAMP AS OF '2026-09-04 05:00:00'"
        )

      SparkTimeTravelSql
        .rewriteEmittedSnapshotPinsByDataPath(
          timestampProgram,
          bindings(Seq(verified), timestampPins),
          timestampPins
        )
        .fold(error => fail(error), identity) shouldBe timestampExpected
    }

    it("hard-fails CREATE write and REFRESH apply before movement when a same-path source has a new metadata id") {
      val verified = sourceIdentity(OperationalArmCollection)
      val rebound  = verified.copy(deltaTableMetadataId = "33333333-3333-3333-3333-333333333333")
      val persisted = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> verified)
      )
      val current = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> rebound)
      )
      val currentBindings = bindings(Seq(rebound), current)

      assertCheckpointFailure(
        SparkTimeTravelSql.PinIdentityOperation.Create,
        SparkTimeTravelSql.PinBindingCheckpoint.CreateBeforeWrite,
        currentBindings,
        persisted,
        "metadata.id",
        createArtifactsCleaned = false,
        refreshRestored = false
      )
      assertCheckpointFailure(
        SparkTimeTravelSql.PinIdentityOperation.Refresh,
        SparkTimeTravelSql.PinBindingCheckpoint.RefreshBeforeApply,
        currentBindings,
        persisted,
        "metadata.id",
        createArtifactsCleaned = false,
        refreshRestored = false
      )
    }

    it("hard-fails after staging and before MERGE without moving the pre-version, watermarks, or consumed changes") {
      val verified = sourceIdentity(OperationalArmCollection)
      val rebound = verified.copy(
        deltaLogDataPath =
          "abfss://workspace@onelake.dfs.fabric.microsoft.com/rebound-lakehouse/Tables/arm_collection_dim",
        deltaTableMetadataId = "55555555-5555-5555-5555-555555555555"
      )
      val persisted = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> verified)
      )
      val current = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> rebound)
      )

      assertCheckpointFailure(
        SparkTimeTravelSql.PinIdentityOperation.Refresh,
        SparkTimeTravelSql.PinBindingCheckpoint.RefreshAfterStagingBeforeMerge,
        bindings(Seq(rebound), current),
        persisted,
        "deltalog.datapath",
        createArtifactsCleaned = false,
        refreshRestored = false
      )
    }

    it("detects a rebind during movement and compensates CREATE and REFRESH state at the post-check") {
      val verified = sourceIdentity(OperationalArmCollection)
      val rebound = verified.copy(
        deltaLogDataPath =
          "abfss://workspace@onelake.dfs.fabric.microsoft.com/rebound-lakehouse/Tables/arm_collection_dim",
        deltaTableMetadataId = "44444444-4444-4444-4444-444444444444"
      )
      val persisted = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> verified)
      )
      val current = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> rebound)
      )
      val currentBindings = bindings(Seq(rebound), current)

      assertCheckpointFailure(
        SparkTimeTravelSql.PinIdentityOperation.Create,
        SparkTimeTravelSql.PinBindingCheckpoint.CreateAfterWrite,
        currentBindings,
        persisted,
        "deltalog.datapath",
        createArtifactsCleaned = true,
        refreshRestored = false
      )
      assertCheckpointFailure(
        SparkTimeTravelSql.PinIdentityOperation.Refresh,
        SparkTimeTravelSql.PinBindingCheckpoint.RefreshAfterApply,
        currentBindings,
        persisted,
        "deltalog.datapath",
        createArtifactsCleaned = false,
        refreshRestored = true
      )
    }

    it("detects a same-path Delta recreate between PRE, read, and POST checks") {
      val verified  = sourceIdentity(OperationalArmCollection)
      val recreated = verified.copy(deltaTableMetadataId = "66666666-6666-6666-6666-666666666666")
      val persisted = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> verified)
      )
      val current = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> recreated)
      )
      val currentBindings = bindings(Seq(recreated), current)

      assertCheckpointFailure(
        SparkTimeTravelSql.PinIdentityOperation.Create,
        SparkTimeTravelSql.PinBindingCheckpoint.CreateAfterWrite,
        currentBindings,
        persisted,
        "metadata.id",
        createArtifactsCleaned = true,
        refreshRestored = false
      )
      assertCheckpointFailure(
        SparkTimeTravelSql.PinIdentityOperation.Refresh,
        SparkTimeTravelSql.PinBindingCheckpoint.RefreshAfterApply,
        currentBindings,
        persisted,
        "metadata.id",
        createArtifactsCleaned = false,
        refreshRestored = true
      )
    }

    it("requires recreation for legacy pinned metadata that lacks persisted physical identities") {
      val operational = sourceIdentity(OperationalArmCollection)
      val pins = resolvedPins(
        windowedStagingSql,
        Map(ExactArmCollectionRef -> operational)
      )

      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.IdempotentCreate,
        Seq(operational),
        pins,
        None,
        "recreate"
      )
      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.Refresh,
        Seq(operational),
        pins,
        None,
        "recreate"
      )
      assertHardFailure(
        SparkTimeTravelSql.PinIdentityOperation.Advance,
        Seq(operational),
        pins,
        None,
        "recreate"
      )
    }

    it("keeps legacy unpinned metadata valid for idempotent CREATE and REFRESH") {
      val operational = sourceIdentity("__fabric_encoded_unpinned.dim")

      validate(SparkTimeTravelSql.PinIdentityOperation.IdempotentCreate, Seq(operational), Seq.empty, None) shouldBe
        Right(())
      validate(SparkTimeTravelSql.PinIdentityOperation.Refresh, Seq(operational), Seq.empty, None) shouldBe Right(())
      telemetry("SELECT id FROM dim", Seq(operational), Seq.empty).status shouldBe TimeTravelPinStatus.NotApplicable
    }
  }
}
