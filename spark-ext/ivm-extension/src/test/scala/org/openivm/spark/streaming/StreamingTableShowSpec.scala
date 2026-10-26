package org.openivm.spark.streaming

import org.openivm.spark.commands.ShowStreamingTablesCommand
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.time.{Millis, Seconds, Span}

class StreamingTableShowSpec extends AnyFunSpec with StreamingTableTestFixture with Eventually {
  describe("filtered streaming status") {
    it("avoids catalog enumeration for exact names before direct metadata resolution") {
      var listings = 0
      def list(): Seq[Seq[String]] = {
        listings += 1
        Seq(Seq("catalog", "schema", "listed"))
      }
      StreamingTableManager.directOrListedCatalogNames(Some(Seq("catalog", "schema")), Some("target_42"))(
        list()
      ) shouldBe
        Seq(Seq("catalog", "schema", "target_42"))
      listings shouldBe 0
      StreamingTableManager.directOrListedCatalogNames(None, Some("target_42"))(list()) shouldBe Seq(Seq("target_42"))
      listings shouldBe 0
      Seq(Some("target*"), Some("target|other"), Some("target.42"), None).foreach { pattern =>
        StreamingTableManager.directOrListedCatalogNames(Some(Seq("catalog", "schema")), pattern)(list())
      }
      listings shouldBe 4
    }

    it("preserves native lookup cause chains without reporting successful completion") {
      val session = spark.newSession()
      session.conf.set("spark.sql.catalog.direct_status", classOf[DirectStatusCatalog].getName)
      val name = Seq("direct_status", "schema", "known_native_target")
      val target = StreamingTableTarget(
        name,
        StreamingTableMetadata.canonicalIdentity(session, name),
        StreamingTableMetadata.quoteMultipart(name),
        "/warehouse/known_native_target",
        "delta-id",
        "table-id"
      )
      val entry = StreamingTableRegistry.entry(session, StreamingTableRegistry.targetKey(target), target)
      entry.definitionHash = "known-definition"
      entry.queryId = "known-query"
      entry.runId = "known-run"
      try {
        val analysis = new org.apache.spark.sql.AnalysisException(
          "_LEGACY_ERROR_TEMP_2273",
          Map("message" -> "native catalog lookup timed out"),
          Some(new java.io.IOException("OneLake listPaths timeout"))
        )
        Seq[Throwable](
          analysis,
          new IllegalStateException("native load failed", new java.io.IOException("storage timeout"))
        ).foreach { failure =>
          DirectStatusCatalog.failure = Some(failure)
          DirectStatusCatalog.loads.set(0)
          DirectStatusCatalog.listings.set(0)
          val status = StreamingTableManager.show(session, Some(name.dropRight(1)), Some(name.last)).head
          status.status shouldBe "metadata_error"
          status.isActive shouldBe false
          status.queryId shouldBe Some("known-query")
          status.runId shouldBe Some("known-run")
          status.definitionHash shouldBe Some("known-definition")
          status.lastFailure.get should include(failure.getMessage)
          status.lastFailure.get should include("java.io.IOException")
          status.lastFailure.get should not include "may have been moved"
          DirectStatusCatalog.loads.get() shouldBe 1
          DirectStatusCatalog.listings.get() shouldBe 0
        }
      } finally {
        DirectStatusCatalog.failure = None
        StreamingTableRegistry.forget(session, StreamingTableRegistry.targetKey(target))
      }
    }

    it("classifies only connection timeouts as transient catalog failures") {
      val metastore = new RuntimeException(
        "Failed to get database metadata",
        new java.net.SocketTimeoutException("Read timed out")
      )
      StreamingTableManager.transientCatalogFailure(metastore) shouldBe true
      StreamingTableManager.transientCatalogFailure(
        new IllegalStateException("load failed", new java.net.SocketTimeoutException("Read timed out"))
      ) shouldBe true
      StreamingTableManager.transientCatalogFailure(
        new IllegalStateException("native load failed", new java.io.IOException("storage timeout"))
      ) shouldBe false
      StreamingTableManager.transientCatalogFailure(
        new org.apache.spark.sql.catalyst.analysis.NoSuchTableException("schema", "absent")
      ) shouldBe false
    }

    it("retries transient catalog failures until they clear or the retry budget is spent") {
      val session = spark.newSession()
      session.conf.set(org.openivm.spark.common.FeatureGate.StreamingCatalogRetryTimeoutKey, "5s")
      var attempts = 0
      StreamingTableManager.retryTransientCatalogFailure(session) {
        attempts += 1
        if (attempts < 3) throw new RuntimeException("Failed to get database metadata")
        "resolved"
      } shouldBe "resolved"
      attempts shouldBe 3

      attempts = 0
      an[IllegalStateException] should be thrownBy {
        StreamingTableManager.retryTransientCatalogFailure(session) {
          attempts += 1
          throw new IllegalStateException("permanent")
        }
      }
      attempts shouldBe 1

      session.conf.set(org.openivm.spark.common.FeatureGate.StreamingCatalogRetryTimeoutKey, "300ms")
      attempts = 0
      val started = System.nanoTime()
      val error = intercept[RuntimeException] {
        StreamingTableManager.retryTransientCatalogFailure(session) {
          attempts += 1
          throw new RuntimeException("Failed to get database metadata", new java.net.SocketTimeoutException())
        }
      }
      error.getMessage shouldBe "Failed to get database metadata"
      attempts should be > 1
      (System.nanoTime() - started) / 1000000L should be < 5000L
    }

    it("retries a known target's metastore timeout before reporting metadata_error") {
      val session = spark.newSession()
      session.conf.set("spark.sql.catalog.direct_status", classOf[DirectStatusCatalog].getName)
      session.conf.set(org.openivm.spark.common.FeatureGate.StreamingCatalogRetryTimeoutKey, "300ms")
      val name = Seq("direct_status", "schema", "transient_native_target")
      val target = StreamingTableTarget(
        name,
        StreamingTableMetadata.canonicalIdentity(session, name),
        StreamingTableMetadata.quoteMultipart(name),
        "/warehouse/transient_native_target",
        "delta-id",
        "table-id"
      )
      StreamingTableRegistry.entry(session, StreamingTableRegistry.targetKey(target), target)
      try {
        DirectStatusCatalog.failure = Some(
          new RuntimeException("Failed to get database metadata", new java.net.SocketTimeoutException("Read timed out"))
        )
        DirectStatusCatalog.loads.set(0)
        val status = StreamingTableManager.show(session, Some(name.dropRight(1)), Some(name.last)).head
        DirectStatusCatalog.loads.get() should be > 1
        status.status shouldBe "metadata_error"
        status.lastFailure.get should include("Read timed out")
      } finally {
        DirectStatusCatalog.failure = None
        StreamingTableRegistry.forget(session, StreamingTableRegistry.targetKey(target))
      }
    }

    it("reports the session's active run while metastore reads keep timing out") {
      val session = spark.newSession()
      session.conf.set("spark.sql.catalog.direct_status", classOf[DirectStatusCatalog].getName)
      session.conf.set(org.openivm.spark.common.FeatureGate.StreamingCatalogRetryTimeoutKey, "300ms")
      val name = Seq("direct_status", "schema", "transient_active_target")
      val target = StreamingTableTarget(
        name,
        StreamingTableMetadata.canonicalIdentity(session, name),
        StreamingTableMetadata.quoteMultipart(name),
        "/warehouse/transient_active_target",
        "delta-id",
        "table-id"
      )
      val key   = StreamingTableRegistry.targetKey(target)
      val query = session.readStream.format("rate").load().writeStream.format("noop").start()
      try {
        StreamingTableRegistry.register(session, key, target, "active-definition", query)
        DirectStatusCatalog.failure = Some(
          new RuntimeException("Failed to get database metadata", new java.net.SocketTimeoutException("Read timed out"))
        )
        val status = StreamingTableManager.show(session, Some(name.dropRight(1)), Some(name.last)).head
        status.isActive shouldBe true
        status.queryId shouldBe Some(query.id.toString)
        status.runId shouldBe Some(query.runId.toString)
        status.lastFailure shouldBe None
      } finally {
        DirectStatusCatalog.failure = None
        query.stop()
        StreamingTableRegistry.forget(session, key)
      }
    }

    it("bounds and redacts native resolution diagnostics") {
      val error =
        new IllegalStateException("storage failure " + ("x" * 4000), new java.io.IOException("root storage failure"))
      val name = Seq("diagnostic_bound_target")
      val target = StreamingTableTarget(
        name,
        StreamingTableMetadata.canonicalIdentity(spark, name),
        StreamingTableMetadata.quoteMultipart(name),
        "/warehouse/diagnostic_bound_target",
        "delta-id",
        "table-id"
      )
      StreamingTableManager.resolutionFailureDetails(error) should include("java.lang.IllegalStateException")
      val session = spark.newSession()
      session.conf.set("spark.sql.catalog.direct_status", classOf[DirectStatusCatalog].getName)
      val nativeName = Seq("direct_status", "schema", "bounded_native_target")
      val native = target.copy(
        name = nativeName,
        identity = StreamingTableMetadata.canonicalIdentity(session, nativeName),
        sqlIdentifier = StreamingTableMetadata.quoteMultipart(nativeName)
      )
      StreamingTableRegistry.entry(session, StreamingTableRegistry.targetKey(native), native)
      try {
        DirectStatusCatalog.failure = Some(error)
        val status = StreamingTableManager.show(session, Some(nativeName.dropRight(1)), Some(nativeName.last)).head
        status.status shouldBe "metadata_error"
        status.lastFailure.get.length should be <= 2048
        status.lastFailure.get should include("root storage failure")
        StreamingTableManager.resolutionFailureDetails(
          new IllegalStateException("token=private-marker")
        ) should not include "private-marker"
      } finally {
        DirectStatusCatalog.failure = None
        StreamingTableRegistry.forget(session, StreamingTableRegistry.targetKey(native))
      }
    }

    it("loads an exact V2 target without calling listTables") {
      val session = spark.newSession()
      session.conf.set("spark.sql.catalog.direct_status", classOf[DirectStatusCatalog].getName)
      DirectStatusCatalog.listings.set(0)
      DirectStatusCatalog.loads.set(0)
      StreamingTableManager.show(session, Some(Seq("direct_status", "schema")), Some("missing_target")) shouldBe empty
      DirectStatusCatalog.listings.get() shouldBe 0
      DirectStatusCatalog.loads.get() shouldBe 1
      StreamingTableManager.show(session, Some(Seq("direct_status", "schema")), Some("missing*")) shouldBe empty
      DirectStatusCatalog.listings.get() shouldBe 1
    }

    it("retains registered active rows and failures without querying unrelated targets") {
      createSource("show_active_source")
      val status = createStreaming(
        "show_active_target",
        readStream("show_active_source"),
        "SELECT id, value, part FROM STREAM show_active_source"
      )
      val active = spark.sql("SHOW STREAMING TABLES LIKE 'show_active_target'").collect().head
      active.getAs[Boolean]("is_active") shouldBe true
      active.getAs[String]("query_id") shouldBe status.queryId.get
      val query = spark.streams.get(status.queryId.get)
      query.stop()
      val target =
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq("show_active_target"), requireTableIdMarker = true)
      StreamingTableRegistry.entry(spark, StreamingTableRegistry.targetKey(target), target).lastFailure =
        "upstream failed"
      val failed = spark.sql("SHOW STREAMING TABLES LIKE 'show_active_target'").collect().head
      failed.getAs[String]("status") shouldBe "failed"
      failed.getAs[Boolean]("is_active") shouldBe false
      failed.getAs[String]("last_failure") shouldBe "upstream failed"
      spark.sql("DROP TABLE show_active_target").collect()
      val missing = spark.sql("SHOW STREAMING TABLES LIKE 'show_active_target'").collect().head
      missing.getAs[String]("status") shouldBe "metadata_error"
      missing.getAs[String]("last_failure") should not be null
    }

    it("reconciles a completed native run when its termination callback is unavailable") {
      createSource("show_listener_gap_source")
      appendRows("show_listener_gap_source", "(1, 'one', 'p')")
      val status = createStreaming(
        "show_listener_gap_target",
        readStream("show_listener_gap_source"),
        "SELECT id, value, part FROM STREAM show_listener_gap_source",
        options = Map("trigger" -> "availableNow")
      )
      val query = spark.streams.get(status.queryId.get)
      query.awaitTermination(30000L) shouldBe true
      val target =
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq("show_listener_gap_target"), requireTableIdMarker = true)
      val key = StreamingTableRegistry.targetKey(target)
      eventually(timeout(Span(30, Seconds)), interval(Span(10, Millis))) {
        StreamingTableRegistry.entry(spark, key, target).snapshot.terminationUnconfirmed shouldBe false
      }
      StreamingTableRegistry.forget(spark, key)
      StreamingTableRegistry.register(spark, key, target, status.definitionHash.get, query)

      val row = spark.sql("SHOW STREAMING TABLES LIKE 'show_listener_gap_target'").collect().head
      row.getAs[String]("status") shouldBe "stopped"
      row.getAs[Boolean]("is_active") shouldBe false
      row.getAs[String]("query_id") shouldBe status.queryId.get
      row.getAs[String]("run_id") shouldBe status.runId.get
      row.getAs[String]("last_progress") should not be null
      row.getAs[String]("last_failure") shouldBe null
      StreamingTableRegistry.entry(spark, key, target).snapshot.terminationUnconfirmed shouldBe false
    }

    it("does not reconcile an explicit stop before its own termination confirmation") {
      createSource("show_pending_stop_source")
      val status = createStreaming(
        "show_pending_stop_target",
        readStream("show_pending_stop_source"),
        "SELECT id, value, part FROM STREAM show_pending_stop_source",
        options = Map("trigger" -> "availableNow")
      )
      val query = spark.streams.get(status.queryId.get)
      query.awaitTermination(30000L) shouldBe true
      val target =
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq("show_pending_stop_target"), requireTableIdMarker = true)
      val key = StreamingTableRegistry.targetKey(target)
      eventually(timeout(Span(30, Seconds)), interval(Span(10, Millis))) {
        StreamingTableRegistry.entry(spark, key, target).snapshot.terminationUnconfirmed shouldBe false
      }
      StreamingTableRegistry.forget(spark, key)
      StreamingTableRegistry.register(spark, key, target, status.definitionHash.get, query)
      val entry = StreamingTableRegistry.entry(spark, key, target)
      entry.beginStop(status.runId.get)
      try {
        val row = spark.sql("SHOW STREAMING TABLES LIKE 'show_pending_stop_target'").collect().head
        row.getAs[String]("status") shouldBe "stopping"
        row.getAs[Boolean]("is_active") shouldBe false
        entry.snapshot.terminationUnconfirmed shouldBe true
      } finally entry.finish(status.runId.get, None)
    }

    it("parses quoted namespaces and optional Spark SHOW patterns") {
      val command = spark.sessionState.sqlParser
        .parsePlan("SHOW STREAMING TABLES IN `catalog`.`schema with spaces` LIKE 'target*|other';")
        .asInstanceOf[ShowStreamingTablesCommand]
      command.namespace shouldBe Some(Seq("catalog", "schema with spaces"))
      command.pattern shouldBe Some("target*|other")
      spark.sessionState.sqlParser
        .parsePlan("SHOW STREAMING TABLES LIKE 'target'")
        .asInstanceOf[ShowStreamingTablesCommand]
        .namespace shouldBe None
      spark.sessionState.sqlParser
        .parsePlan("SHOW STREAMING TABLES")
        .asInstanceOf[ShowStreamingTablesCommand]
        .pattern shouldBe None
      an[org.apache.spark.sql.catalyst.parser.ParseException] should be thrownBy {
        spark.sessionState.sqlParser.parsePlan("SHOW STREAMING TABLES LIKE target")
      }
    }

    it("resolves one of seventy candidates before any target metadata lookup") {
      val names = (1 to 70).map(index => Seq("catalog", "schema", s"target_$index"))
      var reads = Vector.empty[Seq[String]]
      def lookup(name: Seq[String]): Option[String] = {
        reads :+= name
        Some(name.last)
      }
      StreamingTableManager.resolveShowCandidates(names, Some("target_42"))(lookup) shouldBe Seq("target_42")
      reads shouldBe Vector(Seq("catalog", "schema", "target_42"))
      reads = Vector.empty
      StreamingTableManager.resolveShowCandidates(names, Some("absent"))(lookup) shouldBe empty
      reads shouldBe empty
      StreamingTableManager.resolveShowCandidates(names, None)(lookup).size shouldBe 70
      reads.size shouldBe 70
      StreamingTableManager.resolveShowCandidates(names, Some("TARGET_42|target_43"))(lookup) shouldBe
        Seq("target_42", "target_43")
    }

    it("preserves selected-target metadata validation, status schema and unfiltered enumeration") {
      createSource("show_source")
      appendRows("show_source", "(1, 'one', 'p')")
      val status = createStreaming(
        "show_target",
        readStream("show_source"),
        "SELECT id, value, part FROM STREAM show_source",
        options = Map("trigger" -> "availableNow")
      )
      spark.streams.get(status.queryId.get).awaitTermination(30000L) shouldBe true
      val filtered = spark.sql("SHOW STREAMING TABLES LIKE 'show_target'")
      filtered.columns.toSeq shouldBe Seq(
        "table_name",
        "query_id",
        "run_id",
        "status",
        "is_active",
        "checkpoint_location",
        "definition_hash",
        "last_progress",
        "last_failure"
      )
      val row = filtered.collect().head
      row.getAs[String]("status") shouldBe "stopped"
      row.getAs[Boolean]("is_active") shouldBe false
      row.getAs[String]("query_id") shouldBe status.queryId.get
      row.getAs[String]("run_id") shouldBe status.runId.get
      row.getAs[String]("checkpoint_location") shouldBe status.checkpointLocation.get
      row.getAs[String]("definition_hash") shouldBe status.definitionHash.get
      spark.sql("SHOW STREAMING TABLES").collect().map(_.getAs[String]("table_name")) should contain(status.tableName)
      spark.sql("SHOW STREAMING TABLES LIKE 'absent'").collect() shouldBe empty
      val target = StreamingTableMetadata.resolveDeltaTarget(spark, Seq("show_target"), requireTableIdMarker = true)
      StreamingTableRegistry.forget(spark, StreamingTableRegistry.targetKey(target))
      StreamingTableRegistry.registeredTargets(spark).map(_.identity) should not contain target.identity
      val persisted = spark.sql("SHOW STREAMING TABLES LIKE 'show_target'").collect().head
      persisted.getAs[String]("status") shouldBe "stopped"
      persisted.getAs[Boolean]("is_active") shouldBe false
      persisted.getAs[String]("checkpoint_location") shouldBe status.checkpointLocation.get
      persisted.getAs[String]("definition_hash") shouldBe status.definitionHash.get
      spark.sql("ALTER TABLE show_target SET TBLPROPERTIES ('openivm.streaming.delta-id' = 'replaced')").collect()
      val invalid = spark.sql("SHOW STREAMING TABLES LIKE 'show_target'").collect().head
      invalid.getAs[String]("status") shouldBe "metadata_error"
      invalid.getAs[String]("last_failure") should not be null
    }
  }
}

object DirectStatusCatalog {
  @volatile var failure: Option[Throwable] = None
  val listings                             = new java.util.concurrent.atomic.AtomicInteger()
  val loads                                = new java.util.concurrent.atomic.AtomicInteger()
}

/** Native-style catalog proving exact status does not enumerate a namespace. */
class DirectStatusCatalog extends org.apache.spark.sql.connector.catalog.TableCatalog {
  import org.apache.spark.sql.connector.catalog._
  private var catalogName = ""
  override def initialize(name: String, options: org.apache.spark.sql.util.CaseInsensitiveStringMap): Unit =
    catalogName = name
  override def name(): String = catalogName
  override def listTables(namespace: Array[String]): Array[Identifier] = {
    DirectStatusCatalog.listings.incrementAndGet()
    Array.empty
  }
  override def loadTable(identifier: Identifier): Table = {
    DirectStatusCatalog.loads.incrementAndGet()
    DirectStatusCatalog.failure.foreach(throw _)
    throw new org.apache.spark.sql.catalyst.analysis.NoSuchTableException(identifier)
  }
  override def createTable(
      identifier: Identifier,
      schema: org.apache.spark.sql.types.StructType,
      partitions: Array[org.apache.spark.sql.connector.expressions.Transform],
      properties: java.util.Map[String, String]
  ): Table = throw new UnsupportedOperationException()
  override def alterTable(identifier: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException()
  override def dropTable(identifier: Identifier): Boolean = throw new UnsupportedOperationException()
  override def renameTable(oldIdentifier: Identifier, newIdentifier: Identifier): Unit =
    throw new UnsupportedOperationException()
}
