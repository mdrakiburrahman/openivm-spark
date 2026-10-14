package org.apache.spark.sql.openivm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, NamespaceChange, SupportsNamespaces}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.util.UUID

class SparkSessionAccessSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  private val warehouse           = new File(s"target/test-session-access-${UUID.randomUUID()}")
  private var spark: SparkSession = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("native-namespace-clone")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.sql.catalog.opaque", classOf[OpaqueNamespaceCatalog].getName)
      .getOrCreate()
  }
  override def afterAll(): Unit = {
    try {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      def remove(file: File): Unit = {
        if (file.isDirectory) Option(file.listFiles()).toSeq.flatten.foreach(remove)
        file.delete()
        ()
      }
      remove(warehouse)
    } finally super.afterAll()
  }
  describe("native namespace cloning") {
    it("preserves inherited opaque V1 database without revalidating its public name") {
      val session = spark.newSession()
      val encoded =
        "chimcobldhq2akqh9gg58pbcclmmat3if4g2c829dpq6ar3cd5jmarj3ckg2q825ah620b908hinc81d41mm8sjic5k6qobe4lkmssr9ctk78sqve9gne9b4c9ng"
      val field = classOf[SessionCatalog].getDeclaredField("currentDb")
      field.setAccessible(true)
      field.set(session.sessionState.catalog, encoded)
      session.sessionState.catalog.getCurrentDatabase shouldBe encoded
      val clone = SparkSessionAccess.cloneSession(session)
      clone.sessionState.catalog.getCurrentDatabase shouldBe encoded
      clone.catalog.currentDatabase shouldBe session.catalog.currentDatabase
      clone should not be session
    }
    it("preserves non-default V2 catalog and multi-part namespace") {
      val session = spark.newSession()
      session.catalog.setCurrentCatalog("opaque")
      session.catalog.setCurrentDatabase("`namespace.with.dot`.`schema with spaces`")
      val clone = SparkSessionAccess.cloneSession(session)
      clone.catalog.currentCatalog() shouldBe "opaque"
      clone.sessionState.catalogManager.currentNamespace.toSeq shouldBe Seq("namespace.with.dot", "schema with spaces")
      clone should not be session
    }
    it("isolates temporary views and restores the active session") {
      val session = spark.newSession()
      session.sql("SELECT 1 AS value").createOrReplaceTempView("inherited")
      SparkSessionAccess.withActiveSession(session) {
        SparkSessionAccess.withIsolatedSession(session) { clone =>
          SparkSession.getActiveSession shouldBe Some(clone)
          clone.sql("SELECT * FROM inherited").collect().head.getInt(0) shouldBe 1
          clone.sql("SELECT 2 AS value").createOrReplaceTempView("isolated_only")
        }
        SparkSession.getActiveSession shouldBe Some(session)
      }
      session.catalog.tableExists("isolated_only") shouldBe false
    }
  }
}

/** Minimal public namespace catalog exercising V2 clone state propagation. */
class OpaqueNamespaceCatalog extends CatalogPlugin with SupportsNamespaces {
  private var catalogName                                                                     = ""
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit              = catalogName = name
  override def name(): String                                                                 = catalogName
  override def defaultNamespace(): Array[String]                                              = Array("default")
  override def namespaceExists(namespace: Array[String]): Boolean                             = true
  override def listNamespaces(): Array[Array[String]]                                         = Array(Array("default"))
  override def listNamespaces(namespace: Array[String]): Array[Array[String]]                 = Array.empty
  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] = new java.util.HashMap()
  override def createNamespace(namespace: Array[String], metadata: java.util.Map[String, String]): Unit = ()
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit                = ()
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean                       = true
}
