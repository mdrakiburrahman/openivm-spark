package org.openivm.spark.common.rocksdb

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

class OpenIvmRocksDBRegistrySpec extends AnyFunSpec with BeforeAndAfterEach with Matchers {

  private val sparks = ArrayBuffer.empty[SparkSession]
  private val dirs   = ArrayBuffer.empty[File]

  override def afterEach(): Unit = {
    OpenIvmRocksDBRegistry.closeAll()
    sparks.reverse.foreach { spark =>
      try spark.stop()
      catch {
        case _: Throwable => ()
      }
    }
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparks.clear()
    dirs.reverse.foreach(deleteRecursively)
    dirs.clear()
    super.afterEach()
  }

  private def newDir(prefix: String): File = {
    val dir = new File(s"target/test-rocksdb-registry-$prefix-${UUID.randomUUID().toString.take(8)}")
    dir.mkdirs()
    dirs += dir
    dir
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
    ()
  }

  private def newSpark(appName: String): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val warehouse = newDir(s"$appName-warehouse")
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .getOrCreate()

    sparks += spark
    spark
  }

  describe("OpenIvmRocksDBRegistry") {
    it("returns the same instance for repeated opens of the same path") {
      val spark = newSpark("registry-same-instance")
      val dbDir = newDir("same-instance-db")

      val first  = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta", "props"))
      val second = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta"))

      first should be theSameInstanceAs second
    }

    it("allows column-family subsets and rejects missing column families with a clear message") {
      val spark = newSpark("registry-cf-subset")
      val dbDir = newDir("cf-subset-db")

      val first  = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta", "props"))
      val second = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta"))

      first should be theSameInstanceAs second

      val error = intercept[IllegalArgumentException] {
        OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta", "staging"))
      }
      error.getMessage should include("staging")
      error.getMessage should include("Registered column families")
    }

    it("closes only the databases associated with the given Spark application id") {
      val spark = newSpark("registry-app-id")
      val appId = spark.sparkContext.applicationId

      val dbOneDir = newDir("app-one-db")
      val dbTwoDir = newDir("app-two-db")

      val dbOne = OpenIvmRocksDBRegistry.getOrOpen(spark, dbOneDir.getAbsolutePath, Seq("meta"))
      val dbTwo = OpenIvmRocksDBRegistry.getOrOpen(spark, dbTwoDir.getAbsolutePath, Seq("meta"))

      OpenIvmRocksDBRegistry.overrideAppIdsForTesting(dbTwoDir.getAbsolutePath, Set("other-app"))
      OpenIvmRocksDBRegistry.closeAllForSparkContext(appId)

      an[IllegalStateException] should be thrownBy dbOne.currentVersion
      noException should be thrownBy dbTwo.currentVersion
    }

    it("closes everything idempotently") {
      val spark = newSpark("registry-close-all")
      val dbDir = newDir("close-all-db")
      val db    = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta"))

      OpenIvmRocksDBRegistry.closeAll()
      OpenIvmRocksDBRegistry.closeAll()

      an[IllegalStateException] should be thrownBy db.currentVersion
    }
  }
}
