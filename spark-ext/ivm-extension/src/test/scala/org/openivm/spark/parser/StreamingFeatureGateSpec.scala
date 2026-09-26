package org.openivm.spark.parser

import java.io.File
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParseException
import org.openivm.spark.commands.CreateStreamingTableCommand
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StreamingFeatureGateSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("StreamingFeatureGateSpec")
      .config("spark.sql.extensions", "org.openivm.spark.OpenIvmSparkExtensions")
      .config("spark.openivm.enabled", "false")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.warehouse.dir",
        new File(
          s"target/test-warehouse-streaming-gate-${UUID.randomUUID().toString.take(8)}"
        ).getCanonicalPath
      )
      .getOrCreate()
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
    } finally {
      super.afterAll()
    }

  it("delegates the streaming-table surface to Spark while the master gate is disabled") {
    try {
      val plan = spark.sessionState.sqlParser.parsePlan(
        "CREATE STREAMING TABLE sink AS SELECT * FROM STREAM source"
      )
      plan should not be a[CreateStreamingTableCommand]
    } catch {
      case _: ParseException => succeed
    }
  }
}
