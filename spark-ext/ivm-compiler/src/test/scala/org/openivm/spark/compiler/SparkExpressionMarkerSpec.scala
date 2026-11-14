package org.openivm.spark.compiler

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkExpressionMarkerSpec extends AnyFlatSpec with Matchers {
  import SparkExpressionMarker._

  "Spark expression markers" should "restore nested arguments without losing casts or nulls" in {
    val bridge = new SparkExpressionMarker
    val split = bridge.encode(Seq("CAST(id AS VARCHAR)", "','"), TextArray)(args => s"split(${args.mkString(", ")})")
    val size = bridge.encode(Seq(split), Integer)(args => s"size(${args.head})")
    val emitted = size.replace(" AS VARCHAR", " AS STRING")
    bridge.restore(emitted) shouldBe "CAST(size(split(CAST(id AS STRING), ',')) AS INTEGER)"
    bridge.requireAllSeen()
  }

  it should "preserve constants, nested structs, and arbitrary field names" in {
    val bridge = new SparkExpressionMarker
    val json = bridge.encode(
      Seq("named_struct('Odd Name', NULL, 'nested', named_struct('Mixed', id))", "map('x', 'y')"),
      Text
    )(args => s"to_json(${args.mkString(", ")})")
    val hash = bridge.encode(Seq(json, "256"), Text)(args => s"sha2(${args.mkString(", ")})")
    bridge.restore(hash) shouldBe
      "sha2(to_json(named_struct('Odd Name', NULL, 'nested', named_struct('Mixed', id)), map('x', 'y')), 256)"
    bridge.requireAllSeen()
  }

  it should "reject missing or malformed markers" in {
    val bridge = new SparkExpressionMarker
    val marker = bridge.encode(Seq("'Ä'", "'(?i)ä'"), Boolean)(args => s"regexp_like(${args.mkString(", ")})")
    intercept[IllegalStateException](bridge.restore(marker.replace("row(", "other(")))
    intercept[IllegalStateException](bridge.requireAllSeen())
  }
}
