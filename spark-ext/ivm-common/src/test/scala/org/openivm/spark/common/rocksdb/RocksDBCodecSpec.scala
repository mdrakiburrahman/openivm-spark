package org.openivm.spark.common.rocksdb

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RocksDBCodecSpec extends AnyFunSpec with Matchers {

  private def startsWith(bytes: Array[Byte], prefix: Array[Byte]): Boolean =
    bytes.length >= prefix.length && prefix.indices.forall(index => bytes(index) == prefix(index))

  describe("RocksDBCodec.compositeKey + splitComposite") {
    it("round-trips two-part keys whose final part contains zero bytes") {
      val parts = Seq(Array[Byte](1, 0, 2), Array[Byte](3, 0, 4))

      val decoded = RocksDBCodec.splitComposite(RocksDBCodec.compositeKey(parts), 2)

      decoded.map(_.toSeq) shouldBe parts.map(_.toSeq)
    }

    it("supports exact first-part prefixes via a trailing empty part") {
      val prefix      = RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8("group"), Array.emptyByteArray))
      val matching    = RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8("group"), RocksDBCodec.utf8("member")))
      val nonMatching = RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8("grouped"), RocksDBCodec.utf8("member")))

      startsWith(matching, prefix) shouldBe true
      startsWith(nonMatching, prefix) shouldBe false
    }

    it("decodes keys written with the earlier single-byte separator scheme") {
      val legacy = Array[Byte]('a'.toByte, 0.toByte, 'b'.toByte, 0.toByte, 'c'.toByte)

      val decoded = RocksDBCodec.splitComposite(legacy).map(RocksDBCodec.fromUtf8)

      decoded shouldBe Seq("a", "b", "c")
    }
  }
}
