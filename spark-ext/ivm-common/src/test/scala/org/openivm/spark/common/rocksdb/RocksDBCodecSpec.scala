package org.openivm.spark.common.rocksdb

import java.util.Base64

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RocksDBCodecSpec extends AnyFunSpec with Matchers {

  private def startsWith(bytes: Array[Byte], prefix: Array[Byte]): Boolean =
    bytes.length >= prefix.length && prefix.indices.forall(index => bytes(index) == prefix(index))

  describe("bounded RocksDB shard path encoding") {
    def legacy(name: String): String =
      Base64.getUrlEncoder.withoutPadding.encodeToString(RocksDBCodec.utf8(name))

    it("preserves every valid legacy component through the 255-byte boundary") {
      val names = Seq("", "db.table", "db.public__ivm_data", "x" * 189, "x" * 190, "x" * 191, "界" * 63)
      names.foreach { name =>
        withClue(s"UTF-8 bytes=${RocksDBCodec.utf8(name).length}: ") {
          legacy(name).length should be <= 255
          RocksDBCodec.safePathSegment(name) shouldBe legacy(name)
        }
      }
      RocksDBCodec.safePathSegment("x" * 191).length shouldBe 255
    }

    it("uses the full deterministic SHA-256 digest once legacy encoding exceeds NAME_MAX") {
      legacy("x" * 192).length shouldBe 256
      RocksDBCodec.safePathSegment("x" * 192) shouldBe
        "sha256.f5f3b40552876b425eea612377873720c5ab7b00c002f8ddf8f50417a02209fc"
      val unicode = RocksDBCodec.safePathSegment("界" * 64)
      unicode should fullyMatch regex "sha256\\.[0-9a-f]{64}"
      unicode.length shouldBe 71
      unicode shouldBe RocksDBCodec.safePathSegment("界" * 64)
    }

    it("keeps long qualified Fabric identities distinct without case-folding, truncation or namespace collisions") {
      val root =
        "delta.abfss://11111111-1111-1111-1111-111111111111@test-onelake.dfs.fabric.microsoft.com/" +
          "22222222-2222-2222-2222-222222222222/Tables/synthetic_schema_012345678901/"
      val source = root + "synthetic_" + ("r" * 50) + "__ivm_data"
      RocksDBCodec.utf8(source.stripPrefix("delta.")).length shouldBe 227
      legacy(source.stripPrefix("delta.")).length shouldBe 303
      val names = Seq(
        source,
        source.replace("22222222", "33333333"),
        source.replace("synthetic_r", "Synthetic_r"),
        source + "_other",
        root + ("same_prefix_" * 30) + "one",
        root + ("same_prefix_" * 30) + "two"
      )
      val encoded = names.map(RocksDBCodec.safePathSegment)
      names.foreach(name => legacy(name).length should be > 255)
      encoded.distinct.size shouldBe names.size
      encoded.foreach { segment =>
        segment should fullyMatch regex "sha256\\.[0-9a-f]{64}"
        segment.length shouldBe 71
      }
      // A dot cannot occur in the legacy base64url alphabet.
      legacy("sha256." + ("0" * 64)) should not include "."
    }
  }

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
