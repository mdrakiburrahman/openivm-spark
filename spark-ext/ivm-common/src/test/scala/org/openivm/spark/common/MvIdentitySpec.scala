package org.openivm.spark.common

import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class MvIdentitySpec extends AnyFunSpec with Matchers {

  private def metadata(querySql: String, properties: Map[String, String] = Map.empty): MvMetadata =
    MvMetadata(
      name = TableIdentifier("mv", Some("db")),
      querySql = querySql,
      refreshType = RefreshTypeCode.AggregateGroup,
      refreshTypeName = "AGGREGATE_GROUP",
      lastVersion = 3L,
      sourceTables = Seq("db.src"),
      sourceSchemaFingerprint = "schema",
      location = "/warehouse/db/mv",
      createdAt = new Timestamp(1L),
      properties = properties
    )

  describe("MvCatalog.mvIdentity") {
    it("keeps the legacy body-sensitive identity when no stable identity exists") {
      MvCatalog.mvIdentity(metadata("SELECT * FROM db.src VERSION AS OF 1")) should not be
        MvCatalog.mvIdentity(metadata("SELECT * FROM db.src VERSION AS OF 2"))
    }

    it("uses the persisted stable identity across source-version pin changes") {
      val original = metadata("SELECT * FROM db.src VERSION AS OF 1")
      val stable   = MvCatalog.mvIdentity(original)
      val advanced = metadata(
        "SELECT * FROM db.src VERSION AS OF 2",
        Map(MvMetadata.DefinitionIdentityKey -> stable)
      )

      MvCatalog.mvIdentity(advanced) shouldBe stable
    }

    it("blocks downstream identity reads while an advancement journal is present") {
      val inFlight = metadata(
        "SELECT * FROM db.src VERSION AS OF 1",
        Map(
          MvMetadata.SourceVersionAdvancePreMvVersionKey -> "3",
          MvMetadata.SourceVersionAdvanceCascadePathKey  -> "/warehouse/_ivm/delta"
        )
      )

      val error = the[IllegalStateException] thrownBy MvCatalog.mvIdentity(inFlight)
      error.getMessage should include("in-flight source-version advancement")
    }
  }
}
