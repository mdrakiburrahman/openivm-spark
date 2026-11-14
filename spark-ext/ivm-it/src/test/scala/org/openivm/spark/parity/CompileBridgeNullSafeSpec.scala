package org.openivm.spark.parity

import org.openivm.spark.common.MvCatalog
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class CompileBridgeNullSafeSpec extends IvmParitySpecBase("compile-bridge-null-safe") with InterceptMode {
  describe("null-safe comparison and reserved identifiers") {
    it("preserves negated null-safe equality through incremental refresh") {
      sql("CREATE TABLE cbns_src (id INT, collation STRING, previous STRING) USING DELTA")
      sql("INSERT INTO cbns_src VALUES (1, NULL, NULL), (2, NULL, 'a'), (3, 'a', 'a'), (4, 'b', 'a')")
      val mvName   = "cbns_mv"
      val viewBody = "SELECT id, collation FROM cbns_src WHERE NOT (previous <=> collation)"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(mvName)
      MvCatalog
        .lookup(spark, id)
        .getOrElse(fail(s"Missing MV metadata for $mvName"))
        .refreshTypeName should not equal "FULL_REFRESH"
      assertMvCorrect(mvName, viewBody)
      sql("INSERT INTO cbns_src VALUES (5, 'a', NULL), (6, 'z', 'z')")
      refreshMv(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }
}
