package org.openivm.spark.parity

import org.openivm.spark.common.MvCatalog
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class CompileBridgeArraySpec extends IvmParitySpecBase("compile-bridge-array") with InterceptMode {
  describe("Spark array function compiler shims") {
    it("preserves split, negative element_at and size null behavior after refresh") {
      sql("CREATE TABLE cbar_src (id INT, raw STRING) USING DELTA")
      sql("INSERT INTO cbar_src VALUES (1, '/a/b'), (2, NULL), (3, 'a,b,c')")
      val mvName   = "cbar_mv"
      val viewBody = "SELECT id, element_at(split(raw, '/'), -1) AS last, size(split(raw, ',')) AS n FROM cbar_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(mvName)
      MvCatalog
        .lookup(spark, id)
        .getOrElse(fail(s"Missing MV metadata for $mvName"))
        .refreshTypeName should not equal "FULL_REFRESH"
      assertMvCorrect(mvName, viewBody)
      sql("INSERT INTO cbar_src VALUES (4, '/x/'), (5, '')")
      refreshMv(mvName)
      assertMvCorrect(mvName, viewBody)
    }
  }
}
