package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

import org.apache.spark.sql.catalyst.TableIdentifier

class EffectivizeSpec extends IvmParitySpecBase("effectivize") with InterceptMode {

  describe("effectivization") {
    it("drops canceling SIMPLE_PROJECTION duplicate-row deltas without changing EXCEPT ALL correctness") {
      val offRows = runDuplicateProjectionCase("e4_off", enabled = false)
      val onRows  = runDuplicateProjectionCase("e4_on", enabled = true)

      offRows should be >= 0L
      onRows should be <= offRows
      onRows shouldBe 0L
    }
  }

  private def runDuplicateProjectionCase(prefix: String, enabled: Boolean): Long = {
    restartSpark(Map(FeatureGate.RefreshEffectivizeEnabledKey -> enabled.toString))

    val table = s"${prefix}_users"
    val mv    = s"${prefix}_mv"
    sql(s"CREATE TABLE IF NOT EXISTS $table(user_id INT, name STRING) USING DELTA")
    sql(s"INSERT INTO $table VALUES (1, 'dup'), (2, 'dup')")
    sql(s"CREATE MATERIALIZED VIEW $mv AS SELECT name FROM $table")

    val meta            = MvCatalog.lookup(spark, TableIdentifier(mv)).getOrElse(fail(s"$mv metadata missing"))
    val escapedLocation = meta.location.replace("`", "``")
    val preRefreshVersion = spark
      .sql(s"DESCRIBE HISTORY delta.`$escapedLocation`")
      .selectExpr("max(version) AS version")
      .head()
      .getAs[Long]("version")

    sql(s"DELETE FROM $table WHERE user_id = 1")
    sql(s"INSERT INTO $table VALUES (3, 'dup')")
    refreshMv(mv)
    assertMvCorrect(mv, s"SELECT name FROM $table")

    spark
      .sql(s"DESCRIBE HISTORY delta.`$escapedLocation`")
      .where(s"version > $preRefreshVersion")
      .selectExpr("aggregate(collect_list(cast(operationMetrics['numOutputRows'] AS BIGINT)), 0L, (acc, x) -> acc + x)")
      .head()
      .getLong(0)
  }
}
