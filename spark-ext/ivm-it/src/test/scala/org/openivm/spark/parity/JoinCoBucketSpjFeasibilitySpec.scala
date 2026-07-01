package org.openivm.spark.parity

import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

/** Feasibility guard for W7.4: Spark SPJ needs true bucket metadata, but Delta
  * 3.2 rejects classic bucketing and liquid clustering is only a data-layout
  * hint, not a reported storage partitioning that can satisfy join distribution.
  */
class JoinCoBucketSpjFeasibilitySpec extends IvmParitySpecBase("join-cobucket-spj-feas") with InterceptMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.sql.sources.v2.bucketing.enabled" -> "true",
    "spark.sql.sources.bucketing.enabled"    -> "true",
    "spark.sql.adaptive.enabled"             -> "false",
    "spark.sql.autoBroadcastJoinThreshold"   -> "-1",
    "spark.sql.shuffle.partitions"           -> "4"
  )

  describe("W7.4 Delta co-bucketing feasibility") {
    it("rejects classic bucketed Delta table DDL required by Spark SPJ") {
      val err = intercept[Exception] {
        sql("CREATE TABLE w74_bucket_a(k INT, v STRING) USING DELTA CLUSTERED BY (k) INTO 4 BUCKETS")
      }

      err.getMessage should include("Bucketing")
      err.getMessage should include("is not supported for Delta tables")
    }

    it("keeps shuffle exchanges for liquid-clustered Delta joins") {
      sql("CREATE TABLE w74_liquid_a(k INT, v STRING) USING DELTA CLUSTER BY (k)")
      sql("CREATE TABLE w74_liquid_b(k INT, w STRING) USING DELTA CLUSTER BY (k)")
      sql("INSERT INTO w74_liquid_a VALUES (1, 'a'), (2, 'b')")
      sql("INSERT INTO w74_liquid_b VALUES (1, 'x'), (3, 'y')")

      spark
        .sql("SELECT * FROM w74_liquid_a JOIN w74_liquid_b ON w74_liquid_a.k = w74_liquid_b.k")
        .count() shouldBe 1L

      val plan = sql(
        "EXPLAIN FORMATTED SELECT * FROM w74_liquid_a JOIN w74_liquid_b ON w74_liquid_a.k = w74_liquid_b.k"
      ).collect().map(_.getString(0)).mkString("\n")

      plan should include("SortMergeJoin")
      plan should include("Exchange")
      plan should include("hashpartitioning")
    }
  }
}
