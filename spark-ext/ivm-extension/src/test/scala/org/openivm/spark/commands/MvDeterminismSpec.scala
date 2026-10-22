package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class MvDeterminismSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("mv-determinism")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark
      .range(3)
      .selectExpr("id AS k", "id * 10 AS v", "array(1, 2, 3) AS xs", "DATE '2024-01-01' AS d")
      .createOrReplaceTempView("mvd_src")
    spark.udf.register("mvd_random_udf", udf((x: Long) => x + scala.util.Random.nextLong()).asNondeterministic())
    ()
  }

  override def afterAll(): Unit =
    try if (spark != null) spark.stop()
    finally super.afterAll()

  private def varying(query: String): Seq[String] =
    MvDeterminism.varyingExpressions(spark.sql(query).queryExecution.analyzed).map(MvDeterminism.describe)

  private def assertReports(cases: (String, String)*): Unit =
    cases.foreach { case (query, expected) =>
      withClue(s"$query reported ${varying(query)}: ") {
        varying(query).exists(_.toLowerCase(java.util.Locale.ROOT).contains(expected)) shouldBe true
      }
    }

  describe("MvDeterminism.varyingExpressions") {

    it("reports random expressions wherever they appear") {
      assertReports(
        "SELECT k, rand() AS r FROM mvd_src"                                          -> "rand",
        "SELECT k, randn(7) AS r FROM mvd_src"                                        -> "randn",
        "SELECT k, uuid() AS u FROM mvd_src"                                          -> "uuid",
        "SELECT k, shuffle(xs) AS s FROM mvd_src"                                     -> "shuffle",
        "SELECT k, monotonically_increasing_id() AS i FROM mvd_src"                   -> "monotonically_increasing_id",
        "SELECT k, input_file_name() AS f FROM mvd_src"                               -> "input_file_name",
        "SELECT k, spark_partition_id() AS p FROM mvd_src"                            -> "spark_partition_id",
        "SELECT k, mvd_random_udf(v) AS r FROM mvd_src"                               -> "mvd_random_udf",
        "SELECT k FROM mvd_src WHERE rand() < 0.5"                                    -> "rand",
        "SELECT k, (SELECT max(v) FROM mvd_src WHERE rand() < 2.0) AS m FROM mvd_src" -> "rand"
      )
    }

    it("reports time-dependent expressions") {
      assertReports(
        "SELECT k, current_date() AS d FROM mvd_src"                                           -> "current_date",
        "SELECT k, current_timestamp() AS t FROM mvd_src"                                      -> "current_timestamp",
        "SELECT k, now() AS t FROM mvd_src"                                                    -> "now",
        "SELECT k, localtimestamp() AS t FROM mvd_src"                                         -> "localtimestamp",
        "SELECT k, current_timezone() AS z FROM mvd_src"                                       -> "current_timezone",
        "SELECT k, unix_timestamp() AS u FROM mvd_src"                                         -> "current_timestamp",
        "SELECT k, SUM(CASE WHEN d >= current_date() THEN v END) AS s FROM mvd_src GROUP BY k" -> "current_date"
      )
    }

    it("ignores deterministic expressions, window functions and order-dependent aggregates") {
      Seq(
        "SELECT k, v + 1 AS w, upper(CAST(k AS STRING)) AS s FROM mvd_src WHERE d >= DATE '2020-01-01'",
        "SELECT k, SUM(v) AS total, array_sort(collect_list(v)) AS vs, first(v) AS f, last(v) AS l " +
          "FROM mvd_src GROUP BY k",
        "SELECT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn, " +
          "last_value(v, true) OVER (PARTITION BY k ORDER BY v) AS lv, " +
          "first_value(v) OVER (PARTITION BY k ORDER BY v) AS fv FROM mvd_src"
      ).foreach(query => withClue(query)(varying(query) shouldBe empty))
    }
  }
}
