package org.openivm.spark.bench

import org.openivm.spark.parity.base.{InterceptMode, IvmParityMode, IvmParitySpecBase}
import org.scalatest.Tag

import scala.util.Try

object MicroBenchmarkTag extends Tag("org.openivm.spark.tags.MicroBenchmark")

class RefreshBreakEvenHarness extends RefreshBreakEvenScenarios with InterceptMode

private final case class BenchShape(
    name: String,
    shortName: String,
    setup: (String, Int) => Unit,
    insertDelta: (String, Int, Int) => Unit,
    viewSql: String => String
)

private final case class BenchResult(fraction: Double, incrementalMs: Long, fullMs: Long) {
  def winner: String = if (incrementalMs < fullMs) "incremental" else "full"
}

abstract class RefreshBreakEvenScenarios extends IvmParitySpecBase("refresh-break-even") {
  self: IvmParityMode =>

  private val baseRows: Int =
    sys.props.get("openivm.breakeven.baseRows").flatMap(value => Try(value.toInt).toOption).getOrElse(200)
  private val deltaFractions: Seq[Double] = sys.props
    .get("openivm.breakeven.fractions")
    .map(_.split(",").toSeq.map(_.trim).filter(_.nonEmpty).map(_.toDouble))
    .getOrElse(Seq(0.001, 0.01, 0.05, 0.20, 0.50, 1.00))

  describe("incremental-vs-full refresh break-even micro-bench") {
    it("prints break-even timing tables for representative MV shapes", MicroBenchmarkTag) {
      val shapes = Seq(aggregateGroupShape, windowPartitionShape, joinAggregateShape)
      shapes.foreach { shape =>
        val results = deltaFractions.zipWithIndex.map { case (fraction, index) => runOne(shape, fraction, index) }
        printTable(shape.name, results)
      }
    }
  }

  private def runOne(shape: BenchShape, fraction: Double, index: Int): BenchResult = {
    val prefix    = s"rbe_${shape.shortName}_$index"
    val deltaRows = math.max(1, math.round(baseRows * fraction).toInt)
    shape.setup(prefix, baseRows)

    val viewSql = shape.viewSql(prefix)
    sql(s"CREATE MATERIALIZED VIEW ${prefix}_mv AS $viewSql")
    shape.insertDelta(prefix, baseRows, deltaRows)

    val incrementalMs = timeMs {
      refreshMv(s"${prefix}_mv")
    }
    assertMvCorrect(s"${prefix}_mv", viewSql)

    val fullTable = s"${prefix}_full"
    sql(s"DROP TABLE IF EXISTS $fullTable")
    val fullMs = timeMs {
      sql(s"CREATE TABLE $fullTable USING DELTA AS $viewSql").collect()
    }
    sql(s"DROP TABLE IF EXISTS $fullTable")

    BenchResult(fraction, incrementalMs, fullMs)
  }

  private def timeMs(body: => Unit): Long = {
    val start = System.nanoTime()
    body
    math.max(1L, (System.nanoTime() - start) / 1000000L)
  }

  private def printTable(shapeName: String, results: Seq[BenchResult]): Unit = {
    println()
    println(s"Refresh break-even: $shapeName (base_rows=$baseRows)")
    println("delta_fraction | incremental_ms | full_ms | winner")
    results.foreach { result =>
      println(
        f"${result.fraction}%.3f          | ${result.incrementalMs}%14d | ${result.fullMs}%7d | ${result.winner}"
      )
    }
    println(s"crossover_fraction: ${crossover(results)}")
  }

  private def crossover(results: Seq[BenchResult]): String = {
    results.find(result => result.fullMs <= result.incrementalMs) match {
      case Some(result) => f"${result.fraction}%.3f"
      case None         => "not observed (incremental won every measured point)"
    }
  }

  private def aggregateGroupShape: BenchShape = BenchShape(
    name = "AGGREGATE_GROUP SUM/COUNT GROUP BY",
    shortName = "agg",
    setup = { (prefix, rows) =>
      val groups = 32
      sql(s"CREATE TABLE ${prefix}_sales(id BIGINT, k INT, amount BIGINT) USING DELTA")
      sql(
        s"""
           |INSERT INTO ${prefix}_sales
           |SELECT id, CAST(id % $groups AS INT), CAST((id % 100) + 1 AS BIGINT)
           |FROM range($rows)
           |""".stripMargin
      )
    },
    insertDelta = { (prefix, offset, rows) =>
      val groups = 32
      sql(
        s"""
           |INSERT INTO ${prefix}_sales
           |SELECT $offset + id, CAST(($offset + id) % $groups AS INT), CAST((($offset + id) % 100) + 1 AS BIGINT)
           |FROM range($rows)
           |""".stripMargin
      )
    },
    viewSql = prefix => s"""
         |SELECT k, SUM(amount) AS total_amount, COUNT(*) AS row_count
         |FROM ${prefix}_sales
         |GROUP BY k
         |""".stripMargin
  )

  private def windowPartitionShape: BenchShape = BenchShape(
    name = "WINDOW_PARTITION cumulative MIN/MAX",
    shortName = "win",
    setup = { (prefix, rows) =>
      val parts = 16
      sql(s"CREATE TABLE ${prefix}_events(id BIGINT, part INT, metric BIGINT) USING DELTA")
      sql(
        s"""
           |INSERT INTO ${prefix}_events
           |SELECT id, CAST(id % $parts AS INT), CAST((id * 17) % 1000 AS BIGINT)
           |FROM range($rows)
           |""".stripMargin
      )
    },
    insertDelta = { (prefix, offset, rows) =>
      val parts = 16
      sql(
        s"""
           |INSERT INTO ${prefix}_events
           |SELECT $offset + id, CAST(($offset + id) % $parts AS INT), CAST((($offset + id) * 17) % 1000 AS BIGINT)
           |FROM range($rows)
           |""".stripMargin
      )
    },
    viewSql = prefix => s"""
         |SELECT
         |  id,
         |  part,
         |  metric,
         |  MIN(metric) OVER (
         |    PARTITION BY part
         |    ORDER BY id
         |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         |  ) AS running_min,
         |  MAX(metric) OVER (
         |    PARTITION BY part
         |    ORDER BY id
         |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         |  ) AS running_max
         |FROM ${prefix}_events
         |""".stripMargin
  )

  private def joinAggregateShape: BenchShape = BenchShape(
    name = "JOIN aggregate SUM/COUNT GROUP BY dimension",
    shortName = "jag",
    setup = { (prefix, rows) =>
      val dims = 24
      sql(s"CREATE TABLE ${prefix}_dim(dim_id INT, dim_name STRING) USING DELTA")
      sql(
        s"""
           |INSERT INTO ${prefix}_dim
           |SELECT CAST(id AS INT), CONCAT('dim_', CAST(id AS STRING))
           |FROM range($dims)
           |""".stripMargin
      )
      sql(s"CREATE TABLE ${prefix}_fact(id BIGINT, dim_id INT, amount BIGINT) USING DELTA")
      sql(
        s"""
           |INSERT INTO ${prefix}_fact
           |SELECT id, CAST(id % $dims AS INT), CAST((id % 250) + 10 AS BIGINT)
           |FROM range($rows)
           |""".stripMargin
      )
    },
    insertDelta = { (prefix, offset, rows) =>
      val dims = 24
      sql(
        s"""
           |INSERT INTO ${prefix}_fact
           |SELECT $offset + id, CAST(($offset + id) % $dims AS INT), CAST((($offset + id) % 250) + 10 AS BIGINT)
           |FROM range($rows)
           |""".stripMargin
      )
    },
    viewSql = prefix => s"""
         |SELECT d.dim_name, SUM(f.amount) AS total_amount, COUNT(*) AS row_count
         |FROM ${prefix}_fact f
         |JOIN ${prefix}_dim d ON f.dim_id = d.dim_id
         |GROUP BY d.dim_name
         |""".stripMargin
  )

}
