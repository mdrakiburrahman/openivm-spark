package org.openivm.spark.parity

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.types._
import org.openivm.spark.optimizer.DistinctJoinReductionRule
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

/** Exact bag and applicability coverage for deduplicated existence joins. */
class DistinctJoinReductionCdfSpec extends IvmParitySpecBase("distinct-join-reduction") with CdfMode {
  private val RuleName = "org.openivm.spark.optimizer.DistinctJoinReductionRule"
  private val Excluded = "spark.sql.optimizer.excludedRules"

  private def register(name: String, schema: StructType, rows: Seq[Row]): Unit =
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema).createOrReplaceTempView(name)

  private def semiCount(p: LogicalPlan): Int = p.collect {
    case j: Join if j.joinType == LeftSemi => j
  }.size

  private case class Result(rows: Seq[Row], plan: LogicalPlan)
  private def execute(query: String, enabled: Boolean): Result = {
    val old       = spark.conf.getOption(Excluded)
    val unrelated = old.toSeq.flatMap(_.split(",")).map(_.trim).filter(r => r.nonEmpty && r != RuleName)
    spark.conf.set(Excluded, (unrelated ++ (if (enabled) Seq.empty else Seq(RuleName))).mkString(","))
    try {
      val df   = spark.sql(query)
      val plan = df.queryExecution.optimizedPlan
      val rows = df.collect().toVector
      Result(rows, plan)
    } finally
      old match {
        case Some(v) => spark.conf.set(Excluded, v)
        case None    => spark.conf.unset(Excluded)
      }
  }
  private def bag(rows: Seq[Row]): Map[Row, Int] = rows.groupBy(identity).map { case (r, rs) => r -> rs.size }
  private def compare(query: String, mustReduce: Boolean = true): Unit = {
    val baseline  = execute(query, enabled = false)
    val candidate = execute(query, enabled = true)
    bag(candidate.rows) shouldBe bag(baseline.rows)
    if (mustReduce) {
      withClue(candidate.plan.treeString) { semiCount(candidate.plan) should be > semiCount(baseline.plan) }
    } else semiCount(candidate.plan) shouldBe semiCount(baseline.plan)
  }

  private val leftSchema = new StructType()
    .add("account", LongType)
    .add("broker", LongType)
    .add("score", DoubleType)
  private val rightSchema = new StructType().add("account", LongType).add("flag", IntegerType)
  private val leftRows = Vector(
    Row(1L, 10L, 2.0),
    Row(1L, 10L, 2.0),
    Row(2L, 20L, 1.0),
    Row(3L, null, 3.0),
    Row(null, 40L, 4.0),
    Row(4L, 50L, 5.0)
  )
  private val rightRows = Vector(Row(1L, 1), Row(1L, 1), Row(1L, 0), Row(3L, 1), Row(null, 1))
  private def setupSmall(left: Seq[Row] = leftRows, right: Seq[Row] = rightRows): Unit = {
    register("sj_left", leftSchema, left)
    register("sj_right", rightSchema, right)
  }

  describe("DISTINCT over one-sided inner join") {
    it("preserves exact bags for duplicate sides, NULLs, composite predicates and group-only aggregates") {
      setupSmall()
      compare("SELECT DISTINCT l.broker FROM sj_left l JOIN sj_right r ON l.account = r.account")
      compare("SELECT /*+ BROADCAST(r) */ DISTINCT l.broker FROM sj_left l JOIN sj_right r ON l.account = r.account")
      compare(
        "SELECT DISTINCT l.broker, l.score FROM sj_left l JOIN sj_right r ON l.account <=> r.account AND r.flag = 1"
      )
      compare("SELECT l.broker FROM sj_left l JOIN sj_right r ON l.account = r.account GROUP BY l.broker")
      compare("SELECT DISTINCT l.broker FROM sj_left l JOIN sj_right r ON l.account = r.account WHERE l.score > 1")
      setupSmall(right = Vector(Row(99L, 1)))
      compare("SELECT DISTINCT l.broker FROM sj_left l JOIN sj_right r ON l.account = r.account")
      setupSmall(right = Vector.empty)
      compare("SELECT DISTINCT l.broker FROM sj_left l JOIN sj_right r ON l.account = r.account")
      setupSmall(left = Vector.empty)
      compare("SELECT DISTINCT l.broker FROM sj_left l JOIN sj_right r ON l.account = r.account")
    }

    it("rejects SUM COUNT ordinary bags right output nondeterministic expressions and outer joins") {
      setupSmall()
      val rule = new DistinctJoinReductionRule(spark)
      val negative = Seq(
        "SELECT l.broker, SUM(l.score) FROM sj_left l JOIN sj_right r ON l.account=r.account GROUP BY l.broker",
        "SELECT l.broker, COUNT(*) FROM sj_left l JOIN sj_right r ON l.account=r.account GROUP BY l.broker",
        "SELECT l.broker FROM sj_left l JOIN sj_right r ON l.account=r.account",
        "SELECT DISTINCT l.broker, r.flag FROM sj_left l JOIN sj_right r ON l.account=r.account",
        "SELECT DISTINCT l.broker, rand() FROM sj_left l JOIN sj_right r ON l.account=r.account",
        "SELECT DISTINCT l.broker FROM sj_left l JOIN sj_right r ON l.account=r.account WHERE rand() > .5",
        "SELECT DISTINCT l.broker FROM sj_left l LEFT JOIN sj_right r ON l.account=r.account",
        "SELECT DISTINCT l.broker FROM sj_left l JOIN sj_right r ON l.account=r.account WHERE r.flag=1",
        "SELECT DISTINCT l.broker FROM (SELECT *, rand() AS noise FROM sj_left) l JOIN sj_right r ON l.account=r.account",
        "SELECT DISTINCT l.broker FROM sj_left l JOIN sj_right r ON l.account=r.account WHERE EXISTS (SELECT 1 FROM sj_right r2 WHERE r2.account=l.account)"
      )
      negative.foreach { q =>
        val analyzed = spark.sql(q).queryExecution.analyzed
        rule(analyzed).fastEquals(analyzed) shouldBe true
      }
      negative.take(4).foreach(q => compare(q, mustReduce = false))
      val leftHinted =
        "SELECT /*+ BROADCAST(l) */ DISTINCT l.broker FROM sj_left l JOIN sj_right r ON l.account=r.account"
      val hintedPlan = execute(leftHinted, enabled = false).plan
      rule(hintedPlan).fastEquals(hintedPlan) shouldBe true
      compare(leftHinted, mustReduce = false)
    }

    it("reduces the native broker membership CTE through an anti join and dense rank") {
      setupSmall()
      // Native t6/t7/t8 structure, including CTE column aliases and the unused
      // right account output that ColumnPruning must remove before reduction.
      val query = """WITH
        t3_block (broker, trade_count, notional) AS (
          SELECT broker, COUNT(*), SUM(score) FROM sj_left GROUP BY broker),
        t4_scan (account, broker) AS (SELECT account, broker FROM sj_left),
        t5_scan (account) AS (SELECT account FROM sj_right),
        t6_join (left_account, broker, right_account) AS (
          SELECT l.account, l.broker, r.account
          FROM t4_scan l INNER JOIN t5_scan r ON l.account=r.account),
        t7_block (scalar_0, broker) AS (
          SELECT 1, broker FROM t6_join WHERE broker IS NOT NULL),
        t8_distinct (scalar_0, broker) AS (
          SELECT DISTINCT scalar_0, broker FROM t7_block),
        t9_join (broker, trade_count, notional) AS (
          SELECT a.broker, a.trade_count, a.notional FROM t3_block a
          ANTI JOIN t8_distinct b ON a.broker <=> b.broker)
        SELECT broker, trade_count, notional,
          DENSE_RANK() OVER (ORDER BY notional DESC NULLS LAST) AS rk
        FROM t9_join"""
      compare(query)
    }

    it("reduces duplicate-producing membership joins without a native DISTINCT wrapper") {
      setupSmall()
      for (membership <- Seq("EXISTS", "NOT EXISTS")) {
        compare(s"""SELECT o.broker FROM sj_left o WHERE $membership (
          SELECT 1 FROM sj_left l JOIN sj_right r ON l.account = r.account
          WHERE l.broker <=> o.broker)""")
        compare(s"""SELECT o.broker, COUNT(*) FROM sj_left o WHERE $membership (
          SELECT 1 FROM sj_left l JOIN sj_right r ON l.account <=> r.account AND r.flag = 1
          WHERE l.broker <=> o.broker) GROUP BY o.broker""")
      }
      setupSmall(right = Vector.empty)
      compare("""SELECT o.broker FROM sj_left o WHERE NOT EXISTS (
        SELECT 1 FROM sj_left l JOIN sj_right r ON l.account = r.account
        WHERE l.broker <=> o.broker)""")
    }

    it("keeps membership joins whose correlation needs the right-side output") {
      setupSmall()
      for (membership <- Seq("EXISTS", "NOT EXISTS")) {
        compare(
          s"""SELECT o.broker FROM sj_left o WHERE $membership (
          SELECT 1 FROM sj_left l JOIN sj_right r ON l.account = r.account
          WHERE l.broker <=> o.broker AND r.flag = o.score)""",
          mustReduce = false
        )
      }
    }
  }
}
