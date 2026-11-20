package org.openivm.spark.parity

import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class AssemblySplitSpec extends IvmParitySpecBase("assembly-split") with CdfMode {
  override protected def extraSparkConf: Map[String, String] =
    Map("spark.databricks.delta.snapshotPartitions" -> "1")

  it("repeats actual incremental writes on synthetic four-source nested SQL") {
    val main = "asmsp_fact"
    val others = Seq("asmsp_left", "asmsp_right", "asmsp_aux")
    (main +: others).foreach { name =>
      sql(s"CREATE TABLE $name (k INT, v INT, label STRING) USING DELTA")
      sql(s"INSERT INTO $name VALUES (1, 10, 'first'), (2, 20, NULL)")
    }
    val nested = "asmsp_nested"
    val expressions = (1 to 22).map(i => s"CASE WHEN f.v >= $i THEN f.v + $i ELSE 0 END AS Metric$i")
    val body = s"""WITH prep AS (
                  | SELECT f.k, f.v, f.label, d.v AS right_value, ${expressions.mkString(", ")}
                  | FROM $main f JOIN ${others(0)} l ON f.k = l.k
                  | JOIN ${others(1)} d ON f.k = d.k JOIN ${others(2)} a ON f.k = a.k
                  |), shifted AS (SELECT * FROM prep WHERE v >= 0)
                  |SELECT * FROM shifted WHERE right_value >= 0""".stripMargin
    sql(s"CREATE MATERIALIZED VIEW $nested AS $body")
    assertMvCorrect(nested, body)
    Seq(false, true, false, true).zipWithIndex.foreach { case (all, index) =>
      val current = mvDataVersion(nested)
      val k       = 3 + index
      (main +: others).zipWithIndex.foreach { case (name, j) =>
        if (all || j == 0) {
          sql(s"INSERT INTO $name VALUES (1, ${11 + index}, NULL)")
          sql(s"INSERT INTO $name VALUES ($k, ${30 + index}, 'seed')")
        }
      }
      refreshMv(nested)
      assert(mvDataVersion(nested) > current)
      assertMvCorrect(nested, body)
    }
  }
}
