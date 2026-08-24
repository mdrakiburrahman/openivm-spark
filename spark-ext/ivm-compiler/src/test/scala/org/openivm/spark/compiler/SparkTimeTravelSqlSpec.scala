package org.openivm.spark.compiler

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Unit tests for [[SparkTimeTravelSql]].
  *
  * The split must remove the Spark `temporalClause` and nothing else: the
  * de-pinned SQL is what the DuckDB compile bridge sees, so any lost relation
  * or mangled literal would silently compile a different view than the user
  * wrote.
  */
class SparkTimeTravelSqlSpec extends AnyFunSpec with Matchers {

  private def normalized(sql: String): String = sql.replaceAll("\\s+", " ").trim

  describe("split") {
    it("removes VERSION AS OF from a qualified relation and reports the pin") {
      val sql   = "SELECT region, count(*) AS c FROM arc_sql_db_bi.billing_meter_dim VERSION AS OF 366 GROUP BY region"
      val split = SparkTimeTravelSql.split(sql)
      normalized(split.sql) shouldBe
        "SELECT region, count(*) AS c FROM arc_sql_db_bi.billing_meter_dim GROUP BY region"
      split.pins.map(_.tableRef) shouldBe Seq("arc_sql_db_bi.billing_meter_dim")
      split.pins.map(_.clause) shouldBe Seq("VERSION AS OF 366")
      split.pins.map(_.shortName) shouldBe Seq("billing_meter_dim")
    }

    it("removes a pin from a bare relation") {
      val split = SparkTimeTravelSql.split("SELECT id FROM src VERSION AS OF 3")
      normalized(split.sql) shouldBe "SELECT id FROM src"
      split.pins.map(_.clause) shouldBe Seq("VERSION AS OF 3")
    }

    it("removes a pin from a backtick-quoted relation") {
      val split = SparkTimeTravelSql.split("SELECT id FROM `default`.`src` VERSION AS OF 3 WHERE id > 1")
      normalized(split.sql) shouldBe "SELECT id FROM `default`.`src` WHERE id > 1"
      split.pins.map(_.segments) shouldBe Seq(Seq("default", "src"))
    }

    it("accepts the optional FOR prefix") {
      val split = SparkTimeTravelSql.split("SELECT id FROM src FOR VERSION AS OF 7")
      normalized(split.sql) shouldBe "SELECT id FROM src"
      split.pins.map(_.clause) shouldBe Seq("FOR VERSION AS OF 7")
    }

    it("accepts SYSTEM_VERSION") {
      val split = SparkTimeTravelSql.split("SELECT id FROM src SYSTEM_VERSION AS OF 7")
      normalized(split.sql) shouldBe "SELECT id FROM src"
      split.pins.map(_.clause) shouldBe Seq("SYSTEM_VERSION AS OF 7")
    }

    it("accepts a string version literal") {
      val split = SparkTimeTravelSql.split("SELECT id FROM src VERSION AS OF '7'")
      normalized(split.sql) shouldBe "SELECT id FROM src"
      split.pins.map(_.clause) shouldBe Seq("VERSION AS OF '7'")
    }

    it("accepts TIMESTAMP AS OF with a string literal") {
      val split = SparkTimeTravelSql.split("SELECT id FROM src TIMESTAMP AS OF '2024-01-01 00:00:00'")
      normalized(split.sql) shouldBe "SELECT id FROM src"
      split.pins.map(_.clause) shouldBe Seq("TIMESTAMP AS OF '2024-01-01 00:00:00'")
    }

    it("accepts TIMESTAMP AS OF with a function-call expression") {
      val split = SparkTimeTravelSql.split("SELECT id FROM src TIMESTAMP AS OF date_sub(current_date(), 1)")
      normalized(split.sql) shouldBe "SELECT id FROM src"
      split.pins.map(_.clause) shouldBe Seq("TIMESTAMP AS OF date_sub(current_date(), 1)")
    }

    it("removes pins from every relation of a join, preserving both") {
      val sql =
        "SELECT d.region, f.amount FROM dim VERSION AS OF 2 d JOIN fact VERSION AS OF 5 f ON f.dim_id = d.id"
      val split = SparkTimeTravelSql.split(sql)
      normalized(split.sql) shouldBe
        "SELECT d.region, f.amount FROM dim d JOIN fact f ON f.dim_id = d.id"
      split.pins.map(_.tableRef) shouldBe Seq("dim", "fact")
      split.pins.map(_.clause) shouldBe Seq("VERSION AS OF 2", "VERSION AS OF 5")
    }

    it("removes a pin from only the pinned side of a mixed join") {
      val sql   = "SELECT d.region FROM dim VERSION AS OF 2 d JOIN fact f ON f.dim_id = d.id"
      val split = SparkTimeTravelSql.split(sql)
      normalized(split.sql) shouldBe "SELECT d.region FROM dim d JOIN fact f ON f.dim_id = d.id"
      split.pins.map(_.tableRef) shouldBe Seq("dim")
    }

    it("removes a pin inside a subquery") {
      val sql   = "SELECT x FROM (SELECT id AS x FROM src VERSION AS OF 4) t"
      val split = SparkTimeTravelSql.split(sql)
      normalized(split.sql) shouldBe "SELECT x FROM (SELECT id AS x FROM src) t"
      split.pins.map(_.tableRef) shouldBe Seq("src")
    }

    it("removes a pin inside a CTE") {
      val sql   = "WITH a AS (SELECT id FROM src VERSION AS OF 4) SELECT id FROM a"
      val split = SparkTimeTravelSql.split(sql)
      normalized(split.sql) shouldBe "WITH a AS (SELECT id FROM src) SELECT id FROM a"
      split.pins.map(_.tableRef) shouldBe Seq("src")
    }
  }

  describe("split — text that must not be touched") {
    it("leaves an unpinned query byte-identical") {
      val sql = "SELECT region, count(*) AS c FROM dim GROUP BY region"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
    }

    it("leaves a string literal that spells a temporal clause untouched") {
      val sql = "SELECT id FROM src WHERE note = 'VERSION AS OF 366'"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
    }

    it("leaves a line comment that spells a temporal clause untouched") {
      val sql = "SELECT id -- VERSION AS OF 366\nFROM src"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
    }

    it("leaves a block comment that spells a temporal clause untouched") {
      val sql = "SELECT id /* VERSION AS OF 366 */ FROM src"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
    }

    it("leaves a column named version aliased to of untouched") {
      val sql = "SELECT version AS of FROM src"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
    }

    it("leaves unparseable SQL untouched instead of guessing") {
      val sql = "SELECT id FROM src VERSION AS OF"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
    }

    it("refuses to split a source pinned at two different versions") {
      val sql = "SELECT a.id FROM src VERSION AS OF 2 a JOIN src VERSION AS OF 5 b ON a.id = b.id"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
    }

    it("splits a source pinned twice at the same version") {
      val sql   = "SELECT a.id FROM src VERSION AS OF 2 a JOIN src VERSION AS OF 2 b ON a.id = b.id"
      val split = SparkTimeTravelSql.split(sql)
      normalized(split.sql) shouldBe "SELECT a.id FROM src a JOIN src b ON a.id = b.id"
      split.pins.map(_.clause).distinct shouldBe Seq("VERSION AS OF 2")
    }

    it("refuses to split a source that is also read live") {
      val sql = "SELECT a.id FROM src VERSION AS OF 2 a JOIN src b ON a.id = b.id"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
    }

    it("refuses to split when a CTE shadows the pinned source name") {
      val sql = "WITH src AS (SELECT 1 AS id) SELECT s.id FROM db.src VERSION AS OF 2 s JOIN src c ON s.id = c.id"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
    }
  }

  describe("hasSnapshotPin") {
    it("is true only when a real pin is present") {
      SparkTimeTravelSql.hasSnapshotPin("SELECT id FROM src VERSION AS OF 3") shouldBe true
      SparkTimeTravelSql.hasSnapshotPin("SELECT id FROM src") shouldBe false
      SparkTimeTravelSql.hasSnapshotPin("SELECT id FROM src WHERE n = 'VERSION AS OF 3'") shouldBe false
      SparkTimeTravelSql.hasSnapshotPin(null) shouldBe false
    }
  }

  describe("stripSnapshotPins") {
    it("is idempotent") {
      val once  = SparkTimeTravelSql.stripSnapshotPins("SELECT id FROM src VERSION AS OF 3")
      val twice = SparkTimeTravelSql.stripSnapshotPins(once)
      once shouldBe twice
    }

    it("produces SQL that parses without a time-travel node") {
      val stripped = SparkTimeTravelSql.stripSnapshotPins(
        "SELECT region, count(*) AS c FROM db.dim VERSION AS OF 366 GROUP BY region"
      )
      stripped should not include "VERSION AS OF"
      stripped should include("db.dim")
    }
  }

  describe("pin resolution against a view's source tables") {
    it("resolves a bare pin to the qualified source with the same short name") {
      val sql = "SELECT id FROM src VERSION AS OF 9"
      SparkTimeTravelSql.pinsByQualifiedSource(sql, Seq("default.src")) shouldBe
        Map("default.src" -> "VERSION AS OF 9")
      SparkTimeTravelSql.pinsByShortSource(sql, Seq("default.src")) shouldBe
        Map("src" -> "VERSION AS OF 9")
    }

    it("resolves a qualified pin") {
      val sql = "SELECT id FROM arc_sql_db_bi.billing_meter_dim VERSION AS OF 366"
      SparkTimeTravelSql.pinsByQualifiedSource(sql, Seq("arc_sql_db_bi.billing_meter_dim")) shouldBe
        Map("arc_sql_db_bi.billing_meter_dim" -> "VERSION AS OF 366")
    }

    it("resolves only the pinned source of a mixed join") {
      val sql = "SELECT d.region FROM dim VERSION AS OF 2 d JOIN fact f ON f.dim_id = d.id"
      SparkTimeTravelSql.pinsByQualifiedSource(sql, Seq("default.dim", "default.fact")) shouldBe
        Map("default.dim" -> "VERSION AS OF 2")
    }

    it("does not resolve a pin whose qualifier disagrees with the source") {
      val sql = "SELECT id FROM other_db.src VERSION AS OF 9"
      SparkTimeTravelSql.pinsByQualifiedSource(sql, Seq("default.src")) shouldBe empty
    }

    it("returns no pins for an unpinned view body") {
      SparkTimeTravelSql.pinsByQualifiedSource("SELECT id FROM src", Seq("default.src")) shouldBe empty
    }
  }

  describe("identifierSegments") {
    it("unquotes and lower-cases every segment") {
      SparkTimeTravelSql.identifierSegments("`Db`.`My Table`") shouldBe Seq("db", "my table")
      SparkTimeTravelSql.identifierSegments("DB.T") shouldBe Seq("db", "t")
      SparkTimeTravelSql.identifierSegments("t") shouldBe Seq("t")
    }
  }
}
