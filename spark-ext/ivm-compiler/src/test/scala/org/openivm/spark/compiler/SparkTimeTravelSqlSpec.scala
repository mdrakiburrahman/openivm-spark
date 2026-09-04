package org.openivm.spark.compiler

import org.openivm.spark.common.{TimeTravelPinReason, TimeTravelPinStatus}
import org.scalatest.OptionValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Unit tests for [[SparkTimeTravelSql]].
  *
  * The split must remove the Spark `temporalClause` and nothing else: the
  * de-pinned SQL is what the DuckDB compile bridge sees, so any lost relation
  * or mangled literal would silently compile a different view than the user
  * wrote.
  */
class SparkTimeTravelSqlSpec extends AnyFunSpec with Matchers with OptionValues {

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

  /** Aliased relations are the shape dbt emits for every `ref()`
    * (`` from `cat`.`sch`.`model` as p ``), and they are also where a
    * pin-aware LPTS front-end is easiest to get wrong: Spark's grammar is
    * `identifierReference temporalClause? tableAlias`, so the clause sits
    * BETWEEN the relation and its alias. Re-emitting it after the alias
    * (`... p VERSION AS OF 2`) is invalid in both dialects.
    *
    * The bridge sidesteps that entirely — the clause never reaches DuckDB —
    * but only if the split keeps the alias attached to the relation, and only
    * if the alias is never mistaken for part of the version value or for the
    * pinned relation itself. These cases lock that down.
    */
  describe("split — aliased dbt-style relations") {
    it("keeps a bare alias attached to the relation and pins the relation, not the alias") {
      val split = SparkTimeTravelSql.split("SELECT p.id FROM db.dim VERSION AS OF 2 p WHERE p.id > 1")
      normalized(split.sql) shouldBe "SELECT p.id FROM db.dim p WHERE p.id > 1"
      split.pins.map(_.tableRef) shouldBe Seq("db.dim")
      split.pins.map(_.clause) shouldBe Seq("VERSION AS OF 2")
    }

    it("keeps an AS alias attached to the relation") {
      val split = SparkTimeTravelSql.split("SELECT p.id FROM db.dim VERSION AS OF 2 AS p")
      normalized(split.sql) shouldBe "SELECT p.id FROM db.dim AS p"
      split.pins.map(_.tableRef) shouldBe Seq("db.dim")
    }

    it("keeps the alias of a backtick-qualified dbt relation") {
      val sql =
        "select p.grp, sum(p.val) as total " +
          "from `spark_catalog`.`analytics`.`billing_meter_dim` version as of 366 as p " +
          "group by p.grp"
      val split = SparkTimeTravelSql.split(sql)
      normalized(split.sql) shouldBe
        "select p.grp, sum(p.val) as total from `spark_catalog`.`analytics`.`billing_meter_dim` as p group by p.grp"
      split.pins.map(_.segments) shouldBe Seq(Seq("spark_catalog", "analytics", "billing_meter_dim"))
      split.pins.map(_.shortName) shouldBe Seq("billing_meter_dim")
    }

    it("keeps the alias of a pinned relation inside a dbt CTE chain") {
      val sql =
        """with source as (
          |  select id, grp, val from `cat`.`sch`.`dim` version as of 366 as d
          |),
          |renamed as (
          |  select id as dim_id, grp, val from source
          |)
          |select grp, sum(val) as total from renamed group by grp""".stripMargin
      val split = SparkTimeTravelSql.split(sql)
      normalized(split.sql) should include("from `cat`.`sch`.`dim` as d")
      split.pins.map(_.shortName) shouldBe Seq("dim")
    }

    it("keeps both aliases when only one side of a dbt-style join is pinned") {
      val sql =
        "select d.region, f.amount from `cat`.`sch`.`dim` version as of 2 as d " +
          "inner join `cat`.`sch`.`fact` as f on f.dim_id = d.dim_id"
      val split = SparkTimeTravelSql.split(sql)
      normalized(split.sql) shouldBe
        "select d.region, f.amount from `cat`.`sch`.`dim` as d " +
        "inner join `cat`.`sch`.`fact` as f on f.dim_id = d.dim_id"
      split.pins.map(_.shortName) shouldBe Seq("dim")
    }

    it("keeps an alias written on the next line") {
      val split = SparkTimeTravelSql.split("SELECT p.id\nFROM db.dim VERSION AS OF 2\n  AS p")
      normalized(split.sql) shouldBe "SELECT p.id FROM db.dim AS p"
      split.pins.map(_.clause) shouldBe Seq("VERSION AS OF 2")
    }

    it("does not swallow the alias into a TIMESTAMP pin value") {
      val split = SparkTimeTravelSql.split("SELECT p.id FROM db.dim TIMESTAMP AS OF '2024-01-01' p")
      normalized(split.sql) shouldBe "SELECT p.id FROM db.dim p"
      split.pins.map(_.clause) shouldBe Seq("TIMESTAMP AS OF '2024-01-01'")
    }

    it("refuses a function-valued TIMESTAMP pin instead of swallowing the alias") {
      val sql = "SELECT p.id FROM db.dim TIMESTAMP AS OF current_timestamp() AS p"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
      SparkTimeTravelSql.hasUnsupportedSnapshotPin(sql) shouldBe true
    }

    it("never emits a temporal clause after the alias") {
      val bodies = Seq(
        "SELECT p.id FROM db.dim VERSION AS OF 2 p",
        "SELECT p.id FROM db.dim VERSION AS OF 2 AS p",
        "select p.id from `cat`.`sch`.`dim` version as of 2 as p",
        "SELECT p.id FROM db.dim TIMESTAMP AS OF '2024-01-01' p"
      )
      bodies.foreach { sql =>
        val split = SparkTimeTravelSql.split(sql)
        withClue(s"$sql -> ${split.sql}: ") {
          split.pins should have size 1
          split.sql.toUpperCase should not include "AS OF"
          normalized(split.sql) should endWith("p")
        }
      }
    }

    it("leaves an aliased but unpinned dbt relation byte-identical") {
      val sql = "select p.grp from `cat`.`sch`.`dim` as p"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
    }

    it("resolves an aliased pin against the qualified source, ignoring the alias") {
      val sql = "select p.grp from `cat`.`sch`.`dim` version as of 2 as p"
      SparkTimeTravelSql.pinsByQualifiedSource(sql, Seq("cat.sch.dim")) shouldBe
        Map("cat.sch.dim" -> "version as of 2")
      SparkTimeTravelSql.pinsByQualifiedSource(sql, Seq("cat.sch.p")) shouldBe empty
    }
  }

  /** A comment is TRIVIA: Spark allows one between a relation and its temporal
    * clause, so `FROM t -- freeze at load time\n VERSION AS OF 4` pins `t`.
    * Scanning backwards through the emitted text would bind the last word of
    * the comment (`time`) as the relation instead, and a pin on a relation the
    * view does not read resolves to NO source: the compile would succeed, the
    * frozen relation would be maintained from live rows, and nothing would say
    * so. Spark's parser is the authority on which relation carries the clause.
    */
  describe("split — comments around a pin") {
    it("binds the pin to the relation, not to the last word of a line comment") {
      val sql   = "SELECT id FROM t -- freeze at load time\n VERSION AS OF 4"
      val split = SparkTimeTravelSql.split(sql)
      split.pins.map(_.tableRef) shouldBe Seq("t")
      split.pins.map(_.clause) shouldBe Seq("VERSION AS OF 4")
      split.sql should include("-- freeze at load time")
      split.sql.toUpperCase should not include "VERSION AS OF"
      SparkTimeTravelSql.pinsByQualifiedSource(sql, Seq("db.t")) shouldBe Map("db.t" -> "VERSION AS OF 4")
    }

    it("does not bind a table named in a comment") {
      val sql   = "SELECT id FROM db.t -- see also other_table\n VERSION AS OF 4"
      val split = SparkTimeTravelSql.split(sql)
      split.pins.map(_.tableRef) shouldBe Seq("db.t")
      SparkTimeTravelSql.pinsByQualifiedSource(sql, Seq("db.other_table")) shouldBe empty
      SparkTimeTravelSql.unresolvedPins(sql, Seq("db.other_table")) should have size 1
    }

    it("binds through a block comment between the relation and its clause") {
      val sql   = "SELECT p.id FROM `cat`.`sch`.`dim` /* dbt: pinned ref */ VERSION AS OF 366 AS p"
      val split = SparkTimeTravelSql.split(sql)
      split.pins.map(_.shortName) shouldBe Seq("dim")
      split.pins.map(_.clause) shouldBe Seq("VERSION AS OF 366")
      normalized(split.sql) shouldBe "SELECT p.id FROM `cat`.`sch`.`dim` /* dbt: pinned ref */ AS p"
    }

    it("drops a block comment written between the clause keywords") {
      val split = SparkTimeTravelSql.split("SELECT id FROM db.t VERSION /* pinned */ AS OF 4")
      split.pins.map(_.tableRef) shouldBe Seq("db.t")
      split.pins.map(_.clause) shouldBe Seq("VERSION AS OF 4")
    }

    it("never carries a line comment into the re-applied clause text") {
      val split = SparkTimeTravelSql.split("SELECT p.id FROM db.t VERSION -- pinned\n AS OF 4 AS p")
      split.pins.map(_.tableRef) shouldBe Seq("db.t")
      // A clause re-emitted as `VERSION -- pinned AS OF 4` would comment out the
      // rest of the statement Spark executes.
      split.pins.map(_.clause) shouldBe Seq("VERSION AS OF 4")
      normalized(split.sql) shouldBe "SELECT p.id FROM db.t AS p"
    }

    it("binds a quoted, qualified relation inside a subquery") {
      val sql =
        "SELECT y.id FROM (SELECT x.id FROM `cat`.`sch`.`dim` /* pin */ VERSION AS OF 2 x) y"
      val split = SparkTimeTravelSql.split(sql)
      split.pins.map(_.segments) shouldBe Seq(Seq("cat", "sch", "dim"))
      split.sql.toUpperCase should not include "VERSION AS OF"
    }

    it("leaves a comment-only lookalike untouched") {
      val sql = "SELECT id FROM src /* VERSION AS OF 4 */ WHERE id > 1"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
      SparkTimeTravelSql.hasUnsupportedSnapshotPin(sql) shouldBe false
    }
  }

  /** A pin OpenIVM honors is FROZEN once: the clause it re-applies is fixed
    * text and staged deltas for the pinned source are dropped. A value that
    * moves with wall-clock time would therefore be evaluated at CREATE and then
    * maintained against a different snapshot on every refresh — silently wrong.
    * Only stable literals are lifted; everything else must fall back to
    * FULL_REFRESH, which re-evaluates the user's expression each run.
    */
  describe("split — moving pin values are refused") {
    it("refuses every non-literal TIMESTAMP AS OF value") {
      Seq(
        "SELECT id FROM src TIMESTAMP AS OF current_timestamp()",
        "SELECT id FROM src TIMESTAMP AS OF now()",
        "SELECT id FROM src TIMESTAMP AS OF date_sub(current_date(), 1)",
        "SELECT id FROM src TIMESTAMP AS OF date_add(current_date(), -1)",
        "SELECT id FROM src SYSTEM_TIME AS OF current_timestamp()",
        "SELECT id FROM src TIMESTAMP AS OF date '2024-01-01'"
      ).foreach { sql =>
        withClue(s"$sql: ") {
          SparkTimeTravelSql.split(sql).sql shouldBe sql
          SparkTimeTravelSql.split(sql).pins shouldBe empty
          SparkTimeTravelSql.hasUnsupportedSnapshotPin(sql) shouldBe true
        }
      }
    }

    it("leaves a pin Spark itself rejects to the downstream parser") {
      // Spark refuses `current_date` here ("timestamp expression cannot refer to
      // any columns"), so there is no parsed pin to refuse: the body is passed
      // through unchanged and DuckDB aborts the compile, as it always did.
      val sql = "SELECT id FROM src TIMESTAMP AS OF current_date"
      SparkTimeTravelSql.split(sql).sql shouldBe sql
      SparkTimeTravelSql.split(sql).pins shouldBe empty
      SparkTimeTravelSql.hasUnsupportedSnapshotPin(sql) shouldBe false
    }

    it("refuses a moving pin even when another source is pinned to a literal") {
      val sql = "SELECT d.region, f.amount FROM dim VERSION AS OF 2 d " +
        "JOIN fact TIMESTAMP AS OF current_timestamp() f ON f.dim_id = d.id"
      SparkTimeTravelSql.split(sql).pins shouldBe empty
      SparkTimeTravelSql.hasUnsupportedSnapshotPin(sql) shouldBe true
    }

    it("still accepts the stable literal forms") {
      Seq(
        "SELECT id FROM src TIMESTAMP AS OF '2024-01-01'",
        "SELECT id FROM src SYSTEM_TIME AS OF '2024-01-01 12:30:00'",
        "SELECT id FROM src VERSION AS OF 366",
        "SELECT id FROM src VERSION AS OF '366'"
      ).foreach { sql =>
        withClue(s"$sql: ") {
          SparkTimeTravelSql.split(sql).pins should have size 1
          SparkTimeTravelSql.hasUnsupportedSnapshotPin(sql) shouldBe false
        }
      }
    }
  }

  describe("unresolvedPins / unsupportedSnapshotPinReason") {
    it("flags a pin that resolves to no tracked source") {
      val sql = "SELECT id FROM other_db.src VERSION AS OF 9"
      SparkTimeTravelSql.unresolvedPins(sql, Seq("default.src")).map(_.tableRef) shouldBe Seq("other_db.src")
      SparkTimeTravelSql.unsupportedSnapshotPinReason(sql, Seq("default.src")).value should include("other_db.src")
    }

    it("flags a pin that resolves to several tracked sources") {
      val sql = "SELECT id FROM t VERSION AS OF 2"
      SparkTimeTravelSql.unresolvedPins(sql, Seq("db1.t", "db2.t")) should have size 1
      SparkTimeTravelSql.unsupportedSnapshotPinReason(sql, Seq("db1.t", "db2.t")) shouldBe defined
    }

    it("accepts a pin that resolves to exactly one tracked source, qualified either way") {
      SparkTimeTravelSql.unresolvedPins("SELECT id FROM sales VERSION AS OF 9", Seq("tpcdi.sales")) shouldBe empty
      SparkTimeTravelSql.unresolvedPins("SELECT id FROM tpcdi.sales VERSION AS OF 9", Seq("sales")) shouldBe empty
      SparkTimeTravelSql.unsupportedSnapshotPinReason(
        "SELECT id FROM tpcdi.sales VERSION AS OF 9",
        Seq("tpcdi.sales")
      ) shouldBe None
    }

    it("skips the resolution check when no sources are supplied") {
      SparkTimeTravelSql.unsupportedSnapshotPinReason("SELECT id FROM other_db.src VERSION AS OF 9", Nil) shouldBe None
    }

    it("reports the un-liftable shapes regardless of sources") {
      SparkTimeTravelSql
        .unsupportedSnapshotPinReason(
          "SELECT a.id FROM src VERSION AS OF 2 a JOIN src VERSION AS OF 5 b ON a.id = b.id",
          Seq("default.src")
        )
        .value should include("two different versions")
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

  describe("hasUnsupportedSnapshotPin") {
    // OpenIVM re-applies a pin per SOURCE. Shapes it cannot honor were, until
    // now, caught by accident: the un-split body still carried `VERSION AS OF`,
    // so DuckDB's parser aborted the compile. The pinned LPTS (`dbac36de`)
    // accepts Spark's `temporalClause` — aliased and two-version reads
    // included — which removes that accident, so the bridge has to recognise
    // them itself.
    it("is true for a source read at two different versions") {
      SparkTimeTravelSql.hasUnsupportedSnapshotPin(
        "SELECT a.id FROM src VERSION AS OF 2 a JOIN src VERSION AS OF 5 b ON a.id = b.id"
      ) shouldBe true
    }

    it("is true for an aliased dbt-shaped source read at two different versions") {
      SparkTimeTravelSql.hasUnsupportedSnapshotPin(
        "select a.id from `cat`.`sch`.`dim` version as of 2 as a " +
          "inner join `cat`.`sch`.`dim` version as of 5 as b on a.id = b.id"
      ) shouldBe true
    }

    it("is true for a source that is pinned in one place and read live in another") {
      SparkTimeTravelSql.hasUnsupportedSnapshotPin(
        "SELECT a.id FROM src VERSION AS OF 2 a JOIN src b ON a.id = b.id"
      ) shouldBe true
    }

    it("is true when a CTE shadows the pinned source name") {
      SparkTimeTravelSql.hasUnsupportedSnapshotPin(
        "WITH src AS (SELECT 1 AS id) SELECT s.id FROM db.src VERSION AS OF 2 s JOIN src c ON s.id = c.id"
      ) shouldBe true
    }

    it("is false for every shape the splitter handles") {
      Seq(
        "SELECT id FROM src VERSION AS OF 3",
        "SELECT p.id FROM db.dim VERSION AS OF 2 AS p",
        "select p.grp from `cat`.`sch`.`dim` version as of 2 as p",
        "SELECT a.id FROM src VERSION AS OF 2 a JOIN src VERSION AS OF 2 b ON a.id = b.id",
        "SELECT d.region FROM dim VERSION AS OF 2 d JOIN fact f ON f.dim_id = d.id",
        "SELECT id FROM src TIMESTAMP AS OF '2024-01-01'"
      ).foreach { sql =>
        withClue(s"$sql: ") { SparkTimeTravelSql.hasUnsupportedSnapshotPin(sql) shouldBe false }
      }
    }

    it("is false for text that only mentions a temporal clause") {
      SparkTimeTravelSql.hasUnsupportedSnapshotPin("SELECT id FROM src WHERE n = 'VERSION AS OF 3'") shouldBe false
      SparkTimeTravelSql.hasUnsupportedSnapshotPin("SELECT id -- VERSION AS OF 3\nFROM src") shouldBe false
      SparkTimeTravelSql.hasUnsupportedSnapshotPin("SELECT version AS of FROM src") shouldBe false
    }

    it("is false for an unpinned body and for SQL that does not parse") {
      SparkTimeTravelSql.hasUnsupportedSnapshotPin("SELECT id FROM src") shouldBe false
      SparkTimeTravelSql.hasUnsupportedSnapshotPin("SELECT id FROM src VERSION AS OF") shouldBe false
      SparkTimeTravelSql.hasUnsupportedSnapshotPin(null) shouldBe false
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

  describe("pinStatus / pinIdentity") {
    it("reports APPLIED with a stable identity for a resolved pin") {
      val sql = "SELECT id FROM arc_sql_db_bi.billing_meter_dim VERSION AS OF 366"
      SparkTimeTravelSql.pinStatus(sql, Seq("arc_sql_db_bi.billing_meter_dim")) shouldBe "APPLIED"
      SparkTimeTravelSql.pinIdentity(sql, Seq("arc_sql_db_bi.billing_meter_dim")) shouldBe
        Seq("arc_sql_db_bi.billing_meter_dim=VERSION AS OF 366")
    }

    it("reports APPLIED through a dbt-style alias and keeps the user's clause case") {
      val sql = "select p.id from `db`.`dim` version as of 2 as p"
      SparkTimeTravelSql.pinStatus(sql, Seq("db.dim")) shouldBe "APPLIED"
      SparkTimeTravelSql.pinIdentity(sql, Seq("db.dim")) shouldBe Seq("db.dim=version as of 2")
    }

    it("sorts a multi-source identity on the rendered entry") {
      val sql =
        "SELECT c.id FROM db.customer VERSION AS OF 3 c JOIN db.customer_address VERSION AS OF 7 a ON a.id = c.id"
      SparkTimeTravelSql.pinIdentity(sql, Seq("db.customer_address", "db.customer")) shouldBe
        Seq("db.customer=VERSION AS OF 3", "db.customer_address=VERSION AS OF 7")
      SparkTimeTravelSql.pinStatus(sql, Seq("db.customer_address", "db.customer")) shouldBe "APPLIED"
    }

    it("reports NOT_APPLICABLE for an unpinned body") {
      SparkTimeTravelSql.pinStatus("SELECT id FROM src", Seq("default.src")) shouldBe "NOT_APPLICABLE"
      SparkTimeTravelSql.pinIdentity("SELECT id FROM src", Seq("default.src")) shouldBe empty
    }

    it("reports COMPILE_FAILED for a source read at two versions") {
      val sql = "SELECT a.id FROM src VERSION AS OF 1 a JOIN src VERSION AS OF 2 b ON a.id = b.id"
      SparkTimeTravelSql.pinStatus(sql, Seq("default.src")) shouldBe "COMPILE_FAILED"
      SparkTimeTravelSql.pinIdentity(sql, Seq("default.src")) shouldBe empty
    }

    it("reports COMPILE_FAILED for a moving pin value") {
      val sql = "SELECT id FROM src TIMESTAMP AS OF current_timestamp()"
      SparkTimeTravelSql.pinStatus(sql, Seq("default.src")) shouldBe "COMPILE_FAILED"
    }

    it("reports COMPILE_FAILED for a pin that binds to no tracked source") {
      val sql = "SELECT id FROM other_db.src VERSION AS OF 9"
      SparkTimeTravelSql.pinStatus(sql, Seq("default.src")) shouldBe "COMPILE_FAILED"
      SparkTimeTravelSql.pinIdentity(sql, Seq("default.src")) shouldBe empty
    }
  }

  describe("pinTelemetry") {
    it("is operation-invariant: the same body and sources yield the same telemetry") {
      val sql     = "SELECT id FROM db.dim VERSION AS OF 366"
      val sources = Seq("db.dim")

      // CREATE derives from the user body + collected sources; REFRESH derives
      // from the SAME two values read back from MvMetadata.
      val atCreate  = SparkTimeTravelSql.pinTelemetry(sql, sources)
      val atRefresh = SparkTimeTravelSql.pinTelemetry(sql, sources)

      atRefresh shouldBe atCreate
      atCreate.status shouldBe TimeTravelPinStatus.Applied
      atCreate.reason shouldBe TimeTravelPinReason.PinsResolved
      atCreate.pins shouldBe Seq("db.dim=VERSION AS OF 366")
      atCreate.detail shouldBe None
    }

    it("reports NOT_APPLICABLE with no_user_pin for an unpinned body") {
      val telemetry = SparkTimeTravelSql.pinTelemetry("SELECT id FROM db.src", Seq("db.src"))
      telemetry.status shouldBe TimeTravelPinStatus.NotApplicable
      telemetry.reason shouldBe TimeTravelPinReason.NoUserPin
      telemetry.pins shouldBe empty
    }

    it("refuses a pinned body that tracks no source at all") {
      val telemetry = SparkTimeTravelSql.pinTelemetry("SELECT id FROM db.src VERSION AS OF 3", Nil)
      telemetry.status shouldBe TimeTravelPinStatus.CompileFailed
      telemetry.reason shouldBe TimeTravelPinReason.NoTrackedSources
      telemetry.pins shouldBe empty
      telemetry.detail.value should include("tracks no source")
    }

    it("still reports NOT_APPLICABLE for an unpinned body with no tracked source") {
      val telemetry = SparkTimeTravelSql.pinTelemetry("SELECT id FROM db.src", Nil)
      telemetry.status shouldBe TimeTravelPinStatus.NotApplicable
      telemetry.reason shouldBe TimeTravelPinReason.NoUserPin
    }

    it("lifts no pin identity for a refused body") {
      val ambiguous =
        SparkTimeTravelSql.pinTelemetry(
          "SELECT a.id FROM db.src VERSION AS OF 1 a JOIN db.src VERSION AS OF 2 b ON a.id = b.id",
          Seq("db.src")
        )
      ambiguous.status shouldBe TimeTravelPinStatus.CompileFailed
      ambiguous.reason shouldBe TimeTravelPinReason.UnsupportedPinShape
      ambiguous.pins shouldBe empty

      val unresolved =
        SparkTimeTravelSql.pinTelemetry("SELECT id FROM other_db.src VERSION AS OF 9", Seq("default.src"))
      unresolved.status shouldBe TimeTravelPinStatus.CompileFailed
      unresolved.reason shouldBe TimeTravelPinReason.PinNotResolvedToSingleSource
      unresolved.pins shouldBe empty
      unresolved.detail.value should include("other_db.src")
    }

    it("agrees with the compile bridge's own refusal check") {
      val refused = Seq(
        "SELECT a.id FROM db.src VERSION AS OF 1 a JOIN db.src VERSION AS OF 2 b ON a.id = b.id",
        "SELECT id FROM other_db.src VERSION AS OF 9",
        "SELECT id FROM db.src TIMESTAMP AS OF current_timestamp()"
      )
      refused.foreach { sql =>
        withClue(s"$sql: ") {
          SparkTimeTravelSql
            .pinRefusal(sql, Seq("db.src"), requireTrackedSources = true)
            .map(_.detail) shouldBe SparkTimeTravelSql.unsupportedSnapshotPinReason(
            sql,
            Seq("db.src"),
            requireTrackedSources = true
          )
          SparkTimeTravelSql.pinTelemetry(sql, Seq("db.src")).status shouldBe TimeTravelPinStatus.CompileFailed
        }
      }
    }

    it("keeps every emitted reason inside the telemetry vocabulary") {
      val bodies = Seq(
        "SELECT id FROM db.src"                                                                  -> Seq("db.src"),
        "SELECT id FROM db.src VERSION AS OF 3"                                                  -> Seq("db.src"),
        "SELECT id FROM db.src VERSION AS OF 3"                                                  -> Nil,
        "SELECT id FROM other_db.src VERSION AS OF 9"                                            -> Seq("db.src"),
        "SELECT a.id FROM db.src VERSION AS OF 1 a JOIN db.src VERSION AS OF 2 b ON a.id = b.id" -> Seq("db.src")
      )
      bodies.foreach { case (sql, sources) =>
        val telemetry = SparkTimeTravelSql.pinTelemetry(sql, sources)
        withClue(s"$sql / $sources: ") {
          TimeTravelPinStatus.All should contain(telemetry.status)
          TimeTravelPinReason.All should contain(telemetry.reason)
        }
      }
    }
  }

  describe("identifierSegments") {
    it("unquotes and lower-cases every segment") {
      SparkTimeTravelSql.identifierSegments("`Db`.`My Table`") shouldBe Seq("db", "my table")
      SparkTimeTravelSql.identifierSegments("DB.T") shouldBe Seq("db", "t")
      SparkTimeTravelSql.identifierSegments("t") shouldBe Seq("t")
    }
  }

  describe("repinVersions") {
    it("rewrites an exact multi-source VERSION map and preserves the pin contract") {
      val sql =
        """SELECT a.id, b.name
          |FROM db.a VERSION AS OF 4 a
          |JOIN `db`.`b` FOR SYSTEM_VERSION AS OF '9' b ON a.id = b.id""".stripMargin

      val repin = SparkTimeTravelSql
        .repinVersions(sql, Seq("db.a", "db.b"), Map("a" -> 5L, "db.b" -> 12L))
        .right
        .toOption
        .value

      repin.currentVersions shouldBe Map("db.a" -> 4L, "db.b" -> 9L)
      repin.targetVersions shouldBe Map("db.a" -> 5L, "db.b" -> 12L)
      normalized(repin.querySql) shouldBe
        "SELECT a.id, b.name FROM db.a VERSION AS OF 5 a JOIN `db`.`b` VERSION AS OF 12 b ON a.id = b.id"
      repin.pins shouldBe Seq("db.a=VERSION AS OF 5", "db.b=VERSION AS OF 12")
    }

    it("accepts an identical map for idempotent retry") {
      val repin = SparkTimeTravelSql
        .repinVersions(
          "SELECT id FROM db.src VERSION AS OF 7",
          Seq("db.src"),
          Map("db.src" -> 7L)
        )
        .right
        .toOption
        .value

      repin.querySql shouldBe "SELECT id FROM db.src VERSION AS OF 7"
      repin.currentVersions shouldBe repin.targetVersions
    }

    it("rejects partial, duplicate-resolved, backward, and timestamp maps") {
      val multi =
        "SELECT a.id FROM db.a VERSION AS OF 4 a JOIN db.b VERSION AS OF 9 b ON a.id = b.id"

      SparkTimeTravelSql
        .repinVersions(multi, Seq("db.a", "db.b"), Map("db.a" -> 5L))
        .left
        .toOption
        .value should include("missing: db.b")

      SparkTimeTravelSql
        .repinVersions(
          "SELECT id FROM db.src VERSION AS OF 4",
          Seq("db.src"),
          Map("src" -> 5L, "db.src" -> 5L)
        )
        .left
        .toOption
        .value should include("more than once")

      SparkTimeTravelSql
        .repinVersions(
          "SELECT id FROM db.src VERSION AS OF 4",
          Seq("db.src"),
          Map("db.src" -> 3L)
        )
        .left
        .toOption
        .value should include("moves backwards")

      SparkTimeTravelSql
        .repinVersions(
          "SELECT id FROM db.src TIMESTAMP AS OF '2026-01-01'",
          Seq("db.src"),
          Map("db.src" -> 5L)
        )
        .left
        .toOption
        .value should include("only VERSION AS OF")
    }
  }
}
