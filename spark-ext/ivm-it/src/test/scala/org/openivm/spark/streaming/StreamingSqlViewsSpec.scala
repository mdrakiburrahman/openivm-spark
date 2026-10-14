package org.openivm.spark.streaming

import org.scalatest.funspec.AnyFunSpec

class StreamingSqlViewsSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("catalog views inside STREAM boundaries") {
    it("preserves nested stored namespaces, casts, aliases and UNION ALL branch filters across restart") {
      sys.props.get("openivm.test.packagedJar").foreach { expected =>
        new java.io.File(
          StreamingViewResolver.getClass.getProtectionDomain.getCodeSource.getLocation.toURI
        ).getCanonicalPath shouldBe new java.io.File(expected).getCanonicalPath
      }
      val db     = "strsql_views_namespace"
      val source = s"$db.events"
      val view   = s"$db.nested"
      val target = "strsql_views_nested_target"
      spark.sql(s"CREATE DATABASE $db").collect()
      spark.sql(s"CREATE TABLE $source (id INT, value STRING, part STRING) USING DELTA").collect()
      spark.sql(s"INSERT INTO $source VALUES (1, 'one', 'p'), (2, 'two', 'p'), (3, 'three', 'p')").collect()
      spark.sql(s"USE $db").collect()
      try {
        spark
          .sql(
            """CREATE VIEW branches AS
            |SELECT CAST(id AS BIGINT) AS key, value AS label FROM events WHERE id <= 2
            |UNION ALL
            |SELECT CAST(id AS BIGINT) AS key, upper(value) AS label FROM events WHERE id >= 2""".stripMargin
          )
          .collect()
        spark.sql("CREATE VIEW nested AS SELECT key AS event_key, label FROM branches WHERE key > 0").collect()
      } finally spark.sql("USE default").collect()
      val declaration =
        s"""CREATE STREAMING TABLE $target AS
           |SELECT v.event_key, v.label FROM STREAM($view)
           |WITH ('maxFilesPerTrigger' = '1') AS v""".stripMargin
      val first = createStreamingSql(declaration)
      process(first)
      assertBagEqual(target, s"SELECT event_key, label FROM $view")
      spark.sql(s"ALTER STREAMING TABLE $target STOP").collect()
      val resumed = createStreamingSql(declaration)
      resumed.queryId shouldBe first.queryId
      resumed.checkpointLocation shouldBe first.checkpointLocation
      resumed.definitionHash shouldBe first.definitionHash
      spark.sql(s"INSERT INTO $source VALUES (4, 'four', 'p')").collect()
      process(resumed)
      assertBagEqual(target, s"SELECT event_key, label FROM $view")
    }

    it("propagates semantic reader options to every Delta branch") {
      val source = "strsql_views_options_source"
      val view   = "strsql_views_options_view"
      val target = "strsql_views_options_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'original', 'p')")
      spark.sql(s"UPDATE $source SET value = 'changed' WHERE id = 1").collect()
      insertRows(source, "(2, 'later', 'p')")
      spark.sql(s"CREATE VIEW $view AS SELECT * FROM $source UNION ALL SELECT * FROM $source").collect()
      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE $target AS SELECT * FROM STREAM($view)
           |WITH ('startingVersion' = '0', 'skipChangeCommits' = 'true')""".stripMargin
      )
      process(status)
      assertBagEqual(
        target,
        """SELECT 1 AS id, 'original' AS value, 'p' AS part UNION ALL
          |SELECT 2, 'later', 'p' UNION ALL SELECT 1, 'original', 'p' UNION ALL SELECT 2, 'later', 'p'""".stripMargin
      )
    }

    it("does not zip the next STREAM marker's options onto a repeated view leaf") {
      val left   = "strsql_views_mixed_left"
      val right  = "strsql_views_mixed_right"
      val view   = "strsql_views_mixed_view"
      val target = "strsql_views_mixed_target"
      createDeltaSource(left, "id INT, value STRING, part STRING")
      createDeltaSource(right, "id INT, value STRING, part STRING")
      insertRows(left, "(1, 'left', 'p')")
      insertRows(right, "(2, 'right-before', 'p')")
      insertRows(right, "(3, 'right-after', 'p')")
      spark.sql(s"CREATE VIEW $view AS SELECT * FROM $left UNION ALL SELECT * FROM $left").collect()
      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE $target AS
           |SELECT * FROM STREAM($view) WITH ('startingVersion' = '0')
           |UNION ALL
           |SELECT * FROM STREAM($right) WITH ('startingVersion' = '${sourceVersion(right)}')""".stripMargin
      )
      process(status)
      assertBagEqual(
        target,
        """SELECT 1 AS id, 'left' AS value, 'p' AS part UNION ALL
          |SELECT 1, 'left', 'p' UNION ALL SELECT 3, 'right-after', 'p'""".stripMargin
      )
    }

    it("streams explicit Delta path leaves while retaining a batch reference outside the boundary") {
      val source = "strsql_views_path_source"
      val view   = "strsql_views_path_view"
      val target = "strsql_views_path_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")
      val path = spark.sql(s"DESCRIBE DETAIL $source").select("location").collect().head.getString(0)
      spark.sql(s"CREATE VIEW $view AS SELECT * FROM delta.`$path`").collect()
      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE $target AS SELECT live.id, reference.value, live.part
           |FROM STREAM($view) live JOIN $view reference ON live.id = reference.id""".stripMargin
      )
      process(status)
      assertBagEqual(target, s"SELECT * FROM $source")
    }

    it("leaves relations outside STREAM static and supports complete-mode aggregate views") {
      val source = "strsql_views_aggregate_source"
      val view   = "strsql_views_aggregate_view"
      val target = "strsql_views_aggregate_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p'), (2, 'two', 'p')")
      spark.sql(s"CREATE VIEW $view AS SELECT part, COUNT(*) AS n FROM $source GROUP BY part").collect()
      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE $target OPTIONS ('outputMode' = 'complete')
           |AS SELECT * FROM STREAM($view)""".stripMargin
      )
      val query = process(status)
      assertBagEqual(target, s"SELECT * FROM $view")
      insertRows(source, "(3, 'three', 'p')")
      process(query)
      assertBagEqual(target, s"SELECT * FROM $view")
    }

    it("rejects changed view semantics rather than resetting its managed checkpoint") {
      val source = "strsql_views_changed_source"
      val view   = "strsql_views_changed_view"
      val target = "strsql_views_changed_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")
      spark.sql(s"CREATE VIEW $view AS SELECT * FROM $source WHERE id > 0").collect()
      val declaration = s"CREATE STREAMING TABLE $target AS SELECT * FROM STREAM($view)"
      val status      = createStreamingSql(declaration)
      process(status)
      spark.sql(s"ALTER STREAMING TABLE $target STOP").collect()
      spark.sql(s"ALTER VIEW $view AS SELECT * FROM $source WHERE id > 1").collect()
      val error = intercept[Exception](createStreamingSql(declaration))
      error.getMessage.toLowerCase should include("definition")
      assertBagEqual(target, "SELECT 1 AS id, 'one' AS value, 'p' AS part")
    }

    it("rejects reordered leaf sets and recreated Delta tables without replaying a checkpoint") {
      val left   = "strsql_views_identity_left"
      val right  = "strsql_views_identity_right"
      val view   = "strsql_views_identity_view"
      val target = "strsql_views_identity_target"
      createDeltaSource(left, "id INT, value STRING, part STRING")
      createDeltaSource(right, "id INT, value STRING, part STRING")
      insertRows(left, "(1, 'left', 'p')")
      insertRows(right, "(2, 'right', 'p')")
      spark.sql(s"CREATE VIEW $view AS SELECT * FROM $left UNION ALL SELECT * FROM $right").collect()
      val declaration = s"CREATE STREAMING TABLE $target AS SELECT * FROM STREAM($view)"
      val status      = createStreamingSql(declaration)
      process(status)
      spark.sql(s"ALTER STREAMING TABLE $target STOP").collect()
      spark.sql(s"ALTER VIEW $view AS SELECT * FROM $right UNION ALL SELECT * FROM $left").collect()
      intercept[Exception](createStreamingSql(declaration)).getMessage.toLowerCase should include("definition")
      spark.sql(s"ALTER VIEW $view AS SELECT * FROM $left UNION ALL SELECT * FROM $right").collect()
      val replacementPath = scratchPath("replacement-delta")
      spark.sql(s"DROP TABLE $left").collect()
      spark
        .sql(s"CREATE TABLE $left (id INT, value STRING, part STRING) USING DELTA LOCATION '$replacementPath'")
        .collect()
      insertRows(left, "(3, 'replacement', 'p')")
      intercept[Exception](createStreamingSql(declaration)).getMessage.toLowerCase should include("definition")
      assertBagEqual(target, "SELECT 1 AS id, 'left' AS value, 'p' AS part UNION ALL SELECT 2, 'right', 'p'")
    }

    it("rejects frozen time-travel views and delegates unsupported streaming operators to Spark") {
      val source = "strsql_views_unsupported_source"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")
      spark.sql(s"CREATE VIEW strsql_views_time_travel AS SELECT * FROM $source VERSION AS OF 0").collect()
      val frozen = intercept[Exception] {
        createStreamingSql(
          "CREATE STREAMING TABLE strsql_views_time_target AS SELECT * FROM STREAM(strsql_views_time_travel)"
        )
      }
      frozen.getMessage should include("time-travel")
      spark.sql(s"CREATE VIEW strsql_views_sort AS SELECT * FROM $source ORDER BY id").collect()
      val sorted = intercept[Exception] {
        createStreamingSql(
          "CREATE STREAMING TABLE strsql_views_sort_target AS SELECT * FROM STREAM(strsql_views_sort)"
        )
      }
      sorted.getMessage.toLowerCase should include("sort")
    }

    it("explicitly rejects non-Delta and unsupported leaves and schema-changing reader options") {
      val nonDelta = "strsql_views_parquet_source"
      val badView  = "strsql_views_parquet_view"
      spark.sql(s"CREATE TABLE $nonDelta (id INT) USING PARQUET").collect()
      spark.sql(s"CREATE VIEW $badView AS SELECT * FROM $nonDelta").collect()
      val nonDeltaError = intercept[Exception] {
        createStreamingSql(s"CREATE STREAMING TABLE strsql_views_bad_target AS SELECT * FROM STREAM($badView)")
      }
      nonDeltaError.getMessage should include("requires physical Delta leaves")
      spark.sql("CREATE VIEW strsql_views_values AS SELECT 1 AS id").collect()
      val valuesError = intercept[Exception] {
        createStreamingSql(
          "CREATE STREAMING TABLE strsql_views_values_target AS SELECT * FROM STREAM(strsql_views_values)"
        )
      }
      valuesError.getMessage should include("unsupported/non-Delta leaf")
      val source = "strsql_views_cdf_source"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      spark.sql(s"ALTER TABLE $source SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')").collect()
      spark.sql(s"CREATE VIEW strsql_views_cdf_view AS SELECT * FROM $source").collect()
      val schemaError = intercept[Exception] {
        createStreamingSql(
          """CREATE STREAMING TABLE strsql_views_cdf_target AS SELECT * FROM STREAM(strsql_views_cdf_view)
            |WITH ('readChangeFeed' = 'true')""".stripMargin
        )
      }
      schemaError.getMessage should include("stored view schema")
    }
  }
}
