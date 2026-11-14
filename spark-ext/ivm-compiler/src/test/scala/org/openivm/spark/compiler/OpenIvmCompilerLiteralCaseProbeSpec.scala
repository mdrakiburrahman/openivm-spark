package org.openivm.spark.compiler

import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OpenIvmCompilerLiteralCaseProbeSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val extensionPath =
    sys.env.getOrElse("OPENIVM_EXTENSION_PATH", "/opt/openivm/openivm.duckdb_extension")
  private var compiler: OpenIvmCompiler = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    compiler = OpenIvmCompiler.build(extensionPath)
  }

  override def afterAll(): Unit = {
    if (compiler != null) compiler.close()
    super.afterAll()
  }

  "compile" should "preserve mixed-case literals in the initial SQL for a joined ROW_NUMBER window view" in {
    val x11Schema = StructType(
      Seq(
        StructField("x2", StringType),
        StructField("x3", StringType),
        StructField("x4", StringType),
        StructField("x5", StringType),
        StructField("x6", IntegerType),
        StructField("x7", BooleanType),
        StructField("x8", TimestampType),
        StructField("x9", StringType)
      )
    )
    val x27Schema = StructType(
      Seq(
        StructField("x3", StringType),
        StructField("x4", StringType),
        StructField("x28", StringType),
        StructField("x17", StringType),
        StructField("x19", StringType),
        StructField("x13", StringType),
        StructField("x15", StringType),
        StructField("x21", IntegerType),
        StructField("x23", DecimalType(18, 2)),
        StructField("x25", TimestampType),
        StructField("x9", StringType)
      )
    )
    val viewSql =
      """WITH x1 AS (
        | SELECT x2,x3,x4,x5,x6,x7,x8,x9 FROM `x10`.x11
        |), x12 AS (
        | SELECT x3,x4,x13 AS x14,x15 AS x16,x17 AS x18,x19 AS x20,
        | CAST(x21 AS INT) AS x22,CAST(x23 / 1024 AS INT) AS x24,
        | ROW_NUMBER() OVER (PARTITION BY x3,x4 ORDER BY x25 DESC) AS x26
        | FROM `x10`.x27 WHERE x28 = 'NmezoWggEewangew' AND x28 LIKE 'Nmezo%'
        |), x29 AS (
        | SELECT x3,x4,CONCAT_WS('|','NmezoWggEewangew',COALESCE(x14,'JD'),
        | COALESCE(x16,'JD'),COALESCE(x18,'gEwlf'),COALESCE(x20,'JD'),
        | CAST(COALESCE(x22,0) AS STRING),CAST(COALESCE(x24,0) AS STRING),'JD') AS x30
        | FROM x12 WHERE x26=1
        |), x31 AS (
        | SELECT x32.x4,x32.x3,x32.x2,x32.x5,x32.x6,x32.x7,x32.x9,
        | ROW_NUMBER() OVER (PARTITION BY x32.x3,x32.x4 ORDER BY x32.x8 DESC,x32.x2 DESC) AS x26
        | FROM x1 x32
        |), x33 AS (
        | SELECT x34.*,CASE WHEN x34.x7 THEN 1 WHEN NOT x34.x7 THEN 0 ELSE CAST(NULL AS INT) END AS x35
        | FROM x31 x34 WHERE x34.x26=1
        |), x36 AS (
        | SELECT x37.*,CASE WHEN x37.x35>0 THEN 'Not Ready'
        | WHEN 0>0 THEN 'Needs Review' WHEN x37.x35=0 AND 0=0 THEN 'Ready' ELSE 'Unknown' END AS x38
        | FROM x33 x37
        |)
        |SELECT LOWER(CONCAT_WS('|',TRIM(x34.x3),TRIM(x34.x4))) AS x39,
        | LOWER(CONCAT_WS('|',TRIM(x34.x3),TRIM(x34.x4))) AS x40,
        | LOWER(TRIM(x34.x3)) AS x41,CAST(NULL AS STRING) AS x42,
        | CAST(NULL AS STRING) AS x43,CAST(NULL AS STRING) AS x44,x34.x9 AS x45,
        | COALESCE(x46.x30,'-1') AS x47,
        | CAST(CASE WHEN x34.x38='Ready' THEN 1 ELSE 0 END AS INT) AS x48,
        | CAST(0 AS INT) AS x49,
        | CAST(CASE WHEN x34.x38='Not Ready' THEN 1 ELSE 0 END AS INT) AS x50,
        | x34.x38 AS x51,
        | CASE x34.x38 WHEN 'FmvTlnodjzf' THEN 'High' WHEN 'KmfCjnus' THEN 'High'
        | WHEN 'Not Ready' THEN 'High' WHEN 'XmhpcmoavkqzuCzsezfny' THEN 'Medium'
        | WHEN 'Needs Review' THEN 'Medium' WHEN 'Tqsdowgl' THEN 'Low'
        | WHEN 'Ready' THEN 'Low' ELSE 'Unknown' END AS x52,
        | CAST(NULL AS DECIMAL(18,2)) AS x53,CAST(NULL AS DECIMAL(18,2)) AS x54,
        | CAST(NULL AS DECIMAL(18,2)) AS x55
        |FROM x36 x34 LEFT JOIN x29 x46 ON x34.x3=x46.x3 AND x34.x4=x46.x4""".stripMargin
    val request = CompileRequest(
      viewName = "literal_case_probe",
      viewSql = viewSql,
      sources = Map("x11" -> x11Schema, "x27" -> x27Schema),
      sourceQualifiedNames = Map("x11" -> "x10.x11", "x27" -> "x10.x27")
    )

    val compiled = compiler.compile(request)

    compiled.refreshTypeName shouldBe "WINDOW_PARTITION"
    Seq("'Not Ready'", "'Needs Review'", "'Ready'", "'High'", "'Medium'", "'Low'", "'Nmezo%'")
      .foreach(compiled.initialLoadSql should include(_))
    Seq("'not ready'", "'needs review'", "'ready'", "'high'", "'medium'", "'low'")
      .foreach(compiled.initialLoadSql should not include _)
  }
}
