package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class MixedCaseLiteralSpec extends IvmParitySpecBase("mixed-case-literals") with InterceptMode {

  override protected def extraSparkConf: Map[String, String] =
    Map("spark.openivm.compile.classificationCache.enabled" -> "true")

  it("preserves mixed-case literals in a two-source composite ROW_NUMBER MV on create and refresh") {
    sql("CREATE TABLE mcl_flag_base (row_id INT, org_id STRING, contract_id STRING, changed_at TIMESTAMP, is_blocked BOOLEAN, needs_review BOOLEAN, is_clear BOOLEAN, reason STRING) USING DELTA")
    sql("CREATE TABLE mcl_sku_base (row_id INT, org_id STRING, contract_id STRING, changed_at TIMESTAMP, family STRING, model STRING) USING DELTA")
    sql(
      "INSERT INTO mcl_flag_base VALUES " +
        "(1, 'org-a', 'contract-1', TIMESTAMP'2024-01-01 00:00:00', false, false, true, 'MiXeD'), " +
        "(2, 'org-a', 'contract-1', TIMESTAMP'2024-01-02 00:00:00', true, false, false, 'Needs Review'), " +
        "(3, 'org-b', 'contract-2', TIMESTAMP'2024-01-01 00:00:00', false, false, true, 'MiXeD'), " +
        "(4, 'org-c', 'contract-3', TIMESTAMP'2024-01-01 00:00:00', false, false, true, 'mixed')"
    )
    sql(
      "INSERT INTO mcl_sku_base VALUES " +
        "(1, 'org-a', 'contract-1', TIMESTAMP'2024-01-01 00:00:00', 'Contoso', 'Widget'), " +
        "(2, 'org-a', 'contract-1', TIMESTAMP'2024-01-02 00:00:00', 'Fabrikam', 'Gadget'), " +
        "(3, 'org-b', 'contract-2', TIMESTAMP'2024-01-01 00:00:00', 'Contoso', 'Widget'), " +
        "(4, 'org-c', 'contract-3', TIMESTAMP'2024-01-01 00:00:00', 'Fabrikam', 'Gadget')"
    )

    sql("CREATE MATERIALIZED VIEW mcl_flag_source AS SELECT * FROM mcl_flag_base")
    sql("CREATE MATERIALIZED VIEW mcl_sku_source AS SELECT * FROM mcl_sku_base")

    val viewSql =
      "WITH ranked_flags AS (" +
        "SELECT row_id, org_id, contract_id, changed_at, reason, " +
        "CASE WHEN is_blocked THEN 2 WHEN needs_review THEN 1 WHEN is_clear THEN 0 END AS blocker_count, " +
        "ROW_NUMBER() OVER (PARTITION BY org_id, contract_id ORDER BY changed_at DESC, row_id DESC) AS rn " +
        "FROM mcl_flag_source), " +
        "ranked_skus AS (" +
        "SELECT org_id, contract_id, changed_at, row_id, family, model, " +
        "CONCAT_WS('-', family, model) AS sku, " +
        "ROW_NUMBER() OVER (PARTITION BY org_id, contract_id ORDER BY changed_at DESC, row_id DESC) AS rn " +
        "FROM mcl_sku_source), " +
        "classified_flags AS (" +
        "SELECT *, CASE WHEN blocker_count > 0 THEN 'Not Ready' WHEN 0 > 0 THEN 'Needs Review' " +
        "WHEN blocker_count = 0 AND 0 = 0 THEN 'Ready' ELSE 'Unknown' END AS readiness, " +
        "CASE WHEN reason = 'MiXeD' THEN 'ExactMatch' WHEN reason LIKE 'Needs%' THEN 'PatternMatch' ELSE 'NoMatch' END AS reason_match " +
        "FROM ranked_flags) " +
        "SELECT LOWER(CONCAT_WS('-', f.org_id, f.contract_id)) AS contract_key, " +
        "LOWER(CONCAT_WS('-', f.org_id, f.contract_id)) AS contract_key_copy, " +
        "f.org_id, f.contract_id, f.blocker_count, f.readiness, " +
        "CASE WHEN f.readiness = 'Not Ready' THEN 'High' WHEN f.readiness = 'Needs Review' THEN 'Medium' " +
        "WHEN f.readiness = 'Ready' THEN 'Low' ELSE 'Unknown' END AS effort, " +
        "f.reason_match, CASE WHEN f.readiness = 'Ready' THEN 'Yes' ELSE 'No' END AS is_ready, " +
        "CAST(NULL AS STRING) AS optional_text, CAST(NULL AS INT) AS optional_number, " +
        "s.sku, s.family, s.model, CAST(NULL AS STRING) AS null_marker, " +
        "CONCAT_WS('-', s.family, s.model) AS sku_key " +
        "FROM classified_flags f LEFT JOIN ranked_skus s " +
        "ON f.org_id = s.org_id AND f.contract_id = s.contract_id AND s.rn = 1 " +
        "WHERE f.rn = 1"

    sql(s"CREATE MATERIALIZED VIEW mcl_parent AS $viewSql")
    val parentId = spark.sessionState.sqlParser.parseTableIdentifier("mcl_parent")
    MvCatalog.lookup(spark, parentId).get.refreshType shouldBe RefreshTypeCode.WindowPartition
    assertMvCorrect("mcl_parent", viewSql)

    sql("UPDATE mcl_flag_base SET is_blocked = false, is_clear = true, reason = 'MiXeD' WHERE row_id = 2")
    sql("UPDATE mcl_sku_base SET model = 'Gadget-X' WHERE row_id = 2")
    refreshMv("mcl_flag_source")
    refreshMv("mcl_sku_source")
    refreshMv("mcl_parent")
    assertMvCorrect("mcl_parent", viewSql)
  }
}
