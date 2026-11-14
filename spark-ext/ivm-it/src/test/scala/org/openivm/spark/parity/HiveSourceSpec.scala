package org.openivm.spark.parity

import java.util.UUID

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{MvCatalog, MvMetadata, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class HiveSourceScenarios extends IvmParitySpecBase("hive-source") {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.sql.catalogImplementation"           -> "hive",
    "spark.sql.legacy.createHiveTableByDefault" -> "true",
    "javax.jdo.option.ConnectionURL" -> s"jdbc:derby:memory:hivesource${UUID.randomUUID().toString.replace("-", "")};create=true"
  )

  override def beforeAll(): Unit = {
    Configurator.setLevel("org.apache.hadoop.hive", Level.ERROR)
    super.beforeAll()
    sql("CREATE DATABASE hivesource")
    spark.catalog.setCurrentDatabase("hivesource")
  }

  private def checkClassification(name: String): Unit = {
    val meta = MvCatalog.lookup(spark, TableIdentifier(name, Some("hivesource"))).get
    meta.properties(MvMetadata.CompileRefreshTypeKey) should not be "COMPILE_FAILED"
    meta.refreshType shouldBe RefreshTypeCode.FullRefresh
    meta.properties(MvMetadata.RefreshReasonKey) shouldBe "unversioned_hive_source"
  }

  private def externalWrite(statement: String): Unit = {
    val writer = spark.newSession()
    writer.conf.set("spark.openivm.enabled", "false")
    writer.sql(statement).collect()
  }

  describe("Hive source compilation and refresh") {
    it("keeps a Delta-only view incremental") {
      sql("CREATE TABLE hsrc_control (id BIGINT, label STRING) USING DELTA")
      sql("INSERT INTO hsrc_control VALUES (1, 'first')")
      val query = "SELECT id, label FROM hsrc_control"
      sql(s"CREATE MATERIALIZED VIEW hivesource.hsrc_control_mv AS $query")
      val meta = MvCatalog.lookup(spark, TableIdentifier("hsrc_control_mv", Some("hivesource"))).get
      meta.properties(MvMetadata.CompileRefreshTypeKey) should not be "COMPILE_FAILED"
      meta.refreshType should not be RefreshTypeCode.FullRefresh
      sql("INSERT INTO hsrc_control VALUES (2, 'second')")
      refreshMv("hivesource.hsrc_control_mv")
      assertMvCorrect("hivesource.hsrc_control_mv", query)
    }

    it("recomputes converted Hive Parquet relations after Hive writes") {
      sql("CREATE TABLE hsrc_converted (id BIGINT, label STRING) STORED AS PARQUET")
      sql("INSERT INTO hsrc_converted VALUES (1, 'first')")
      val query = "SELECT id, label FROM hsrc_converted"
      sql(s"CREATE MATERIALIZED VIEW hivesource.hsrc_converted_mv AS $query")
      checkClassification("hsrc_converted_mv")
      externalWrite("INSERT INTO hivesource.hsrc_converted VALUES (2, 'second')")
      refreshMv("hivesource.hsrc_converted_mv")
      assertMvCorrect("hivesource.hsrc_converted_mv", query)
    }

    it("compiles a qualified Hive seed and observes external changes on every refresh") {
      sql("CREATE TABLE hivesource.hsrc_seed (id BIGINT, label STRING)")
      externalWrite("INSERT INTO hivesource.hsrc_seed VALUES (1, 'first')")
      sql("CREATE MATERIALIZED VIEW hivesource.hsrc_mv AS SELECT id, label FROM hivesource.hsrc_seed")
      checkClassification("hsrc_mv")
      assertMvCorrect("hivesource.hsrc_mv", "SELECT id, label FROM hivesource.hsrc_seed")

      externalWrite("INSERT INTO hivesource.hsrc_seed VALUES (2, 'second')")
      refreshMv("hivesource.hsrc_mv")
      assertMvCorrect("hivesource.hsrc_mv", "SELECT id, label FROM hivesource.hsrc_seed")
      externalWrite("INSERT OVERWRITE TABLE hivesource.hsrc_seed SELECT 3L, 'replacement'")
      refreshMv("hivesource.hsrc_mv")
      assertMvCorrect("hivesource.hsrc_mv", "SELECT id, label FROM hivesource.hsrc_seed")
    }

    it("tracks an unqualified partitioned Hive source joined to Delta and propagates recomputes") {
      sql("CREATE TABLE hsrc_partitioned (id BIGINT, label STRING) PARTITIONED BY (part STRING)")
      externalWrite("INSERT INTO hivesource.hsrc_partitioned PARTITION (part='a') SELECT 1L, 'first'")
      sql("CREATE TABLE hsrc_delta (id BIGINT, amount INT) USING DELTA")
      sql("INSERT INTO hsrc_delta VALUES (1, 10), (2, 20)")
      val query = "SELECT h.id, h.label, h.part, d.amount FROM hsrc_partitioned h JOIN hsrc_delta d ON h.id = d.id"
      sql(s"CREATE MATERIALIZED VIEW hivesource.hsrc_join_mv AS $query")
      checkClassification("hsrc_join_mv")
      sql("CREATE MATERIALIZED VIEW hivesource.hsrc_downstream AS SELECT id, label, part, amount FROM hsrc_join_mv")
      assertMvCorrect("hsrc_join_mv", query)

      externalWrite("INSERT INTO hivesource.hsrc_partitioned PARTITION (part='b') SELECT 2L, 'second'")
      refreshMv("hivesource.hsrc_join_mv")
      refreshMv("hivesource.hsrc_downstream")
      assertMvCorrect("hsrc_join_mv", query)
      assertMvCorrect("hsrc_downstream", query)
    }
  }
}

class HiveSourceSpec    extends HiveSourceScenarios with InterceptMode
class HiveSourceCdfSpec extends HiveSourceScenarios with CdfMode
