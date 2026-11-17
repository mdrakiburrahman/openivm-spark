package org.openivm.spark.common.rocksdb

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{FeatureGate, MvCatalog}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

import java.io.File

class StateSyncRecoverySpec extends IvmParitySpecBase("state-sync-recovery") with InterceptMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.StateSyncUriKey -> new File(warehouseDir, "state-sync-remote").toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey -> "false"
  )

  it("restores a chained view's metadata and continues incremental refresh") {
    sql("CREATE TABLE state_sync_recovery_src(id INT, amount INT) USING DELTA")
    sql("INSERT INTO state_sync_recovery_src VALUES (1, 10)")
    sql(
      "CREATE MATERIALIZED VIEW state_sync_recovery_up AS " +
        "SELECT id, SUM(amount) AS total FROM state_sync_recovery_src GROUP BY id"
    )
    sql(
      "CREATE MATERIALIZED VIEW state_sync_recovery_down AS " +
        "SELECT id, total FROM state_sync_recovery_up"
    )
    sql("INSERT INTO state_sync_recovery_src VALUES (1, 20)")
    refreshMv("state_sync_recovery_up")
    refreshMv("state_sync_recovery_down")

    OpenIvmRocksDBRegistry.closeAll()
    deleteDir(new File(warehouseDir, "_openivm"))
    OpenIvmStateSync.resetForTesting()
    MvCatalog.lookup(spark, TableIdentifier("state_sync_recovery_down")) should not be empty
    assertMvCorrect("state_sync_recovery_down", "SELECT id, SUM(amount) AS total FROM state_sync_recovery_src GROUP BY id")

    sql("INSERT INTO state_sync_recovery_src VALUES (1, 30), (2, 5)")
    refreshMv("state_sync_recovery_up")
    refreshMv("state_sync_recovery_down")
    assertMvCorrect("state_sync_recovery_down", "SELECT id, SUM(amount) AS total FROM state_sync_recovery_src GROUP BY id")
  }
}
