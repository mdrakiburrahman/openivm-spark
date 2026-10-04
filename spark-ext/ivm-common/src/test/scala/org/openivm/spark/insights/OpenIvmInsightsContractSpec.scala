package org.openivm.spark.insights

import org.openivm.spark.insights.OpenIvmInsightsContract.{BranchCode, ExecutionMode, Materialization, Operation}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OpenIvmInsightsContractSpec extends AnyFunSpec with Matchers {

  describe("OpenIVM insight contract v1") {
    it("keeps schema v1 while exposing the complete operation and branch vocabularies") {
      OpenIvmInsightsContract.SchemaVersion shouldBe 1
      Operation.Values shouldBe Set(
        Operation.Request,
        Operation.Preflight,
        Operation.Create,
        Operation.Refresh,
        Operation.Drop,
        Operation.Streaming,
        Operation.RegularSpark
      )
      BranchCode.Values shouldBe Set(
        BranchCode.S1,
        BranchCode.S2,
        BranchCode.S3,
        BranchCode.S4,
        BranchCode.S5,
        BranchCode.S6,
        BranchCode.S7,
        BranchCode.S8,
        BranchCode.M1,
        BranchCode.M2,
        BranchCode.M3,
        BranchCode.M4,
        BranchCode.M5,
        BranchCode.M6,
        BranchCode.M7,
        BranchCode.M8,
        BranchCode.R1,
        BranchCode.R2,
        BranchCode.R3
      )
      BranchCode.Labels shouldBe Map(
        BranchCode.S1 -> "NEW_STREAM",
        BranchCode.S2 -> "SAME_DEFINITION_ACTIVE",
        BranchCode.S3 -> "SAME_DEFINITION_RESUME",
        BranchCode.S4 -> "OPERATIONAL_TUNING_RESTART",
        BranchCode.S5 -> "SEMANTIC_CHANGE_REBUILD_CASCADE",
        BranchCode.S6 -> "SEMANTIC_CHANGE_REJECTED",
        BranchCode.S7 -> "REBUILD_RECOVERY",
        BranchCode.S8 -> "EXPLICIT_DROP_CASCADE",
        BranchCode.M1 -> "NEW_MV_INCREMENTAL",
        BranchCode.M2 -> "INCREMENTAL_REFRESH",
        BranchCode.M3 -> "NO_PENDING_DELTAS",
        BranchCode.M4 -> "NATIVE_FULL_REFRESH",
        BranchCode.M5 -> "DEMOTED_TO_FULL_REFRESH",
        BranchCode.M6 -> "SIGNED_DELTA_RECOMPUTE",
        BranchCode.M7 -> "QUERY_CHANGE_REBUILD_CASCADE",
        BranchCode.M8 -> "QUERY_CHANGE_REJECTED",
        BranchCode.R1 -> "REGULAR_SPARK",
        BranchCode.R2 -> "REGULAR_SPARK_WITH_OPENIVM_CHANGE_CAPTURE",
        BranchCode.R3 -> "REGULAR_SPARK_FAILED"
      )
      OpenIvmInsightsContract.MetricField.Values shouldBe Set(
        "wall_duration_ms",
        "job_count",
        "stage_count",
        "task_count",
        "input_records",
        "input_bytes",
        "output_records",
        "output_bytes",
        "shuffle_read_records",
        "shuffle_read_bytes",
        "shuffle_write_records",
        "shuffle_write_bytes",
        "memory_spill_bytes",
        "disk_spill_bytes",
        "files_read",
        "files_written"
      )
    }

    it("derives stable execution modes without rewriting declared materialization") {
      OpenIvmInsightsContract.ModelContext
        .fromMaterialization(Materialization.StreamingTable, "analytics.stream_target") shouldBe
        OpenIvmInsightsContract.ModelContext(
          ExecutionMode.Streaming,
          Materialization.StreamingTable,
          "analytics.stream_target"
        )
      OpenIvmInsightsContract.ModelContext
        .fromMaterialization(Materialization.MaterializedView, "analytics.mv_target") shouldBe
        OpenIvmInsightsContract.ModelContext(
          ExecutionMode.MaterializedView,
          Materialization.MaterializedView,
          "analytics.mv_target"
        )
      OpenIvmInsightsContract.ModelContext
        .fromMaterialization("incremental", "analytics.table_target") shouldBe
        OpenIvmInsightsContract.ModelContext(
          ExecutionMode.RegularSpark,
          "incremental",
          "analytics.table_target"
        )
    }
  }
}
