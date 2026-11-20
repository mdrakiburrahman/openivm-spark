"""Summarize measured diagnostics without treating rejected savings as a win."""
import hashlib
import json
from pathlib import Path
import statistics

root = Path(".research")
report = {
    "outcome": "point-read prototype rejected; no production optimization adopted",
    "baseline_commit": "38f571cc1c4c5d1b14df1cdc6499aa26940f10bd",
    "fixture_commit_spark35": "b67cb1f3c1e5f30720f242b2e11522e743e4d3e8",
    "fixture_commit_spark41": "b67cb1f3c1e5f30720f242b2e11522e743e4d3e8",
    "branch": "mdrakiburrahman/campaign-schema-catalog-probe",
    "scope": {
        "production_files_modified": [],
        "native_files_modified": [],
        "fabric_calls": 0,
        "end_to_end_refresh_sql_measured": False,
        "measured_command": "schema-only command: fresh spark.table(source).schema, "
                            "actual catalog identity reads, and schema fingerprint; "
                            "not a complete REFRESH MATERIALIZED VIEW command",
        "why_no_refresh_claim": "The candidate fails the schema-gate equivalence "
                                "counterexample before dispatcher integration. "
                                "Timing it as a valid complete REFRESH would be misleading.",
        "cold_registry_cost": "Registry teardown is outside timed commands; native "
                              "reopens and per-source validation are inside them.",
        "security_scope": "Fresh Spark source schema resolution remains first in both "
                          "variants. This local fixture does not establish external "
                          "authorization behavior and no authorization path was changed.",
    },
    "counterexample": {
        "setup": "200 valid authoritative MV shards including a matching upstream "
                 "source, then an unrelated shard with CURRENT naming a missing manifest",
        "baseline": "MvCatalog.list throws RocksDBException; the existing allMvsCached "
                    "catch converts the entire catalog read to an empty sequence",
        "point_candidate": "Verified source point lookup succeeds and never observes "
                           "the unrelated shard failure, even with missing/legacy/error fallback",
        "consequence": "Same fresh source schema and persisted upstream identity produce "
                       "different fingerprints: baseline schema_drift, prototype pass.",
        "decision": "Do not replace schema resolution with this point prototype. "
                    "Checking only successful candidate names cannot preserve whole-read "
                    "failure semantics. This is a counterexample to the tested candidate, "
                    "not a proof that every possible catalog optimization is impossible.",
    },
    "alias_and_mutation_findings": [
        "Schema identity matching is exact metadata db.table first, then an unqualified "
        "metadata key. It is not a consumer reverse-dependency lookup.",
        "An unrelated namespace may intentionally fall back to a db-less MV with the same "
        "short name; exact namespace matches override that fallback.",
        "A real registered Delta operational alias shares a physical location and schema "
        "with its source but contributes no MV identity when its lexical key is absent.",
        "Legacy missing definition identities use full enumeration and legacy hashing.",
        "Create/drop/recreate and definition-property changes are re-read; in-flight "
        "source-version advancements still reject dependent schema resolution.",
        "A simultaneous upsert writer and point readers observed current generations; "
        "a completed generation was always visible without a success cache.",
        "Fresh local state restored from a real file-backed state-sync publication "
        "retained the same lookup and full-read identities.",
        "OpenIvmStateBarrier.withMutation takes a read lock and is gated by state sync. "
        "It is not a writer-excluding catalog epoch; sharing snapshots across concurrent "
        "commands would require additional independently proven mutation semantics.",
        "The original allMvsCached value is reused by downstream/cascade decisions. "
        "Moving a schema scan can merely move the same enumeration downstream, so no "
        "whole-refresh improvement is inferred from this diagnostic.",
    ],
    "no_win_policy": "No production switch or cache was introduced. Base-table/no-match "
                     "paths retain a full enumeration and add validation work in the "
                     "prototype; the prototype remains unadopted on every path.",
    "validation": [
        {"command": "npx --no-install nx run spark-ext:test --configuration=spark-3.5 -- "
                    "'ivmCommon/testOnly org.openivm.spark.common.SchemaCatalogReadProbeSpec'",
         "result": "passed 7/7"},
        {"command": "npx --no-install nx run spark-ext:test --configuration=spark-4.1 -- "
                    "'ivmCommon/testOnly org.openivm.spark.common.SchemaCatalogReadProbeSpec'",
         "result": "passed 7/7 after fixing diagnostic-only cross-version deprecations"},
        {"command": "npx --no-install nx run spark-ext:lint --configuration=spark-3.5",
         "result": "passed"},
        {"command": "npx --no-install nx run spark-ext:lint --configuration=spark-4.1",
         "result": "passed"},
        {"command": "npx --no-install nx run spark-ext:verify-all",
         "result": "not run: bounded diagnostic-only revision; no production code changed"},
    ],
    "runs": {},
}

for runtime in ("spark35", "spark41"):
    rows = [json.loads(line) for line in
            (root / f"schema-catalog-evidence-{runtime}.jsonl").read_text().splitlines()]
    samples = [json.loads(line) for line in
               (root / f"schema-catalog-resources-{runtime}.jsonl").read_text().splitlines()]
    resources = [row for row in samples if row["kind"] == "sample"]
    orphan = [entry for row in resources for entry in row["java"] if entry["orphan"]]
    cpu = [entry["cpu_one_core_percent"] for entry in orphan
           if entry["cpu_one_core_percent"] is not None]
    groups = {}
    for cold in (False, True):
        for enabled in (False, True):
            batch = [row for row in rows if row["kind"] == "schema_command_batch"
                     and row["cold_registry"] == cold and row["point_prototype"] == enabled]
            groups[f'{"cold" if cold else "warm"}_{"point" if enabled else "full"}'] = {
                "batches": len(batch),
                "full_enumerations_per_batch": [row["full_enumerations"] for row in batch],
                "shard_get_or_open_per_batch": [row["shard_get_or_open"] for row in batch],
                "native_opens_per_batch": [row["native_opens"] for row in batch],
                "median_batch_elapsed_ms": statistics.median(row["batch_elapsed_ns"] for row in batch) / 1e6,
                "median_command_mean_ms": statistics.median(row["command_sum_ns"] / 4 for row in batch) / 1e6,
                "median_point_validation_mean_ms": statistics.median(
                    row["point_validation_sum_ns"] / 4 for row in batch) / 1e6,
            }
    no_win = {}
    for enabled in (False, True):
        batch = [row for row in rows if row["kind"] == "no_win_schema_command"
                 and row["point_prototype"] == enabled]
        no_win["point" if enabled else "full"] = {
            "median_command_ms": statistics.median(row["command_elapsed_ns"] for row in batch) / 1e6,
            "median_validation_ms": statistics.median(row["point_validation_ns"] for row in batch) / 1e6,
            "full_enumerations_per_command": [row["full_enumerations"] for row in batch],
            "shard_get_or_open_per_command": [row["shard_get_or_open"] for row in batch],
        }
    report["runs"][runtime] = {
        "same_data_and_artifact_within_off_on_pairs": True,
        "order_warm": "ABBA BAAB",
        "order_cold": "ABBA",
        "groups": groups,
        "no_win": no_win,
        "negatives": [row for row in rows if row["kind"] != "schema_command_batch"
                      and row["kind"] != "no_win_schema_command"],
        "resource_context": {
            "interval_sample_count": len(resources),
            "sampling_interval_seconds": 1,
            "logical_cpus": resources[0]["logical_cpus"],
            "machine_cpu_busy_percent_max": max(
                row["machine_cpu_busy_percent"] for row in resources if row["machine_cpu_busy_percent"] is not None),
            "machine_cpu_busy_percent_mean": statistics.mean(
                row["machine_cpu_busy_percent"] for row in resources if row["machine_cpu_busy_percent"] is not None),
            "orphan_present": bool(orphan),
            "orphan_pid": 574877,
            "orphan_rss_mib_max": max(entry["rss_bytes"] for entry in orphan) / 1024**2,
            "orphan_max_one_core_cpu_percent": max(cpu),
            "orphan_mean_one_core_cpu_percent": statistics.mean(cpu),
            "minimum_memory_available_gib": min(row["memory_available_kib"] for row in resources) / 1024**2,
            "observed_non_orphan_java_pids": sorted({entry["pid"] for row in resources
                                                    for entry in row["java"] if not entry["orphan"]}),
            "assessment": "Only expected sbt and its single fork plus the untouched orphan "
                          "were observed. Low orphan CPU and ample memory look locally "
                          "uncontended, but the orphan remains and these are not isolated "
                          "machine or production performance proofs.",
        },
    }

report["artifact_sha256"] = {}
for filename in [
    "package-lock.json",
    "spark-ext/dev/pins.env",
    "spark-ext/dev/targets/spark-3.5.env",
    "spark-ext/dev/targets/spark-4.1.env",
    "spark-ext/ivm-common/src/test/scala/org/openivm/spark/common/SchemaCatalogReadProbeSpec.scala",
    "spark-ext/ivm-common/target/scala-2.12/test-classes/org/openivm/spark/common/SchemaCatalogReadProbeSpec.class",
    "spark-ext/ivm-common/target/scala-2.13/test-classes/org/openivm/spark/common/SchemaCatalogReadProbeSpec.class",
    "/opt/openivm/duckdb",
    "/opt/openivm/openivm.duckdb_extension",
]:
    path = Path(filename)
    if path.exists():
        report["artifact_sha256"][filename] = hashlib.file_digest(path.open("rb"), "sha256").hexdigest()

(root / "schema-catalog-findings.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps({"outcome": report["outcome"], "artifact_sha256": report["artifact_sha256"]}, indent=2))
