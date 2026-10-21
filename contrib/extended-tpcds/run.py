"""Isolated SF1 experiment; not the ivm-bench Spark engine integration."""

import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import Counter

import duckdb

repo = Path(__file__).resolve().parents[2]
runner = Path(__file__).resolve().parent
bench = repo / "benchmark-source"
benchmark_commit = subprocess.check_output(["git", "-C", str(bench), "rev-parse", "HEAD"], text=True).strip()
sys.path.insert(0, str(bench / "src/workloads/extended-tpcds"))
import generate
import mutations
import pipeline
import prepare_experiment

out = repo / "sf1-results"
out.mkdir(exist_ok=False)
start = time.monotonic()
phase_name = "startup"
finished = threading.Event()
peak_rss = 0
monthly_probe = os.environ.get("EXTENDED_TPCDS_MONTHLY_PROBE") == "1"
monthly = "fact_engagement_status_monthly_snapshot"
selected = pipeline.selected_models([monthly]) if monthly_probe else pipeline.selected_models()
duckdb_memory = "6GB" if monthly_probe else "8GB"
spill_limit = "24GB" if monthly_probe else "64GB"


def save(name, value):
    path = out / name
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2) + "\n")
    temp.replace(path)


def phase(name):
    global phase_name
    phase_name = name
    save("phase.json", {"phase": name, "elapsed_s": time.monotonic() - start})
    print("PHASE", name, flush=True)


def monitor():
    global peak_rss
    while not finished.wait(2):
        try:
            rows = subprocess.check_output(["ps", "-eo", "pid=,ppid=,rss="], text=True)
            processes = [tuple(map(int, row.split())) for row in rows.splitlines()]
            owned = {os.getpid()}
            while True:
                added = {pid for pid, ppid, rss in processes if ppid in owned} - owned
                if not added:
                    break
                owned.update(added)
            rss = sum(rss for pid, ppid, rss in processes if pid in owned)
            peak_rss = max(peak_rss, rss)
            save("resources.json", {"phase": phase_name, "peak_process_tree_rss_mib": peak_rss / 1024,
                                   "current_process_tree_rss_mib": rss / 1024,
                                   "elapsed_s": time.monotonic() - start,
                                   "free_disk_gib": shutil.disk_usage(out).free / 1024**3})
        except Exception as error:
            print("RESOURCE MONITOR", repr(error), flush=True)


def timed(connection, sql):
    timer = threading.Timer(1800, connection.interrupt)
    timer.start()
    try:
        return connection.execute(sql)
    finally:
        timer.cancel()


def prepare_inputs():
    retained = repo.parent / "extended-tpcds-retained"
    metadata = retained / "provenance.json"
    if metadata.is_file():
        previous = json.loads(metadata.read_text())
        cached_plan = retained / "prepared/plan"
        cached_manifest = json.loads((cached_plan / "manifest.json").read_text())
        cached_refs = retained / "reference"
        if (previous.get("benchmark_commit") == benchmark_commit
                and cached_manifest.get("plan_id") == "24fdc6d2172de3a9"
                and all((cached_refs / ("batch" + str(batch)) / (name + ".parquet")).is_file()
                        for batch in ([1] if monthly_probe else [1, 2, 3]) for name in selected)):
            phase("verify_retained_inputs")
            images = list(cached_manifest["initial"].values())
            for batch in cached_manifest["batches"]:
                for operation in batch["operations"]:
                    images.extend(operation[key] for key in ("before", "after") if operation[key] is not None)
            for image in images:
                if mutations.file_checksum(cached_plan / image["path"]) != image["sha256"]:
                    raise RuntimeError("Retained source checksum mismatch: " + image["path"])
            save("manifest.json", cached_manifest)
            save("retained-inputs.json", {"directory": str(retained), "plan_id": cached_manifest["plan_id"]})
            subprocess.run([sys.executable, str(runner / "translate.py"), str(bench), str(cached_plan), str(out)], check=True)
            return cached_plan, cached_refs
    phase("source_preparation")
    options = {"profile": "native", "mode": "mixed", "seed": 17, "report_date": "2002-12-31",
               "history_months": 12, "days": 3, "mutation_cohort": "all"}
    step = {"insert_pct": "1", "update_pct": "1", "delete_pct": "1"}
    os.environ.update(DUCKDB_MEM_LIMIT=duckdb_memory, DUCKDB_THREADS="4", DUCKDB_MAX_TEMP_DIRECTORY_SIZE=spill_limit)
    # Interrupt long preparation statements without changing the canonical generator.
    original_connect = duckdb.connect

    class BoundedConnection:
        def __init__(self, *args, **kwargs):
            # The canonical planner opens an anonymous database and copies all
            # sources plus mutation pools into it. Persist its working tables so
            # the registry export can evict them while sorting at SF1.
            if not args and "database" not in kwargs:
                kwargs["database"] = str(out / "preparation-state.duckdb")
            self.connection = original_connect(*args, **kwargs)
            self.connection.execute("SET extension_directory=" + generate.quote(repo / "candidate-native/extensions"))
            self.connection.execute("SET TimeZone='UTC'")

        def execute(self, sql, *args, **kwargs):
            print("PREPARE", str(sql).splitlines()[0][:100], flush=True)
            timer = threading.Timer(1800, self.connection.interrupt)
            timer.start()
            try:
                self.connection.execute(sql, *args, **kwargs)
                return self
            finally:
                timer.cancel()

        def __getattr__(self, name):
            return getattr(self.connection, name)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.connection.close()

    duckdb.connect = BoundedConnection
    try:
        preparation = prepare_experiment.prepare(out / "prepared", 1.0, options, "100", [step, step])
    finally:
        duckdb.connect = original_connect
    save("preparation.json", preparation)
    plan = out / "prepared/plan"
    manifest = json.loads((plan / "manifest.json").read_text())
    save("manifest.json", manifest)
    subprocess.run([sys.executable, str(runner / "translate.py"), str(bench), str(plan), str(out)], check=True)

    phase("reference_generation")
    refs = out / "reference"
    refs.mkdir()
    with duckdb.connect(str(out / "reference.duckdb"), config={"memory_limit": duckdb_memory, "threads": 4,
                                                            "max_temp_directory_size": spill_limit}) as connection:
        connection.execute("SET TimeZone='UTC'")
        mutations.load_initial(connection, manifest, plan)
        for batch in ([None] if monthly_probe else [None, *manifest["batches"]]):
            number = 1 if batch is None else batch["number"]
            if batch:
                mutations.apply_batch(connection, manifest, plan, batch)
            directory = refs / ("batch" + str(number))
            directory.mkdir()
            parents = {name: list(filter(None, row["parent_models"].split(";")))
                       for name, row in pipeline.mapping().items() if name in selected}
            remaining = Counter(parent for entries in parents.values() for parent in entries)
            for name in selected:
                print("REFERENCE", number, name, flush=True)
                timed(connection, "CREATE OR REPLACE TABLE " + name + " AS " +
                      pipeline.render_model(name, manifest["source_configuration"]))
                timed(connection, "COPY " + name + " TO " + generate.quote(directory / (name + ".parquet")) +
                      " (FORMAT PARQUET)")
                for parent in parents[name]:
                    remaining[parent] -= 1
                    if remaining[parent] == 0:
                        connection.execute("DROP TABLE " + parent)
                if remaining[name] == 0:
                    connection.execute("DROP TABLE " + name)
    (out / "reference.duckdb").unlink()

    return plan, refs


def main():
    reserve = 60 if monthly_probe else 100
    if shutil.disk_usage(out).free < reserve * 1024**3:
        raise RuntimeError("Experiment needs at least " + str(reserve) + " GiB free scratch space")
    plan, refs = prepare_inputs()
    models = json.loads((out / "models.json").read_text())
    if monthly_probe:
        models = [model for model in models if model["name"] in selected]
        save("models.json", models)
        candidate_refs = out / "probe-reference/batch1"
        candidate_refs.mkdir(parents=True)
        original = next(model["duckdb_sql"] for model in models if model["name"] == monthly)
        assert original.count("SUM(effective_units)") == 1
        fixed = original.replace("SUM(effective_units)", "SUM(CAST(effective_units AS DECIMAL(28,10)))")
        with duckdb.connect(config={"memory_limit": duckdb_memory, "threads": 4, "max_temp_directory_size": spill_limit}) as connection:
            connection.execute("SET TimeZone='UTC'")
            connection.execute("CREATE VIEW int_engagement_status_transaction AS SELECT * FROM read_parquet(" +
                               generate.quote(refs / "batch1/int_engagement_status_transaction.parquet") + ")")
            timed(connection, "COPY (" + fixed + ") TO " + generate.quote(candidate_refs / (monthly + "_fixed.parquet")) + " (FORMAT PARQUET)")
            expected = "SELECT * FROM read_parquet(" + generate.quote(refs / ("batch1/" + monthly + ".parquet")) + ")"
            probes = []
            for threads in (1, 2, 4):
                connection.execute("SET threads=" + str(threads))
                timed(connection, "CREATE OR REPLACE TEMP TABLE float_probe AS " + original)
                difference = "(SELECT * FROM float_probe EXCEPT ALL " + expected + ") UNION ALL (" + expected + " EXCEPT ALL SELECT * FROM float_probe)"
                count = timed(connection, "SELECT count(*) FROM (" + difference + ") d").fetchone()[0]
                rows = timed(connection, "SELECT * FROM (" + difference + ") d LIMIT 100").fetchall()
                columns = [column[0] for column in connection.description]
                probes.append({"threads": threads, "differences": count,
                               "rows": [dict(zip(columns, row)) for row in rows]})
            save("duckdb-float-probes.json", probes)
    phase("spark_execution")
    classpath = (repo / "spark-classpath.txt").read_text().strip() + ":" + str(runner)
    java = Path(os.environ["JAVA_HOME"]) / "bin/java"
    opts = ["--add-opens=java.base/" + name + "=ALL-UNNAMED" for name in [
        "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio", "java.util",
        "java.util.concurrent", "java.util.concurrent.atomic", "sun.nio.ch", "sun.nio.cs",
        "sun.security.action", "sun.util.calendar"]]
    env = dict(os.environ, SPARK_LOCAL_IP="127.0.0.1", OPENIVM_CLI_PATH=str(runner / "compiler-adapter.py"),
               OPENIVM_EXTENSION_PATH=str(repo / "candidate-native/openivm.duckdb_extension"),
               EXTENDED_TPCDS_NATIVE_CLI=str(repo / "candidate-native/duckdb"),
               EXTENDED_TPCDS_COMPILER_INIT=str(repo / "candidate-native/init.sql"),
               EXTENDED_TPCDS_COMPILE_TRACE=str(out / "compiler-trace"),
               OPENIVM_COMPILE_FAILURE_BUNDLE_DIR=str(out / "compile-failures"))
    command = [str(java), "-Xmx6g" if monthly_probe else "-Xmx12g",
               "-XX:ActiveProcessorCount=4" if monthly_probe else "-XX:ActiveProcessorCount=8",
               "-Dextendedtpcds.workers=" + ("4" if monthly_probe else "8"),
               "-Dextendedtpcds.sparkCommit=" + os.environ["GITHUB_SHA"],
               "-Dextendedtpcds.monthlyProbe=" + str(monthly_probe).lower(), *opts, "-cp", classpath,
               "ExtendedTpcdsSpark", str(out), str(plan), str(out / "models.json"), str(refs)]
    if monthly_probe:
        command.append(str(out / "probe-reference"))
    process = subprocess.Popen(command, env=env, cwd=out, start_new_session=True)
    try:
        status = process.wait(timeout=21600)
    finally:
        # Includes any native compiler process left behind by a timed-out JVM.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    if status:
        raise RuntimeError("Spark exited with code " + str(status))
    phase("finished")


thread = threading.Thread(target=monitor, daemon=True)
thread.start()
provenance = {"scale_factor": 1, "mode": "mixed", "snapshots": 1 if monthly_probe else 3, "models": len(selected),
              "purpose": "monthly_precision_probe" if monthly_probe else "full_benchmark",
              "seed": 17, "report_date": "2002-12-31", "spark_commit": os.environ["GITHUB_SHA"],
              "spark_candidate_parent": "e84d79a6393dec1bb26ae62236de251295f7ba31",
              "benchmark_commit": benchmark_commit,
              "native_commit": "91ae4040438cb1b0cf11c04b9f68a20cfadf9c48",
              "native_artifact_run": 39226213916,
              "native_extension_sha256": hashlib.file_digest(
                  (repo / "candidate-native/openivm.duckdb_extension").open("rb"), "sha256").hexdigest(),
              "validation": "Independent DuckDB full-DAG references, bidirectional EXCEPT ALL, float rounding 10 places",
              "compiler_adapter": "Experimental standalone compatibility adapter, not a production pin upgrade",
              "limits": {"container_memory_gib": 14 if monthly_probe else 32,
                         "container_cpus": 4 if monthly_probe else 8, "java_heap_gib": 6 if monthly_probe else 12,
                         "duckdb_memory_gb": 6 if monthly_probe else 8, "duckdb_threads": 4,
                         "preparation_working_tables": "disk-backed",
                         "statement_timeout_s": 1800, "spark_timeout_s": 21600}}
save("provenance.json", provenance)
try:
    main()
except BaseException as error:
    save("failure.json", {"phase": phase_name, "error": repr(error)})
    raise
finally:
    finished.set()
    thread.join(timeout=5)
    provenance.update(elapsed_s=time.monotonic() - start, peak_process_tree_rss_mib=peak_rss / 1024,
                      worker_maxrss_mib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
                      child_maxrss_mib=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024)
    save("provenance.json", provenance)
