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
    timer = threading.Timer(300, connection.interrupt)
    timer.start()
    try:
        return connection.execute(sql)
    finally:
        timer.cancel()


def main():
    if shutil.disk_usage(out).free < 100 * 1024**3:
        raise RuntimeError("SF1 needs at least 100 GiB free scratch space")
    phase("source_preparation")
    options = {"profile": "native", "mode": "mixed", "seed": 17, "report_date": "2002-12-31",
               "history_months": 12, "days": 3, "mutation_cohort": "all"}
    step = {"insert_pct": "1", "update_pct": "1", "delete_pct": "1"}
    os.environ.update(DUCKDB_MEM_LIMIT="8GB", DUCKDB_THREADS="8", DUCKDB_MAX_TEMP_DIRECTORY_SIZE="64GB")
    # Interrupt long preparation statements without changing the canonical generator.
    original_connect = duckdb.connect

    class BoundedConnection:
        def __init__(self, *args, **kwargs):
            self.connection = original_connect(*args, **kwargs)
            self.connection.execute("SET extension_directory=" + generate.quote(repo / "candidate-native/extensions"))
            self.connection.execute("SET TimeZone='UTC'")

        def execute(self, sql, *args, **kwargs):
            print("PREPARE", str(sql).splitlines()[0][:100], flush=True)
            timer = threading.Timer(300, self.connection.interrupt)
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
    with duckdb.connect(str(out / "reference.duckdb"), config={"memory_limit": "8GB", "threads": 8,
                                                            "max_temp_directory_size": "64GB"}) as connection:
        connection.execute("SET TimeZone='UTC'")
        mutations.load_initial(connection, manifest, plan)
        for batch in [None, *manifest["batches"]]:
            number = 1 if batch is None else batch["number"]
            if batch:
                mutations.apply_batch(connection, manifest, plan, batch)
            directory = refs / ("batch" + str(number))
            directory.mkdir()
            parents = {name: list(filter(None, row["parent_models"].split(";")))
                       for name, row in pipeline.mapping().items()}
            remaining = Counter(parent for entries in parents.values() for parent in entries)
            for name in pipeline.selected_models():
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

    phase("spark_execution")
    classpath = (repo / "spark-classpath.txt").read_text().strip() + ":" + str(runner)
    java = Path(os.environ["JAVA_HOME"]) / "bin/java"
    opts = ["--add-opens=java.base/" + name + "=ALL-UNNAMED" for name in [
        "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio", "java.util",
        "java.util.concurrent", "java.util.concurrent.atomic", "sun.nio.ch", "sun.nio.cs",
        "sun.security.action", "sun.util.calendar"]]
    subprocess.run([str(java.with_name("javac")), "-proc:none", "-cp", classpath,
                    str(runner / "ExtendedTpcdsSpark.java")], check=True)
    env = dict(os.environ, SPARK_LOCAL_IP="127.0.0.1", OPENIVM_CLI_PATH=str(runner / "compiler-adapter.py"),
               OPENIVM_EXTENSION_PATH=str(repo / "candidate-native/openivm.duckdb_extension"),
               EXTENDED_TPCDS_NATIVE_CLI=str(repo / "candidate-native/duckdb"),
               EXTENDED_TPCDS_COMPILER_INIT=str(repo / "candidate-native/init.sql"),
               EXTENDED_TPCDS_COMPILE_TRACE=str(out / "compiler-trace"),
               OPENIVM_COMPILE_FAILURE_BUNDLE_DIR=str(out / "compile-failures"))
    command = [str(java), "-Xmx12g", "-XX:ActiveProcessorCount=8",
               "-Dextendedtpcds.sparkCommit=" + os.environ["GITHUB_SHA"], *opts, "-cp", classpath,
               "ExtendedTpcdsSpark", str(out), str(plan), str(out / "models.json"), str(refs)]
    process = subprocess.Popen(command, env=env, cwd=out, start_new_session=True)
    try:
        status = process.wait(timeout=7200)
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
provenance = {"scale_factor": 1, "mode": "mixed", "snapshots": 3, "models": 96,
              "seed": 17, "report_date": "2002-12-31", "spark_commit": os.environ["GITHUB_SHA"],
              "spark_candidate_parent": "98f133b01fbd95175fdca86a589c33762dbd71a0",
              "benchmark_commit": "aa44dc36c15a5f25d97b889a3ad408debbef1c77",
              "native_commit": "91ae4040438cb1b0cf11c04b9f68a20cfadf9c48",
              "native_artifact_run": 39226213916,
              "native_extension_sha256": hashlib.file_digest(
                  (repo / "candidate-native/openivm.duckdb_extension").open("rb"), "sha256").hexdigest(),
              "validation": "Independent DuckDB full-DAG references, bidirectional EXCEPT ALL, float rounding 10 places",
              "compiler_adapter": "Experimental standalone compatibility adapter, not a production pin upgrade",
              "limits": {"container_memory_gib": 32, "container_cpus": 8, "java_heap_gib": 12,
                         "duckdb_memory_gb": 8, "statement_timeout_s": 300, "spark_timeout_s": 7200}}
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
