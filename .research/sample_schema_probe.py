"""Record bounded local resource context while an Nx probe runs."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

output = Path(".research/schema-catalog-resources.jsonl")
output.parent.mkdir(exist_ok=True)
hz = os.sysconf("SC_CLK_TCK")
page = os.sysconf("SC_PAGE_SIZE")
previous = {}
previous_cpu = None
started = time.monotonic()
command = sys.argv[1:]
with output.open("a", encoding="utf-8") as evidence:
    evidence.write(json.dumps({"kind": "start", "utc": time.time(),
                               "command": command, "orphan_pid": 574877}) + "\n")
    evidence.flush()
    process = subprocess.Popen(command)
    while True:
        now = time.monotonic()
        java = []
        for directory in Path("/proc").iterdir():
            if not directory.name.isdigit():
                continue
            try:
                raw = (directory / "stat").read_text()
                name = raw[raw.index("(") + 1:raw.rindex(")")]
                if name != "java":
                    continue
                fields = raw[raw.rindex(")") + 2:].split()
                ticks = int(fields[11]) + int(fields[12])
                pid = int(directory.name)
                old = previous.get(pid)
                cpu = None if old is None else 100 * (ticks - old[1]) / hz / (now - old[0])
                previous[pid] = (now, ticks)
                java.append({"pid": pid, "state": fields[0], "ppid": int(fields[1]),
                             "cpu_one_core_percent": cpu, "rss_bytes": int(fields[21]) * page,
                             "lifetime_cpu_seconds": ticks / hz, "orphan": pid == 574877})
            except (OSError, ValueError, IndexError):
                pass
        available = next(line for line in Path("/proc/meminfo").read_text().splitlines()
                         if line.startswith("MemAvailable:"))
        ticks = [int(value) for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]
        current_cpu = (sum(ticks), ticks[3] + ticks[4])
        busy = None if previous_cpu is None else 100 * (
            1 - (current_cpu[1] - previous_cpu[1]) / max(1, current_cpu[0] - previous_cpu[0]))
        previous_cpu = current_cpu
        evidence.write(json.dumps({"kind": "sample", "utc": time.time(),
                                   "elapsed_seconds": now - started,
                                   "loadavg": Path("/proc/loadavg").read_text().strip(),
                                   "logical_cpus": os.cpu_count(),
                                   "machine_cpu_busy_percent": busy,
                                   "memory_available_kib": int(available.split()[1]),
                                   "java": java}) + "\n")
        evidence.flush()
        code = process.poll()
        if code is not None:
            evidence.write(json.dumps({"kind": "end", "utc": time.time(), "exit_code": code}) + "\n")
            break
        time.sleep(1)
sys.exit(code)
