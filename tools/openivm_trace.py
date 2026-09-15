#!/usr/bin/env python3
"""Convert OpenIVM JSON/JSONL span rows to Chrome/Perfetto trace JSON."""

import argparse
import json
from pathlib import Path


def load_rows(path: Path):
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        return json.loads(text)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def parse_detail(detail):
    fields = {}
    for item in (detail or "").split(";"):
        if "=" in item:
            key, value = item.split("=", 1)
            fields[key] = value
    return fields


def event_for(row):
    if row.get("step_name") == "query_span" or row.get("stepName") == "query_span":
        detail = parse_detail(row.get("detail"))
        start_ms = int(detail["start_epoch_ms"])
        end_ms = int(detail["end_epoch_ms"])
        return {
            "name": row.get("view_name") or row.get("viewName") or row.get("refresh_id"),
            "cat": "openivm-refresh",
            "ph": "X",
            "ts": start_ms * 1000,
            "dur": max(0, end_ms - start_ms) * 1000,
            "pid": "openivm",
            "tid": detail.get("thread", "driver"),
            "args": {
                "refresh_id": row.get("refresh_id") or row.get("refreshId"),
                "outcome": detail.get("outcome", "unknown"),
            },
        }

    task_id = row.get("taskId") or row.get("task_id")
    start_ms = row.get("startedEpochMs") or row.get("started_epoch_ms")
    end_ms = row.get("endedEpochMs") or row.get("ended_epoch_ms")
    if task_id is not None and start_ms is not None and end_ms is not None:
        return {
            "name": task_id,
            "cat": "openivm-ctas",
            "ph": "X",
            "ts": int(start_ms) * 1000,
            "dur": max(0, int(end_ms) - int(start_ms)) * 1000,
            "pid": "openivm",
            "tid": row.get("threadName") or row.get("thread_name") or "driver",
            "args": {
                "outcome": row.get("outcome", "unknown"),
                "queue_nanos": row.get("queueNanos") or row.get("queue_nanos", 0),
                "inflight": row.get("inflight", 0),
                "capacity_drop": row.get("capacityDrop") or row.get("capacity_drop", False),
            },
        }
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="JSON array or JSON-lines span rows")
    parser.add_argument("output", type=Path, help="Chrome/Perfetto trace JSON")
    args = parser.parse_args()

    events = [event for row in load_rows(args.input) if (event := event_for(row)) is not None]
    events.sort(key=lambda event: (event["ts"], event["name"]))
    args.output.write_text(json.dumps({"traceEvents": events}, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(events)} spans to {args.output}")


if __name__ == "__main__":
    main()
