#!/usr/bin/env python3
"""Fit RefreshCostModel ridge-regression coefficients from ivm-bench telemetry."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import subprocess
import sys
from pathlib import Path


DEFAULT_INPUT = "/home/mdrrahman/openivm-spark/.temp/ivm-bench/mount/metrics/*/processed/metrics_by_model.parquet"
DEFAULT_DUCKDB = "/home/mdrrahman/wt-openivm/p23-constraints/build/release/duckdb"
CONTINUOUS_FEATURES = [
    "records_read",
    "records_written",
    "files_scanned",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "spill_memory_bytes",
    "spill_disk_bytes",
]
REFRESH_TYPE_FEATURES = [f"refresh_type_{code}" for code in range(10)]
FEATURES = CONTINUOUS_FEATURES + REFRESH_TYPE_FEATURES

REFRESH_TYPES = {
    "AGGREGATE_GROUP": 0,
    "SIMPLE_AGGREGATE": 1,
    "SIMPLE_PROJECTION": 2,
    "FULL_REFRESH": 3,
    "AGGREGATE_HAVING": 4,
    "WINDOW_PARTITION": 5,
    "GROUP_RECOMPUTE": 6,
    "TOP_K": 7,
    "DISTINCT_INCREMENTAL": 8,
    "SEMI_ANTI_RECOMPUTE": 9,
}

WINDOW_MODELS = {"daily_market", "dim_customer", "trades_history"}
AGGREGATE_MODELS = {
    "broker_performance",
    "customer_concentration",
    "daily_market_pulse",
    "market_volatility",
    "trade_volume_stats",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=DEFAULT_INPUT, help="metrics_by_model parquet glob")
    parser.add_argument("--duckdb-bin", default=DEFAULT_DUCKDB, help="DuckDB CLI with parquet support")
    parser.add_argument("--output", required=True, help="JSON file to write")
    parser.add_argument("--alpha", type=float, default=1.0, help="ridge penalty")
    parser.add_argument(
        "--engine",
        default="spark-openivm",
        help="engine to calibrate; use ALL to include vanilla Spark as FULL_REFRESH samples",
    )
    return parser.parse_args()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_rows(duckdb_bin: str, parquet_glob: str, engine: str) -> list[dict[str, str]]:
    where_engine = "" if engine == "ALL" else f"AND engine = {sql_literal(engine)}"
    query = f"""
SELECT
  engine,
  CAST(batch AS BIGINT) AS batch,
  dbt_model,
  wall_clock_ms,
  COALESCE(records_read, 0) AS records_read,
  COALESCE(records_written, 0) AS records_written,
  COALESCE(files_scanned, 0) AS files_scanned,
  COALESCE(shuffle_read_bytes, 0) AS shuffle_read_bytes,
  COALESCE(shuffle_write_bytes, 0) AS shuffle_write_bytes,
  COALESCE(spill_memory_bytes, 0) AS spill_memory_bytes,
  COALESCE(spill_disk_bytes, 0) AS spill_disk_bytes
FROM read_parquet({sql_literal(parquet_glob)})
WHERE wall_clock_ms IS NOT NULL
  AND dbt_model IS NOT NULL
  {where_engine}
ORDER BY engine, batch, dbt_model
"""
    result = subprocess.run(
        [duckdb_bin, "-csv", "-c", query],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return list(csv.DictReader(result.stdout.splitlines()))


def infer_refresh_type(engine: str, model: str) -> int:
    if engine != "spark-openivm":
        return REFRESH_TYPES["FULL_REFRESH"]
    if model in WINDOW_MODELS:
        return REFRESH_TYPES["WINDOW_PARTITION"]
    if model in AGGREGATE_MODELS:
        return REFRESH_TYPES["AGGREGATE_GROUP"]
    if model.startswith("fact_") or model in {"watches", "dim_trade"}:
        return REFRESH_TYPES["GROUP_RECOMPUTE"]
    return REFRESH_TYPES["SIMPLE_PROJECTION"]


def vectorize(row: dict[str, str]) -> tuple[list[float], float, int]:
    refresh_type = infer_refresh_type(row["engine"], row["dbt_model"])
    values = [float(row[name] or 0.0) for name in CONTINUOUS_FEATURES]
    values.extend(1.0 if code == refresh_type else 0.0 for code in range(10))
    return values, float(row["wall_clock_ms"]), refresh_type


def standardize(xs: list[list[float]]) -> tuple[list[list[float]], list[float], list[float]]:
    cols = len(xs[0])
    means = [sum(row[j] for row in xs) / len(xs) for j in range(cols)]
    scales = []
    for j in range(cols):
        variance = sum((row[j] - means[j]) ** 2 for row in xs) / len(xs)
        scale = math.sqrt(variance)
        scales.append(scale if scale > 0.0 else 1.0)
    return [[(row[j] - means[j]) / scales[j] for j in range(cols)] for row in xs], means, scales


def solve_linear(a: list[list[float]], b: list[float]) -> list[float]:
    n = len(b)
    aug = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(aug[r][col]))
        if abs(aug[pivot][col]) < 1e-12:
            raise ValueError("singular ridge system")
        aug[col], aug[pivot] = aug[pivot], aug[col]
        denom = aug[col][col]
        aug[col] = [value / denom for value in aug[col]]
        for row in range(n):
            if row == col:
                continue
            factor = aug[row][col]
            if factor:
                aug[row] = [aug[row][j] - factor * aug[col][j] for j in range(n + 1)]
    return [aug[i][n] for i in range(n)]


def fit_ridge(xs: list[list[float]], ys: list[float], alpha: float) -> tuple[float, list[float], float]:
    scaled_xs, means, scales = standardize(xs)
    design = [[1.0] + row for row in scaled_xs]
    cols = len(design[0])
    xtx = [[0.0 for _ in range(cols)] for _ in range(cols)]
    xty = [0.0 for _ in range(cols)]
    for row, y in zip(design, ys):
        for i in range(cols):
            xty[i] += row[i] * y
            for j in range(cols):
                xtx[i][j] += row[i] * row[j]
    for i in range(1, cols):
        xtx[i][i] += alpha
    standardized = solve_linear(xtx, xty)
    raw_weights = [standardized[i + 1] / scales[i] for i in range(len(FEATURES))]
    intercept = standardized[0] - sum(raw_weights[i] * means[i] for i in range(len(FEATURES)))
    preds = [intercept + sum(weight * value for weight, value in zip(raw_weights, row)) for row in xs]
    y_mean = sum(ys) / len(ys)
    ss_tot = sum((y - y_mean) ** 2 for y in ys)
    ss_res = sum((y - pred) ** 2 for y, pred in zip(ys, preds))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0.0 else 1.0
    return intercept, raw_weights, r2


def main() -> int:
    args = parse_args()
    rows = load_rows(args.duckdb_bin, args.input, args.engine)
    if not rows:
        raise ValueError("no telemetry rows matched the requested filter")
    vectors = [vectorize(row) for row in rows]
    xs = [item[0] for item in vectors]
    ys = [item[1] for item in vectors]
    refresh_counts: dict[str, int] = {}
    for _, _, refresh_type in vectors:
        refresh_counts[str(refresh_type)] = refresh_counts.get(str(refresh_type), 0) + 1
    intercept, raw_weights, r2 = fit_ridge(xs, ys, args.alpha)
    payload = {
        "schema_version": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_glob": args.input,
        "engine_filter": args.engine,
        "target": "wall_clock_ms",
        "row_count": len(rows),
        "ridge_alpha": args.alpha,
        "r2": r2,
        "features": FEATURES,
        "intercept": intercept,
        "weights": {name: raw_weights[i] for i, name in enumerate(FEATURES)},
        "refresh_type_counts": refresh_counts,
        "refresh_type_mapping": REFRESH_TYPES,
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {out} with {len(rows)} rows; R^2={r2:.6f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
