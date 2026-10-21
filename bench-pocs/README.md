# Conditional frequency counts experiment

Experimental branch based on the last measured native Spark extension pin,
`7383ad194edbe2003a26f6d44c278e9b2f34ca5b`. Not promoted to the SF100 pin yet.

The production candidate replaces historical-input expansion with conditional
counts when `(group, channel)` determines one missing frequency value. This is
a structural condition, not a group-count cutoff. Multi-value requests keep the
original plan. The wide request uses the existing byte cap and broadcast budget.
Ordinary aggregate expressions, state bytes, checkpoints and publication are
unchanged. The profile records which lookup strategy was selected.

Review before benchmarking:

- Each request row is unique by group, so the left join preserves input
  multiplicity. Nullable composite keys use the same typed struct encoding as
  the existing path; strings are binary and dates use epoch days.
- Conditional COUNT ignores missing requests and NULL source values. Frequencies
  absent from the current output are zero, as in the previous implementation;
  absent RocksDB cache entries still mean unknown.
- Ordinary aggregates run over unexpanded, unmodified payload rows. The result
  is repacked only after aggregation. Requested values retain their original
  JSON bytes. Existing count/state/cardinality/byte checks still apply.
- Existing tests now cover cold and warm multi-group requests, partial channel
  requests, nullable composite keys, string/date values, last-duplicate removal,
  payload-only updates, unchanged state before publication and multi-value
  fallback. Existing rollback, timestamp and decimal tests remain enabled.
- The extra request join may outweigh saved expansion for small inputs. This
  must be measured, not assumed. No group-count tuning knob is introduced.

`FrequencyCountProbe` compares the exact previous implementation (renamed in
`ReferenceGroupedDistinctState.scala`) with the candidate, in one JVM. It uses
1M initial rows, then 1% and 0.01% inserts, touching 39 and 1 groups respectively,
with four DISTINCT channels. Both modes use the same Parquet inputs and RocksDB
durability/metrics. Implementation and settings order alternate; the first
round is warmup, followed by four measured rounds. Every refreshed summary is
checked with bidirectional EXCEPT ALL, outside the timed preparation span.
State publication time is recorded separately. No target writes, rank postquery,
remote backup, dbt, or SF100 result is claimed. The cached non-IVM comparison is
unavailable.

Run through the pinned devcontainer's locked Nx target:

```sh
npx --no-install nx run spark-ext:window-benchmark --configuration=spark-3.5
npx --no-install nx run spark-ext:window-benchmark --configuration=spark-4.1
```

The existing source GCI runs full verification and then this probe on both
supported targets. `dev/run.sh` temporarily runs the pinned formatter and prints
its whitespace patch before normal verification, because this host has no
devcontainer runtime. Apply the recovered formatting and remove that temporary
formatting hook before any SF100 pin or production review. No verification,
inventory, assembly guard, parity tests or workflow gates are removed.

## First result and follow-up

Run 39387596565 passed all production verification suites on Spark 3.5 and 4.1.
The Spark 4.1 probe alone failed on the Scala 2.13 Array-to-varargs deprecation;
using `toIndexedSeq` fixes that without changing the test or query.

Spark 3.5's four measured-round medians versus 7383ad1:

| Settings | Preparation B2 baseline/candidate | Preparation B3 baseline/candidate |
|---|---:|---:|
| Fabric-like | 1.454 / 1.236s (-15.0%) | 0.582 / 0.681s (+17.0%) |
| Spark-like | 1.132 / 0.799s (-29.4%) | 0.494 / 0.455s (-8.0%) |

All 80 bidirectional EXCEPT ALL checks passed. This is not sufficient to promote
with a Fabric-like B3 regression. The candidate launched 15 preparation jobs in
Fabric-like settings versus 14/13 for the baseline's B2/B3; the result-metadata
join adds work even though requests are already bounded and driver-local.

The follow-up removes that result join. Under the verified `(group,channel)`
uniqueness condition, the existing request map recovers the original frequency
key after the bounded collect. The frequencies themselves still come from Spark
COUNT, never from an assumed cache value. Byte checks include the recovered key.
Missing channel requests contribute zero and are filtered before map lookup.
The fallback path, numerical aggregates, cache format and publication are
unchanged. This removes an execution stage instead of choosing a hardcoded
request-count threshold. Runtime results are pending; the first-run regression
has not yet been proven fixed.

## Regression diagnostic (measurement only)

This branch retains exactly the production and test code verified at `963a477`.
It changes no Spark/native/runtime dependency pin and no production algorithm.
Both existing runtimes run the same baseline/candidate pair. The ongoing Fabric
SF100 run is separate and must not be duplicated or cancelled by this probe.

The first probe reported ratios of separately aggregated medians and included
bootstrap in its component total. Bootstrap implementations are identical, yet
its medians differed by 5.5% under 4.1/Fabric-like settings. Therefore that whole
component-total difference cannot be attributed to the new maintenance path.
In round-paired 4.1/Fabric-like data, B2 changes sign twice; B3 is consistently
slower (+17/+36/+22/+215ms), and has one additional job. Job count alone does
not establish the cause. Spark 3.5 B3 also changes sign across rounds despite a
lower candidate median. Do not present these small samples as a pipeline fix.

This diagnostic adds only a test-scoped Spark listener and a 2x2 cross of the
already used settings: shuffle partitions 16/200 and broadcast threshold
25MiB/100MiB. It does not tune those settings in production or propose a cutoff.
It records process CPU/GC, each job and stage, task CPU/input/shuffle/spill/GC,
initial/final AQE plans, and final per-operator SQL metrics. Listener snapshots
wait for event delivery outside the timed preparation span and require complete
job/SQL bindings; no sleep-based correctness or best-effort profiling. Plans
are emitted as structured lines for measured rounds 1 and 4, rather than one
oversized log record. These are instrumented diagnostics, not untraced speedups.
All existing full verification, inventory and EXCEPT ALL gates remain enabled.

Falsifiable explanations to check before proposing another performance change:

- Incremental `CollectLimit` fetching: extra result jobs within the same SQL
  execution, fetching different output partitions. Rule it out if the extra
  job is instead a broadcast exchange.
- AQE/join choice: one crossed setting changes join/exchange plans and stage
  critical path. Compare one setting at a time, not platform labels alone.
- Conditional-count CPU tradeoff: expanded output rows decrease, but task CPU
  rises with the unchanged scanned input. Whole-stage timings overlap operator
  timings; never sum them as independent costs.
- Driver/GC/noise: unchanged work and plans with elapsed changes explained by
  GC, planning or scheduling intervals. SQL intervals and parallel stages must
  be unioned or shown on a timeline, not naively added together.
