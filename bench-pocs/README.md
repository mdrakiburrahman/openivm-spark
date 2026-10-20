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
