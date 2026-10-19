# Security-membership component experiment

Benchmark-only branch. No production refresh, SQL dispatcher, pin or durability
configuration changes. This is ready for measurement, not production promotion.

The existing pinned devcontainer/Nx `spark-ext:window-benchmark` target runs the
standalone unmanaged Scala probe after the existing window probe. It uses the
real Spark parser/complete captured trade-volume query and existing OpenIVM
RocksDB KV, batched reads, bounded prefix scan and durable batch/undo/manifest
publication. No host Spark/Java toolchain or new library is used. Spark 3.5 and
4.1 run through the existing source GCI matrix; no workflow changes are needed.

The experimental right-side cache stores exact Long-key counts. Duplicate cash
join handling is deliberately outside this smaller direct-membership probe.
NULL keys do not match ordinary SQL equality. Negative transitions, stale
publication and replay fail; reopen and last-match deletion are checked. All
Spark jobs run outside RocksDB sessions. Each operation publishes one ordinary
WAL-enabled batch, never changes durability policy, and preserves all output
rank/global expressions. Full-query bidirectional EXCEPT ALL gates each mode
before timing and every measured pair afterwards.

The fixture has one million trades and one million repeated watches across100
keys, then 1%/0.01% trade inserts and two membership inserts per batch. This
tests a large duplicate-heavy RHS, not a faithful SF100 cardinality distribution.
Both alternatives share the exact same precomputed grouped summary and source
snapshots. Candidate timings include bootstrap/delta grouping, state reads,
typed membership-frame creation, unchanged post-query, durable publication,
opening in B1 and closing in B3. Source population, grouped-summary maintenance,
target writes, compiler/admission and remote backup are excluded. Therefore
these are component timings, NOT total/dbt/SF100 performance or a drop-in
production comparison. No production source-identity, target-recovery or general
key/collation support is claimed.

Modes use Fabric-like200shuffle partitions/25MiB static broadcast and Spark-like
16/100MiB, with adaptive broadcast promotion disabled consistently. Each has
four alternating-order trials; the first is warmup. The single structured
MEMBERSHIP_POC_JSON record contains every raw sample and correctness count.
Previous production-commit and cached non-IVM GCI comparisons are unavailable.
Do not infer universal gains from this bounded synthetic fixture.

Host processor/load metadata and each mode's actual B2 executed plans are
captured with the samples. The CI matrix may share host resources: opposite-order
trials help but do not prove isolation. Inspect the recorded plans and repeat
controls before assigning any difference to Fabric-specific execution.

Review before dispatch: state is private to this probe, Long-key-only and bounded;
existing production code is unchanged. No new ordered data structure, cost model,
worker configuration, source pin, full-refresh fallback, target/state boundary or
rank algorithm is introduced. This probe is isolated from the already queued
read-counter SF100 experiment and will not dispatch another SF100.
