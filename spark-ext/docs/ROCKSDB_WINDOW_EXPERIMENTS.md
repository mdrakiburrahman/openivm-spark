# RocksDB window/rank experiments: implementation, measurements and handoff

Reconstructed **2026-10-28** from preserved source archives, review notes, exact test logs and result JSON. This records the paused/uncommitted work as well as the grouped-state work that later reached PR62. It does not resume or commit any prototype. Seconds below are local measurements unless explicitly labeled SF100.

## Status and conclusions

- The **ordered rank-index POC is paused, archived and not in the production PR**. Its cheap sparse updates worked, but extra Spark extraction, mapping and Delta writes often erased the kernel advantage. It has no verified production SF100 result.
- The **grouped DISTINCT-frequency state design** is different: cache exact frequencies in RocksDB, then run the original Spark post-group/window query on the grouped summary. Its fused-query descendant is in PR62. It accelerates the input to rank windows; it does not maintain output ranks through the experimental ordered index.
- Some rejected prototypes have attractive B2/B3 timings despite slower B1. Those are worth retaining for refresh-heavy pipelines, but extrapolation to 100 refreshes is not a measured long-running result. Growing state, changing keys, global rank shifts and cold misses can alter the cost materially.
- Current production follow-up `306b8b0e` keeps bounded cached JSON in a typed local relation with known cardinality and simplifies grouped publication. It is a separate, not-yet-benchmarked change, not promotion of the rank POC. [Source CI 40709531266](https://github.com/mdrakiburrahman/openivm-spark/actions/runs/40709531266) passed both runtime families and identical test inventory.

## What we built and why

### 1. Ordered rank trace using existing RocksDB primitives

The external `RocksOrderedRankPrototype.scala` implements a flat ordered peer/member index. It uses the existing OpenIVM RocksDB registry and wrapped batch API, with `get`, `multiGet`, predecessor/seek and bounded ordered range scans. It adds no persistent tree, native RocksDB rank-select API, separate storage engine or durability shortcut.

Peer keys encode SQL ordering; member keys distinguish payloads and retain exact signed multiplicity. The prototype tracks peer counts, member counts and saved rank labels in disjoint prefixes. Nullable ordering, ASC/DESC, multiple sort fields, numeric/string encodings and UTF-8 ordering were exercised. Overflow, state-size and identity checks fail closed. Default prototype limits are fixture bounds, not evidence that arbitrary high-cardinality windows fit on the driver.

`RANK = 1 + number of rows before the peer`, while `DENSE_RANK = 1 + number of peers before it`. Therefore inserting a duplicate into an existing first peer changes later RANK labels but leaves later DENSE_RANK labels unchanged. A genuinely new first peer changes every later dense rank. The kernel consolidates signed changes, skips label intervals whose requested labels are unchanged, uses predecessor/range access for affected intervals, and emits exact old/new ranked rows for members whose labels change. A new minimum can still require O(N) changed outputs; faster lookup cannot remove that output cost. These are structural complexity observations, not a measured universal bound for the whole Spark command.

We specialized execution for RankOnly / DenseOnly / Both, preserving the default Both behavior. Function choice is part of reusable-state identity; switching the requested function cannot silently reuse an incompatible cache. DenseOnly avoids work for unused weighted ranks. We also replaced repeated changed-member reads with a bounded multiGet; shifted intervals still need ordered member enumeration.

### 2. Bootstrap and real Delta publication

The 76k-row fixture added actual Delta target writes and exact net-CDF checks. Shared bootstrap persisted the original Spark ranked result, wrote that result to the target, and seeded the derived index from the same bounded materialization. Persistence, collection, cache construction and blocking unpersist stayed inside B1. The saved target UUID/path/version is real in these lifecycle tests; only the earlier operator-only test simulated target stamps.

Shared bootstrap reduced the previous POC's initial work, but initial cache creation remained much slower than a full Spark write. It did not change B2/B3 code; variations in those batches between separate single trials are not evidence that the B1 change caused an incremental speedup.

### 3. Multi-order rank-vector bridge and affected-key writes

`SharedRankMappingPrototype.scala` coordinated multiple ordered traces for three window orderings and a shared projection context. The bridge supported a bounded exact identity mapping, rollback/reopen guards and one target version per update. Variants tried typed local frames, direct snapshots of maintained rank vectors instead of repeatedly scanning/mapping the target, and existing affected-key MERGE / proven all-new append routes instead of full target replacement.

The affected-key path requires a complete unique output identity. Changed payloads and membership count even if rank labels do not change. Global context can change outputs outside the rows whose ranks changed; the fallback must widen the affected output exactly. The fixture's nullable Long SUM / String identity proof is not a general proof for arbitrary floating expressions, hidden order columns, joins or bag outputs. Integration should reuse `MaterializedViewCommands.buildWindowCdfPartitionMerge` rather than introduce a duplicate applier.

### 4. Native grouped transition plus ordered-rank lifecycle

A separate unmanaged POC reused compiler-generated native aggregate update expressions before rank maintenance. Group state, ordered rank state and the consumed-source / target checkpoint were published in one ordinary wrapped batch in the same MV shard. Spark actions occurred outside RocksDB sessions; the existing per-MV mutex and target guards remained.

We tried collecting bounded native contributions once, directly consolidating signed integral/binary-string rows instead of Spark EXCEPT ALL, and exact affected-key Delta predicate replacement where the fixture proved one complete row per group. Escaped/null keys and empty replacement were tested. Unsupported types/shapes retained the general route. The direct codec deliberately did not claim floating-point equality or arbitrary SQL collation support.

This reduced bridge work, but Delta application and grouped preparation still dominated. In the final local variant B2 was 1.198s: grouped preparation 0.468s, Delta apply 0.700s, rank preparation 0.0048s and state commit 0.0095s. These are measured substeps within the command, not extra seconds to add to its total. B3 showed the same ordering. Further rank-index micro-optimization would not address that dominant interval.

### 5. Actual broker query: exact grouped DISTINCT frequency state

This line of experiments retained the actual broker query: four COUNT(DISTINCT) channels, three DENSE_RANK expressions, cash anti-membership, HAVING and global statistics. RocksDB stores exact frequency state; cached absence means **unknown**, not frequency zero. Cold requested frequencies are read from current pinned history. Signed changes update counts at zero crossings. We retained Spark's original floating SUM/AVG/ROUND computations rather than substitute incremental floating arithmetic.

The first demand-cache candidate paid for separate frequency and ordinary-summary queries. Sharing a materialized current input still added Spark actions and did not help. The successful local candidate fused ordinary aggregation and requested frequency calculation in one current-history aggregate, using Spark's existing Expand/null-padding machinery and a known-small request relation. With equal AQE/broadcast settings it removed repeated scans and enabled a broadcast lookup. It used no new rank tree or approximate state.

The typed/versioned variant replaced Python descriptor extraction with a typed Spark descriptor and published exact all-source / target version vectors with grouped state. The measured descriptor phase was 12.1/35.4ms versus 0.56–0.68s in earlier trials; the change in total time cannot all be attributed to that phase because trials and guards differ. This work subsequently went through command-level admission, schema/codec/membership and recovery integration; the ordered-rank POC did not.

### 6. RocksDB remote-backup reuse experiments

Whole-snapshot exact-content reuse was rejected using a concrete profiling-only counterexample: every `RefreshProfile.flush` modifies its separate RocksDB root, so the whole shared archive changes even if MV data does not. It passed 55 Spark3.5 common durability/archive tests and the extra counterexample, but Spark4.1, cloud CI and GCI were not run for this rejected approach. Exact equality of the entire archive therefore misses the benchmark hot path.

Component-snapshot v1/v2 tried reusing unchanged durable components while preserving a complete recoverable snapshot. A counterbalanced **backup-only** storage proxy used synthetic 4MiB state plus frozen actual profile/query-log rows; it did not execute SF100 queries. V1 B1/B2/B3 changed **7.849/8.268/9.196 → 9.006/7.935/8.924s** (**+14.7%/−4.0%/−3.0%**, total **+2.2%**). Streaming hashes during packing and limiting pre-hashing to plausible unchanged components in v2 changed **7.880/8.241/9.162 → 8.861/7.687/8.702s** (**+12.4%/−6.7%/−5.0%**, total **−0.13%**). Four exact state/counter/telemetry recovery checks and 63 focused tests per runtime passed for v2. V2 upload bytes in the forward pair fell about 215→136MB at B2 and 223→138MB at B3, but CPU/packing/hash costs consumed most latency savings. No meaningful non-IVM counterpart exists for this internal storage proxy. These figures do not establish a whole-command or cloud win. Async remote publication in the production branch is a different behavior and must retain its explicit recovery-window caveat.

## How we tested and what the clocks include

- Historical local targets were Spark **3.5.1 / Scala 2.12.17 / Delta 3.2.0 / JDK17** and Spark **4.1.0 / Scala 2.13.17 / Delta 4.2.0 / JDK21**, with RocksDB 8.3.2 where recorded. These are historical local versions, not a claim about current cloud runtime patches.
- Operator fixture: 10k peers, then 100 and one duplicate insert into the first peer; five alternating paired trials after warmup. RocksDB wrapped commits were real, target stamps simulated, no Delta target write.
- 76k fixture: actual Delta writes/CDF, seven stages including initial load, sparse updates/reopen, new-minimum full shifts, duplicate insertion/deletion and complete deletion. One paired timing trial per variant/runtime; not SF100.
- Multi-order fixture: 10k child rows, three orderings and global context, fresh JVM forward/reverse trials. Exact target bags and signed net CDF were checked through 20 stages, then 24 stages for the affected-key version, with one actual Delta version per refresh. Six kernel/lifecycle cases passed on both supported runtime families.
- Native grouped/rank fixture: 1M rows / 1000 groups, favorable updates to an existing group's unchanged sort key. Four mixed-DML / lifecycle tests passed per runtime per variant. Timed B1 included target creation, native compilation, state/bootstrap and cleanup.
- Actual broker fixture: 1M facts / 16 brokers, local[1], shuffle1; 19 exact query/group-state/CDF scenarios on both runtimes, plus two admission/shape tests and four publication-failure boundaries for the typed/versioned variant. Equal AQE=true / 100MiB auto-broadcast was applied to every alternative in the planning-profile pairs. Earlier test-profile and later planning-profile runs are distinct experiments.
- Full-command bootstrap stress: 1M facts for each of broker/security, 80k/38k groups, most summary groups hidden from the published output, 32-character string keys. Correctness must seed all summary groups, not just visible outputs. Both revisions used local[1], shuffle16, AQE and ordinary Delta defaults. The aggregate reported below is the sum of two model command durations, not concurrent pipeline elapsed time.
- Exact validation used independent original-Spark-query oracles, bidirectional EXCEPT ALL bag equality, signed net target CDF, real target identity/version checks, nulls/ties/duplicates, mixed updates/deletes and reopen/retry/failure injection. Exception/close-reopen tests are **not power-loss or machine-crash proof**. Existing durability issues remain separate.
- Source generation/writes, independent oracles and assertions were outside the compared maintenance clocks equally. Cache construction, target/state commits and required cleanup were inside. Diagnostic runs with event/GC logging are not performance trials. Never sum nested phase times with their enclosing totals.
- Most local current-extension controls shared parked support edits. A pristine previous-production-commit comparison is **unavailable** for those trials; the high-cardinality bootstrap trial explicitly used clean `da793f6`. Matching cached non-IVM GCI is **unavailable** for local POCs; the tables use the same-trial plain Spark control where available.

## Per-batch local results

Positive change means slower; negative means faster. Every cell gives candidate / comparator seconds, absolute change and percentage change. Keep fixture sizes and runtime families separate. These are recovered historical results, not new benchmark executions.

### Ordered-rank kernel and real-target bridge

Operator-only specialization compares with the unspecialized Both kernel:

| Experiment | B1 candidate / control s (change) | B2 candidate / control s (change) | B3 candidate / control s (change) | Three-batch total s (change) |
| --- | --- | --- | --- | --- |
| DenseOnly operator | 0.242 / 0.270 (-0.028s; -10.3%) | 0.006 / 0.098 (-0.092s; -93.8%) | 0.006 / 0.087 (-0.081s; -92.9%) | 0.254 / 0.455 (-44.1%) |

All following rank-lifecycle rows compare with their own paired full-Spark target write:

| Experiment | B1 candidate / control s (change) | B2 candidate / control s (change) | B3 candidate / control s (change) | Three-batch total s (change) |
| --- | --- | --- | --- | --- |
| DenseOnly real Delta 76k | 2.573 / 0.893 (+1.680s; +188.1%) | 0.262 / 0.909 (-0.648s; -71.2%) | 0.321 / 0.790 (-0.469s; -59.3%) | 3.155 / 2.592 (+21.7%) |
| Batched member reads 76k | 2.636 / 0.946 (+1.690s; +178.5%) | 0.378 / 0.992 (-0.614s; -61.9%) | 0.349 / 0.699 (-0.350s; -50.0%) | 3.363 / 2.637 (+27.5%) |
| Shared bootstrap 76k Spark3.5 | 2.151 / 0.894 (+1.257s; +140.7%) | 0.299 / 1.005 (-0.705s; -70.2%) | 0.312 / 0.728 (-0.416s; -57.1%) | 2.763 / 2.627 (+5.2%) |
| Shared bootstrap 76k Spark4.1 | 2.383 / 1.190 (+1.194s; +100.3%) | 0.488 / 0.735 (-0.247s; -33.6%) | 0.505 / 0.629 (-0.124s; -19.7%) | 3.376 / 2.554 (+32.2%) |
| Multi-order bridge 10k | 12.973 / 8.354 (+4.618s; +55.3%) | 3.980 / 2.864 (+1.116s; +39.0%) | 3.681 / 2.835 (+0.847s; +29.9%) | 20.634 / 14.053 (+46.8%) |
| Typed local frame 10k | 13.291 / 8.082 (+5.209s; +64.4%) | 3.801 / 2.929 (+0.872s; +29.8%) | 3.592 / 3.236 (+0.356s; +11.0%) | 20.684 / 14.247 (+45.2%) |
| Direct snapshot 10k | 5.476 / 4.064 (+1.412s; +34.7%) | 1.854 / 1.660 (+0.194s; +11.7%) | 1.904 / 1.731 (+0.173s; +10.0%) | 9.234 / 7.455 (+23.9%) |
| Affected-key apply 10k | 6.201 / 5.078 (+1.123s; +22.1%) | 1.749 / 1.916 (-0.167s; -8.7%) | 1.862 / 1.980 (-0.118s; -5.9%) | 9.813 / 8.974 (+9.3%) |

The typed-local / direct-snapshot / affected-key sequence uses two fresh JVM orders, not many repetitions. The affected-key trial's favorable first-order total flipped from a 35.8% win to a 97.6% loss when order reversed. The balanced result is shown above; neither a favorable single order nor a fast sparse kernel justified promotion.

DenseOnly new-minimum full shift took 1.061s versus 0.745s (+42.3%); complete deletion took 4.140s versus 0.584s (+608.5%). Batched member reads cut complete-deletion preparation 3.069s → 1.767s (−42.4%) and total 4.140s → 2.876s (−30.5% versus the earlier POC), but still lost against its 0.535s full-Spark control. This is why update distribution matters.

### Native grouped-transition/rank bridge

Comparator is each trial's plain Spark full query:

| Experiment | B1 candidate / control s (change) | B2 candidate / control s (change) | B3 candidate / control s (change) | Three-batch total s (change) |
| --- | --- | --- | --- | --- |
| Native lifecycle first | 1.621 / 0.458 (+1.162s; +253.7%) | 1.728 / 0.362 (+1.365s; +376.8%) | 1.185 / 0.720 (+0.465s; +64.6%) | 4.534 / 1.541 (+194.3%) |
| Native lifecycle bridge | 1.833 / 0.468 (+1.365s; +291.9%) | 1.379 / 0.309 (+1.070s; +345.8%) | 0.887 / 0.822 (+0.065s; +7.9%) | 4.098 / 1.599 (+156.3%) |
| Native lifecycle replacement | 1.662 / 0.472 (+1.190s; +252.0%) | 1.198 / 0.383 (+0.815s; +212.6%) | 0.825 / 0.785 (+0.040s; +5.0%) | 3.684 / 1.641 (+124.6%) |

Versus the same-trial current extension (not pristine commit):

| Variant | B1 | B2 | B3 | Total |
| --- | --- | --- | --- | --- |
| Native lifecycle first | -5.5% | +39.6% | +86.9% | +26.4% |
| Native lifecycle bridge | +14.3% | -0.7% | +41.6% | +13.3% |
| Native lifecycle replacement | -0.3% | -3.9% | +33.7% | +4.4% |

### Actual full broker query

Comparator is the same-trial plain Spark full query, not the current extension:

| Experiment | B1 candidate / control s (change) | B2 candidate / control s (change) | B3 candidate / control s (change) | Three-batch total s (change) |
| --- | --- | --- | --- | --- |
| Broker demand cache | 5.651 / 4.948 (+0.703s; +14.2%) | 9.558 / 4.423 (+5.135s; +116.1%) | 3.864 / 4.886 (-1.022s; -20.9%) | 19.073 / 14.257 (+33.8%) |
| Broker shared current | 5.587 / 4.716 (+0.870s; +18.5%) | 10.692 / 4.323 (+6.369s; +147.3%) | 4.782 / 4.850 (-0.068s; -1.4%) | 21.061 / 13.889 (+51.6%) |
| Broker fused test profile forward | 5.587 / 4.795 (+0.792s; +16.5%) | 9.925 / 4.426 (+5.499s; +124.2%) | 2.641 / 5.037 (-2.397s; -47.6%) | 18.152 / 14.259 (+27.3%) |
| Broker fused test profile reverse | 7.035 / 4.957 (+2.078s; +41.9%) | 9.525 / 5.039 (+4.486s; +89.0%) | 2.605 / 4.308 (-1.703s; -39.5%) | 19.165 / 14.304 (+34.0%) |
| Broker fused planning forward | 5.034 / 4.013 (+1.021s; +25.4%) | 4.438 / 3.741 (+0.697s; +18.6%) | 1.873 / 4.153 (-2.280s; -54.9%) | 11.344 / 11.906 (-4.7%) |
| Broker fused planning reverse | 6.024 / 4.161 (+1.864s; +44.8%) | 3.852 / 4.204 (-0.351s; -8.4%) | 1.942 / 3.670 (-1.728s; -47.1%) | 11.818 / 12.034 (-1.8%) |
| Broker typed/versioned forward | 4.346 / 4.063 (+0.283s; +7.0%) | 4.260 / 3.753 (+0.508s; +13.5%) | 1.973 / 4.239 (-2.266s; -53.5%) | 10.579 / 12.054 (-12.2%) |
| Broker typed/versioned reverse | 5.756 / 4.326 (+1.430s; +33.0%) | 3.832 / 4.174 (-0.342s; -8.2%) | 1.967 / 3.667 (-1.700s; -46.4%) | 11.555 / 12.167 (-5.0%) |

The same broker candidates versus their same-trial current extension controls:

| Variant | B1 | B2 | B3 | Total |
| --- | --- | --- | --- | --- |
| Broker demand cache | -33.6% | +82.0% | -23.5% | +1.3% |
| Broker shared current | -28.5% | +111.0% | -2.7% | +18.4% |
| Broker fused test profile forward | -32.2% | +88.5% | -46.9% | -1.8% |
| Broker fused test profile reverse | +16.3% | +85.1% | -52.4% | +15.0% |
| Broker fused planning forward | -27.4% | -23.3% | -64.2% | -36.8% |
| Broker fused planning reverse | +16.3% | -33.4% | -65.5% | -28.7% |
| Broker typed/versioned forward | -36.9% | -29.1% | -61.0% | -41.1% |
| Broker typed/versioned reverse | +17.3% | -30.7% | -64.2% | -27.5% |

The fused test-profile B2 was slower in both orders. Under the equally applied GCI planning flags, B2/B3 improved in both orders. Plan evidence showed broadcast frequency lookup; this is not permission to credit an algorithm with a change caused solely by settings or extrapolate local[1] to cloud parallelism.

### B1 summary reuse and later rejected experiments

- JSON summary reuse (full broker+security commands, one forward pair): clean `da793f6` B1/B2/B3 **54.490 / 28.801 / 16.600s**, draft **64.695 / 28.057 / 21.193s**; **+18.7% / −2.6% / +27.7%**, total **99.891 → 113.946s (+14.1%)**. Not promoted.
- Typed raw-struct bootstrap removed JSON decode/distributed caching structurally and passed 28 correctness cases per runtime. The actual paired command result still regressed:

| Experiment | B1 candidate / control s (change) | B2 candidate / control s (change) | B3 candidate / control s (change) | Three-batch total s (change) |
| --- | --- | --- | --- | --- |
| Typed bootstrap full commands | 39.658 / 35.955 (+3.703s; +10.3%) | 39.126 / 17.241 (+21.885s; +126.9%) | 11.578 / 12.450 (-0.872s; -7.0%) | 90.362 / 65.647 (+37.6%) |

  In its separate diagnostic, broker collect tasks fell 38→7 and security 40→9, yet original DISTINCT shuffle bytes stayed the same. Whole-fork peak RSS was 2478→2542MiB and security CREATE peak 1870→2419MiB. These are sampled process measurements, not isolated retained seed size. The encoded 64MiB guard does not strictly bound boxed/raw driver heap. The unchanged refresh implementation means its +126.9% B2 observation requires investigation, not a claim that bootstrap logically adds that much B2 work. It was not promoted.
- Later conditional-frequency / bounded-gather / local-plan-reuse experiments also failed to establish a robust end-to-end win and were excluded from the prepared PR. Example: Spark3.5 Fabric-like component B1/B2/B3 **0.987 / 1.465 / 0.674 → 0.985 / 1.155 / 0.637s** (−0.2% / −21.2% / −5.5%; total −10.8%), while Spark4.1 Fabric-like **0.934 / 1.141 / 0.669 → 0.985 / 1.174 / 0.743s** (+5.5% / +2.8% / +11.1%; total +7.9%). Four later Spark4.1 B3 pairs were all slower with one extra job. Passing correctness/CI and a faster component did not qualify the changes as a whole-pipeline speedup. Related archives: `2026-10-21/frequency-counts-39437651878`, `2026-10-22/wf-pr62-preparation/state.json`, SF100 experiments `39565645761` / `39738177441`. This note does not invent exact final SF100 timing where it has not reconstructed the completed run.

### Changes relative to the preceding local POC

These separate-trial comparisons are useful history, not pristine commit A/B or causal proof. Their same-trial full controls often moved too.

| Candidate vs preceding POC | B1 change | B2 change | B3 change | Total change |
| --- | --- | --- | --- | --- |
| Batched member reads 76k vs DenseOnly real Delta 76k | +2.5% | +44.3% | +8.8% | +6.6% |
| Shared bootstrap 76k Spark3.5 vs Batched member reads 76k | -18.4% | -20.7% | -10.6% | -17.9% |
| Typed local frame 10k vs Multi-order bridge 10k | +2.5% | -4.5% | -2.4% | +0.2% |
| Direct snapshot 10k vs Typed local frame 10k | -58.8% | -51.2% | -47.0% | -55.4% |
| Affected-key apply 10k vs Direct snapshot 10k | +13.2% | -5.6% | -2.2% | +6.3% |
| Native lifecycle bridge vs Native lifecycle first | +13.1% | -20.2% | -25.2% | -9.6% |
| Native lifecycle replacement vs Native lifecycle bridge | -9.3% | -13.1% | -7.0% | -10.1% |

For direct snapshot the raw prototype total fell 55.4%, but the unchanged full-Spark control also fell 47.7%; it remained 23.9% slower than its paired control. Do not present that cross-run drop as a 55% production win.

## Could slower B1 be worthwhile after 100 refreshes?

Yes, **if the incremental saving persists**. For a candidate `C` and comparator `F`, with one initialization and `N` identical refreshes of type j:

```
T_candidate(N) = C1 + N * Cj
T_control(N)   = F1 + N * Fj
initial premium = C1 - F1
saving per refresh = Fj - Cj
```

With a positive initial premium and positive saving, the first strictly cheaper integer refresh count is `floor(premium / saving) + 1`. If the refresh itself is slower, extra refreshes do not repay that positive premium. A single B2 and B3 do not establish steady state; later refreshes can enlarge state, introduce new peers, change global context, miss the frequency cache or trigger full rank shifts.

The table below is **arithmetic projection only**, one B1 plus **100 subsequent refreshes**, half at the measured B2 cost and half at the measured B3 cost. It is not a compounded-refresh benchmark, and 100 total batches would instead mean 99 refreshes. Comparator remains that row's paired plain Spark write. The full-query grouped version and rank-only lifecycle are different candidate families.

| Experiment | Break-even if all B2-like | Break-even if all B3-like | Projected candidate / control s, B1 + 100 refreshes | Difference |
| --- | --- | --- | --- | --- |
| DenseOnly real Delta 76k | 3 refreshes | 4 refreshes | 31.71 / 85.85 | -54.14s (-63.1%) |
| Batched member reads 76k | 3 refreshes | 5 refreshes | 38.99 / 85.50 | -46.51s (-54.4%) |
| Shared bootstrap 76k Spark3.5 | 2 refreshes | 4 refreshes | 32.74 / 87.54 | -54.80s (-62.6%) |
| Shared bootstrap 76k Spark4.1 | 5 refreshes | 10 refreshes | 52.03 / 69.40 | -17.37s (-25.0%) |
| Multi-order bridge 10k | No repayment | No repayment | 396.04 / 293.29 | +102.75s (+35.0%) |
| Typed local frame 10k | No repayment | No repayment | 382.94 / 316.31 | +66.63s (+21.1%) |
| Direct snapshot 10k | No repayment | No repayment | 193.39 / 173.61 | +19.77s (+11.4%) |
| Affected-key apply 10k | 7 refreshes | 10 refreshes | 186.79 / 199.89 | -13.10s (-6.6%) |
| Broker typed/versioned forward | No repayment | 1 refresh | 316.01 / 403.63 | -87.62s (-21.7%) |
| Broker typed/versioned reverse | 5 refreshes | 1 refresh | 295.71 / 396.37 | -100.67s (-25.4%) |

Interpretation: the 76k favorable DenseOnly/shared-bootstrap updates are promising for frequently refreshed pipelines, while the 10k affected-key bridge's smaller gains need more refreshes to pay for bootstrap. Multi-order/typed-local/direct-snapshot versions have slower maintenance in these paired means and do not amortize through repetition. A meaningful next experiment would use the existing compounded-refresh harness with mixed insert/update/delete and new-peer/context changes, record every refresh and state-growth trajectory, and compare total command time over the whole sequence. Do not start that extra benchmark solely from this document.

## Production SF100 context — what is actually in PR62

Latest verified full SF100 is [40667200914](https://github.com/mdrakiburrahman/ivm-bench/actions/runs/40667200914), Spark source `5c37b45fab85f7b43f8f8e4a5e3f17651c96b679`, bench `594bd5d7f0cdb45d583235a89d009036690f7835`, native `e773415d2b8b8551e9f052a84abed1ce7ddbe480`, LPTS `29c606ee18ad8e4d04cab940f361cbf6fbe5b881`. SF100 inserts 100/1/0.01, updates/deletes zero, one repetition, four dbt threads, serial engines, WF/grouped/classification-cache ON, async backups ON, validation/profiling/query logs ON. All 49 models × three batches × both engines passed the harness's `count_hash_rounded` validation; this is not the exact EXCEPT ALL oracle used in local tests.

| Engine / metric | B1 s | B2 s | B3 s | Total s |
| --- | --- | --- | --- | --- |
| Spark OpenIVM dbt invocation | 900.91 | 245.93 | 215.34 | 1362.18 |
| Fabric OpenIVM dbt invocation | 1251.90 | 557.65 | 439.89 | 2249.44 |
| Spark OpenIVM engine batch | 961.836 | 254.755 | 223.023 | 1439.614 |
| Fabric OpenIVM engine batch | 1297.445 | 560.249 | 441.353 | 2299.047 |

This is the **whole workload with the production grouped cache and running-window work**, not an ordered-rank-index result. Compared with preceding verified async run `40569379743` (`bd2` source, bench `43904f7`), dbt changes were:

| Engine | B1 change | B2 change | B3 change | Total change |
| --- | --- | --- | --- | --- |
| Spark | -1.04s (-0.1%) | -23.41s (-8.7%) | +15.96s (+8.0%) | -8.49s (-0.6%) |
| Fabric | +33.04s (+2.7%) | +5.80s (+1.1%) | +2.46s (+0.6%) | +41.30s (+1.9%) |

Matching same-pin/resource non-IVM complete-command comparison: **unavailable**. Historical paper dbt baselines are retained but are not a contemporaneous command-level ablation. Current production grouped preparation is active, but broker preparation on Fabric can offset the faster post-summary window SQL. For the archived WF-only analysis, see `2026-10-28/wf-model-analysis/wf-comparison.{json,csv}`. Old `query_span` excludes outer backup/finalization; it cannot be presented as complete command latency. Async command spans also end before background upload completion. Keep dbt, batch and full experiment wall as regression checks.

## Reproduction, preservation and resumption

The canonical preserved input archives live at `/Users/ila/.cache/openivm-performance/2026-10-15/` and `.../2026-10-16/`; later rejected experiments live under Oct21/22. Original `/private/tmp` paths in command records may no longer exist. The current main workspace is not the old experimental checkout. Do not recover code by resetting it, overwrite its work, combine multiple duplicate POC objects, or enable the uncommitted POC in production.

The raw evidence bundle is retained locally, separate from this checked-in report and the production PR. Its absolute paths identify recovery locations on the original workstation; they are not links to repository files. The bundle is at:

```
/Users/ila/.cache/openivm-performance/2026-10-28/rocksdb-handoff/
  build_report.py          # reconstructs this report and arithmetic from archived result JSON
  results.json             # full-precision normalized trials, comparisons, evidence/archives SHA256
  batch-comparisons.csv    # one row per trial/comparator/batch and total
  evidence/               # selected original summaries, code, patch and exact command records
```

Use Python `tarfile` to read selected members. Check archive/member hashes from `results.json` and original `*.manifest.json` before restoring into a new isolated worktree. A typical historical test added `RocksOrderedRankPrototype.scala`, `SharedRankMappingPrototype.scala`, and `SharedRankMappingCorrectness.scala` as unmanaged `ivmIt / Test` sources and ran `org.openivm.spark.parity.SharedRankMappingCorrectnessCdfSpec`; exact archived command records specify each source/native dependency. Do not compile different variants' identical object definitions together.

Those archived commands used the historical macOS local setup. **Current repository policy requires the pinned Linux devcontainer / Nx workflow**; adapt the test plumbing there, preserve runtime versions in the manifest, and do not copy obsolete host Java paths into current instructions. Normal current verification is `npx --no-install nx run spark-ext:verify-all` inside that environment, for each supported runtime target. This report's reconstruction itself uses Python only and does not execute Spark, RocksDB or native tests.

Before any future promotion: prove compiler-owned pre-window signed deltas (not raw joined-source CDF), all orderings and hidden keys, exact floating/global-context semantics, full identity/admission and source/target pinning, replay across partial publication, bounded memory, same-MV single atomic state checkpoint and one Delta target commit, original command fallback and cascades. Preserve the existing WAL/fsync/undo/manifest and concurrency policy; no rank optimization justifies relaxing them. Test failure boundaries without claiming a power-loss guarantee. Prove a complete-command win against a paired preceding source pin and paired plain engine, then do SF100; do not infer that from a millisecond kernel.

## Current rerun scope

Requested follow-up: source `306b8b0e02e8cd31903a8fd024ddb14e3ecd025c`, same native/LPTS, SF100 Spark OpenIVM + Fabric OpenIVM, validation/profile ON, one repetition, same 100/1/0.01 inserts and four dbt threads. This tests bounded-local JSON decoding / grouped simplification, **not the paused ordered-rank POC**. Both Spark source ARG pins are updated in bench PR88 at `dd73941322ab95515e33d5995487fc0f196630e7`. Its `.github/gci.json` selector started [SF100 run 40716787231](https://github.com/mdrakiburrahman/ivm-bench/actions/runs/40716787231) automatically after both source runtime CIs passed. No duplicate manual dispatch was made. The run was in progress when this document was written. The preceding comparison is verified run `40667200914`; a matching new non-IVM complete-command control remains unavailable. No timing is claimed for the new source until artifacts arrive.
