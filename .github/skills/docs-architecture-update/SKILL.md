---
name: docs-architecture-update
description: "Fan parallel subagents across spark-ext/, .temp/openivm, and .temp/lpts to re-study all three layers and REWRITE docs/architecture/ (+ docs/README.md) AS IF AUTHORED FOR THE FIRST TIME — full clean prose, real file:line citations, no amendments or diff-style patches. Restructuring allowed; README regenerated to match. Auto-commit + push on non-main."
user-invocable: true
---

# Architecture Docs — First-Principles Rewrite

You are running this skill from `/home/mdrrahman/openivm-spark` (the
`openivm-spark` repo root).

The user wants **the architecture docs regenerated as if written for the first
time**. You are not patching. You are not appending changelogs. You are
authoring a fresh, demo-driven explanation of how the system works *today*, from
the source as it currently stands.

Your deliverable is a fully rewritten doc tree:

```
docs/README.md                        # index: 3 layer tables + nav
docs/architecture/openivm-spark/*.md  # Layer A — Scala/Spark integration (spark-ext/)
docs/architecture/openivm/*.md        # Layer B — DuckDB IVM engine (.temp/openivm)
docs/architecture/lpts/*.md           # Layer C — LogicalPlan→SQL emitter (.temp/lpts)
```

Cover all three layers + README **every run**. Subagents do the studying and
the writing; you orchestrate, verify, and commit.

---

## CRITICAL RULES (read before touching anything)

### CR-A. First-time authorship — no amendment language

Every chapter must read as if it is the first and only draft. **Banned**:
"updated", "amended", "previously", "now also", "as of this change",
"changelog", "newly added", diff markers, or any reference to a prior version of
the doc. Rewrite the whole file from scratch — do NOT merge edits into existing
prose. If a chapter exists, replace its entire body, don't tweak it.

### CR-B. Never fabricate a citation

Docs cite source as `path/file.ext:LINE` or `:LINE-LINE`. Every citation MUST
resolve in the tree it names (`spark-ext/**` for Layer A, `.temp/openivm/**` for
B, `.temp/lpts/**` for C). A claim without a verifiable anchor is a bug.
Phase 4 fails the run if any cite is dangling. Prefer fewer, correct cites over
many guessed ones.

### CR-C. Never weaken correctness/parity facts

This mirrors the repo's testing ethos. Do not soften the RefreshType table, the
intersection-of-dialects MV-body constraint, the depth-≤2 MV-over-MV limit, the
`EXCEPT ALL` correctness oracle, or any FULL_REFRESH-is-a-regression framing. If
source contradicts an old doc claim, the **source wins** — write the truth.

### CR-D. Restructuring is allowed; README must follow

Subagents MAY add, merge, split, or renumber chapters when that yields clearer
first-time docs. Keep `0.OVERVIEW.md` per layer. Preserve the ⭐ flagship picks
unless a better one exists. After P2, the final file set is the source of truth;
P3 regenerates `docs/README.md` to match it exactly — no stale links.

### CR-E. Keep the verified-by-human banner

`docs/README.md` opens with `# 📚 openivm-spark Architecture Documentation`
then the line:
`> This document was written by AI but verified and proof-read by a human - `@mdrakiburrahman``
Keep it. Keep the isolated-venv probe recipe + reproducibility section.

### CR-F. Parallelism is mandatory

This is a fan-out skill. Study and writing happen in **parallel subagents**, not
inline. Throw as many agents as the work needs (per-layer for study, per-chapter
for writing). Do not serially write 37 chapters yourself.

### CR-G. Static only

Read source, cite source. Do NOT run the benchmark, build, or live probes. The
isolated-venv recipe stays in README as documentation, but you don't execute it.

### CR-H. Auto-commit on non-main; skip on main

Check `git symbolic-ref --short HEAD`. If not `main`, commit + push docs changes
without prompting, prefix `[docs-architecture-update]`. If `main`, do NOT push —
write docs, report the gap, stop.

---

## PHASE 0 — Sync, gate, inventory

```bash
cd /home/mdrrahman/openivm-spark
./spark-ext/dev/dev.sh pins-sync 2>&1 | tee /tmp/docs-pins-sync.out
git symbolic-ref --short HEAD
```

- `pins-sync` aligns `.temp/openivm` + `.temp/lpts` to `spark-ext/dev/pins.env`
  so Layer B/C cites match the documented commits. On `⚠` drift, hard-reset the
  `.temp` checkouts to their pinned commits, re-run, then proceed.
- Record the four SHAs (openivm, lpts, ivm-bench, openivm-spark HEAD) for the
  README banner block.
- Inventory current chapters: `docs/architecture/{openivm-spark,openivm,lpts}/`.
  This is the *starting* set; P1 may revise it.

---

## PHASE 1 — Parallel study fan-out

Launch explore agents (≥1 per layer; chunk a layer into 2–3 agents if large).
Each returns a chapter plan + verified `file:line` anchors. Source trees:

- **Layer A** `spark-ext/` — `ivm-executor → ivm-common → ivm-compiler →
  ivm-extension → ivm-it`; entry `OpenIvmSparkExtensions`, grammar
  `IvmSqlBase.g4`, `IvmDmlInterceptorRule`, `StagingCatalog`, `MvCatalog`,
  `OpenIvmCompiler`, `LptsSparkDialect`, `SparkRefreshRewriter`,
  `MaterializedViewCommands`.
- **Layer B** `.temp/openivm/src/` — classifier, `rules/`, `upsert/`,
  `compile_facts.cpp`, system catalogs, RefreshType ordinals.
- **Layer C** `.temp/lpts/src/` — `lpts_ast_builder`, `lpts_ast_flattener`,
  renderers, `dialect_function_map`, `cte_nodes`.

Tell each agent: list final chapters (keep `0.OVERVIEW`, pick a ⭐ flagship),
and for each, 5–15 `path:line` anchors that exist HEAD. No prose yet.

---

## PHASE 2 — Parallel chapter rewrite

One writer agent per chapter, fed only its study notes + the style guide. Full
first-time rewrite, overwrite the whole file. **Style guide (verbatim to each
writer):**

- Present tense, short declarative sentences, demo-driven. No "updated/amended".
- `0.X` numbered sections; start with a scope section; end with reading-order.
- Cite real `file:line`; reuse ascii/diagram conventions from sibling chapters.
- Intersection-of-dialects MV body; depth-≤2 cascade; `EXCEPT ALL` oracle;
  FULL_REFRESH = regression. Source wins over old prose.

Cap concurrent writers reasonably (~8); cover every chapter in the P1 set.

---

## PHASE 3 — README regeneration

Rebuild `docs/README.md` from the FINAL file set: title + verified-by-human
banner, 3-layer ascii stack, "How to read this" nav, one table per layer (`#` /
linked chapter / "what it answers"), reproducibility venv recipe, banner SHAs.
No link may point at a missing/renamed file.

---

## PHASE 4 — Verify

- Every `path:line` cite resolves in its layer tree (grep each, fail on dangling).
- Every relative link in README + chapters resolves.
- No amendment language anywhere (`updated|amended|previously|changelog`).
- RefreshType count, depth-2, dialect, EXCEPT-ALL facts intact.

Fix or re-dispatch writers on failure.

---

## PHASE 5 — Commit + completion

Non-main: `git add docs && git commit -m "[docs-architecture-update] rewrite
architecture docs" && git push`. On `main`: skip, report.

```
{ "status": "Succeeded" }
```

only when all 3 layers + README are rewritten, citations verified, README links
resolve. Else `{ "status": "Failed" }` with the dangling-cite list.
