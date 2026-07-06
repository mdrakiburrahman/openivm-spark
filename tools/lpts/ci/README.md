# lpts CI — local Docker replica

This directory contains the **complete, self-contained tooling to run the
upstream lpts GitHub CI pipeline locally via Docker**, against any branch of
the lpts fork (default: the SHA pinned in
[`spark-ext/dev/pins.env`](../../../spark-ext/dev/pins.env)).

It mirrors the reusable workflows lpts calls from
[`duckdb/extension-ci-tools` @ `v1.5-variegata`](https://github.com/duckdb/extension-ci-tools/tree/v1.5-variegata)
(`_extension_code_quality.yml` + `_extension_distribution.yml`):

| Leg            | Base image                               | What it runs                        |
| -------------- | ---------------------------------------- | ----------------------------------- |
| `format-check` | `ubuntu:22.04` + clang-format-11 + black | `make format-check`                 |
| `tidy-check`   | `ubuntu:24.04` + clang-tidy              | `make tidy-check`                   |
| `build`        | `quay.io/pypa/manylinux_2_28_x86_64`     | `make` (vcpkg + ninja, linux_amd64) |
| `test`         | same as build                            | `./build/release/test/unittest`     |

> macOS, Windows, and WASM matrix legs from `_extension_distribution.yml` are
> **out of scope**. linux_amd64 + code-quality only.

This is a sibling of [`tools/openivm/ci`](../../openivm/ci); the two share the
same Docker tooling (clang-format-11, clang-tidy, vcpkg
`84bab45d…`). The only differences are the pinned DuckDB /
extension-ci-tools versions (lpts tracks **`v1.5.3` / `v1.5-variegata`**,
openivm tracks `v1.5.2`), the extension name, and the submodule set.

## Why this exists

The lpts GitHub Actions pipeline gates the SQL-emitting library that openivm
(and therefore openivm-spark) is built on. We want to:

1. Validate proposed lpts changes **before** opening/updating a fork PR, with
   no PR-trigger ceremony and no waiting for cloud runners.
2. Resolve merge conflicts against `cwida/lpts:main` and prove the merged
   tree still passes format + tidy + build + test locally.
3. Bisect lpts SHAs locally to attribute parity regressions surfaced by
   openivm-spark to a specific upstream commit.

## lpts source code is never committed to this repo

The lpts checkout lives at **`.temp/lpts/`** at this repo's root. `.temp` is
gitignored, so nothing under it can ever be staged or committed by accident.
Pass `--lpts-dir <path>` to `run-all.sh` or export `LPTS_DIR=<path>` to use a
different checkout — but **do not** clone lpts under `tools/` or any tracked
path.

## One-time setup

You need:

- Docker engine with internet access for the first `docker build`.
- Git on the host (drives submodule init inside `.temp/lpts`).
- ~10 GB free disk for the build container layer, ccache volume, and
  `.temp/lpts/build/release/`.

Then clone lpts at the pinned SHA:

```bash
./tools/lpts/ci/setup-lpts.sh
```

This reads `LPTS_REPO` / `LPTS_BRANCH` / `LPTS_COMMIT` from
`spark-ext/dev/pins.env`, clones (or fast-forwards) the repo into
`.temp/lpts/`, and detaches HEAD at the pinned SHA. Re-running is a no-op
when already at the SHA.

You can also point it at a different fork or arbitrary SHA:

```bash
./tools/lpts/ci/setup-lpts.sh --repo https://github.com/cwida/lpts.git --sha main
./tools/lpts/ci/setup-lpts.sh --sha 43d3d25
```

## Running the CI

```bash
./tools/lpts/ci/run-all.sh                          # all 4 legs
./tools/lpts/ci/run-all.sh --only format
./tools/lpts/ci/run-all.sh --only tidy
./tools/lpts/ci/run-all.sh --only build
./tools/lpts/ci/run-all.sh --only test

# Re-run a single sqllogictest file without rebuilding:
./tools/lpts/ci/run-all.sh --only test --unittest 'test/sql/<file>.test'

# Cold-start (nuke build/release before build):
./tools/lpts/ci/run-all.sh --only build --clean

# Test the PR merge result against the base branch (cwida/lpts:main):
./tools/lpts/ci/run-all.sh --merge-base main

# Use a custom lpts checkout location:
./tools/lpts/ci/run-all.sh --lpts-dir /path/to/lpts
LPTS_DIR=/path/to/lpts ./tools/lpts/ci/run-all.sh
```

Logs land under `.logs/lpts-ci/<UTC-timestamp>/` (gitignored). The symlink
`.logs/lpts-ci/latest` always points at the most recent run.

`--merge-base` fetches from the `cwida` remote by default (the base repo of
lpts PR #13). Override with `MERGE_BASE_REMOTE` / `MERGE_BASE_URL`.

## Wall-clock expectations

Comparable to the openivm replica on a warm host:

| Leg            | Time        | Notes                               |
| -------------- | ----------- | ----------------------------------- |
| `format-check` | ~2 s        | clang-format + black + cmake-format |
| `tidy-check`   | ~25 min     | clang-tidy — dominates the total    |
| `build`        | ~4 min      | vcpkg cache makes this fast         |
| `test`         | ~3 min      | sqllogictest suite                  |
| **all**        | **~35 min** | tidy is ~80% of total wall-clock    |

Cold start (no Docker image cache, no vcpkg binary cache) adds ~15–20 min.

Fast smoke without tidy:

```bash
./tools/lpts/ci/run-all.sh --only format && \
./tools/lpts/ci/run-all.sh --only build  && \
./tools/lpts/ci/run-all.sh --only test
```

## What gets pinned for CI parity

| Pin                      | Value                                      |
| ------------------------ | ------------------------------------------ |
| duckdb submodule         | `v1.5.3`                                   |
| extension-ci-tools       | `v1.5-variegata`                           |
| vcpkg commit             | `84bab45d415d22042bd0b9081aea57f362da3f35` |
| vcpkg target triplet     | `x64-linux-release`                        |
| vcpkg binary cache       | `https://vcpkg-cache.duckdb.org` (read)    |
| tidy threads             | `4`                                        |
| `LINUX_CI_IN_DOCKER`     | `1`                                        |
| `ENABLE_EXTENSION_AUTO*` | `1` (loading + install)                    |

These are hard-coded near the top of `run-all.sh`. Bump them when lpts bumps
its `MainDistributionPipeline.yml`.

## Layout

```
tools/lpts/ci/
├── docker/
│   ├── Dockerfile.build    # manylinux_2_28_x86_64 + vcpkg + ninja + ccache
│   ├── Dockerfile.format   # ubuntu:22.04 + clang-format-11 + black + cmake-format
│   └── Dockerfile.tidy     # ubuntu:24.04 + clang-tidy
├── setup-lpts.sh           # clone .temp/lpts at the SHA pinned in pins.env
├── run-all.sh              # orchestrator
└── README.md               # this file

(generated at runtime, gitignored)
.temp/lpts/                          # lpts checkout (NEVER committed)
.logs/lpts-ci/<timestamp>/           # per-leg .log files
.logs/lpts-ci/latest -> <ts>         # symlink to most recent run
```

## Troubleshooting

- **`lpts checkout not found at .temp/lpts`** — run
  `./tools/lpts/ci/setup-lpts.sh` first, or pass `--lpts-dir`.
- **`ducklake headers missing`** warning — `third_party/ducklake` was not
  initialised. `run-all.sh` inits it on every invocation; if it still fails,
  run `cd .temp/lpts && git submodule update --init third_party/ducklake`.
- **Stale `CMakeCache.txt` directory mismatch** on build — a prior build was
  run from a different path. Re-run with `--clean`.
- **Permission errors on `.temp/lpts/build`** — the build leg writes as root
  inside Docker; `run-all.sh` chowns back on exit. If killed mid-run, re-run
  with `--clean` or `sudo chown -R "$(id -u):$(id -g)" .temp/lpts/build`.
