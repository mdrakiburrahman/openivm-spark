# openivm CI — local Docker replica

This directory contains the **complete, self-contained tooling to run the
upstream openivm GitHub CI pipeline locally via Docker**, against any branch
of the openivm fork (default: the SHA pinned in
[`spark-ext/dev/pins.env`](../../../spark-ext/dev/pins.env)).

It mirrors the four CI legs from
[`duckdb/extension-ci-tools` @ `v1.5.2`](https://github.com/duckdb/extension-ci-tools/tree/v1.5.2)
that openivm itself uses:

| Leg            | Base image                               | What it runs                        |
| -------------- | ---------------------------------------- | ----------------------------------- |
| `format-check` | `ubuntu:22.04` + clang-format-11 + black | `make format-check`                 |
| `tidy-check`   | `ubuntu:24.04` + clang-tidy              | `make tidy-check`                   |
| `build`        | `quay.io/pypa/manylinux_2_28_x86_64`     | `make` (vcpkg + ninja, linux_amd64) |
| `test`         | same as build                            | `./build/release/test/unittest`     |

> macOS, Windows, and WASM matrix legs from
> `_extension_distribution.yml` are **out of scope**. linux_amd64 +
> code-quality only.

## Why this exists

The openivm GitHub Actions pipeline is the source of truth for shipping the
DuckDB extension that openivm-spark depends on. We want a way to:

1. Validate proposed openivm changes **before** opening a PR upstream, with no
   PR-trigger ceremony and no waiting for cloud runners.
2. Iterate on a single sqllogictest file in seconds instead of waiting 30+
   minutes for a re-run.
3. Bisect across openivm SHAs locally to attribute parity regressions
   surfaced by `openivm-spark/ivm-it` to a specific upstream commit.

## openivm source code is never committed to this repo

The openivm checkout lives at **`.temp/openivm/`** at this repo's root.
`.temp` is gitignored (see [`.gitignore`](../../../.gitignore) line 7), so
nothing under it can ever be staged or committed by accident. If you want to
place the checkout somewhere else, pass `--openivm-dir <path>` to
`run-all.sh` or export `OPENIVM_DIR=<path>` — but **do not** clone openivm
under `tools/`, the working tree, or any other tracked path.

## One-time setup

You need:

- Docker engine with internet access for the first `docker build` (pulls
  `quay.io/pypa/manylinux_2_28_x86_64`, ~3 GB, plus two Ubuntu base images).
- Git on the host (drives submodule init inside `.temp/openivm`).
- ~10 GB free disk for the build container layer, ccache volume, and
  `.temp/openivm/build/release/`.

Then clone openivm at the pinned SHA:

```bash
./tools/openivm/ci/setup-openivm.sh
```

This reads `OPENIVM_REPO` / `OPENIVM_BRANCH` / `OPENIVM_COMMIT` from
`spark-ext/dev/pins.env`, clones (or fast-forwards) the repo into
`.temp/openivm/`, and detaches HEAD at the pinned SHA. Re-running is a no-op
when already at the SHA.

You can also point it at a different fork or arbitrary SHA:

```bash
./tools/openivm/ci/setup-openivm.sh --repo https://github.com/ila/openivm.git --sha main
./tools/openivm/ci/setup-openivm.sh --sha 76173ed
```

## Running the CI

```bash
./tools/openivm/ci/run-all.sh                          # all 4 legs
./tools/openivm/ci/run-all.sh --only format
./tools/openivm/ci/run-all.sh --only tidy
./tools/openivm/ci/run-all.sh --only build
./tools/openivm/ci/run-all.sh --only test

# Re-run a single sqllogictest file without rebuilding:
./tools/openivm/ci/run-all.sh --only test --unittest 'test/sql/cascade_window_partition_delta.test'

# Cold-start (nuke build/release before build):
./tools/openivm/ci/run-all.sh --only build --clean

# Test the PR merge result against upstream main:
./tools/openivm/ci/run-all.sh --only build --merge-base main

# Use a custom openivm checkout location:
./tools/openivm/ci/run-all.sh --openivm-dir /path/to/openivm
OPENIVM_DIR=/path/to/openivm ./tools/openivm/ci/run-all.sh
```

Logs land under `.logs/openivm-ci/<UTC-timestamp>/` (gitignored). The
symlink `.logs/openivm-ci/latest` always points at the most recent run.

A typical summary at the end of a green run:

```
=== Summary (UTC 20260528T021500Z) ===
JOB            STATUS TIME     LOG
format-check   PASS   2s       .logs/openivm-ci/20260528T021500Z/format-check.log
tidy-check     PASS   1875s    .logs/openivm-ci/20260528T021500Z/tidy-check.log
build          PASS   177s     .logs/openivm-ci/20260528T021500Z/build.log
test           PASS   243s     .logs/openivm-ci/20260528T021500Z/test.log
```

## Wall-clock expectations

From a recent green run on a 16-core / 32-GB host with warm Docker layers
and a warm vcpkg binary cache:

| Leg            | Time        | Notes                               |
| -------------- | ----------- | ----------------------------------- |
| `format-check` | ~2 s        | clang-format + black + cmake-format |
| `tidy-check`   | ~31 min     | clang-tidy — dominates the total    |
| `build`        | ~3 min      | vcpkg cache makes this fast         |
| `test`         | ~4 min      | 51 cases / 7 200+ assertions        |
| **all**        | **~38 min** | tidy is ~80% of total wall-clock    |

Cold start (no Docker image cache, no vcpkg binary cache, full rebuild)
adds roughly 15–20 min on top.

If you just want a fast smoke without tidy:

```bash
./tools/openivm/ci/run-all.sh --only format && \
./tools/openivm/ci/run-all.sh --only build  && \
./tools/openivm/ci/run-all.sh --only test
```

That covers everything except clang-tidy style nits and runs in ~7 min.

## What gets pinned for CI parity

| Pin                      | Value                                         |
| ------------------------ | --------------------------------------------- |
| duckdb submodule         | `v1.5.2` (overrides openivm superproject SHA) |
| extension-ci-tools       | `v1.5.2` (overrides openivm superproject SHA) |
| vcpkg commit             | `84bab45d415d22042bd0b9081aea57f362da3f35`    |
| vcpkg target triplet     | `x64-linux-release`                           |
| vcpkg binary cache       | `https://vcpkg-cache.duckdb.org` (read-only)  |
| tidy threads             | `4`                                           |
| `LINUX_CI_IN_DOCKER`     | `1`                                           |
| `ENABLE_EXTENSION_AUTO*` | `1` (loading + install)                       |

These are hard-coded near the top of `run-all.sh`. Bump them when upstream
extension-ci-tools cuts a new release.

## Layout

```
tools/openivm/ci/
├── docker/
│   ├── Dockerfile.build    # manylinux_2_28_x86_64 + vcpkg + ninja + ccache
│   ├── Dockerfile.format   # ubuntu:22.04 + clang-format-11 + black + cmake-format
│   └── Dockerfile.tidy     # ubuntu:24.04 + clang-tidy
├── setup-openivm.sh        # clone .temp/openivm at the SHA pinned in pins.env
├── run-all.sh              # orchestrator
└── README.md               # this file

(generated at runtime, gitignored)
.temp/openivm/                       # openivm checkout (NEVER committed)
.logs/openivm-ci/<timestamp>/        # per-leg .log files
.logs/openivm-ci/latest -> <ts>      # symlink to most recent run
```

## Common workflows

### Validate an in-flight openivm patch before pushing it

```bash
# Hack on .temp/openivm. Then:
./tools/openivm/ci/run-all.sh --only format
./tools/openivm/ci/run-all.sh --only build
./tools/openivm/ci/run-all.sh --only test --unittest 'test/sql/<your_file>.test'

# Once happy, run the full suite before pushing:
./tools/openivm/ci/run-all.sh
```

### Bump the pinned openivm SHA after merging a fork PR

```bash
# Edit spark-ext/dev/pins.env: bump OPENIVM_COMMIT to the new SHA.
./tools/openivm/ci/setup-openivm.sh        # picks up the new SHA
./tools/openivm/ci/run-all.sh              # full green-light
```

### Bisect a parity regression seen in ivm-it

```bash
cd .temp/openivm
git bisect start
git bisect bad  <new SHA>
git bisect good <old SHA>
git bisect run /home/mdrrahman/openivm-spark/tools/openivm/ci/run-all.sh \
    --only test --unittest 'test/sql/<failing>.test'
```

## Troubleshooting

- **`openivm checkout not found at .temp/openivm`** — run
  `./tools/openivm/ci/setup-openivm.sh` first, or pass `--openivm-dir`.
- **Build leg fails with `undefined reference to RemapFunctionNameForDialect`**
  — the lpts pin was bumped but `CMakeLists.txt` is missing
  `${LPTS_DIR}/src/dialect_function_map.cpp`. Add it next to the other
  `${LPTS_DIR}` entries in `EXTENSION_SOURCES`.
- **Permission errors on `.temp/openivm/build`** — the build leg writes as
  root inside Docker; `run-all.sh` chowns back on exit. If you killed it
  mid-run with `Ctrl+C` and the trap didn't fire, re-run with `--clean` or
  manually: `sudo chown -R "$(id -u):$(id -g)" .temp/openivm/build`.
- **`lpts/ducklake header missing`** warning in bootstrap — `lpts` was
  initialised without recursive submodules. Re-run after:
  `cd .temp/openivm/third_party/lpts && git submodule update --init --recursive`.
  `run-all.sh` already does this for you on every invocation.
