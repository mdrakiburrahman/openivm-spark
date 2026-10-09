# Development

## Layout

```text
spark-ext/
├── build.sbt
├── project/{Dependencies.scala, Settings.scala, plugins.sbt, build.properties}
├── .sbtopts, .scalafmt.conf
├── docs/              # focused usage and development guides
├── ivm-executor/      # executor-side classes (DeltaStagingExec, MergeWriterExec)
├── ivm-common/        # library: catalogs, metadata, assemblers, FeatureGate
├── ivm-compiler/      # OpenIvmCompiler (DuckDB CLI) + LptsSparkDialect
├── ivm-extension/     # SparkSessionExtensions entry + ANTLR grammar + commands + rules
├── ivm-it/            # integration tests + 100-spec parity suite vs openivm
└── dev/
    ├── dev            # single entry-point CLI wrapper (build, test, verify, shell, …)
    ├── docker/        # multi-stage Dockerfile + docker-compose.yml
    ├── pins.env       # shared OpenIVM / LPTS / DuckDB pins
    └── targets/       # target-specific Spark / Delta / Scala / JDK pins
```

## Dev loop (prebuilt devcontainer + Nx)

The host starts with Docker, Git, and Bash. From the repository root, install
the pinned Node/npm launcher and locked Nx tooling:

```bash
./contrib/bootstrap-dev-env.sh
npx --no-install nx run devcontainer:up
```

Run the standard targets inside the devcontainer:

```bash
npx --no-install nx run devcontainer:exec -- \
  npx --no-install nx run spark-ext:lint

npx --no-install nx run devcontainer:exec -- \
  npx --no-install nx run spark-ext:build --configuration=spark-3.5

npx --no-install nx run devcontainer:exec -- \
  npx --no-install nx run spark-ext:test --configuration=spark-4.1

npx --no-install nx run devcontainer:exec -- \
  npx --no-install nx run spark-ext:verify-all
```

Inside an attached VS Code devcontainer, omit the outer `devcontainer:exec`
and run the same Nx targets directly. Nx delegates to
`spark-ext/dev/run.sh`; do not invoke that implementation directly.
`spark-ext/dev/dev.sh` remains only as a Compose-compatible legacy wrapper and
for commands without Nx targets, including `publish`, `publish-all`, and
`pins-fix`.

`publish` reads `MAVEN_URL` and `MAVEN_PAT` from the gitignored root `.env`,
computes one immutable version as
`<epoch>.<working-tree-content-hash-int>.0`, and uses native sbt publishing to
upload the assembly classifier. `publish` uses the selected target; `publish-all`
uses one version for both coordinates:

```text
org.openivm:ivmextension-spark-3.5_2.12:jar:assembly:<version>
ivmextension-spark-3.5_2.12-<version>-assembly.jar

org.openivm:ivmextension-spark-4.1_2.13:jar:assembly:<version>
ivmextension-spark-4.1_2.13-<version>-assembly.jar
```

The content hash covers every tracked file plus every untracked, non-ignored
file, so publishing completed but not-yet-committed feature work cannot reuse
the identity of an older source tree. Ignored credentials, build outputs,
`.temp/`, and `.research/` remain excluded.

The feed contract is intentionally assembly-only: the thin main jar is not
published. The generated POM retains only Spark/Delta/SLF4J dependencies marked
`provided`; internal OpenIVM modules and other compile dependencies are already
inside the fat jar. Consumers must request the `assembly` classifier rather
than the unclassified artifact. Local `ivmExtension/assembly` output retains the
legacy `ivmExtension-<version>-assembly.jar` filename used by existing image
builds; Maven artifact metadata publishes the same bytes under the lowercase,
Scala-suffixed filename shown above.
Copy `.env.example` to `.env` and populate the private-feed values before use.

Every SBT publication entrypoint (`publish`, `publishLocal`, and `publishM2`)
first builds the final shaded assembly and initializes
`org.openivm.spark.parser.gen.IvmSqlBaseLexer` in an isolated classloader whose
only application artifact is that JAR. This forces serialized-ATN
deserialization before any upload or local publication. Target-dependent ANTLR
outputs and task caches live under runtime-and-ANTLR-version-specific target
directories, so switching runtimes cannot reuse generated sources.
The runner removes the obsolete `target/scala-2.12` and `target/scala-2.13`
extension directories before starting SBT so stale pre-isolation assemblies
cannot be selected by downstream JAR discovery.

For an explicitly requested pre-test publication, export the private-feed
values from the gitignored root `.env`, compute one immutable version using the
content-hash convention above, and invoke the guarded SBT publication through
Nx:

```bash
PACKAGE_VERSION="$VERSION" npx --no-install nx run spark-ext:assembly --configuration=spark-3.5 -- \
  'ivmExtension/publish'
PACKAGE_VERSION="$VERSION" npx --no-install nx run spark-ext:assembly --configuration=spark-4.1 -- \
  'ivmExtension/publish'
```

Run these sequentially because the runtimes share the SBT meta-build tree.
The packaged-assembly guard still runs even when ordinary tests are skipped.
Verify both feed coordinates and report the shared version as **unverified**
before executing tests. After publication run the normal validation; if source
changes, publish a new version rather than overwriting the old one. This opt-in
administrative sequence does not replace the canonical publisher.

`spark-ext:verify` is the canonical one-target command. It first runs
`pins-sync` (cloning any missing `.temp/{openivm,lpts,ivm-bench}` checkouts,
fetching origin, and aligning each to its pinned branch, plus shallow-cloning
the read-only `.temp/{spark,delta}` upstream references at their pinned release
tags), then lints, compiles, assembles the fat jar, runs the packaged-assembly
guard plus its incompatible-ATN regression, and runs every unit, integration,
and parity suite in one sbt JVM. `spark-ext:verify-all` runs that pipeline for
both targets and fails if they discover different tests.

`pins-sync` exits non-zero only when a pinned repo or branch is missing on
GitHub (or `.temp/` is corrupt). Drift between the local HEAD and the pinned
COMMIT — or between the `ivm-bench` Dockerfile's `ARG OPENIVM_/LPTS_*`
defaults and `pins.env` — is reported as a `⚠ WARNING` but does not block
`verify`. Bumping any pinned SHA therefore requires editing **both**
`spark-ext/dev/pins.env` **and** the matching `ARG` in
`.temp/ivm-bench/src/containers/spark-openivm-build/Dockerfile`.

`pins-fix` automates that bump end-to-end. Given any combination of
uncommitted changes across `openivm-spark` and `.temp/{openivm,lpts,
ivm-bench}`, it commits each working tree, pushes to `origin/<branch>`
(refusing `main` / `master` / detached HEAD in any of the four repos, and
aborting on rebase conflicts), then deterministically rewrites
`spark-ext/dev/pins.env` and the `ivm-bench` Dockerfile `ARG` defaults so the
next `pins-sync` reports `✓` green. The ordering is designed around the
chicken-and-egg where bumping `IVM_BENCH_COMMIT` advances `openivm-spark`
origin past whatever was just baked into Dockerfile `OPENIVM_SPARK_COMMIT`:
the final pin lags by exactly one commit whose only diff is `pins.env`,
which `pins-sync` accepts as a "benign lag". The command is idempotent —
running it on an already-aligned tree is a no-op.

### Environment variables

| Variable                    | Default            | Scope   | Effect                                                                                                     |
| --------------------------- | ------------------ | ------- | ---------------------------------------------------------------------------------------------------------- |
| `OPENIVM_TEST_FORKS`        | build default      | tests   | Positive fork cap forwarded as `openivm.test.forks`.                                                       |
| `OPENIVM_TEST_LOG_ROOT`     | repository `.logs` | tests   | Parent for `.logs/test-<timestamp>/` and the per-fork debug logs.                                          |
| `PRE_CLEAN`                 | `0`                | legacy  | `dev.sh` only: when `1`, removes running Docker containers before the retained Compose-compatible command. |
| `openivm.test.forks` (`-D`) | `32`               | sbt JVM | Direct system-property override, for example `spark-ext:verify -- -Dopenivm.test.forks=8`.                 |

The normal development and CI image is
`ghcr.io/mdrakiburrahman/openivm-spark-devcontainer:<content-hash>`. The tag
covers the devcontainer definition, npm lockfile, SBT build definitions, native
pins, and target pins. `DUCKDB_REF` and `DUCKDB_COMMIT` in `pins.env`
explicitly select DuckDB v1.5.2 for both the CLI and native extension; do not
infer the deployed ABI from OpenIVM's upstream submodule or CI version. JDBC
stays on 1.5.2.1. `NATIVE_BUILD_JOBS` bounds native build parallelism (default
8); lower it on shared builders.
