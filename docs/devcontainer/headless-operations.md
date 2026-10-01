# Headless devcontainer operations

The repository can be built, tested, and published from a WSL shell without
opening VS Code. The host contract is intentionally small: Docker, Git, Bash,
and the standard Ubuntu/WSL utilities used by the bootstrap.

The published image currently targets `linux/amd64`, matching the supported
Windows/WSL development hosts and self-hosted CI runner.

## Bootstrap the host

From the repository root:

```bash
./contrib/bootstrap-dev-env.sh
```

The bootstrap validates Docker, installs the repository's pinned Node/npm
runtime, and runs `npm ci`. Java, Scala, sbt, Nx, the Dev Container CLI, native
build tools, and Spark dependencies belong to the devcontainer rather than the
WSL host.

## Build the image outside the devcontainer

```bash
npx --no-install nx run devcontainer:build
npx --no-install nx run devcontainer:test
```

The image tag is derived from the devcontainer inputs. Re-running the build
reuses the local image unless `--force` is forwarded to the target.

To publish the immutable tag, copy `.env.example` to the gitignored `.env` and
set:

```dotenv
GHCR_USERNAME=mdrakiburrahman
GHCR_TOKEN=<classic-personal-access-token-with-write-packages>
```

Then run:

```bash
npx --no-install nx run devcontainer:publish
```

The publisher fails before pushing when credentials are absent, never prints
the token, and skips an image that already exists remotely.

After the first GHCR push, the repository owner must set the package visibility
to **Public** in GitHub Packages and verify an unauthenticated pull.

## Change the image safely

The normal PR workflow never executes PR-controlled image automation on the
persistent Docker-capable runner. For a maintainer change to Dockerfile,
toolchain, pin, or npm-lock inputs:

1. Run the local `devcontainer:publish` target, or push the branch and manually
   dispatch the **devcontainer image** workflow for that trusted branch.
2. Confirm the immutable tag exists in GHCR.
3. Commit the updated consumer references.
4. Open or update the pull request.

This ordering ensures the devcontainer and CI never reference an unpublished
tag.

## Run a one-shot verification

This pattern always tears down the container and preserves the verification
exit status:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

started=0
cleanup() {
    status=$?
    trap - EXIT
    if [[ "${started}" == "1" ]]; then
        npx --no-install nx run devcontainer:down || true
    fi
    exit "${status}"
}
trap cleanup EXIT

npx --no-install nx run devcontainer:up
started=1
npx --no-install nx run devcontainer:exec -- \
    npx --no-install nx run spark-ext:verify-all
```

Use the same form for a single target:

```bash
npx --no-install nx run devcontainer:exec -- \
    npx --no-install nx run spark-ext:verify --configuration=spark-3.5
```

## Keep the container running

```bash
npx --no-install nx run devcontainer:up
npx --no-install nx run devcontainer:exec -- bash
```

Run repository commands from another WSL shell:

```bash
npx --no-install nx run devcontainer:exec -- \
    npx --no-install nx run spark-ext:lint

npx --no-install nx run devcontainer:exec -- \
    npx --no-install nx run spark-ext:test --configuration=spark-4.1
```

Remove the headless container when finished:

```bash
npx --no-install nx run devcontainer:down
```

## Inspect or recover a session

The lifecycle targets use the Dev Container CLI's workspace identity, so they
address only this repository's container rather than killing unrelated Docker
workloads.

```bash
npx --no-install nx run devcontainer:up
npx --no-install nx run devcontainer:exec -- \
    git status --short --branch
```

If a command is interrupted, running `devcontainer:down` is safe and
idempotent. Build caches and the published image remain available for the next
session.

## Security notes

- The devcontainer mounts the host Docker socket for image build and publish
  targets. Code in the container therefore has Docker-host privileges.
- Keep `GHCR_TOKEN` only in the ignored `.env` or the process environment.
- The normal development and CI image is pinned by immutable content hash.
  Do not replace it with a mutable `latest` tag.
- The public GHCR image can be pulled without credentials. Credentials are
  required only to publish a new image.
