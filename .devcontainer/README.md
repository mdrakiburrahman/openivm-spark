# Devcontainer operations

Run the headless commands from the repository root after `npm ci`.

This PR 62 control branch packages its runtime from the published toolchain image
at immutable digest `sha256:1fa530da86a95918417c3728239cf553727e260013cfac416c31cd94580fca87`.
The final Dockerfile target replaces the base image's native compiler assets with
those embedded in the verified assembly
`org.openivm:ivmextension-spark-3.5_2.12:jar:assembly:1793685422.230497165.0`.
It verifies assembly SHA256
`a49fe6c36626853a672f7a4d43250b7e798d01814b3b98b40c8e8885ec0286eb`
before extracting the assets and regenerating their native checksum manifest.
No engine assembly or native source compilation is involved in this image target.

To build or publish this branch's image, download that immutable assembly using
the approved feed credentials, place it in a dedicated directory as
`ivmextension.jar`, and set `OPENIVM_PR62_ASSEMBLY_DIR` to that directory's absolute
local path. The dedicated directory must contain only this non-secret artifact.
BuildKit mounts it from a named build context; the assembly is not retained in
the image. Use the existing `devcontainer:tag`, `devcontainer:build`,
`devcontainer:test`, and `devcontainer:publish` commands; do not relabel an
unrelated image under a computed content tag. Normal consumers only pull the
published image and do not need the assembly file or feed credentials.

Start or reuse the pinned devcontainer:

```bash
npx --no-install nx run devcontainer:up
```

Run a command in that exact container:

```bash
npx --no-install nx run devcontainer:exec -- <command...>
```

The `--` is required so Nx forwards every remaining argument to the command instead of parsing it as
an Nx option. For shell syntax such as pipes, redirects, or multiple commands, invoke a shell
explicitly:

```bash
npx --no-install nx run devcontainer:exec -- bash -lc 'set -euo pipefail; npx --no-install nx show projects'
```

Stop and remove the recorded container:

```bash
npx --no-install nx run devcontainer:down
```

`up` records the returned container ID in the ignored `.devcontainer/.container-id` file. `exec`
uses that ID and propagates the command's nonzero status. `down` removes only that container and is
safe to repeat.

Remove stale, labeled runtime-smoke containers without affecting unrelated containers:

```bash
npx --no-install nx run devcontainer:cleanup
```

The published image contract is **Linux amd64 only**. Image builds force `linux/amd64`, the
Dockerfile rejects any other `TARGETARCH`, and runtime smoke verifies the loaded image reports
`linux/amd64`. ARM hosts require amd64 container emulation and are otherwise unsupported.

The image includes the pinned GitHub CLI for pull-request, issue, and Actions operations from
headless agents. The `devcontainer:publish` target always runs `devcontainer:test` first, so the
exact immutable image is built and runtime-smoked before any registry push.
