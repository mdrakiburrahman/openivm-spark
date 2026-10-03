# Devcontainer operations

Run the headless commands from the repository root after `npm ci`.

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
