# Headless Devcontainer Operations

Run these commands from the WSL host. They start the repository's pinned
devcontainer and run commands inside it without opening VS Code.

## Prepare the host

Run the bootstrap once from the repository:

```bash
repo_root="$(git rev-parse --show-toplevel)"
"$repo_root/contrib/bootstrap-dev-env.sh"
```

## Run a command and keep the devcontainer

```bash
(
  set -euo pipefail
  repo_root="$(git rev-parse --show-toplevel)"
  cd "$repo_root"

  npx --no-install nx run devcontainer:up
  container_id="$(<.devcontainer/.container-id)"
  npx --no-install nx run devcontainer:exec -- \
    --container-id "$container_id" \
    npx --no-install nx run spark-ext:verify-all
)
```

`up` starts or reuses the devcontainer and records its exact ID. `exec` uses
that ID, streams the command output, and returns the command's exit status.
Replace the final `npx` command with any command that should run inside the
devcontainer; use `bash` for an interactive shell. The container remains
running.

## Run a command and remove the devcontainer

```bash
(
  set -euo pipefail
  repo_root="$(git rev-parse --show-toplevel)"
  cd "$repo_root"

  npx --no-install nx run devcontainer:up
  container_id="$(<.devcontainer/.container-id)"
  trap 'npx --no-install nx run devcontainer:down -- --container-id "$container_id"' EXIT
  npx --no-install nx run devcontainer:exec -- \
    --container-id "$container_id" \
    npx --no-install nx run spark-ext:verify-all
)
```

The `EXIT` trap removes only that container, including when the command fails.
Every argument after `devcontainer:exec --` is passed to the command inside the
devcontainer.
