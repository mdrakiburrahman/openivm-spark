# Contributing

If you're a Windows user, we only use Windows to get into WSL - everything is
Linux from there.

Spark and Delta workloads are notoriously slow on Windows. Therefore, the
development environment we support is Linux, using a
[VS Code Dev Container](https://code.visualstudio.com/docs/devcontainers/containers).
This repo's CI runs the exact same prebuilt devcontainer and Nx targets.

Therefore, if the tests pass locally, they are highly likely to pass in CI as
well.

## How to use, on a Linux machine

1. Windows pre-reqs

   ```powershell
   winget install -e --id Microsoft.VisualStudioCode
   ```

1. Clone the repo on Windows, then check out and push the branch you want to work on:

   ```powershell
   git clone https://github.com/mdrakiburrahman/openivm-spark.git
   ```

1. From an **elevated PowerShell 7** prompt in the checkout, install [Task](https://taskfile.dev) and run
   [`contrib/bootstrap-dev-env.yaml`](contrib/bootstrap-dev-env.yaml):

   > ⚠️ This **deletes every WSL distribution** on the machine before recreating `Ubuntu-24.04`, and removes
   > Docker Desktop if you have it installed. Push any work inside WSL first.

   ```powershell
   winget install Task.Task
   $env:PATH = [Environment]::GetEnvironmentVariable('PATH', 'Machine') + ';' + [Environment]::GetEnvironmentVariable('PATH', 'User')
   task --taskfile contrib/bootstrap-dev-env.yaml
   ```

   Without any prompts, this recreates WSL on the drive with the most free space and clones your current
   branch into `~/openivm-spark` with this checkout's git identity; Linux > Windows drive commits via `/mnt/c`
   are extremely slow for Spark I/O. It then runs
   [`contrib/bootstrap-dev-env.sh`](contrib/bootstrap-dev-env.sh), which idempotently installs or validates
   Git, Docker Engine, Buildx, Compose, and the pinned Node/npm version, configures parallel Docker
   transfers, pulls the pinned devcontainer image, then runs `npm ci`. Java, Scala, sbt, and Spark stay
   inside the devcontainer, which it starts and opens in VS Code through `spark.code-workspace`. After a
   restart, run `task --taskfile contrib/bootstrap-dev-env.yaml up` instead; it keeps WSL and reopens the
   devcontainer.

2. All builds and tests should now run green inside the devcontainer:

   ```bash
   npx --no-install nx run spark-ext:verify-all
   ```

   See [Headless devcontainer operations](docs/devcontainer/headless-operations.md)
   to run the same Nx workflow from WSL without opening VS Code.

3. Install recommended developer tooling (optional):

   ```bash
   curl -fsSL https://gh.io/copilot-install | bash
   $HOME/.local/bin/copilot --yolo
   ```

4. Hydrate up `GH_TOKEN` for agentic git pushes:

   ```bash
   line="export GH_TOKEN='ghp_7m...T'"
   sed -i.bak '/^export GH_TOKEN=/d' ~/.bashrc
   printf '%s\n' "$line" >> ~/.bashrc

   source ~/.bashrc
   gh repo list
   ```