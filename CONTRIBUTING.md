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

1. Get a fresh new WSL machine up:

   > ⚠️ Warning: this removes Docker Desktop if you have it installed

   ```powershell
   $GIT_ROOT = git rev-parse --show-toplevel
   & "$GIT_ROOT\contrib\bootstrap-dev-env.ps1"
   ```

1. Clone the repo:

   > ⚠️ Important: We use WSL in `~/` because Linux > Windows drive commits via `/mnt/c` is extremely slow for Spark I/O.
   > You can technically run the Devcontainer using Windows Docker Desktop, but the I/O experience is slow and poor.

   ```bash
   cd ~/

   read -p "Enter your name (e.g. 'FirstName LastName'): " user_name
   read -p "Enter your GitHub email (e.g. 'your-email@blah.com'): " user_email

   git clone https://github.com/mdrakiburrahman/openivm-spark.git

   git config --global user.name "$user_name"
   git config --global user.email "$user_email"
   cd openivm-spark/
   git pull origin
   ```

1. Run the bootstrapper script, that installs the minimal host tooling idempotently:

   ```bash
   GIT_ROOT=$(git rev-parse --show-toplevel)
   chmod +x ${GIT_ROOT}/contrib/bootstrap-dev-env.sh && ${GIT_ROOT}/contrib/bootstrap-dev-env.sh
   ```

   The bootstrap idempotently installs or validates Git, Docker Engine,
   Buildx, Compose, and the pinned Node/npm version, then runs `npm ci`. Java,
   Scala, sbt, and Spark stay inside the devcontainer.

1. Launch the devcontainer from the repository root:

   ```bash
   GIT_ROOT=$(git rev-parse --show-toplevel)
   code "${GIT_ROOT}/spark.code-workspace"
   ```

   Accept **Reopen in Container** when prompted. The workspace exposes only
   `spark-ext` so Metals imports the sbt build without unrelated repository
   folders. If the prompt does not appear, run **Dev Containers: Reopen in
   Container** from the VS Code command palette.

1. All builds and tests should now run green inside the devcontainer:

   ```bash
   npx --no-install nx run spark-ext:verify-all
   ```

   See [Headless devcontainer operations](docs/devcontainer/headless-operations.md)
   to run the same Nx workflow from WSL without opening VS Code.

1. Install recommended developer tooling (optional):

   ```bash
   curl -fsSL https://gh.io/copilot-install | bash
   $HOME/.local/bin/copilot --yolo
   ```
