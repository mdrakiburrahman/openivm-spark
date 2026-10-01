# Contributing

Development, headless automation, and CI use the same prebuilt devcontainer and
the same Nx targets. The WSL host does not need Java, Scala, sbt, Maven, Metals,
Azure CLI, Terraform, or GitHub CLI.

## Windows and WSL prerequisites

Install Visual Studio Code, WSL with Ubuntu 24.04, Git, and Docker. Docker must
be reachable from inside WSL. Keep the repository under the Linux filesystem
(`~/`), not `/mnt/c`, because Spark and Delta workloads are substantially
slower on Windows-mounted paths.

The optional Windows helper installs the VS Code extensions and Ubuntu
distribution without deleting existing distributions or Docker:

```powershell
pwsh -File .\contrib\bootstrap-dev-env.ps1
```

Clone the repository from WSL:

```bash
cd ~
git clone https://github.com/mdrakiburrahman/openivm-spark.git
cd openivm-spark
```

## Bootstrap the minimal host tooling

```bash
./contrib/bootstrap-dev-env.sh
```

This validates Docker, installs the exact Node/npm version used by the
repository, and runs `npm ci`. All build and test toolchains are prebuilt into
the devcontainer image.

## Use VS Code

Open the repository root:

```bash
code .
```

Accept **Reopen in Container** when prompted. The checked-in
`.devcontainer/devcontainer.json` uses the same immutable image as CI.

## Use the devcontainer headlessly

```bash
npx --no-install nx run devcontainer:up
npx --no-install nx run devcontainer:exec -- \
    npx --no-install nx run spark-ext:verify-all
npx --no-install nx run devcontainer:down
```

See [Headless devcontainer operations](docs/devcontainer/headless-operations.md)
for robust cleanup, image build, and publication examples.

## Standard commands

Run these inside the devcontainer:

```bash
npx --no-install nx run spark-ext:lint
npx --no-install nx run spark-ext:build --configuration=spark-3.5
npx --no-install nx run spark-ext:test --configuration=spark-3.5
npx --no-install nx run spark-ext:assembly --configuration=spark-3.5
npx --no-install nx run spark-ext:verify --configuration=spark-3.5
npx --no-install nx run spark-ext:verify-all
```

Build and test the devcontainer itself from WSL:

```bash
npx --no-install nx run devcontainer:build
npx --no-install nx run devcontainer:test
```
