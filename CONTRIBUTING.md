# Contributing

## How to use, on a Windows machine by installing WSL

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

1. Clone the repo, and open VSCode in it:

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

   code .
   ```

1. Run the bootstrapper script, that installs all tools idempotently:

   ```bash
   GIT_ROOT=$(git rev-parse --show-toplevel)
   chmod +x ${GIT_ROOT}/contrib/bootstrap-dev-env.sh && ${GIT_ROOT}/contrib/bootstrap-dev-env.sh
   ```

1. (Optional but recommended) Enable passwordless sudo so `dev.sh` helpers never block on a prompt:

   ```bash
   echo "$USER ALL=(ALL) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/90-$USER-nopasswd >/dev/null && sudo chmod 0440 /etc/sudoers.d/90-$USER-nopasswd
   ```

1. Install recommended developer tooling (optional):

   ```bash
   curl -fsSL https://gh.io/copilot-install | bash

   (type -p wget >/dev/null || (sudo apt update && sudo apt install wget -y)) \
	&& sudo mkdir -p -m 755 /etc/apt/keyrings \
	&& out=$(mktemp) && wget -nv -O$out https://cli.github.com/packages/githubcli-archive-keyring.gpg \
	&& cat $out | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null \
	&& sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
	&& sudo mkdir -p -m 755 /etc/apt/sources.list.d \
	&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
	&& sudo apt update \
	&& sudo apt install gh -y

   gh auth login
   $HOME/.local/bin/copilot --yolo
   ```