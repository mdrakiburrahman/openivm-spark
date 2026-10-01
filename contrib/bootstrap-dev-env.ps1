<#
.SYNOPSIS
  Prepares a non-destructive Windows/WSL entry point for the devcontainer.

.NOTES
  Docker must already be installed and reachable inside Ubuntu-24.04.
  This script never unregisters WSL distributions or removes Docker Desktop.
#>

#Requires -RunAsAdministrator
#Requires -Version 7.0

$ErrorActionPreference = "Stop"

if (Get-Command code -ErrorAction SilentlyContinue) {
    code --install-extension ms-vscode-remote.remote-wsl --force
    code --install-extension ms-vscode-remote.remote-containers --force
}

$distribution = "Ubuntu-24.04"
$installed = wsl --list --quiet |
    ForEach-Object { $_.Trim().Replace("`0", "") } |
    Where-Object { $_ }

if ($installed -notcontains $distribution) {
    Write-Host "Installing $distribution without modifying existing WSL distributions."
    wsl --install --distribution $distribution --no-launch
} else {
    Write-Host "$distribution is already installed."
}

Write-Host @"

WSL preparation complete.

Before cloning the repository, ensure Docker is reachable inside $distribution:
  wsl -d $distribution -- docker info

Then run from the repository root inside WSL:
  ./contrib/bootstrap-dev-env.sh
"@
