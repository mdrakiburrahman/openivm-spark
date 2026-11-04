#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 3 ]]; then
  echo "Usage: $0 numeric-asset-id expected-sha256 new-context-directory" >&2
  exit 2
fi

asset_id="$1"
expected="$2"
directory="$3"
readonly pinned_sha=a49fe6c36626853a672f7a4d43250b7e798d01814b3b98b40c8e8885ec0286eb
readonly pinned_size=100880999

if [[ ! "$asset_id" =~ ^[0-9]+$ || "$expected" != "$pinned_sha" ]]; then
  echo "A numeric same-repository asset ID and the pinned PR62 assembly SHA256 are required." >&2
  exit 2
fi
if [[ "${GITHUB_REPOSITORY:-}" != mdrakiburrahman/openivm-spark ]]; then
  echo "The immutable input must come from the same openivm-spark repository." >&2
  exit 2
fi
: "${GITHUB_TOKEN:?Existing repository token is required}"

mkdir "$directory"
partial="$directory/ivmextension.jar.partial"
if ! printf 'header = "Authorization: Bearer %s"\nheader = "Accept: application/octet-stream"\n' \
  "$GITHUB_TOKEN" |
  curl --config - --fail --silent --show-error --location \
    --output "$partial" \
    "https://api.github.com/repos/mdrakiburrahman/openivm-spark/releases/assets/${asset_id}"; then
  echo "Same-repository draft-asset download failed; no public-release or native-build fallback is permitted." >&2
  exit 1
fi
actual_size="$(stat --format=%s "$partial")"
if [[ "$actual_size" != "$pinned_size" ]]; then
  echo "Immutable assembly size mismatch: expected ${pinned_size}, received ${actual_size}." >&2
  exit 1
fi
printf '%s  %s\n' "$pinned_sha" "$partial" | sha256sum -c -
mv "$partial" "$directory/ivmextension.jar"
printf 'asset_id=%s sha256=%s bytes=%s\n' "$asset_id" "$pinned_sha" "$pinned_size"
