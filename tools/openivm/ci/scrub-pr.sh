#!/usr/bin/env bash
# scrub-pr.sh — scan a PR diff (or working tree) for build artifacts,
# editor junk, Python cache, and gratuitous submodule bumps.
#
# Usage:
#   scrub-pr.sh [--base <ref>] [--all-tree] [--quiet]
#
# Options:
#   --base <ref>   Git ref to diff against  (default: upstream/main)
#   --all-tree     Scan working tree instead of git-diff file list
#   --quiet        Suppress headers; only print offending file lines
#
# Exit codes:
#   0  — clean
#   1  — one or more nasties found
#
# Must be run from inside the openivm checkout directory.
set -euo pipefail

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
BASE="upstream/main"
ALL_TREE=0
QUIET=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base)  BASE="$2"; shift 2 ;;
    --all-tree) ALL_TREE=1; shift ;;
    --quiet)    QUIET=1; shift ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done

# ---------------------------------------------------------------------------
# Submodule whitelist
# Entries are "<path>:<new-SHA-prefix>" — bumps to these exact SHAs are OK.
# Add an entry here when a legitimate submodule bump lands in a PR.
# ---------------------------------------------------------------------------
SUBMODULE_WHITELIST=(
  "third_party/lpts:77093ca"    # lpts PR #9 merged to main
)

# ---------------------------------------------------------------------------
# Build the file list to scan
# ---------------------------------------------------------------------------
if [[ "$ALL_TREE" -eq 1 ]]; then
  # All tracked + untracked files in the working tree
  mapfile -t FILES < <(git ls-files && git ls-files --others --exclude-standard)
else
  mapfile -t FILES < <(git diff "$BASE"..HEAD --name-only 2>/dev/null)
fi

# ---------------------------------------------------------------------------
# Pattern definitions
# Each entry: "<human-label>|<ERE pattern>"
# ---------------------------------------------------------------------------
BUILD_PATTERNS=(
  "clangd-cache|^\.cache/"
  "compile_commands|compile_commands\.json$"
  "build-dir|^build/"
  "out-dir|^out/"
  "target-dir|^target/"
  "object-file|\.o$"
  "shared-lib|\.so(\.[0-9].*)?$"
  "dylib|\.dylib$"
  "ninja-log|\.ninja_log$"
  "ninja-deps|\.ninja_deps$"
)

EDITOR_PATTERNS=(
  "vscode|^\.vscode/"
  "idea|^\.idea/"
  "vim-swap|\.swp$"
  "vim-swpx|\.swo$"
  "emacs-backup|~$"
  "ds-store|\.DS_Store$"
  "thumbs-db|Thumbs\.db$"
)

PYTHON_PATTERNS=(
  "pycache|__pycache__/"
  "pyc|\.pyc$"
  "pyo|\.pyo$"
  "pytest-cache|\.pytest_cache/"
  "mypy-cache|\.mypy_cache/"
  "egg-info|\.egg-info/"
)

JUNK_PATTERNS=(
  "bak-file|\.bak$"
  "tmp-file|\.tmp$"
  "log-outside-logs|^(?!.*logs/).*\.log$"
  "core-dump|^core\."
  "gdb-history|\.gdb_history$"
)

# ---------------------------------------------------------------------------
# Helper: check one file against a pattern group
# ---------------------------------------------------------------------------
found=0

check_file() {
  local file="$1"
  local label="$2"
  local pattern="$3"
  if echo "$file" | grep -qPe "$pattern"; then
    echo "NASTY [$label]: $file"
    found=1
  fi
}

# ---------------------------------------------------------------------------
# Scan files
# ---------------------------------------------------------------------------
if [[ "$QUIET" -eq 0 ]]; then
  echo "=== scrub-pr: scanning $(echo "${FILES[@]}" | wc -w) files against base $BASE ==="
fi

for file in "${FILES[@]}"; do
  for entry in "${BUILD_PATTERNS[@]}" "${EDITOR_PATTERNS[@]}" "${PYTHON_PATTERNS[@]}" "${JUNK_PATTERNS[@]}"; do
    label="${entry%%|*}"
    pattern="${entry##*|}"
    check_file "$file" "$label" "$pattern"
    # Only report the first matching category per file
    if [[ "$found" -eq 1 ]] && echo "$file" | grep -qPe "$pattern"; then
      break
    fi
  done
done

# ---------------------------------------------------------------------------
# Submodule bump check
# ---------------------------------------------------------------------------
# Collect submodule changes from the diff
mapfile -t SUBMODULE_BUMPS < <(
  git diff "$BASE"..HEAD --raw 2>/dev/null \
    | awk '$2 == "160000" || $3 == "160000" { print $NF }' \
    | sort -u
)

for submod in "${SUBMODULE_BUMPS[@]}"; do
  new_sha=$(git diff "$BASE"..HEAD -- "$submod" 2>/dev/null \
    | grep '^+Subproject commit' | awk '{print $3}')
  whitelisted=0
  for wl_entry in "${SUBMODULE_WHITELIST[@]}"; do
    wl_path="${wl_entry%%:*}"
    wl_sha="${wl_entry##*:}"
    if [[ "$submod" == "$wl_path" ]] && [[ "$new_sha" == "$wl_sha"* ]]; then
      whitelisted=1
      break
    fi
  done
  if [[ "$whitelisted" -eq 0 ]]; then
    echo "NASTY [unlisted-submodule-bump]: $submod (new SHA: $new_sha)"
    found=1
  fi
done

# ---------------------------------------------------------------------------
# Summary and exit
# ---------------------------------------------------------------------------
if [[ "$QUIET" -eq 0 ]]; then
  if [[ "$found" -eq 0 ]]; then
    echo "=== scrub-pr: CLEAN ==="
  else
    echo "=== scrub-pr: NASTIES FOUND — fix before merging ==="
  fi
fi

exit "$found"
