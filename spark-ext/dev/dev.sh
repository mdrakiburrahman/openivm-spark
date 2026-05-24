#!/usr/bin/env bash
# ============================================================================
# spark-ext/dev/dev — single-entry dev-loop wrapper.
#
# Subcommands (alphabetical):
#   assembly                Build the ivmExtension fat jar.
#   build                   `sbt compile` inside the dev container.
#   dev-build [all|build|test [filter]]
#                           Quick iteration on local .temp/openivm + .temp/lpts.
#   fmt                     Auto-format Scala sources with scalafmt.
#   help                    Print this message.
#   image-build [args]      `docker compose build` (force rebuild of dev image).
#   openivm-test            Run upstream openivm sqllogictest suite.
#   pins-sync               Clone/align .temp/{openivm,lpts,ivm-bench} to the
#                           branches in pins.env and warn if local HEAD has
#                           drifted from the pinned COMMIT.
#   pins-fix                Commit + push uncommitted changes across
#                           openivm-spark + .temp/{openivm,lpts,ivm-bench}
#                           (refuses to push to main/master), then rewrite
#                           pins.env + the ivm-bench Dockerfile so the next
#                           pins-sync reports green.
#   shell                   Drop into bash inside the spark-ext container.
#   test [sbt-args]         `sbt test` (all suites) inside the dev container.
#                           Extra args are appended to sbt (e.g. `testOnly ...`).
#   verify                  pins-sync + lint + compile + assembly + test in
#                           one Docker call.
#
# Environment: only Docker is required on the host.  Pinned image SHAs come
# from `spark-ext/dev/pins.env`.
#
# Environment variables honoured:
#   PRE_CLEAN=1   ANY subcommand first force-removes every running Docker
#                 container on the host (named cache volumes survive).
# ============================================================================
set -euo pipefail

# ── helpers ────────────────────────────────────────────────────────────────

SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DEV_DIR="$SCRIPTS_DIR"
PROJECT_DIR="$( cd "$DEV_DIR/.." && pwd )"
REPO_ROOT="$( cd "$PROJECT_DIR/.." && pwd )"

PINS_FILE="$DEV_DIR/pins.env"
COMPOSE_FILE="$DEV_DIR/docker/docker-compose.yml"

if [[ ! -f "$PINS_FILE" ]]; then
    echo "[spark-ext/dev] FATAL: $PINS_FILE not found" >&2
    exit 1
fi

# Load pinned SHAs / versions into environment so docker-compose can substitute them.
# shellcheck disable=SC1090
set -a
source "$PINS_FILE"
set +a

# Wrapper around `docker compose` that always points at our compose file.
compose() {
    docker compose --env-file "$PINS_FILE" -f "$COMPOSE_FILE" "$@"
}

# Standard SBT opts — heap, GC, color, encoding. Applied to every `sbt …` call
# routed through this wrapper so the JVM has enough memory to run 24-way forked
# tests in parallel.
sbt_opts() {
    echo "-Xmx10G -XX:+UseG1GC -Dsbt.color=always -Dfile.encoding=UTF-8"
}

# Run sbt inside the `build` service with the standard opts and any caller-
# supplied tasks/arguments.
#
# OPENIVM_TEST_LOG_DIR (set by `setup_test_log_dir`) is forwarded both as an
# env var (via docker-compose's environment block) AND as a -D system
# property on the sbt JVM (via SBT_OPTS). The sbt JVM in turn propagates
# -D's to forked test JVMs through `Test/javaOptions`, so log4j2.properties
# can resolve `${sys:openivm.test.log.dir}` reliably.
run_sbt() {
    local extra_sys_props=""
    if [[ -n "${OPENIVM_TEST_LOG_DIR:-}" ]]; then
        extra_sys_props=" -Dopenivm.test.log.dir=${OPENIVM_TEST_LOG_DIR}"
    fi
    compose run --rm -T -e SBT_OPTS="$(sbt_opts)${extra_sys_props}" build sbt "$@"
}

# Provision the per-run test-log directory on the host BEFORE we invoke the
# docker container, so log4j2's File appender can open files there without
# racing the test-classes resource copy. Sets OPENIVM_TEST_LOG_DIR to the
# IN-CONTAINER path (matching the bind mount), which is what log4j2.properties
# resolves at appender-init time inside each forked test JVM.
#
# A separate per-fork file is emitted under that directory:
#   .logs/test-<sbt-launch-timestamp>/fork-<jvm-startup-millis>-<thread>.log
#
# The PARENT timestamp is fixed for the entire sbt invocation; the per-fork
# filename uses log4j2's ${date:HHmmss-SSS} lookup which resolves at
# appender-init in each forked JVM, yielding a unique file per fork.
setup_test_log_dir() {
    local ts
    ts="$(date +%Y%m%d-%H%M%S)"
    # The container's /work/spark-ext bind-mounts the HOST's spark-ext/ directory
    # (compose YAML: `../..:/work/spark-ext`). So .logs/ MUST live under
    # spark-ext/.logs/ on the host to be visible from both sides.
    local parent_dir="$PROJECT_DIR/.logs"
    local host_dir="$parent_dir/test-$ts"

    # Ensure the parent .logs/ exists and is world-writable, so the host user
    # (mdrrahman) can mkdir subdirectories even if Docker (running as root)
    # has previously created the parent. World-writable is fine — these are
    # disposable per-run debug logs, not secrets.
    if [[ ! -d "$parent_dir" ]]; then
        mkdir -p "$parent_dir" 2>/dev/null \
            || compose run --rm -T build mkdir -p /work/spark-ext/.logs >/dev/null
    fi
    if [[ ! -w "$parent_dir" ]]; then
        compose run --rm -T build chmod 777 /work/spark-ext/.logs >/dev/null
    fi
    mkdir -p "$host_dir"
    export OPENIVM_TEST_LOG_DIR="/work/spark-ext/.logs/test-$ts"
    echo "[dev] Test logs → spark-ext/.logs/test-$ts/ (DEBUG-level full trace; console keeps WARN+ only)"
}

# Honoured by every subcommand below. If PRE_CLEAN=1, force-remove every
# running Docker container on the host before invoking the next compose
# call. Useful for stripping orphan containers (e.g. from a Ctrl+C'd
# previous run) that may still be holding the shared sbt-cache /
# ivy-cache / coursier-cache named volumes. Named volumes themselves
# survive container removal, so the next `compose run` reuses the warm
# cache.
pre_clean_if_requested() {
    if [[ "${PRE_CLEAN:-0}" == "1" ]]; then
        local running
        running="$(docker ps -q)"
        if [[ -n "$running" ]]; then
            echo "[dev] PRE_CLEAN=1 → force-removing $(echo "$running" | wc -l | tr -d ' ') running container(s)..."
            # shellcheck disable=SC2086
            docker rm -f $running
        else
            echo "[dev] PRE_CLEAN=1 → no running containers to remove."
        fi
    fi
}

usage() {
    sed -n '/^# =====/,/^# =====/p' "${BASH_SOURCE[0]}" \
        | sed -e 's/^# \{0,1\}//' -e '/^=\{3,\}$/d'
}

# Read the default value of `ARG <name>=<value>` from a Dockerfile.
#
# Docker exposes no CLI to read ARG defaults without actually building the
# image (no `docker dockerfile inspect`, no `buildx args --print`), so we parse
# the file directly. Awk's pattern is anchored to start-of-line + exact ARG
# name + `=`, which avoids substring collisions between e.g. OPENIVM_COMMIT and
# OPENIVM_SPARK_COMMIT. Surrounding single/double quotes are stripped from the
# returned value. Emits empty string if the ARG is absent or has no default.
dockerfile_arg_default() {
    local file="$1" name="$2"
    awk -v n="$name" '
        $0 ~ "^[[:space:]]*ARG[[:space:]]+" n "=" {
            sub("^[[:space:]]*ARG[[:space:]]+" n "=", "")
            gsub(/^"|"$/, "")
            gsub(/^'\''|'\''$/, "")
            print
            exit
        }' "$file"
}

# ── subcommand implementations ─────────────────────────────────────────────

cmd_fmt()         { pre_clean_if_requested; compose run --rm fmt; }
cmd_build()       { pre_clean_if_requested; compose run --rm build "$@"; }
cmd_assembly()    { pre_clean_if_requested; compose run --rm assembly; }
cmd_shell()       { pre_clean_if_requested; compose run --rm shell; }
cmd_image_build() { pre_clean_if_requested; compose build "$@"; }
cmd_openivm_test(){ pre_clean_if_requested; compose run --rm openivm-test; }

# Ensure $REPO_ROOT/.temp/{openivm,lpts,ivm-bench} are cloned on the branches
# declared in pins.env, then compare each local HEAD against the pinned COMMIT.
#
#   * If the working copy is missing, clone fresh on the pinned branch.
#   * If the working copy exists, fetch origin and switch to the pinned branch
#     (creating a local tracking branch if needed).
#   * If the repo/branch does not exist remotely, abort with a hard error.
#   * After alignment, log per-repo whether HEAD == pin; emit a WARNING for
#     each repo that has drifted. Drift is non-fatal (exit 0).
cmd_pins_sync() {
    local temp_dir="$REPO_ROOT/.temp"
    mkdir -p "$temp_dir"

    # Tuples: dir_name | repo_url_var | branch_var | commit_var
    local entries=(
        "openivm|OPENIVM_REPO|OPENIVM_BRANCH|OPENIVM_COMMIT"
        "lpts|LPTS_REPO|LPTS_BRANCH|LPTS_COMMIT"
        "ivm-bench|IVM_BENCH_REPO|IVM_BENCH_BRANCH|IVM_BENCH_COMMIT"
    )

    local drift=0
    for entry in "${entries[@]}"; do
        IFS='|' read -r name repo_var branch_var commit_var <<< "$entry"
        local repo="${!repo_var}"
        local branch="${!branch_var}"
        local commit="${!commit_var}"
        local dest="$temp_dir/$name"

        echo
        echo "[pins-sync] ── $name ──"
        echo "[pins-sync]   repo   = $repo"
        echo "[pins-sync]   branch = $branch"
        echo "[pins-sync]   pin    = $commit"

        if [[ ! -d "$dest/.git" ]]; then
            if [[ -e "$dest" ]]; then
                echo "[pins-sync] FATAL: $dest exists but is not a git checkout" >&2
                exit 1
            fi
            echo "[pins-sync]   cloning into .temp/$name ..."
            if ! git clone --branch "$branch" "$repo" "$dest"; then
                echo "[pins-sync] FATAL: failed to clone $repo @ $branch" >&2
                echo "[pins-sync]        verify the repository and branch exist remotely." >&2
                exit 1
            fi
        else
            echo "[pins-sync]   already cloned at .temp/$name"
            local current_remote
            current_remote="$(git -C "$dest" remote get-url origin 2>/dev/null || echo '')"
            if [[ -n "$current_remote" && "$current_remote" != "$repo" ]]; then
                echo "[pins-sync]   WARNING: origin remote mismatch"
                echo "[pins-sync]     local  = $current_remote"
                echo "[pins-sync]     pinned = $repo"
            fi

            echo "[pins-sync]   fetching origin ..."
            if ! git -C "$dest" fetch origin --quiet --prune; then
                echo "[pins-sync] FATAL: 'git fetch origin' failed in .temp/$name" >&2
                exit 1
            fi

            if ! git -C "$dest" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
                echo "[pins-sync] FATAL: branch '$branch' does not exist on origin in .temp/$name" >&2
                exit 1
            fi

            local current_branch
            current_branch="$(git -C "$dest" symbolic-ref --short HEAD 2>/dev/null || echo '')"
            if [[ "$current_branch" != "$branch" ]]; then
                echo "[pins-sync]   switching from '${current_branch:-<detached>}' → '$branch'"
                if git -C "$dest" show-ref --verify --quiet "refs/heads/$branch"; then
                    if ! git -C "$dest" checkout --quiet "$branch"; then
                        echo "[pins-sync] FATAL: 'git checkout $branch' failed in .temp/$name" >&2
                        exit 1
                    fi
                else
                    if ! git -C "$dest" checkout --quiet -b "$branch" --track "origin/$branch"; then
                        echo "[pins-sync] FATAL: failed to create tracking branch '$branch' in .temp/$name" >&2
                        exit 1
                    fi
                fi
            fi

            # Fast-forward the local branch to origin/<branch> so we iterate
            # on the latest code rather than a stale local snapshot. Use
            # --ff-only to surface any divergence (e.g. local commits or a
            # dirty working tree blocking the merge) as a hard error rather
            # than silently leaving HEAD behind origin.
            echo "[pins-sync]   pulling origin/$branch (ff-only) ..."
            if ! git -C "$dest" pull --ff-only --quiet origin "$branch"; then
                echo "[pins-sync] FATAL: 'git pull --ff-only origin $branch' failed in .temp/$name" >&2
                echo "[pins-sync]        local branch has diverged or working tree is dirty." >&2
                exit 1
            fi
        fi

        local head
        head="$(git -C "$dest" rev-parse HEAD)"
        if [[ "$head" == "$commit" ]]; then
            echo "[pins-sync]   ✓ HEAD matches pin ($head)"
        else
            echo "[pins-sync]   ⚠ WARNING: HEAD has drifted from pin"
            echo "[pins-sync]     HEAD = $head"
            echo "[pins-sync]     pin  = $commit"
            drift=1
        fi
    done

    # After all three repos are aligned, cross-check that the ivm-bench
    # spark-openivm-build Dockerfile pins the SAME openivm + lpts SHAs as our
    # pins.env. The Dockerfile's own header comment requires this (the spark
    # compiler is ABI-sensitive to the duckdb-side build) — drift here is a
    # silent footgun, so surface it the same way as HEAD drift.
    local dockerfile="$temp_dir/ivm-bench/src/containers/spark-openivm-build/Dockerfile"
    echo
    echo "[pins-sync] ── ivm-bench Dockerfile ARGs ──"
    if [[ ! -f "$dockerfile" ]]; then
        echo "[pins-sync]   ⚠ WARNING: expected Dockerfile not found at"
        echo "[pins-sync]              .temp/ivm-bench/src/containers/spark-openivm-build/Dockerfile"
        echo "[pins-sync]              (skipping ARG validation)"
        drift=1
    else
        local check_args=(
            OPENIVM_REPO
            OPENIVM_BRANCH
            OPENIVM_COMMIT
            LPTS_REPO
            LPTS_BRANCH
            LPTS_COMMIT
        )
        for arg in "${check_args[@]}"; do
            local actual expected
            expected="${!arg}"
            actual="$(dockerfile_arg_default "$dockerfile" "$arg")"
            if [[ -z "$actual" ]]; then
                echo "[pins-sync]   ⚠ WARNING: ARG $arg not declared with a default in Dockerfile"
                drift=1
            elif [[ "$actual" == "$expected" ]]; then
                echo "[pins-sync]   ✓ $arg = $actual"
            else
                echo "[pins-sync]   ⚠ WARNING: $arg mismatch"
                echo "[pins-sync]     Dockerfile = $actual"
                echo "[pins-sync]     pins.env   = $expected"
                drift=1
            fi
        done
    fi

    # Cross-check OPENIVM_SPARK_COMMIT in the Dockerfile against the HEAD of
    # this repo's current branch on origin. The Dockerfile pins the
    # openivm-spark repo (i.e. *this* repo) at a specific SHA — it must match
    # what's been pushed to origin so the benchmark builds the code on the
    # working branch, not a stale snapshot.
    echo
    echo "[pins-sync] ── openivm-spark self-pin (OPENIVM_SPARK_COMMIT) ──"
    if [[ -f "$dockerfile" ]]; then
        # The Dockerfile declares OPENIVM_SPARK_COMMIT in two stages
        # (spark-ext-builder and final). Both must be identical and must
        # match origin HEAD for the current branch.
        local -a dockerfile_commits
        mapfile -t dockerfile_commits < <(
            awk '/^[[:space:]]*ARG[[:space:]]+OPENIVM_SPARK_COMMIT=/ {
                sub(/^[[:space:]]*ARG[[:space:]]+OPENIVM_SPARK_COMMIT=/, "")
                gsub(/"/, ""); gsub(/'"'"'/, "")
                print
            }' "$dockerfile"
        )

        local n_commits=${#dockerfile_commits[@]}
        if [[ "$n_commits" -eq 0 ]]; then
            echo "[pins-sync]   ⚠ WARNING: no ARG OPENIVM_SPARK_COMMIT found in Dockerfile"
            drift=1
        else
            # Verify all occurrences are identical.
            local all_same=1
            for c in "${dockerfile_commits[@]}"; do
                if [[ "$c" != "${dockerfile_commits[0]}" ]]; then
                    all_same=0
                    break
                fi
            done
            if [[ "$all_same" -eq 0 ]]; then
                echo "[pins-sync]   ⚠ WARNING: OPENIVM_SPARK_COMMIT differs across Dockerfile stages:"
                for i in "${!dockerfile_commits[@]}"; do
                    echo "[pins-sync]     occurrence $((i+1)) = ${dockerfile_commits[$i]}"
                done
                drift=1
            else
                echo "[pins-sync]   Dockerfile OPENIVM_SPARK_COMMIT = ${dockerfile_commits[0]} ($n_commits occurrence(s))"
            fi

            local pinned_spark_commit="${dockerfile_commits[0]}"

            # Resolve the current branch and its origin HEAD.
            local current_branch
            current_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '')"
            if [[ -z "$current_branch" || "$current_branch" == "HEAD" ]]; then
                echo "[pins-sync]   ⚠ WARNING: openivm-spark is in detached HEAD state; skipping origin check"
                drift=1
            else
                echo "[pins-sync]   current branch = $current_branch"
                local origin_head
                origin_head="$(git ls-remote --heads origin "refs/heads/$current_branch" 2>/dev/null | awk '{print $1}')"
                if [[ -z "$origin_head" ]]; then
                    echo "[pins-sync]   ⚠ WARNING: branch '$current_branch' not found on origin"
                    drift=1
                else
                    echo "[pins-sync]   origin HEAD    = $origin_head"
                    if [[ "$pinned_spark_commit" == "$origin_head" ]]; then
                        echo "[pins-sync]   ✓ OPENIVM_SPARK_COMMIT matches origin/$current_branch"
                    else
                        # Tolerate a benign lag: Dockerfile pin is an ancestor of
                        # origin HEAD AND the only diff between them is in
                        # files that don't affect the spark-openivm build.
                        # This breaks the chicken-and-egg where bumping
                        # IVM_BENCH_COMMIT in pins.env advances origin HEAD
                        # and would otherwise require yet another Dockerfile +
                        # pins.env round-trip. Extend the array below as new
                        # build-irrelevant paths are added to the repo.
                        local -a benign_lag_patterns=(
                            'spark-ext/dev/pins\.env'  # host-side tooling pin
                            '.*\.md'                   # any markdown doc
                        )
                        local benign_lag_regex
                        benign_lag_regex="^($(IFS='|'; echo "${benign_lag_patterns[*]}"))$"
                        local benign_lag=0
                        if git merge-base --is-ancestor "$pinned_spark_commit" "$origin_head" 2>/dev/null; then
                            local changed_files
                            changed_files="$(git diff --name-only "$pinned_spark_commit" "$origin_head" 2>/dev/null || echo '')"
                            local non_benign
                            non_benign="$(echo "$changed_files" | grep -vE "$benign_lag_regex" | grep -v '^$' || true)"
                            if [[ -z "$non_benign" ]]; then
                                benign_lag=1
                            fi
                        fi
                        if [[ "$benign_lag" -eq 1 ]]; then
                            echo "[pins-sync]   ✓ OPENIVM_SPARK_COMMIT lags origin/$current_branch by benign files only (${benign_lag_patterns[*]})"
                            echo "[pins-sync]     Dockerfile = $pinned_spark_commit"
                            echo "[pins-sync]     origin     = $origin_head"
                        else
                            echo "[pins-sync]   ⚠ WARNING: OPENIVM_SPARK_COMMIT does not match origin/$current_branch"
                            echo "[pins-sync]     Dockerfile = $pinned_spark_commit"
                            echo "[pins-sync]     origin     = $origin_head"
                            drift=1
                        fi
                    fi
                fi
            fi

            # Also verify OPENIVM_SPARK_BRANCH is consistent with the
            # local branch.
            local dockerfile_branch
            dockerfile_branch="$(dockerfile_arg_default "$dockerfile" OPENIVM_SPARK_BRANCH)"
            if [[ -n "$dockerfile_branch" && -n "$current_branch" && "$current_branch" != "HEAD" ]]; then
                if [[ "$dockerfile_branch" == "$current_branch" ]]; then
                    echo "[pins-sync]   ✓ OPENIVM_SPARK_BRANCH = $dockerfile_branch"
                else
                    echo "[pins-sync]   ⚠ WARNING: OPENIVM_SPARK_BRANCH mismatch"
                    echo "[pins-sync]     Dockerfile = $dockerfile_branch"
                    echo "[pins-sync]     local      = $current_branch"
                    drift=1
                fi
            fi
        fi
    fi

    echo
    if [[ "$drift" -eq 0 ]]; then
        echo "[pins-sync] ✓ All 3 repos are on their pinned branches AND match their pinned commits, the ivm-bench Dockerfile ARGs agree with pins.env, AND OPENIVM_SPARK_COMMIT matches origin."
    else
        echo "[pins-sync] ⚠ Drift detected — see WARNINGs above (HEAD drift, Dockerfile ARG mismatch, and/or OPENIVM_SPARK_COMMIT origin mismatch)."
    fi
}

# ── pins-fix helpers ───────────────────────────────────────────────────────

# Rewrite `KEY=...` in pins.env using awk (URLs contain `/`, so sed -i is
# fragile). Idempotent — silent when the value is already correct.
_pins_env_set() {
    local key="$1" value="$2"
    local file="$PINS_FILE"
    if ! grep -qE "^${key}=" "$file"; then
        echo "[pins-fix] FATAL: no '${key}=' line in $file" >&2
        exit 1
    fi
    local current
    current="$(awk -F= -v k="$key" '$1==k {sub(/^[^=]+=/,""); print; exit}' "$file")"
    if [[ "$current" == "$value" ]]; then
        return 0
    fi
    local tmp
    tmp="$(mktemp "${file}.XXXXXX")"
    awk -v k="$key" -v v="$value" '
        BEGIN { done = 0 }
        !done && index($0, k "=") == 1 { print k "=" v; done = 1; next }
        { print }
    ' "$file" > "$tmp"
    mv "$tmp" "$file"
    echo "[pins-fix]   pins.env $key: $current → $value"
}

# Rewrite ALL occurrences of `ARG <name>=...` defaults in a Dockerfile via awk.
# Preserves leading whitespace, logs nothing when the file is unchanged.
# Robust against multi-occurrence ARGs (e.g. OPENIVM_SPARK_COMMIT) where the
# two stages may temporarily disagree — the rewrite normalises them all.
_dockerfile_arg_set() {
    local file="$1" name="$2" value="$3"
    local before
    before="$(dockerfile_arg_default "$file" "$name")"
    local tmp
    tmp="$(mktemp "${file}.XXXXXX")"
    awk -v n="$name" -v v="$value" '
        {
            if (match($0, "^[[:space:]]*ARG[[:space:]]+" n "=")) {
                prefix = substr($0, 1, RLENGTH)
                print prefix v
            } else {
                print
            }
        }
    ' "$file" > "$tmp"
    if cmp -s "$file" "$tmp"; then
        rm -f "$tmp"
        return 0
    fi
    mv "$tmp" "$file"
    echo "[pins-fix]   Dockerfile ARG $name: $before → $value"
}

# Return 0 if $1 is a benign-lag predecessor of $2 in the openivm-spark repo
# (ancestor + only pins.env / *.md diffs). Same definition as cmd_pins_sync.
_pins_fix_is_benign_lag() {
    local from="$1" to="$2"
    [[ -n "$from" && -n "$to" ]] || return 1
    git -C "$REPO_ROOT" merge-base --is-ancestor "$from" "$to" 2>/dev/null || return 1
    local changed non_benign
    changed="$(git -C "$REPO_ROOT" diff --name-only "$from" "$to" 2>/dev/null || true)"
    non_benign="$(echo "$changed" | grep -vE '^(spark-ext/dev/pins\.env|.*\.md)$' | grep -v '^$' || true)"
    [[ -z "$non_benign" ]]
}

# Refuse to push to main/master/detached HEAD, then verify the repo isn't
# mid-rebase/merge/cherry-pick/bisect.
_pins_fix_assert_safe_branch() {
    local dest="$1"
    local br
    br="$(git -C "$dest" rev-parse --abbrev-ref HEAD 2>/dev/null || echo '')"
    if [[ -z "$br" || "$br" == "HEAD" ]]; then
        echo "[pins-fix] FATAL: $dest is in detached HEAD state — check out a branch first" >&2
        exit 1
    fi
    if [[ "$br" == "main" || "$br" == "master" ]]; then
        echo "[pins-fix] FATAL: $dest is on '$br' — pins-fix refuses to push to main/master" >&2
        exit 1
    fi
    local gitdir
    gitdir="$(git -C "$dest" rev-parse --git-dir)"
    if [[ -d "$dest/$gitdir/rebase-merge" || -d "$dest/$gitdir/rebase-apply" \
       || -f "$dest/$gitdir/MERGE_HEAD"   || -f "$dest/$gitdir/CHERRY_PICK_HEAD" \
       || -f "$dest/$gitdir/BISECT_LOG" ]]; then
        echo "[pins-fix] FATAL: $dest has an in-progress rebase/merge/cherry-pick/bisect — resolve it first" >&2
        exit 1
    fi
    echo "$br"
}

# fetch + commit-if-dirty + rebase + push. Used for the three .temp repos
# (where we don't care about commit granularity — any dirty change is "user
# work in this dependency"). NOT used for openivm-spark, which gets
# fine-grained path-specific commits.
_pins_fix_push_dep() {
    local dest="$1" branch="$2" msg="$3"
    git -C "$dest" fetch --quiet --prune origin

    if [[ -n "$(git -C "$dest" status --porcelain)" ]]; then
        echo "[pins-fix]   staging + committing dirty changes in .temp/$(basename "$dest")"
        git -C "$dest" add -A
        git -C "$dest" commit --quiet -m "$msg"
    fi

    if git -C "$dest" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
        if ! git -C "$dest" rebase --quiet "origin/$branch"; then
            echo "[pins-fix] FATAL: rebase onto origin/$branch failed in $dest — resolve conflicts manually" >&2
            git -C "$dest" rebase --abort 2>/dev/null || true
            exit 1
        fi
    fi

    local ahead
    ahead="$(git -C "$dest" rev-list --count "origin/$branch..HEAD" 2>/dev/null || echo 0)"
    if [[ "$ahead" -gt 0 ]]; then
        echo "[pins-fix]   pushing $ahead commit(s) to origin/$branch"
        if ! git -C "$dest" push --quiet origin "$branch"; then
            echo "[pins-fix] FATAL: 'git push origin $branch' failed in $dest" >&2
            exit 1
        fi
    else
        echo "[pins-fix]   nothing to push (origin/$branch is up to date)"
    fi
}

# Commit + push a *path-restricted* bookkeeping change in openivm-spark.
# Verifies the staged diff matches exactly the expected paths so the resulting
# commit can be relied on as a "benign" pins.env-only commit (preserving the
# benign-lag invariant after step 8). Skips silently when nothing to commit.
_pins_fix_commit_paths_and_push() {
    local branch="$1" msg="$2"
    shift 2
    local -a paths=("$@")

    git -C "$REPO_ROOT" add -- "${paths[@]}"
    local staged
    staged="$(git -C "$REPO_ROOT" diff --cached --name-only)"
    if [[ -z "$staged" ]]; then
        return 0
    fi

    # Refuse if the staged set isn't a subset of $paths.
    local p ok=1
    while IFS= read -r f; do
        ok=0
        for p in "${paths[@]}"; do [[ "$p" == "$f" ]] && ok=1 && break; done
        if [[ "$ok" -ne 1 ]]; then
            echo "[pins-fix] FATAL: unexpected staged file '$f' for bookkeeping commit" >&2
            git -C "$REPO_ROOT" reset --quiet HEAD -- "$f" >/dev/null 2>&1 || true
            exit 1
        fi
    done <<< "$staged"

    git -C "$REPO_ROOT" commit --quiet -m "$msg"

    # Absorb any concurrent pushes to origin/<branch> before our own push.
    # Rebase is safe here because our last commit only touches $paths.
    git -C "$REPO_ROOT" fetch --quiet --prune origin
    if git -C "$REPO_ROOT" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
        if ! git -C "$REPO_ROOT" rebase --quiet "origin/$branch"; then
            echo "[pins-fix] FATAL: rebase onto origin/$branch failed during bookkeeping push" >&2
            git -C "$REPO_ROOT" rebase --abort 2>/dev/null || true
            exit 1
        fi
    fi

    if ! git -C "$REPO_ROOT" push --quiet origin "$branch"; then
        echo "[pins-fix] FATAL: 'git push origin $branch' failed during bookkeeping push" >&2
        exit 1
    fi
}

# Idempotent counterpart for "commit ALL dirty files in openivm-spark" — used
# for the *first* push, which intentionally bundles whatever the user had
# uncommitted (including pins.env auto-updates from step 3).
_pins_fix_commit_all_and_push() {
    local branch="$1" msg="$2"
    git -C "$REPO_ROOT" fetch --quiet --prune origin

    if [[ -n "$(git -C "$REPO_ROOT" status --porcelain)" ]]; then
        git -C "$REPO_ROOT" add -A
        git -C "$REPO_ROOT" commit --quiet -m "$msg"
    fi

    if git -C "$REPO_ROOT" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
        if ! git -C "$REPO_ROOT" rebase --quiet "origin/$branch"; then
            echo "[pins-fix] FATAL: rebase onto origin/$branch failed in $REPO_ROOT" >&2
            git -C "$REPO_ROOT" rebase --abort 2>/dev/null || true
            exit 1
        fi
    fi

    local ahead
    ahead="$(git -C "$REPO_ROOT" rev-list --count "origin/$branch..HEAD" 2>/dev/null || echo 0)"
    if [[ "$ahead" -gt 0 ]]; then
        echo "[pins-fix]   pushing $ahead openivm-spark commit(s) to origin/$branch"
        if ! git -C "$REPO_ROOT" push --quiet origin "$branch"; then
            echo "[pins-fix] FATAL: 'git push origin $branch' failed in $REPO_ROOT" >&2
            exit 1
        fi
    else
        echo "[pins-fix]   openivm-spark already at origin/$branch (no commits to push)"
    fi
}

# Commit working-tree changes across openivm-spark + .temp/{openivm,lpts,
# ivm-bench}, push them to origin (refusing main/master), then rewrite
# pins.env + the ivm-bench Dockerfile so the next pins-sync reports ✓ green.
#
# Ordering rationale (see _pins_fix_is_benign_lag for the invariant):
#   1. Push .temp/openivm + .temp/lpts → capture new HEADs.
#   2. Bake those HEADs into pins.env (OPENIVM_COMMIT, LPTS_COMMIT).
#   3. Bake those HEADs + current openivm-spark branch into the Dockerfile
#      (OPENIVM_*/LPTS_*/OPENIVM_SPARK_BRANCH). NOT OPENIVM_SPARK_COMMIT yet.
#   4. First openivm-spark push: bundles user dirty changes + pins.env
#      OPENIVM/LPTS bumps → new spark HEAD C.
#   5. Bake C into the Dockerfile (OPENIVM_SPARK_COMMIT, both stages) ONLY if
#      the current Dockerfile value is NOT already a benign-lag ancestor of
#      C — otherwise we'd churn the world on every pins-fix invocation.
#   6. Push .temp/ivm-bench (carries the Dockerfile edits) → new bench HEAD D.
#   7. Update pins.env IVM_BENCH_COMMIT=D.
#   8. SECOND openivm-spark push, path-restricted to spark-ext/dev/pins.env so
#      origin lags Dockerfile OPENIVM_SPARK_COMMIT by exactly one
#      pins.env-only commit → benign lag ✓.
#   9. Run pins-sync to validate.
#
# Idempotency: when invoked on an already-aligned tree, every phase becomes a
# no-op (no commits, no pushes, no file writes) and the final pins-sync just
# re-prints the same ✓ green report.
cmd_pins_fix() {
    local temp_dir="$REPO_ROOT/.temp"
    local dockerfile="$temp_dir/ivm-bench/src/containers/spark-openivm-build/Dockerfile"

    echo "[pins-fix] ── pre-flight ──"

    # All three .temp/* must already be git checkouts on their pinned branches.
    local entries=(
        "openivm|OPENIVM_REPO|OPENIVM_BRANCH|OPENIVM_COMMIT"
        "lpts|LPTS_REPO|LPTS_BRANCH|LPTS_COMMIT"
        "ivm-bench|IVM_BENCH_REPO|IVM_BENCH_BRANCH|IVM_BENCH_COMMIT"
    )
    for entry in "${entries[@]}"; do
        IFS='|' read -r name _repo_var branch_var _commit_var <<< "$entry"
        local dest="$temp_dir/$name"
        local pinned_branch="${!branch_var}"
        if [[ ! -d "$dest/.git" ]]; then
            echo "[pins-fix] FATAL: $dest is not a git checkout — run pins-sync first to clone it" >&2
            exit 1
        fi
        local actual_branch
        actual_branch="$(_pins_fix_assert_safe_branch "$dest")"
        if [[ "$actual_branch" != "$pinned_branch" ]]; then
            echo "[pins-fix] FATAL: .temp/$name is on '$actual_branch', expected '$pinned_branch' (per pins.env) — run pins-sync first" >&2
            exit 1
        fi
        echo "[pins-fix]   ✓ .temp/$name on '$actual_branch'"
    done

    # And openivm-spark itself must be on a non-default branch.
    local spark_branch
    spark_branch="$(_pins_fix_assert_safe_branch "$REPO_ROOT")"
    echo "[pins-fix]   ✓ openivm-spark on '$spark_branch'"

    if [[ ! -f "$dockerfile" ]]; then
        echo "[pins-fix] FATAL: ivm-bench Dockerfile not found at $dockerfile" >&2
        exit 1
    fi

    # ── Phase 1+2: push .temp/openivm + .temp/lpts, capture new HEADs ──
    declare -A new_head
    for entry in "openivm|OPENIVM_REPO|OPENIVM_BRANCH|OPENIVM_COMMIT" \
                 "lpts|LPTS_REPO|LPTS_BRANCH|LPTS_COMMIT"; do
        IFS='|' read -r name _ branch_var _ <<< "$entry"
        local branch="${!branch_var}"
        local dest="$temp_dir/$name"
        echo
        echo "[pins-fix] ── .temp/$name ──"
        _pins_fix_push_dep "$dest" "$branch" "chore(pins-fix): snapshot working tree"
        new_head[$name]="$(git -C "$dest" rev-parse HEAD)"
        echo "[pins-fix]   HEAD = ${new_head[$name]}"
    done

    # ── Phase 3: bake openivm + lpts HEADs into pins.env ──
    echo
    echo "[pins-fix] ── pins.env (openivm + lpts) ──"
    _pins_env_set OPENIVM_COMMIT "${new_head[openivm]}"
    _pins_env_set LPTS_COMMIT    "${new_head[lpts]}"

    # ── Phase 3b: bake openivm/lpts pins + current spark branch into Dockerfile ──
    echo
    echo "[pins-fix] ── ivm-bench Dockerfile (openivm/lpts + OPENIVM_SPARK_BRANCH) ──"
    _dockerfile_arg_set "$dockerfile" OPENIVM_REPO        "$OPENIVM_REPO"
    _dockerfile_arg_set "$dockerfile" OPENIVM_BRANCH      "$OPENIVM_BRANCH"
    _dockerfile_arg_set "$dockerfile" OPENIVM_COMMIT      "${new_head[openivm]}"
    _dockerfile_arg_set "$dockerfile" LPTS_REPO           "$LPTS_REPO"
    _dockerfile_arg_set "$dockerfile" LPTS_BRANCH         "$LPTS_BRANCH"
    _dockerfile_arg_set "$dockerfile" LPTS_COMMIT         "${new_head[lpts]}"
    _dockerfile_arg_set "$dockerfile" OPENIVM_SPARK_BRANCH "$spark_branch"

    # ── Phase 4: first openivm-spark push (bundles user work + pins.env bumps) ──
    echo
    echo "[pins-fix] ── openivm-spark (first push: user work + pins.env bumps) ──"
    _pins_fix_commit_all_and_push "$spark_branch" "chore(pins-fix): snapshot openivm-spark working tree + bump OPENIVM/LPTS pins"
    local spark_first_head
    spark_first_head="$(git -C "$REPO_ROOT" rev-parse HEAD)"
    echo "[pins-fix]   openivm-spark HEAD = $spark_first_head"

    # ── Phase 5: bake spark_first_head into Dockerfile OPENIVM_SPARK_COMMIT ──
    # Skip when the current Dockerfile value is already a benign-lag ancestor
    # of spark_first_head — otherwise repeated pins-fix runs would churn the
    # Dockerfile + ivm-bench + pins.env forever.
    echo
    echo "[pins-fix] ── ivm-bench Dockerfile (OPENIVM_SPARK_COMMIT) ──"
    local current_dockerfile_spark
    current_dockerfile_spark="$(dockerfile_arg_default "$dockerfile" OPENIVM_SPARK_COMMIT)"
    if [[ "$current_dockerfile_spark" == "$spark_first_head" ]]; then
        echo "[pins-fix]   Dockerfile OPENIVM_SPARK_COMMIT already = $spark_first_head"
    elif _pins_fix_is_benign_lag "$current_dockerfile_spark" "$spark_first_head"; then
        echo "[pins-fix]   Dockerfile OPENIVM_SPARK_COMMIT ($current_dockerfile_spark)"
        echo "[pins-fix]     is a benign-lag ancestor of $spark_first_head — leaving unchanged"
    else
        _dockerfile_arg_set "$dockerfile" OPENIVM_SPARK_COMMIT "$spark_first_head"
    fi

    # ── Phase 6: push .temp/ivm-bench (carries Dockerfile edits) ──
    echo
    echo "[pins-fix] ── .temp/ivm-bench ──"
    _pins_fix_push_dep "$temp_dir/ivm-bench" "$IVM_BENCH_BRANCH" "chore(pins-fix): sync openivm-spark + deps"
    local bench_head
    bench_head="$(git -C "$temp_dir/ivm-bench" rev-parse HEAD)"
    echo "[pins-fix]   .temp/ivm-bench HEAD = $bench_head"

    # ── Phase 7: bake bench_head into pins.env IVM_BENCH_COMMIT ──
    echo
    echo "[pins-fix] ── pins.env (ivm-bench) ──"
    _pins_env_set IVM_BENCH_COMMIT "$bench_head"

    # ── Phase 8: second openivm-spark push — path-restricted to pins.env ──
    # The restriction is what preserves the benign-lag guarantee: any other
    # dirty file slipping into this commit would make Dockerfile
    # OPENIVM_SPARK_COMMIT lag origin by a non-pins.env diff, and pins-sync
    # would report drift.
    echo
    echo "[pins-fix] ── openivm-spark (second push: pins.env IVM_BENCH_COMMIT bump) ──"
    _pins_fix_commit_paths_and_push "$spark_branch" \
        "chore(pins-fix): bump IVM_BENCH_COMMIT to $bench_head" \
        "spark-ext/dev/pins.env"

    # ── Phase 9: re-source pins.env (in-memory vars are stale) and validate ──
    # shellcheck disable=SC1090
    set -a
    source "$PINS_FILE"
    set +a

    echo
    echo "[pins-fix] ── validating with pins-sync ──"
    cmd_pins_sync
}

cmd_test() {
    pre_clean_if_requested
    setup_test_log_dir
    if [[ "$#" -eq 0 ]]; then
        compose run --rm test
    else
        run_sbt "$@"
    fi
}

cmd_verify() {
    # One-shot: lint → compile → assembly → full test in a single sbt JVM.
    # Forwarded arguments are inserted before the task list so callers can pass
    # global `set` commands or `-D…` system properties (e.g. test-fork count).
    #
    # Set PRE_CLEAN=1 to force-remove every running Docker container on the
    # host before invoking sbt (see `pre_clean_if_requested` at the top of
    # this file).
    #
    # Always reconcile .temp/{openivm,lpts,ivm-bench} against pins.env first
    # so a `verify` run flags any drifted dependency before spending compute
    # on the full sbt cycle. Drift is non-fatal (WARNING only); a missing
    # repo/branch hard-fails per cmd_pins_sync's contract.
    cmd_pins_sync
    pre_clean_if_requested
    setup_test_log_dir

    run_sbt "$@" \
        scalafmtCheckAll \
        scalafmtSbtCheck \
        compile \
        Test/compile \
        ivmExtension/assembly \
        test
}

# Quick local-source iteration on .temp/openivm + .temp/lpts.  Forwards
# ACTION + optional filter to the existing per-cycle docker-build flow.
cmd_dev_build() {
    local action="${1:-all}"
    shift || true

    local openivm_local="$REPO_ROOT/.temp/openivm"
    local lpts_local="$REPO_ROOT/.temp/lpts"

    if [[ ! -d "$openivm_local" || ! -d "$lpts_local" ]]; then
        echo "[dev-build] FATAL: expected .temp/openivm and .temp/lpts checkouts in $REPO_ROOT" >&2
        exit 1
    fi

    local base_image="openivm-spark/openivm-tester:${OPENIVM_COMMIT}-${LPTS_COMMIT}"
    local dev_image="openivm-spark/openivm-dev:local"

    # Stage local sources into a build context (omits build/, .git/, large submodules).
    local work
    work=$(mktemp -d /tmp/openivm-dev-XXXXXX)
    trap "rm -rf $work" EXIT

    mkdir -p "$work/openivm" "$work/lpts"
    rsync -a --exclude='build/' --exclude='.git/' --exclude='duckdb/' \
              --exclude='extension-ci-tools/' --exclude='third_party/' \
              "$openivm_local/" "$work/openivm/"
    rsync -a --exclude='build/' --exclude='.git/' --exclude='duckdb/' \
              --exclude='third_party/' \
              "$lpts_local/" "$work/lpts/"

    cat > "$work/Dockerfile" <<EOF
FROM ${base_image}
WORKDIR /src
RUN find /src -maxdepth 1 -mindepth 1 -not -name 'duckdb' -not -name 'extension-ci-tools' \\
                                       -not -name 'third_party' -not -name 'build' \\
                                       -not -name '.git' -exec rm -rf {} + && \\
    find /src/third_party/lpts -maxdepth 1 -mindepth 1 -not -name 'duckdb' \\
                                            -not -name 'third_party' -not -name '.git' \\
                                            -exec rm -rf {} +
COPY openivm/ /src/
COPY lpts/ /src/third_party/lpts/
EOF

    echo "[dev-build] Building dev image with local sources..."
    docker build -t "$dev_image" "$work"

    case "$action" in
        build)
            echo "[dev-build] Compiling openivm + patched lpts..."
            docker run --rm -w /src "$dev_image" bash -c "GEN=ninja make -j\$(nproc)"
            ;;
        all)
            echo "[dev-build] Compiling + running upstream tests..."
            docker run --rm -w /src "$dev_image" bash -c "GEN=ninja make -j\$(nproc) && make test $*"
            ;;
        test)
            echo "[dev-build] Building + running tests..."
            if [[ "$#" -eq 0 ]]; then
                docker run --rm -w /src "$dev_image" bash -c "GEN=ninja make -j\$(nproc) && make test"
            else
                docker run --rm -w /src "$dev_image" \
                    bash -c "GEN=ninja make -j\$(nproc) && build/release/test/unittest $*"
            fi
            ;;
        *)
            echo "[dev-build] Unknown action '$action' — expected one of: build, test, all" >&2
            exit 2
            ;;
    esac
    echo "[dev-build] OK (sources from $openivm_local + $lpts_local)"
}

# ── dispatch ───────────────────────────────────────────────────────────────

if [[ "$#" -eq 0 ]]; then
    usage
    exit 0
fi

cmd="$1"
shift

case "$cmd" in
    fmt)          cmd_fmt "$@" ;;
    build)        cmd_build "$@" ;;
    assembly)     cmd_assembly "$@" ;;
    test)         cmd_test "$@" ;;
    verify)       cmd_verify "$@" ;;
    shell)        cmd_shell "$@" ;;
    image-build|image_build) cmd_image_build "$@" ;;
    openivm-test|openivm_test) cmd_openivm_test "$@" ;;
    pins-sync|pins_sync)     cmd_pins_sync "$@" ;;
    pins-fix|pins_fix)       cmd_pins_fix "$@" ;;
    dev-build|dev_build)     cmd_dev_build "$@" ;;
    help|-h|--help)          usage ;;
    *)
        echo "[spark-ext/dev] Unknown subcommand: $cmd" >&2
        echo >&2
        usage >&2
        exit 2
        ;;
esac
