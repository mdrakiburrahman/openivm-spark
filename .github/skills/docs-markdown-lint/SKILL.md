---
name: docs-markdown-lint
description: "Make docs/architecture/ render correctly on GitHub: rewrite backtick-wrapped LaTeX / math-Unicode into inline $...$ math (Unicode→LaTeX), escape pipes in table math, then run mdformat-gfm with all math protected so $$…$$ and mermaid/ASCII fences are never mangled. Pinned Python tooling in a venv; idempotent. Auto-commit + push on non-main."
user-invocable: true
---

# Docs Markdown Lint — GitHub-correct math + formatting

You run this from `/home/mdrrahman/openivm-spark` (repo root). The job: the
architecture docs must render cleanly on GitHub's markdown renderer (math and
visuals included) and be consistently formatted. Scope is **`docs/architecture/`
only** (37 chapters). Do not touch `docs/README.md` or `docs/todos`.

The fix is fully automated by `mdlint.py` in this folder; you bootstrap the venv,
run it, verify, and commit. Do not hand-edit chapters.

---

## Why this skill exists

GitHub renders math via MathJax with `$…$` (inline) and `$$…$$` (block). The
docs wrap math in **backticks** instead, so it shows as code, not math —
e.g. `` `D \oplus ΔD` ``, `` `V^Δ` ``, `` `R ⋈ S` ``, `` `S \oplus T` ``. Mixed
Unicode (Δ ⊕ ⋈ ℕ ℤ) compounds it. `mdlint.py` rewrites those spans to `$…$`,
maps Unicode→LaTeX, escapes `|` inside table math (raw pipes break GFM tables),
then formats with mdformat — protecting every `$$`/`$` so the formatter cannot
mangle math, and leaving fenced code (47 mermaid + ASCII diagrams) untouched.

---

## CRITICAL RULES

- **Scope is `docs/architecture/`.** Never reformat README/todos/source.
- **Math is content, not code.** Only convert backtick spans that contain a
  LaTeX command (`\foo`) or math-Unicode; leave plain identifiers like `` `R` ``,
  `` `S(x)` `` as code. The fixer already encodes this — don't widen it blindly.
- **Never break `$$` blocks or mermaid.** They are protected before mdformat.
  The verify step asserts `$$` and mermaid counts are unchanged.
- **Idempotent.** Two runs back-to-back must yield zero changes (`--check`).
- **Auto-commit on non-main; skip on main.** Check
  `git symbolic-ref --short HEAD`; if not `main`, commit + push, prefix
  `[docs-markdown-lint]`. If `main`, write files, report, stop.

---

## PHASE 0 — Bootstrap (pinned, isolated)

```bash
cd /home/mdrrahman/openivm-spark
sudo apt-get install -y python3-pip python3.12-venv
python3 -m venv .github/skills/docs-markdown-lint/.venv
.github/skills/docs-markdown-lint/.venv/bin/pip install -q \
  -r .github/skills/docs-markdown-lint/requirements.txt
git symbolic-ref --short HEAD
```

`.venv/` is git-ignored. If pip is already present a venv exists, skip re-install.

## PHASE 1 — Fix + format

```bash
.github/skills/docs-markdown-lint/.venv/bin/python \
  .github/skills/docs-markdown-lint/mdlint.py docs/architecture
```

Stage 1 backtick-math → `$…$` (Unicode→LaTeX, `_word`→`_{word}`, table pipes
`\|`); stage 2 mdformat-gfm with math/code protected.

## PHASE 2 — Verify

```bash
PY=.github/skills/docs-markdown-lint/.venv/bin/python
$PY .github/skills/docs-markdown-lint/mdlint.py --check docs/architecture   # expect 0
grep -rc '```mermaid' docs/architecture | awk -F: '{s+=$2} END{print s}'    # unchanged
! grep -rnP '`[^`]*(\\[a-z]+|⊕|⋈|Δ)[^`]*`' docs/architecture                # no backtick-math
! grep -rn 'MDLM\|MDLINTMATH' docs/architecture                              # no token leaks
```

Spot-check `docs/architecture/openivm/1-math-zsets-dbsp-and-the-paper.md` on
GitHub: notation table, `$V^\Delta$`, `$D \oplus \Delta D$`, `$$…$$` all render.

## PHASE 3 — Commit (non-main only)

```bash
[ "$(git symbolic-ref --short HEAD)" != main ] && git add docs/architecture && \
  git commit -m "[docs-markdown-lint] GitHub-correct math + format docs" && git push
```

Report `{ "status": "Succeeded" }` when --check is clean, mermaid/`$$` counts
hold, no backtick-math/token leaks. Else `{ "status": "Failed" }` with diffs.
