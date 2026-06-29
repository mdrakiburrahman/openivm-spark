#!/usr/bin/env python3
"""Fix GitHub-incompatible math and format docs/architecture markdown.

Two stages, idempotent:
  1. math fix  - backtick-wrapped LaTeX / math-Unicode renders as code on GitHub,
                 not math; rewrite those spans to inline $...$ and Unicode->LaTeX.
  2. format    - mdformat-gfm, with $$...$$ and $...$ protected so the formatter
                 never mangles them. Fenced code (mermaid, sql, ascii) is preserved.

Usage:
  mdlint.py [--check] [ROOT]
ROOT defaults to docs/architecture. --check exits 1 if any file would change.
"""
import re
import sys
from pathlib import Path

UNICODE_TO_TEX = {
    "Δ": r"\Delta", "δ": r"\delta", "⊕": r"\oplus", "⊖": r"\ominus",
    "⋈": r"\bowtie", "⊗": r"\otimes", "ℕ": r"\mathbb{N}", "ℤ": r"\mathbb{Z}",
    "ℝ": r"\mathbb{R}", "∈": r"\in", "∉": r"\notin", "∀": r"\forall",
    "∃": r"\exists", "→": r"\to", "≤": r"\le", "≥": r"\ge", "≠": r"\ne",
    "×": r"\times", "∪": r"\cup", "∩": r"\cap", "⊆": r"\subseteq", "∅": r"\emptyset",
}
MATH_UNICODE = set(UNICODE_TO_TEX)
FENCE = re.compile(r"^(```|~~~)")
BACKTICK_SPAN = re.compile(r"`([^`\n]+)`")
MULTICHAR_SUB = re.compile(r"_([A-Za-z][A-Za-z0-9]+)\b")
LATEX_CMD = re.compile(r"\\[A-Za-z]+")


def _is_math(span: str) -> bool:
    return bool(LATEX_CMD.search(span)) or any(c in MATH_UNICODE for c in span)


def _to_tex(span: str) -> str:
    for u, t in UNICODE_TO_TEX.items():
        span = span.replace(u, t + " ")
    span = MULTICHAR_SUB.sub(r"_{\1}", span)
    span = re.sub(r"\s+", " ", span).strip()
    return f"${span}$"


def fix_math(text: str) -> str:
    out, in_fence = [], False
    for line in text.splitlines(keepends=True):
        if FENCE.match(line.strip()):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            out.append(line)
            continue
        out.append(BACKTICK_SPAN.sub(
            lambda m: _to_tex(m.group(1)) if _is_math(m.group(1)) else m.group(0), line))
    return "".join(out)


def _escape_pipes_in_table_math(text: str) -> str:
    out = []
    for line in text.splitlines(keepends=True):
        if line.lstrip().startswith("|"):
            line = re.sub(r"(?<!\$)\$(?!\$)[^$\n]+\$(?!\$)",
                          lambda m: m.group(0).replace("\\|", "|").replace("|", "\\|"), line)
        out.append(line)
    return "".join(out)


def format_md(text: str) -> str:
    import mdformat
    text = _escape_pipes_in_table_math(text)
    store, holder = [], "MDLINTMATH{}X"

    def stash(m):
        store.append(m.group(0))
        tok = "MDLM{}".format(len(store) - 1)
        return tok + "M" * max(0, len(m.group(0)) - len(tok))

    def stash_block(m):
        store.append(m.group(0).strip())
        return "\n\n" + holder.format(len(store) - 1) + "\n\n"

    text = re.sub(r"\$\$.*?\$\$", stash_block, text, flags=re.S)
    text = re.sub(r"(?<!\$)\$(?!\$)[^$\n]+\$(?!\$)", stash, text)
    text = mdformat.text(text, options={"wrap": "keep"}, extensions={"gfm", "tables"})
    for i, frag in enumerate(store):
        tok = "MDLM{}".format(i)
        text = text.replace(tok + "M" * max(0, len(frag) - len(tok)), frag)
        text = text.replace(holder.format(i), frag)
    return text


def process(text: str) -> str:
    return format_md(fix_math(text))


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    check = "--check" in sys.argv
    root = Path(args[0]) if args else Path("docs/architecture")
    changed = []
    for f in sorted(root.rglob("*.md")):
        orig = f.read_text(encoding="utf-8")
        new = process(orig)
        if new != orig:
            changed.append(f)
            if not check:
                f.write_text(new, encoding="utf-8")
    label = "would change" if check else "fixed"
    for f in changed:
        print(f"{label}: {f}")
    print(f"{len(changed)} file(s) {label}.")
    return 1 if (check and changed) else 0


if __name__ == "__main__":
    sys.exit(main())
