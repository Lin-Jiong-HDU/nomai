#!/usr/bin/env python3
"""Locate spec/plan references in code comments and public docs, classify them,
and optionally strip the one zero-ambiguity pattern (version-trace tails).

One-shot cleanup tool. Does NOT touch public external specs (JSON-RPC 2.0,
MCP, ULID).

Modes:
  scan                 Print every line with a cleanable marker, tagged by
                       category. Lines that only reference public specs are
                       skipped. This is the authoritative hit list for manual
                       rewriting per the plan's rewrite rules.
  strip-version-trace  Remove ", Spec N Plan M / F-xxx-N" tails (zero-ambiguity:
                       leaves the preceding version number intact). Prints a
                       unified diff unless --write is given (dry-run default).

Usage:
  python3 scripts/cleanup_refs.py scan crates/ docs/guide.md docs/reference.md docs/lib.md CHANGELOG.md
  python3 scripts/cleanup_refs.py strip-version-trace CHANGELOG.md docs/reference.md docs/lib.md
  python3 scripts/cleanup_refs.py strip-version-trace --write CHANGELOG.md docs/reference.md docs/lib.md
"""
from __future__ import annotations
import argparse, difflib, pathlib, re, sys

PUBLIC = re.compile(r'\b(JSON-?RPC|MCP|Model Context Protocol|modelcontextprotocol|ulid/spec)\b', re.I)

RX_PATH     = re.compile(r'docs/superpowers/(?:specs|plans)/[^\s)`\'",>]+\.md(?:\s*§[\d.]+)?')
RX_VERTRACE = re.compile(r',\s*Spec\s*\d+\s+Plan\s+\d+\s*/\s*F-[a-z]+-\d+')
RX_SPEC_SEC = re.compile(r'[Ss]pec\s*\d*\s*§\s*[\d.]+')
RX_SPEC_N   = re.compile(r'\b[Ss]pec\s+\d+\b')
RX_PLAN     = re.compile(r'\b[Pp]lan[-\s]+\d+(?:\s+Task\s+\d+)?\b')
RX_TASK     = re.compile(r'\bTasks?\s+\d+(?:\s*[,/]\s*\d+)*')
RX_FID      = re.compile(r'\bF-[a-z]+-\d+\b')
RX_ROADMAP  = re.compile(r'^(\s*-\s*)\*\*Spec\s*\d+\*\*\s*—\s*', re.MULTILINE)

STRIP_PATTERNS = [
    ('path', RX_PATH), ('version_trace', RX_VERTRACE), ('spec_section', RX_SPEC_SEC),
    ('spec_n', RX_SPEC_N), ('plan', RX_PLAN), ('task', RX_TASK), ('f_id', RX_FID), ('roadmap', RX_ROADMAP),
]


def iter_files(paths):
    seen = set()
    for a in paths:
        p = pathlib.Path(a)
        if p.is_dir():
            for f in p.rglob('*'):
                if f.suffix in ('.rs', '.md') and 'target/' not in str(f) and f not in seen:
                    seen.add(f); yield f
        elif p.is_file() and p not in seen:
            seen.add(p); yield p


def scan(files):
    total = 0
    for f in sorted(files):
        try:
            lines = f.read_text(encoding='utf-8').splitlines()
        except Exception as e:
            print(f'# ERROR {f}: {e}', file=sys.stderr); continue
        for i, line in enumerate(lines, 1):
            strip_hits = [name for name, rx in STRIP_PATTERNS if rx.search(line)]
            if not strip_hits:
                continue  # pure public-spec line or clean line
            total += 1
            mixed = ' +PUBLIC_SPEC(keep that part)' if PUBLIC.search(line) else ''
            print(f'{f}:{i}: [{",".join(strip_hits)}{mixed}]\n    {line.rstrip()}')
    print(f'\n# {total} lines to review', file=sys.stderr)


def strip_version_trace(files, write):
    changed = 0
    for f in sorted(files):
        try:
            orig = f.read_text(encoding='utf-8')
        except Exception as e:
            print(f'# ERROR {f}: {e}', file=sys.stderr); continue
        new = RX_VERTRACE.sub('', orig)
        if new != orig:
            changed += 1
            if write:
                f.write_text(new, encoding='utf-8')
            else:
                sys.stdout.writelines(difflib.unified_diff(
                    orig.splitlines(keepends=True), new.splitlines(keepends=True),
                    fromfile=str(f), tofile=str(f) + ' (stripped)'))
    print(f'# {"wrote" if write else "would write (dry-run)"} {changed} file(s)', file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('mode', choices=['scan', 'strip-version-trace'])
    ap.add_argument('paths', nargs='+')
    ap.add_argument('--write', action='store_true', help='modify files (default: dry-run)')
    a = ap.parse_args()
    files = list(iter_files(a.paths))
    if a.mode == 'scan':
        scan(files)
    else:
        strip_version_trace(files, a.write)


if __name__ == '__main__':
    main()
