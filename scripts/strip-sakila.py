#!/usr/bin/env python3
"""Strip triggers and views from the sakila SQLite schema.

Outputs only CREATE TABLE and CREATE [UNIQUE] INDEX statements,
which is all our engine needs to load the sakila schema.

Usage:
    python3 scripts/strip-sakila.py < sqlite-sakila-schema.sql > sakila-schema-stripped.sql
    python3 scripts/strip-sakila.py path/to/sqlite-sakila-schema.sql > sakila-schema-stripped.sql
"""

import re
import sys


def normalize(stmt: str) -> str:
    """Strip SQL comments and collapse whitespace for statement-type detection."""
    s = re.sub(r'--[^\n]*', '', stmt)
    s = re.sub(r'/\*.*?\*/', '', s, flags=re.DOTALL)
    return re.sub(r'\s+', ' ', s).strip().upper()


def split_statements(content: str) -> list[str]:
    """Split SQL content into individual statements on each ';'."""
    statements = []
    current: list[str] = []
    for ch in content:
        current.append(ch)
        if ch == ';':
            statements.append(''.join(current))
            current = []
    if current:
        statements.append(''.join(current))
    return statements


def main() -> None:
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            content = f.read()
    else:
        content = sys.stdin.read()

    statements = split_statements(content)

    kept = []
    for stmt in statements:
        upper = normalize(stmt)
        if (upper.startswith('CREATE TABLE') or
                upper.startswith('CREATE INDEX') or
                upper.startswith('CREATE UNIQUE INDEX')):
            kept.append(stmt)

    print('\n'.join(kept), end='\n')


if __name__ == '__main__':
    main()
