#!/usr/bin/env python3
"""Pre-flight check: replay SQL commands from VHS tape files and fail on errors."""

import subprocess
import sys
import tempfile
import os
import re
import shutil

PROG = os.path.join(os.path.dirname(__file__), "../../target/release/database")
PRELOADED_DB = os.path.join(os.path.dirname(__file__), "demo-preloaded.db")
# (tape_path, seed_db_or_None)  — None means start with a fresh empty database
TAPES = [
    (os.path.join(os.path.dirname(__file__), "repl-sql.tape"), None),
    (os.path.join(os.path.dirname(__file__), "repl-index.tape"), PRELOADED_DB),
]

SKIP_PREFIXES = ("db ", "enter ", "back", "exit")


def sql_commands_from_tape(tape_path):
    commands = []
    with open(tape_path) as f:
        for line in f:
            m = re.match(r'^Type "(.+)"$', line.strip())
            if m:
                cmd = m.group(1)
                if not any(cmd.lower().startswith(p) for p in SKIP_PREFIXES):
                    commands.append(cmd)
    return commands


def run_commands(tape_path, seed_db=None):
    commands = sql_commands_from_tape(tape_path)
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmp_db = f.name

    try:
        if seed_db and os.path.exists(seed_db):
            shutil.copy(seed_db, tmp_db)

        errors = []
        for cmd in commands:
            result = subprocess.run(
                [PROG, tmp_db, "sql", cmd],
                capture_output=True,
                text=True,
            )
            output = result.stdout + result.stderr
            for line in output.splitlines():
                if line.startswith("Error:"):
                    errors.append(f"  cmd: {cmd!r}\n  {line}")

        return errors
    finally:
        os.unlink(tmp_db)


failed = False
for tape, seed_db in TAPES:
    name = os.path.basename(tape)
    errors = run_commands(tape, seed_db)
    if errors:
        print(f"FAIL {name}:")
        for e in errors:
            print(e)
        failed = True
    else:
        print(f"OK   {name}")

sys.exit(1 if failed else 0)
