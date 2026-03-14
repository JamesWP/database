#!/usr/bin/env python3
"""Normalize VHS text output for stable diffing.

VHS captures the full terminal screen on every frame, so the file is a series
of cumulative screen dumps separated by full-width ─ separator lines.
We extract only the last frame that contains actual content.
"""
import sys
import re

sep_re = re.compile(r'^[─]+$')

lines = sys.stdin.read().splitlines()
seps = [i for i, l in enumerate(lines) if sep_re.match(l.strip()) and l.strip()]

for sep in reversed(seps):
    after = [l for l in lines[sep + 1:] if l.strip() and not sep_re.match(l.strip())]
    if after:
        print('\n'.join(after))
        sys.exit(0)

# No separator frames found - input is already normalized, pass through as-is
content = [l for l in lines if l.strip()]
print('\n'.join(content))
