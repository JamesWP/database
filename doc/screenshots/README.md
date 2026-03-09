# Screenshots

Animated GIFs embedded in the README are generated reproducibly from
[VHS](https://github.com/charmbracelet/vhs) tape scripts.

## Regenerating

```bash
make screenshots
```

This rebuilds all GIFs. Requires `vhs` on PATH (or in `.bin/`).

## Files

| Tape | GIF | Shows |
|------|-----|-------|
| `repl-sql.tape` | `repl-sql.gif` | SQL mode: CREATE TABLE, INSERT, SELECT with WHERE and ORDER BY |
| `repl-index.tape` | `repl-index.gif` | Index mode: CREATE INDEX, indexed query |
| `repl-engine.tape` | `repl-engine.gif` | Engine mode: compile SQL → colorized bytecode listing |

## Adding a new GIF

1. Write a `.tape` file in this directory
2. Run `vhs <your>.tape` to test it
3. Add a line to the `screenshots` target in `Makefile`
4. Embed the output GIF in `README.md`
5. Commit both the tape and the GIF

## Dependencies

- `vhs` binary — place in `.bin/` or ensure it is on PATH
- `check.py` — validates that the demo database was created correctly before recording
