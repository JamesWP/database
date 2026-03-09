# Phase AE — TUI Bytecode Debugger

Add an interactive terminal-UI debugger to the engine REPL mode, using ratatui + crossterm, so developers can visually step through compiled bytecode with a real-time split-pane view of instructions and register state.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 103 | 7 | Add `ratatui` + `crossterm` dependencies | — |
| 104 | 7 | `TuiDebugger`: split-pane TUI with bytecode listing, register state, and output log | 103 |
| 105 | 7 | Wire `debug` command into engine REPL mode | 104 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

The engine REPL already supports `step` and `run` commands that print output line-by-line. This phase replaces that text-scroll workflow with a single-screen TUI debugger: the currently-executing instruction is highlighted, registers update in place, and yielded rows accumulate in an output log — all visible at once without scrolling.

The debugger is launched from the existing engine REPL mode with a new `debug` command. It takes over the terminal for the duration of the session, then hands control back to the normal rustyline REPL when the user quits.

---

## 103. Add `ratatui` + `crossterm` dependencies (Track 7)

### What Changes

Add three crate dependencies to `Cargo.toml`:

```toml
ratatui = "0.30"
crossterm = "0.29"
ansi-to-tui = "8"
```

`ratatui` is the TUI framework (widgets, layout). `crossterm` is the cross-platform terminal backend (raw mode, key events, cursor). `ansi-to-tui` converts ANSI-escape strings to ratatui `Text`/`Spans` — this lets us reuse the existing `colored`-based `Display` impls on `Operation`, `Reg`, `JumpTarget`, and `ScalarValue` directly in the TUI without duplicating any formatting logic.

### Background

`ratatui` is the actively maintained successor to `tui-rs`. It works on Linux, macOS, and Windows via `crossterm`. Ratatui re-exports crossterm internally, but we add it as an explicit dependency so `enable_raw_mode`, `ExecutableCommand`, and `event::*` imports compile without relying on ratatui's re-export stability.

`ansi-to-tui` (v8, released January 2026) is part of the official ratatui GitHub organisation. Its API is a single trait:

```rust
use ansi_to_tui::IntoText as _;

let text = format!("{op}").into_text()?;  // Text with cyan/yellow/magenta preserved
```

This means the bytecode pane and register pane inherit all the colors from `program.rs` for free — `Store` in cyan+bold, register names in yellow, jump targets in magenta, etc. The only additional ratatui styling we apply is the `►` current-PC row highlight (yellow background or bold).

Note: `ansi-to-tui` depends on `ratatui-core ^0.1.0` (the extracted core-types package). This is compatible with `ratatui 0.30` — verify with `cargo build` that there are no version conflicts.

Adding these deps does not affect the existing `rustyline`-based REPL.

### Key Files

- `Cargo.toml` — three new dependencies

### Tests

None — just verify `cargo build` succeeds.

### Implementation Steps (1 commit)

#### Step 103.1 — Add ratatui, crossterm, and ansi-to-tui to Cargo.toml

**Commit:** `Deps: add ratatui, crossterm, and ansi-to-tui for TUI debugger`

---

## 104. `TuiDebugger`: split-pane TUI (Track 7)

### What Changes

New module `src/repl/tui_debugger.rs` that implements a self-contained TUI session. It owns a `StepState` (re-used from engine mode), renders three panes, and handles keyboard input until the user quits.

**Layout:**

```
┌─ Bytecode ──────────────────────────┐ ┌─ Registers ──────────────────────┐
│    0  Open(r0, 1, ReadOnly)         │ │  r0 = <cursor>                   │
│    1  MoveCursor(r0, First)         │ │  r1 = NULL                       │
│  ► 2  ReadCursor(r0, r1)            │ │  r2 = 42                         │
│    3  GoToIfFalse(r1, @6)           │ │                                  │
│    4  Yield([r1])                   │ │                                  │
│    5  GoTo(@2)                      │ │                                  │
│    6  Halt                          │ │                                  │
└─────────────────────────────────────┘ └──────────────────────────────────┘
┌─ Output ──────────────────────────────────────────────────────────────────┐
│  Step 2: ReadCursor(r0, r1)                                               │
│  Step 3: GoToIfFalse(r1, @6) → continue                                  │
│  Step 4: Yield([r1])  →  row: 42                                          │
│  Step 5: GoTo(@2)                                                         │
└───────────────────────────────────────────────────────────────────────────┘
 [Space / n] Step   [r] Run to next yield   [R] Restart   [q] Quit
```

The bytecode pane scrolls to keep the current instruction (`►`) visible. The register pane shows all non-empty registers. The output pane accumulates a history of executed steps.

**Key bindings:**

| Key | Action |
|-----|--------|
| `Space` or `n` | Execute one instruction |
| `r` | Run until the next `Yield` or `Halt` |
| `R` | Restart execution from instruction 0 |
| `q` or `Esc` | Exit TUI, return to rustyline REPL |

### Background

`ratatui` uses an immediate-mode rendering model: each frame the app clears and redraws all widgets. The main loop is:

1. Draw current state
2. Poll for a key event (blocking with a timeout)
3. Update state based on key
4. Repeat

Before entering the loop, we switch the terminal to raw mode (`crossterm::terminal::enable_raw_mode`) and hide the cursor. On exit (or panic), we restore the terminal.

### Implementation Approach

```rust
// src/repl/tui_debugger.rs

use crossterm::{
    event::{self, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Terminal,
};

pub struct TuiDebugger {
    program: CompiledProgram,
    step_state: StepState,
    output_log: Vec<String>,
}

impl TuiDebugger {
    pub fn new(program: CompiledProgram, btree: BTree) -> Self {
        let step_state = StepState::new(&program, btree);
        TuiDebugger {
            program,
            step_state,
            output_log: Vec::new(),
        }
    }

    pub fn run(mut self) -> crossterm::Result<()> {
        enable_raw_mode()?;
        let mut stdout = std::io::stdout();
        stdout.execute(EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let result = self.event_loop(&mut terminal);

        // Always restore terminal
        disable_raw_mode()?;
        terminal.backend_mut().execute(LeaveAlternateScreen)?;
        result
    }

    fn event_loop(&mut self, terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>) -> crossterm::Result<()> {
        loop {
            terminal.draw(|f| self.draw(f))?;

            if event::poll(std::time::Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => break,
                        KeyCode::Char(' ') | KeyCode::Char('n') => self.do_step(),
                        KeyCode::Char('r') => self.do_run_to_yield(),
                        KeyCode::Char('R') => self.do_restart(),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    fn draw(&self, frame: &mut ratatui::Frame) {
        // Top row: bytecode (left 60%) | registers (right 40%)
        let top_bottom = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
            .split(frame.area());

        let top_row = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(top_bottom[0]);

        self.draw_bytecode(frame, top_row[0]);
        self.draw_registers(frame, top_row[1]);
        self.draw_output(frame, top_bottom[1]);
    }

    fn draw_bytecode(&self, frame: &mut ratatui::Frame, area: ratatui::layout::Rect) {
        use ansi_to_tui::IntoText as _;
        let pc = self.step_state.pc;
        let items: Vec<ListItem> = self.program.operations.iter().enumerate().map(|(i, op)| {
            // format!("{op}") produces ANSI-escaped output (cyan opcodes, yellow regs, magenta jumps)
            // into_text() converts those escapes to ratatui Spans, preserving all colors.
            let prefix = if i == pc { "► " } else { "  " };
            let ansi_str = format!("{prefix}{i:4}  {op}");
            let mut text = ansi_str.into_text().unwrap_or_default();
            // Highlight the current PC row with a bold underline on top of the existing colors
            if i == pc {
                for line in &mut text.lines {
                    for span in &mut line.spans {
                        span.style = span.style.add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
                    }
                }
            }
            ListItem::new(text)
        }).collect();

        let list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title(" Bytecode "));
        frame.render_widget(list, area);
    }

    fn draw_registers(&self, frame: &mut ratatui::Frame, area: ratatui::layout::Rect) {
        use ansi_to_tui::IntoText as _;
        // format!("{s}") for ScalarValue produces green ANSI output; into_text() preserves it.
        let text: Vec<Line> = self.step_state.engine.registers().iter()
            .filter_map(|(i, val)| {
                use database::engine::registers::RegisterValue;
                let ansi_str = match val {
                    RegisterValue::None => return None,
                    RegisterValue::ScalarValue(s) => format!("  R{i} = {s}"),
                    RegisterValue::CursorHandle(_) => format!("  R{i} = <cursor>"),
                    RegisterValue::RowBuffer(_) => format!("  R{i} = <rowbuffer>"),
                    RegisterValue::GroupTable(_) => format!("  R{i} = <grouptable>"),
                };
                // into_text() returns a Text (multi-line); take first line only
                ansi_str.into_text().ok()?.lines.into_iter().next()
            })
            .collect();

        let para = Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL).title(" Registers "));
        frame.render_widget(para, area);
    }

    fn draw_output(&self, frame: &mut ratatui::Frame, area: ratatui::layout::Rect) {
        let text: Vec<Line> = self.output_log.iter()
            .map(|s| Line::from(s.as_str()))
            .collect();
        let para = Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL)
                .title(" Output  [Space/n] Step  [r] Run to Yield  [R] Restart  [q] Quit "));
        frame.render_widget(para, area);
    }
}
```

**StepState reuse**: `StepState` from `engine.rs` is currently a private struct. Move it to a `pub(crate)` struct in a shared location (or duplicate minimally in `tui_debugger.rs`). The simplest path: keep `StepState` in `engine.rs` and make it `pub(crate)`, then import it in `tui_debugger.rs`.

**Terminal cleanup on panic**: Wrap the event loop in a `std::panic::catch_unwind` guard that calls `disable_raw_mode` + `LeaveAlternateScreen` before re-panicking, so the terminal is never left in raw mode.

### Key Files

- `src/repl/tui_debugger.rs` — new module, `TuiDebugger` struct and impl
- `src/repl/modes/engine.rs` — make `StepState` `pub(crate)`
- `src/repl/mod.rs` — declare `tui_debugger` module

### Tests

No automated tests for the TUI (visual output). Verify manually:

```
engine> compile SELECT id FROM users
engine> debug
# TUI launches: bytecode pane shows instructions, ► on instruction 0
# Press Space several times: ► advances, registers update
# Press r: runs to next Yield, output log shows row
# Press R: resets to instruction 0
# Press q: returns to rustyline prompt
```

### Implementation Steps (1 commit)

#### Step 104.1 — Implement TuiDebugger with three-pane layout

Add `tui_debugger.rs`, make `StepState` `pub(crate)`, declare the module.

**Commit:** `Feature: TUI bytecode debugger with split-pane instruction/register view`

---

## 105. Wire `debug` command into engine REPL mode (Track 7)

### What Changes

Add one new command arm to `EngineMode::execute`:

```rust
["debug"] => {
    let program = match &self.program {
        Some(p) => p.clone(),
        None => return CommandResult::Message(
            "No program loaded. Use 'compile <sql>' first.".to_string()
        ),
    };
    let debugger = TuiDebugger::new(program, (*shared.btree).clone());
    match debugger.run() {
        Ok(()) => CommandResult::Message("Debugger exited.".to_string()),
        Err(e) => CommandResult::Error(format!("TUI error: {e}")),
    }
}
```

Update the help text:

```
debug               Launch interactive TUI debugger (step through bytecode visually)
```

### Key Files

- `src/repl/modes/engine.rs` — new `["debug"]` arm and help text update

### Tests

None beyond manual verification from item 104.

### Implementation Steps (1 commit)

#### Step 105.1 — Add `debug` command to engine mode

Wire `TuiDebugger::new(...).run()` into the `["debug"]` arm; update help text.

**Commit:** `Feature: engine REPL 'debug' command launches TUI bytecode debugger`

---

## Verification

- [ ] `cargo test` — all tests pass (no regressions)
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `engine> compile SELECT 1 + 2` followed by `engine> debug` launches the TUI without errors
- [ ] Space/n steps one instruction; `►` moves to next instruction; registers update
- [ ] `r` runs to next `Yield` or `Halt`; output log shows the yielded row
- [ ] `R` resets the PC to 0 and clears output log
- [ ] `q` / Esc exits cleanly; rustyline prompt reappears with terminal fully restored
- [ ] Running `debug` without a compiled program shows an error message, not a crash
- [ ] Terminal is restored correctly even if an instruction returns an `Err`
- [ ] Each commit is independently buildable
