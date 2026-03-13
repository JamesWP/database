use ansi_to_tui::IntoText as _;
use crossterm::{
    event::{self, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Modifier, Style},
    text::Line,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Terminal,
};

use database::compiler::CompiledProgram;
use database::engine::{Engine, StepSuccess};
use database::engine::registers::RegisterValue;
use database::storage::BTree;

pub struct TuiDebugger {
    program: CompiledProgram,
    btree: BTree,
    engine: Engine,
    halted: bool,
    output_log: Vec<String>,
}

impl TuiDebugger {
    pub fn new(program: CompiledProgram, btree: BTree) -> Self {
        let engine = Engine::from_compiled_with_btree(&program, btree.clone());
        TuiDebugger {
            program,
            btree,
            engine,
            halted: false,
            output_log: Vec::new(),
        }
    }

    pub fn run(mut self) -> std::io::Result<()> {
        enable_raw_mode()?;
        let mut stdout = std::io::stdout();
        stdout.execute(EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.event_loop(&mut terminal)
        }));

        disable_raw_mode()?;
        terminal.backend_mut().execute(LeaveAlternateScreen)?;

        match result {
            Ok(r) => r,
            Err(_) => Err(std::io::Error::other("TUI debugger panicked")),
        }
    }

    fn event_loop(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    ) -> std::io::Result<()> {
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

    fn do_step(&mut self) {
        if self.halted {
            self.output_log
                .push("Halted. Press R to restart.".to_string());
            return;
        }
        let pc = self.engine.pc();
        let op_str = self
            .program
            .operations
            .get(pc)
            .map(|o| format!("{o}"))
            .unwrap_or_default();
        // Strip ANSI for the log
        let op_plain = strip_ansi(&op_str);
        match self.engine.step() {
            Ok(StepSuccess::Halt) => {
                self.halted = true;
                self.output_log
                    .push(format!("Step {pc}: {op_plain}  → [halted]"));
            }
            Ok(StepSuccess::Yield(values)) => {
                let row: Vec<String> = values.iter().map(|v| format!("{v}")).collect();
                self.output_log.push(format!(
                    "Step {pc}: {op_plain}  → row: {}",
                    row.join(", ")
                ));
            }
            Ok(StepSuccess::Continue) => {
                self.output_log.push(format!("Step {pc}: {op_plain}"));
            }
            Err(e) => {
                self.halted = true;
                self.output_log
                    .push(format!("Step {pc}: {op_plain}  → error: {e:?}"));
            }
        }
    }

    fn do_run_to_yield(&mut self) {
        loop {
            if self.halted {
                break;
            }
            let pc = self.engine.pc();
            let op_str = self
                .program
                .operations
                .get(pc)
                .map(|o| format!("{o}"))
                .unwrap_or_default();
            let op_plain = strip_ansi(&op_str);
            match self.engine.step() {
                Ok(StepSuccess::Halt) => {
                    self.halted = true;
                    self.output_log
                        .push(format!("Step {pc}: {op_plain}  → [halted]"));
                    break;
                }
                Ok(StepSuccess::Yield(values)) => {
                    let row: Vec<String> = values.iter().map(|v| format!("{v}")).collect();
                    self.output_log.push(format!(
                        "Step {pc}: {op_plain}  → row: {}",
                        row.join(", ")
                    ));
                    break;
                }
                Ok(StepSuccess::Continue) => {
                    self.output_log.push(format!("Step {pc}: {op_plain}"));
                }
                Err(e) => {
                    self.halted = true;
                    self.output_log
                        .push(format!("Step {pc}: {op_plain}  → error: {e:?}"));
                    break;
                }
            }
        }
    }

    fn do_restart(&mut self) {
        self.engine = Engine::from_compiled_with_btree(&self.program, self.btree.clone());
        self.halted = false;
        self.output_log.clear();
        self.output_log.push("Restarted.".to_string());
    }

    fn draw(&self, frame: &mut ratatui::Frame) {
        let pc = self.engine.pc();

        let top_bottom = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
            .split(frame.area());

        let top_row = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(top_bottom[0]);

        // Bytecode pane
        let items: Vec<ListItem> = self
            .program
            .operations
            .iter()
            .enumerate()
            .map(|(i, op)| {
                let prefix = if i == pc && !self.halted { "► " } else { "  " };
                let ansi_str = format!("{prefix}{i:4}  {op}");
                let mut text = ansi_str.into_text().unwrap_or_default();
                if i == pc && !self.halted {
                    for line in &mut text.lines {
                        for span in &mut line.spans {
                            span.style =
                                span.style.add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
                        }
                    }
                }
                ListItem::new(text)
            })
            .collect();

        // Scroll so PC is visible
        let mut list_state = ListState::default();
        if !self.halted {
            list_state.select(Some(pc));
        }

        let bytecode_list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title(" Bytecode "))
            .highlight_style(Style::default().add_modifier(Modifier::BOLD));
        frame.render_stateful_widget(bytecode_list, top_row[0], &mut list_state);

        // Registers pane
        let reg_lines: Vec<Line> = self
            .engine
            .registers()
            .iter()
            .filter_map(|(i, val)| {
                let ansi_str = match val {
                    RegisterValue::None => return None,
                    RegisterValue::ScalarValue(s) => format!("  r{i} = {s}"),
                    RegisterValue::CursorHandle(_) => format!("  r{i} = <cursor>"),
                    RegisterValue::RowBuffer(_) => format!("  r{i} = <rowbuffer>"),
                    RegisterValue::GroupTable(_) => format!("  r{i} = <grouptable>"),
                };
                ansi_str
                    .into_text()
                    .ok()?
                    .lines
                    .into_iter()
                    .next()
            })
            .collect();

        let reg_para = Paragraph::new(reg_lines)
            .block(Block::default().borders(Borders::ALL).title(" Registers "));
        frame.render_widget(reg_para, top_row[1]);

        // Output pane
        let output_lines: Vec<Line> = self
            .output_log
            .iter()
            .map(|s| Line::from(s.as_str()))
            .collect();
        let output_para = Paragraph::new(output_lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Output  [Space/n] Step  [r] Run to Yield  [R] Restart  [q] Quit "),
            )
            .scroll((
                self.output_log
                    .len()
                    .saturating_sub(top_bottom[1].height as usize - 2) as u16,
                0,
            ));
        frame.render_widget(output_para, top_bottom[1]);
    }
}

fn strip_ansi(s: &str) -> String {
    // Simple ANSI escape stripper for the output log
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Skip until 'm'
            for ch in chars.by_ref() {
                if ch == 'm' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}
