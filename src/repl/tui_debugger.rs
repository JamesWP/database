use ansi_to_tui::IntoText as _;
use colored::Colorize as _;
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
use database::db::build_explain_schema;
use database::engine::registers::RegisterValue;
use database::engine::scalarvalue::ScalarValue;
use database::engine::{Engine, StepSuccess};
use database::explain::format_plan;
use database::planner::LogicalPlan;
use database::storage::BTree;

/// Outcome of a single VM step, used to control `do_run_to_yield`.
enum StepOutcome {
    Continue,
    Stop, // Yield, Halt, or Error — caller should break
}

pub struct TuiDebugger<'a> {
    program: CompiledProgram,
    btree: &'a BTree,
    engine: Engine,
    halted: bool,
    output_log: Vec<String>,
    /// Rows yielded so far, for the query results pane.
    yielded_rows: Vec<Vec<ScalarValue>>,
    /// Cached rendered results table; rebuilt only when `yielded_rows` changes.
    results_cache: Vec<Line<'static>>,
    /// Pre-rendered SQL query lines for the header pane (immutable after construction).
    sql_display: Vec<Line<'static>>,
    /// Pre-rendered EXPLAIN plan lines for the header pane (immutable after construction).
    plan_display: Vec<Line<'static>>,
    /// Height of the header pane in terminal rows (immutable after construction).
    header_height: u16,
}

impl<'a> TuiDebugger<'a> {
    pub fn new(
        program: CompiledProgram,
        btree: &'a BTree,
        source_sql: String,
        logical_plan: Option<LogicalPlan>,
    ) -> Self {
        let engine = Engine::from_compiled_with_btree(&program, btree);

        let sql_display: Vec<Line<'static>> = source_sql
            .lines()
            .map(|l| Line::from(format!(" {l}")))
            .collect();

        let plan_strs: Vec<String> = if let Some(plan) = &logical_plan {
            let schema = build_explain_schema(btree);
            format_plan(plan, &schema)
                .into_iter()
                .map(|(id, text)| format!("{id:>3}  {text}"))
                .collect()
        } else {
            vec!["(no plan)".to_string()]
        };
        let plan_display: Vec<Line<'static>> = plan_strs
            .iter()
            .map(|l| Line::from(format!(" {l}")))
            .collect();

        let header_height = (sql_display.len().max(plan_display.len()).max(1) + 2) as u16;

        TuiDebugger {
            program,
            btree,
            engine,
            halted: false,
            output_log: Vec::new(),
            yielded_rows: Vec::new(),
            results_cache: Vec::new(),
            sql_display,
            plan_display,
            header_height,
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

    /// Execute one VM instruction, append to the log, and record any yielded row.
    /// Returns `Stop` when the caller should break out of a run loop.
    fn execute_one_step(&mut self) -> StepOutcome {
        let pc = self.engine.pc();
        let op_plain = strip_ansi(
            &self
                .program
                .operations
                .get(pc)
                .map(|o| format!("{o}"))
                .unwrap_or_default(),
        );
        match self.engine.step() {
            Ok(StepSuccess::Halt) => {
                self.halted = true;
                self.output_log
                    .push(format!("Step {pc}: {op_plain}  → [halted]"));
                StepOutcome::Stop
            }
            Ok(StepSuccess::Yield(values)) => {
                let row_display: Vec<String> = values.iter().map(|v| format!("{v}")).collect();
                self.output_log.push(format!(
                    "Step {pc}: {op_plain}  → row: {}",
                    row_display.join(", ")
                ));
                self.yielded_rows.push(values);
                self.rebuild_results_cache();
                StepOutcome::Stop
            }
            Ok(StepSuccess::Continue) => {
                self.output_log.push(format!("Step {pc}: {op_plain}"));
                StepOutcome::Continue
            }
            Err(e) => {
                self.halted = true;
                self.output_log
                    .push(format!("Step {pc}: {op_plain}  → error: {e:?}"));
                StepOutcome::Stop
            }
        }
    }

    fn do_step(&mut self) {
        if self.halted {
            self.output_log
                .push("Halted. Press R to restart.".to_string());
            return;
        }
        self.execute_one_step();
    }

    fn do_run_to_yield(&mut self) {
        while !self.halted {
            if matches!(self.execute_one_step(), StepOutcome::Stop) {
                break;
            }
        }
    }

    fn do_restart(&mut self) {
        self.engine = Engine::from_compiled_with_btree(&self.program, self.btree);
        self.halted = false;
        self.output_log.clear();
        self.yielded_rows.clear();
        self.results_cache.clear();
        self.output_log.push("Restarted.".to_string());
    }

    /// Rebuild the cached results table lines from `yielded_rows`.
    /// Called only when a new row is yielded or on restart.
    fn rebuild_results_cache(&mut self) {
        let text = build_results_table(&self.program.column_names, &self.yielded_rows);
        self.results_cache = text
            .lines()
            .filter_map(|s| {
                format!(" {s}")
                    .into_text()
                    .ok()
                    .and_then(|t| t.lines.into_iter().next())
            })
            .collect();
    }

    fn draw(&self, frame: &mut ratatui::Frame) {
        let pc = self.engine.pc();

        let outer = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(self.header_height), Constraint::Min(0)])
            .split(frame.area());
        let inner = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(outer[1]);

        let header_row = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(outer[0]);

        let top_row = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(inner[0]);

        let bottom_row = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(inner[1]);

        // ── SQL query pane ─────────────────────────────────────────────────────
        let sql_para = Paragraph::new(self.sql_display.clone())
            .block(Block::default().borders(Borders::ALL).title(" Query "));
        frame.render_widget(sql_para, header_row[0]);

        // ── Query plan pane ────────────────────────────────────────────────────
        let plan_para = Paragraph::new(self.plan_display.clone())
            .block(Block::default().borders(Borders::ALL).title(" Plan "));
        frame.render_widget(plan_para, header_row[1]);

        // ── Bytecode pane ──────────────────────────────────────────────────────
        let items: Vec<ListItem> = self
            .program
            .operations
            .iter()
            .enumerate()
            .map(|(i, op)| {
                let prefix = if i == pc && !self.halted {
                    "► "
                } else {
                    "  "
                };
                let ansi_str = format!("{prefix}{i:4}  {op}");
                let mut text = ansi_str.into_text().unwrap_or_default();
                if i == pc && !self.halted {
                    for line in &mut text.lines {
                        for span in &mut line.spans {
                            span.style = span
                                .style
                                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
                        }
                    }
                }
                ListItem::new(text)
            })
            .collect();

        let mut list_state = ListState::default();
        if !self.halted {
            list_state.select(Some(pc));
        }

        let bytecode_list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title(" Bytecode "))
            .highlight_style(Style::default().add_modifier(Modifier::BOLD));
        frame.render_stateful_widget(bytecode_list, top_row[0], &mut list_state);

        // ── Registers pane ─────────────────────────────────────────────────────
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
                ansi_str.into_text().ok()?.lines.into_iter().next()
            })
            .collect();

        let reg_para = Paragraph::new(reg_lines)
            .block(Block::default().borders(Borders::ALL).title(" Registers "));
        frame.render_widget(reg_para, top_row[1]);

        // ── Step log pane ──────────────────────────────────────────────────────
        let log_lines: Vec<Line> = self
            .output_log
            .iter()
            .filter_map(|s| s.into_text().ok().and_then(|t| t.lines.into_iter().next()))
            .collect();
        let log_scroll = self
            .output_log
            .len()
            .saturating_sub(bottom_row[0].height as usize - 2) as u16;
        let log_para = Paragraph::new(log_lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Steps  [Space/n] Step  [r] Run  [R] Restart  [q] Quit "),
            )
            .scroll((log_scroll, 0));
        frame.render_widget(log_para, bottom_row[0]);

        // ── Query results pane (pre-rendered cache) ────────────────────────────
        let results_scroll = self
            .results_cache
            .len()
            .saturating_sub(bottom_row[1].height as usize - 2) as u16;
        let results_para = Paragraph::new(self.results_cache.clone())
            .block(Block::default().borders(Borders::ALL).title(" Results "))
            .scroll((results_scroll, 0));
        frame.render_widget(results_para, bottom_row[1]);
    }
}

/// Build an ANSI-colored results table string matching the SQL REPL style:
/// bold-white column headers, gray separators, green cell values.
fn build_results_table(column_names: &[String], rows: &[Vec<ScalarValue>]) -> String {
    if rows.is_empty() && column_names.is_empty() {
        return "(no rows yet)".to_string();
    }

    let num_cols = if !rows.is_empty() {
        rows[0].len()
    } else {
        column_names.len()
    };

    let plain_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(|v| v.plain_string()).collect())
        .collect();

    let mut col_widths = vec![0usize; num_cols];
    for (i, name) in column_names.iter().enumerate() {
        if i < num_cols {
            col_widths[i] = col_widths[i].max(name.len());
        }
    }
    for row in &plain_rows {
        for (i, cell) in row.iter().enumerate() {
            if i < num_cols {
                col_widths[i] = col_widths[i].max(cell.len());
            }
        }
    }

    let sep_col = |s: &str| s.truecolor(90, 90, 90).to_string();
    let mut out = String::new();

    if !column_names.is_empty() {
        let header: Vec<String> = column_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let w = col_widths.get(i).copied().unwrap_or(name.len());
                format!("{:<width$}", name.bold().white(), width = w)
            })
            .collect();
        out += &header.join(&sep_col(" │ "));
        out += "\n";
        let sep: Vec<String> = col_widths
            .iter()
            .map(|w| sep_col(&"─".repeat(*w)))
            .collect();
        out += &sep.join(&sep_col("─┼─"));
        out += "\n";
    }

    if rows.is_empty() {
        out += "(no rows yet)";
        return out;
    }

    for plain_row in &plain_rows {
        let cells: Vec<String> = plain_row
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                let w = col_widths.get(i).copied().unwrap_or(cell.len());
                format!("{:width$}", cell.green(), width = w)
            })
            .collect();
        out += &cells.join(&sep_col(" │ "));
        out += "\n";
    }

    out += &format!(
        "({} row{})",
        rows.len(),
        if rows.len() == 1 { "" } else { "s" }
    );
    out
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
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
