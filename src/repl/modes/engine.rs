use colored::Colorize;

use crate::repl::{tui_debugger::TuiDebugger, CommandResult, Mode, ModeId, SharedState};
use database::compiler::{compile, CompiledProgram};
use database::engine::registers::RegisterValue;
use database::engine::{Engine, StepSuccess};
use database::frontend::parse;
use database::planner::plan;
use database::storage::BTree;

struct StepState {
    engine: Engine,
    pc: usize,
    halted: bool,
}

impl StepState {
    fn new(program: &CompiledProgram, btree: BTree) -> Self {
        StepState {
            engine: Engine::from_compiled_with_btree(program, btree),
            pc: 0,
            halted: false,
        }
    }
}

/// Engine/VM mode - for inspecting and executing compiled bytecode
#[derive(Debug)]
pub struct EngineMode {
    /// Compiled program (bytecode)
    program: Option<CompiledProgram>,
    /// Step-by-step execution state
    #[allow(dead_code)]
    step_state: Option<StepState>,
}

impl std::fmt::Debug for StepState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StepState {{ pc: {}, halted: {} }}",
            self.pc, self.halted
        )
    }
}

impl EngineMode {
    pub fn new() -> Self {
        EngineMode {
            program: None,
            step_state: None,
        }
    }

    fn print_registers(engine: &Engine) -> String {
        let mut output = String::new();
        for (i, val) in engine.registers().iter() {
            let display = match val {
                RegisterValue::None => continue,
                RegisterValue::ScalarValue(s) => format!("{s}"),
                RegisterValue::CursorHandle(_) => "<cursor>".to_string(),
                RegisterValue::RowBuffer(_) => "<rowbuffer>".to_string(),
                RegisterValue::GroupTable(_) => "<grouptable>".to_string(),
            };
            output += &format!("     r{i} = {display}\n");
        }
        output
    }
}

impl Mode for EngineMode {
    fn id(&self) -> ModeId {
        ModeId::Engine
    }

    fn execute(&mut self, tokens: &[&str], shared: &mut SharedState) -> CommandResult {
        match tokens {
            // Compilation
            ["compile", rest @ ..] => {
                let sql = rest.join(" ");
                if sql.is_empty() {
                    return CommandResult::Error("Usage: compile <sql>".to_string());
                }

                match parse(&sql) {
                    Ok(stmt) => match plan(stmt, &shared.btree) {
                        Ok(logical_plan) => {
                            let compiled = compile(&logical_plan);
                            let msg = format!(
                                "Compiled: {} operations, {} registers",
                                compiled.operations.len(),
                                compiled.num_registers
                            );
                            self.program = Some(compiled);
                            self.step_state = None;
                            CommandResult::Message(msg)
                        }
                        Err(e) => CommandResult::Error(format!("Plan error: {:?}", e)),
                    },
                    Err(e) => CommandResult::Error(format!("Parse error: {:?}", e)),
                }
            }

            // Program inspection
            ["program"] | ["show"] | ["list"] => match &self.program {
                Some(p) => {
                    let mut output = format!(
                        "Program ({} ops, {} regs):\n",
                        p.operations.len(),
                        p.num_registers
                    );
                    for (i, op) in p.operations.iter().enumerate() {
                        output += &format!("{}  {}\n", format!("{:4}", i).dimmed(), op);
                    }
                    CommandResult::Message(output)
                }
                None => CommandResult::Message(
                    "No program loaded. Use 'compile <sql>' first.".to_string(),
                ),
            },

            // Step-by-step execution
            ["step"] => {
                let program = match &self.program {
                    Some(p) => p,
                    None => {
                        return CommandResult::Message(
                            "No program loaded. Use 'compile <sql>' first.".to_string(),
                        )
                    }
                };

                let state = self
                    .step_state
                    .get_or_insert_with(|| StepState::new(program, (*shared.btree).clone()));

                if state.halted {
                    return CommandResult::Message(
                        "Program halted. Use 'restart' to reset.".to_string(),
                    );
                }

                let pc = state.pc;
                let op = program
                    .operations
                    .get(pc)
                    .map(|o| format!("{o}"))
                    .unwrap_or_default();
                let result = state.engine.step();
                state.pc += 1;

                let mut output = format!("  {}  {}\n", format!("{pc:4}").dimmed(), op);

                match result {
                    Ok(StepSuccess::Halt) => {
                        state.halted = true;
                        output += "     [halted]\n";
                    }
                    Ok(StepSuccess::Yield(values)) => {
                        let row: Vec<String> = values.iter().map(|v| format!("{v}")).collect();
                        output += &format!("     yield: {}\n", row.join(", "));
                        output += &Self::print_registers(&state.engine);
                    }
                    Ok(StepSuccess::Continue) => {
                        output += &Self::print_registers(&state.engine);
                    }
                    Err(e) => {
                        state.halted = true;
                        output += &format!("     error: {e:?}\n");
                    }
                }

                CommandResult::Message(output)
            }

            ["run"] => {
                let program = match &self.program {
                    Some(p) => p,
                    None => {
                        return CommandResult::Message(
                            "No program loaded. Use 'compile <sql>' first.".to_string(),
                        )
                    }
                };

                let state = self
                    .step_state
                    .get_or_insert_with(|| StepState::new(program, (*shared.btree).clone()));

                if state.halted {
                    return CommandResult::Message(
                        "Program halted. Use 'restart' to reset.".to_string(),
                    );
                }

                let mut output = String::new();
                loop {
                    let pc = state.pc;
                    let op = program
                        .operations
                        .get(pc)
                        .map(|o| format!("{o}"))
                        .unwrap_or_default();
                    let result = state.engine.step();
                    state.pc += 1;

                    output += &format!("  {}  {}\n", format!("{pc:4}").dimmed(), op);

                    match result {
                        Ok(StepSuccess::Halt) => {
                            state.halted = true;
                            output += "     [halted]\n";
                            break;
                        }
                        Ok(StepSuccess::Yield(values)) => {
                            let row: Vec<String> = values.iter().map(|v| format!("{v}")).collect();
                            output += &format!("     yield: {}\n", row.join(", "));
                        }
                        Ok(StepSuccess::Continue) => {}
                        Err(e) => {
                            state.halted = true;
                            output += &format!("     error: {e:?}\n");
                            break;
                        }
                    }
                }

                output += &Self::print_registers(&state.engine);
                CommandResult::Message(output)
            }

            ["registers"] | ["regs"] => match &self.step_state {
                Some(state) => {
                    let regs = Self::print_registers(&state.engine);
                    if regs.is_empty() {
                        CommandResult::Message("All registers empty.".to_string())
                    } else {
                        CommandResult::Message(regs)
                    }
                }
                None => {
                    CommandResult::Message("No step state. Use 'step' or 'run' first.".to_string())
                }
            },

            ["restart"] => match &self.program {
                Some(p) => {
                    self.step_state = Some(StepState::new(p, (*shared.btree).clone()));
                    CommandResult::Message("Execution restarted from instruction 0.".to_string())
                }
                None => CommandResult::Message(
                    "No program loaded. Use 'compile <sql>' first.".to_string(),
                ),
            },

            ["debug"] => {
                let program = match &self.program {
                    Some(p) => p.clone(),
                    None => {
                        return CommandResult::Message(
                            "No program loaded. Use 'compile <sql>' first.".to_string(),
                        )
                    }
                };
                let debugger = TuiDebugger::new(program, (*shared.btree).clone());
                match debugger.run() {
                    Ok(()) => CommandResult::Message("Debugger exited.".to_string()),
                    Err(e) => CommandResult::Error(format!("TUI error: {e}")),
                }
            }

            ["clear"] | ["reset"] => {
                self.program = None;
                self.step_state = None;
                CommandResult::Message("Program cleared".to_string())
            }

            _ => CommandResult::NotHandled,
        }
    }

    fn help(&self) -> String {
        r#"Engine/VM mode commands:
  compile <sql>       Compile SQL to bytecode (requires schema from planner mode)
  program/show/list   Show compiled bytecode listing
  step                Execute one instruction, print result and register state
  run                 Run program to completion, printing each yielded row
  registers/regs      Print current register state
  restart             Reset execution to instruction 0
  debug               Launch interactive TUI debugger (step through bytecode visually)
  clear/reset         Clear compiled program"#
            .to_string()
    }
}
