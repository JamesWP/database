use crate::compiler::{compile, CompiledProgram};
use crate::frontend::parse;
use crate::planner::plan;
use crate::repl::{CommandResult, Mode, ModeId, SharedState};

/// Engine/VM mode - for inspecting and executing compiled bytecode
#[derive(Debug)]
pub struct EngineMode {
    /// Compiled program (bytecode)
    program: Option<CompiledProgram>,
}

impl EngineMode {
    pub fn new() -> Self {
        EngineMode { program: None }
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

                let schema = match &shared.schema {
                    Some(s) => s,
                    None => {
                        return CommandResult::Error(
                            "No schema defined. Use planner mode to 'mock schema' first."
                                .to_string(),
                        )
                    }
                };

                match parse(&sql) {
                    Ok(stmt) => match plan(stmt, schema) {
                        Ok(logical_plan) => {
                            let compiled = compile(&logical_plan);
                            let msg = format!(
                                "Compiled: {} operations, {} registers",
                                compiled.operations.len(),
                                compiled.num_registers
                            );
                            self.program = Some(compiled);
                            CommandResult::Message(msg)
                        }
                        Err(e) => CommandResult::Error(format!("Plan error: {:?}", e)),
                    },
                    Err(e) => CommandResult::Error(format!("Parse error: {:?}", e)),
                }
            }

            // Program inspection
            ["program"] | ["show"] => match &self.program {
                Some(p) => {
                    let mut output = format!(
                        "Program ({} ops, {} regs):\n",
                        p.operations.len(),
                        p.num_registers
                    );
                    for (i, op) in p.operations.iter().enumerate() {
                        output += &format!("{:4}: {:?}\n", i, op);
                    }
                    CommandResult::Message(output)
                }
                None => {
                    CommandResult::Message("No program loaded. Use 'compile <sql>' first.".to_string())
                }
            },

            ["clear"] | ["reset"] => {
                self.program = None;
                CommandResult::Message("Program cleared".to_string())
            }

            _ => CommandResult::NotHandled,
        }
    }

    fn help(&self) -> String {
        r#"Engine/VM mode commands:
  compile <sql>   Compile SQL to bytecode (requires schema from planner mode)
  program/show    Show compiled bytecode listing
  clear/reset     Clear compiled program

Note: Full VM execution requires btree integration (future work)"#
            .to_string()
    }
}
