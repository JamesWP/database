use crate::frontend::parse;
use crate::planner::{plan, schema, LogicalPlan};
use crate::repl::{CommandResult, Mode, ModeId, SharedState};

/// Planner mode - for inspecting query plans
#[derive(Debug)]
pub struct PlannerMode {
    /// Last planned query (for inspection)
    last_plan: Option<LogicalPlan>,
}

impl PlannerMode {
    pub fn new() -> Self {
        PlannerMode { last_plan: None }
    }
}

impl Mode for PlannerMode {
    fn id(&self) -> ModeId {
        ModeId::Planner
    }

    fn execute(&mut self, tokens: &[&str], shared: &mut SharedState) -> CommandResult {
        match tokens {
            // Schema management
            ["schema"] => match &shared.schema {
                Some(s) => CommandResult::Message(format!("Schema:\n{:#?}", s)),
                None => CommandResult::Message(
                    "No schema defined. Use 'mock schema' to create a test schema.".to_string(),
                ),
            },

            ["mock", "schema"] => {
                shared.schema = Some(create_mock_schema());
                CommandResult::Message(
                    "Created mock schema with 'users' table (id, name, age)".to_string(),
                )
            }

            ["clear", "schema"] => {
                shared.schema = None;
                self.last_plan = None;
                CommandResult::Message("Schema cleared".to_string())
            }

            // Planning
            ["plan", rest @ ..] => {
                let sql = rest.join(" ");
                if sql.is_empty() {
                    return CommandResult::Error("Usage: plan <sql>".to_string());
                }

                let schema = match &shared.schema {
                    Some(s) => s,
                    None => {
                        return CommandResult::Error(
                            "No schema defined. Use 'mock schema' first.".to_string(),
                        )
                    }
                };

                match parse(&sql) {
                    Ok(stmt) => match plan(stmt, schema) {
                        Ok(logical_plan) => {
                            let msg = format!("LogicalPlan:\n{:#?}", logical_plan);
                            self.last_plan = Some(logical_plan);
                            CommandResult::Message(msg)
                        }
                        Err(e) => CommandResult::Error(format!("Plan error: {:?}", e)),
                    },
                    Err(e) => CommandResult::Error(format!("Parse error: {:?}", e)),
                }
            }

            ["last"] => match &self.last_plan {
                Some(p) => CommandResult::Message(format!("Last plan:\n{:#?}", p)),
                None => {
                    CommandResult::Message("No plan stored. Use 'plan <sql>' first.".to_string())
                }
            },

            _ => CommandResult::NotHandled,
        }
    }

    fn help(&self) -> String {
        r#"Planner mode commands:
  schema          Show current schema
  mock schema     Create a mock schema (users table with id, name, age)
  clear schema    Remove schema
  plan <sql>      Parse and plan SQL query, show logical plan
  last            Show last planned query"#
            .to_string()
    }
}

fn create_mock_schema() -> schema::Schema {
    schema::Schema {
        tables: vec![schema::Table {
            name: "users".to_string(),
            columns: vec![
                schema::Column {
                    name: "id".to_string(),
                },
                schema::Column {
                    name: "name".to_string(),
                },
                schema::Column {
                    name: "age".to_string(),
                },
            ],
        }],
    }
}
