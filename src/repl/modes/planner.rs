use crate::frontend::parse;
use crate::planner::{plan, LogicalPlan};
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
            ["schema"] => {
                // List all tables in the catalog
                match shared.btree.lookup_table("db_schema") {
                    Some((_, sql)) => CommandResult::Message(format!("Catalog DDL: {}", sql)),
                    None => CommandResult::Message("No catalog found".to_string()),
                }
            }

            ["mock", "schema"] => {
                let users_root = shared.btree.create_tree();
                shared.btree.insert_schema_entry(
                    1,
                    "table",
                    "users",
                    "users",
                    users_root,
                    "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
                );
                CommandResult::Message(format!(
                    "Created 'users' table (id, name, age) at page {}",
                    users_root
                ))
            }

            // Planning
            ["plan", rest @ ..] => {
                let sql = rest.join(" ");
                if sql.is_empty() {
                    return CommandResult::Error("Usage: plan <sql>".to_string());
                }

                match parse(&sql) {
                    Ok(stmt) => match plan(stmt, &shared.btree) {
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
  schema          Show catalog DDL
  mock schema     Create a mock 'users' table in the catalog
  plan <sql>      Parse and plan SQL query, show logical plan
  last            Show last planned query"#
            .to_string()
    }
}
