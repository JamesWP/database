use super::shared::SharedState;

/// Identifies which mode is active
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModeId {
    BTree,
    Parser,
    Planner,
    Engine,
}

impl ModeId {
    pub fn name(&self) -> &'static str {
        match self {
            ModeId::BTree => "btree",
            ModeId::Parser => "parser",
            ModeId::Planner => "planner",
            ModeId::Engine => "engine",
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            ModeId::BTree => "B-tree storage operations",
            ModeId::Parser => "SQL lexer and parser inspection",
            ModeId::Planner => "Query planning and logical plans",
            ModeId::Engine => "VM bytecode execution",
        }
    }

    pub fn all() -> &'static [ModeId] {
        &[ModeId::BTree, ModeId::Parser, ModeId::Planner, ModeId::Engine]
    }
}

impl TryFrom<&str> for ModeId {
    type Error = ();

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "btree" => Ok(ModeId::BTree),
            "parser" => Ok(ModeId::Parser),
            "planner" => Ok(ModeId::Planner),
            "engine" => Ok(ModeId::Engine),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for ModeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Result of executing a command
pub enum CommandResult {
    /// Command executed successfully (no output)
    Ok,
    /// Command executed, print this message
    Message(String),
    /// Switch to a different mode
    SwitchMode(ModeId),
    /// Return to root mode (drops current mode)
    ExitMode,
    /// Exit the REPL
    Exit,
    /// Command not recognized by this mode
    NotHandled,
    /// Error executing command
    Error(String),
}

/// Trait that all modes must implement.
/// Mode lifetime is tied to being "in" the mode - created on enter, dropped on leave.
pub trait Mode: std::fmt::Debug {
    /// Returns the mode identifier
    fn id(&self) -> ModeId;

    /// Returns the prompt string for this mode
    fn prompt(&self) -> String {
        format!("{}> ", self.id().name())
    }

    /// Execute a command. Returns CommandResult.
    /// The command is provided as a slice of whitespace-split tokens.
    fn execute(&mut self, tokens: &[&str], shared: &mut SharedState) -> CommandResult;

    /// Print help for this mode's commands
    fn help(&self) -> String;
}

/// Factory function type for creating modes
pub type ModeFactory = fn(&mut SharedState) -> Box<dyn Mode>;
