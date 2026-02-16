mod mode;
pub mod modes;
mod shared;

use std::collections::HashMap;
use std::io::Write;

pub use mode::{CommandResult, Mode, ModeFactory, ModeId};
pub use shared::SharedState;

use modes::{BTreeMode, EngineMode, ParserMode, PlannerMode, SqlMode};

pub struct Repl {
    shared: SharedState,
    current_mode: Option<Box<dyn Mode>>,
    mode_factories: HashMap<ModeId, ModeFactory>,
}

impl Repl {
    pub fn new(shared: SharedState) -> Self {
        let mut mode_factories: HashMap<ModeId, ModeFactory> = HashMap::new();

        mode_factories.insert(ModeId::BTree, |shared| Box::new(BTreeMode::new(shared)));
        mode_factories.insert(ModeId::Parser, |_| Box::new(ParserMode::new()));
        mode_factories.insert(ModeId::Planner, |_| Box::new(PlannerMode::new()));
        mode_factories.insert(ModeId::Engine, |_| Box::new(EngineMode::new()));
        mode_factories.insert(ModeId::Sql, |_| Box::new(SqlMode::new()));

        Repl {
            shared,
            current_mode: None,
            mode_factories,
        }
    }

    /// Execute a single command and exit (non-interactive mode)
    /// If the first token is a mode name, enters that mode first
    pub fn run_command(&mut self, command_line: &str) {
        let tokens: Vec<&str> = command_line.split_whitespace().collect();

        if tokens.is_empty() {
            return;
        }

        // Check if first token is a mode name
        if let Ok(mode_id) = ModeId::try_from(tokens[0]) {
            self.enter_mode(mode_id);

            // Execute the rest of the command in that mode
            if tokens.len() > 1 {
                let result = self.handle_command(&tokens[1..]);
                self.handle_result(result);
            }
        } else {
            // Execute as global/root command
            let result = self.handle_command(&tokens);
            self.handle_result(result);
        }
    }

    fn handle_result(&self, result: CommandResult) {
        match result {
            CommandResult::Ok => {}
            CommandResult::Message(msg) => println!("{}", msg),
            CommandResult::SwitchMode(_) => {
                eprintln!("Error: Cannot switch modes in non-interactive mode");
                std::process::exit(1);
            }
            CommandResult::ExitMode => {
                eprintln!("Error: Cannot exit mode in non-interactive mode");
                std::process::exit(1);
            }
            CommandResult::Exit => {}
            CommandResult::NotHandled => {
                eprintln!("Unknown command");
                eprintln!("Type 'help' for available commands");
                std::process::exit(1);
            }
            CommandResult::Error(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }

    pub fn run(&mut self) {
        loop {
            // Print prompt
            let prompt = match &self.current_mode {
                None => "db> ".to_string(),
                Some(mode) => mode.prompt(),
            };
            print!("{}", prompt);
            std::io::stdout().flush().unwrap();

            // Read line
            let mut line = String::new();
            let length = std::io::stdin().read_line(&mut line).unwrap();
            if length == 0 {
                break; // EOF
            }

            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Parse into tokens
            let tokens: Vec<&str> = line.split_whitespace().collect();

            // Handle the command
            let result = self.handle_command(&tokens);

            match result {
                CommandResult::Ok => {}
                CommandResult::Message(msg) => println!("{}", msg),
                CommandResult::SwitchMode(mode_id) => self.enter_mode(mode_id),
                CommandResult::ExitMode => self.exit_mode(),
                CommandResult::Exit => break,
                CommandResult::NotHandled => {
                    println!("Unknown command: '{}'", tokens.join(" "));
                    println!("Type 'help' for available commands");
                }
                CommandResult::Error(e) => println!("Error: {}", e),
            }
        }
    }

    fn handle_command(&mut self, tokens: &[&str]) -> CommandResult {
        // Try global commands first
        if let Some(result) = self.handle_global_command(tokens) {
            return result;
        }

        // If in a mode, delegate to mode
        if let Some(mode) = &mut self.current_mode {
            return mode.execute(tokens, &mut self.shared);
        }

        // In root mode, only global commands are valid
        CommandResult::NotHandled
    }

    fn handle_global_command(&self, tokens: &[&str]) -> Option<CommandResult> {
        match tokens {
            ["exit"] | ["quit"] | ["q"] => Some(CommandResult::Exit),

            ["help"] => Some(CommandResult::Message(self.help())),

            ["mode"] => {
                let msg = match &self.current_mode {
                    None => "Current mode: root".to_string(),
                    Some(mode) => format!("Current mode: {}", mode.id()),
                };
                Some(CommandResult::Message(msg))
            }

            ["modes"] => {
                let mut msg = "Available modes:".to_string();
                for mode_id in ModeId::all() {
                    msg += &format!("\n  {:8} - {}", mode_id.name(), mode_id.description());
                }
                Some(CommandResult::Message(msg))
            }

            ["enter", mode_name] => match ModeId::try_from(*mode_name) {
                Ok(mode_id) => Some(CommandResult::SwitchMode(mode_id)),
                Err(_) => Some(CommandResult::Error(format!("Unknown mode: {}", mode_name))),
            },

            ["back"] | ["leave"] => {
                if self.current_mode.is_some() {
                    Some(CommandResult::ExitMode)
                } else {
                    Some(CommandResult::Message("Already in root mode".to_string()))
                }
            }

            _ => None,
        }
    }

    fn help(&self) -> String {
        match &self.current_mode {
            None => self.root_help(),
            Some(mode) => format!("{}\n\n{}", self.global_help(), mode.help()),
        }
    }

    fn root_help(&self) -> String {
        let mut help = "Available modes:".to_string();
        for mode_id in ModeId::all() {
            help += &format!("\n  {:8} - {}", mode_id.name(), mode_id.description());
        }
        help += "\n\nCommands:";
        help += "\n  enter <mode>  - Enter a mode";
        help += "\n  modes         - List available modes";
        help += "\n  exit          - Exit REPL";
        help
    }

    fn global_help(&self) -> String {
        "Global commands:\n  help          - Show this help\n  exit/quit     - Exit REPL\n  back/leave    - Return to root mode\n  mode          - Show current mode\n  modes         - List available modes\n  enter <mode>  - Switch to a mode".to_string()
    }

    fn enter_mode(&mut self, mode_id: ModeId) {
        // Check if already in this mode
        if let Some(current) = &self.current_mode {
            if current.id() == mode_id {
                println!("Already in {} mode", mode_id);
                return;
            }
        }

        // Drop current mode (if any)
        self.current_mode = None;

        // Create new mode
        if let Some(factory) = self.mode_factories.get(&mode_id) {
            let mode = factory(&mut self.shared);
            println!("Entered {} mode", mode_id);
            self.current_mode = Some(mode);
        }
    }

    fn exit_mode(&mut self) {
        if let Some(mode) = &self.current_mode {
            println!("Left {} mode", mode.id());
        }
        self.current_mode = None;
    }
}
