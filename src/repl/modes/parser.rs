use crate::frontend::{lexer, parse};
use crate::repl::{CommandResult, Mode, ModeId, SharedState};

/// Parser mode - stateless, for inspecting tokenization and parsing
#[derive(Debug)]
pub struct ParserMode;

impl ParserMode {
    pub fn new() -> Self {
        ParserMode
    }
}

impl Mode for ParserMode {
    fn id(&self) -> ModeId {
        ModeId::Parser
    }

    fn execute(&mut self, tokens: &[&str], _shared: &mut SharedState) -> CommandResult {
        match tokens {
            ["tokenize", rest @ ..] | ["lex", rest @ ..] => {
                let sql = rest.join(" ");
                if sql.is_empty() {
                    return CommandResult::Error("Usage: tokenize <sql>".to_string());
                }
                let tokens = lexer::lex(&sql);
                CommandResult::Message(format!("Tokens:\n{:#?}", tokens))
            }

            ["parse", rest @ ..] | ["ast", rest @ ..] => {
                let sql = rest.join(" ");
                if sql.is_empty() {
                    return CommandResult::Error("Usage: parse <sql>".to_string());
                }
                match parse(&sql) {
                    Ok(ast) => CommandResult::Message(format!("AST:\n{:#?}", ast)),
                    Err(e) => CommandResult::Error(format!("Parse error: {:?}", e)),
                }
            }

            ["both", rest @ ..] => {
                let sql = rest.join(" ");
                if sql.is_empty() {
                    return CommandResult::Error("Usage: both <sql>".to_string());
                }
                let tokens = lexer::lex(&sql);
                let ast_result = parse(&sql);

                let mut output = format!("Tokens:\n{:#?}\n\n", tokens);
                match ast_result {
                    Ok(ast) => output += &format!("AST:\n{:#?}", ast),
                    Err(e) => output += &format!("Parse error: {:?}", e),
                }
                CommandResult::Message(output)
            }

            _ => CommandResult::NotHandled,
        }
    }

    fn help(&self) -> String {
        r#"Parser mode commands:
  tokenize <sql>    Show lexer tokens for SQL input
  lex <sql>         Alias for tokenize
  parse <sql>       Parse SQL and show AST
  ast <sql>         Alias for parse
  both <sql>        Show both tokens and AST"#
            .to_string()
    }
}
