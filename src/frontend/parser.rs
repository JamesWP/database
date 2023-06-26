use super::{lexer, ast};

struct ParserInput {
    tokens: Vec<lexer::Token>,
    index: usize
}

struct Parser {
    input: ParserInput
}

#[derive(Debug)]
pub enum ParseError {

}

type ParseResult<T> = std::result::Result<T, ParseError>;

impl Parser {
    fn new(tokens: Vec<lexer::Token>) -> Parser {
        Parser { input: ParserInput { tokens, index: 0 } }
    }
}

pub fn parse(tokens: &[lexer::Token]) -> ParseResult<ast::Statement> {
    todo!()
}

#[cfg(test)]
mod test {
    use crate::frontend::{lexer::lex, parser::parse};

    #[test]
    fn test() {
        let input = "select t.col, t.othercol+1, finalcol*2 from tablename as t where col=1 and finalcol>0 limit 23;";
        let output = lex(input);

        let out = parse(output.as_slice()).unwrap();

        println!("out: {out:?}");
    }
}
