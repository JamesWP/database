use proptest::strategy::W;

use super::{ast, lexer};

struct ParserInput {
    tokens: Vec<lexer::Token>,
    curent: usize,
}

struct Parser {
    input: ParserInput,
}

#[derive(Debug)]
pub enum ParseError {
    UnexpectedToken(Expect, lexer::Type),
}

type ParseResult<T> = std::result::Result<T, ParseError>;

impl ParserInput {
    pub fn peek(&mut self) -> lexer::Type {
        todo!()
    }
    pub fn advance(&mut self) -> &lexer::Token {
        if !self.is_at_end() {
            self.curent += 1;
        }
        self.previous()
    }

    fn is_at_end(&self) -> bool {
        self.curent >= self.tokens.len()
    }

    fn previous(&self) -> &lexer::Token {
        &self.tokens[self.curent - 1]
    }

    fn expect(&mut self, t: Expect) -> ParseResult<()> {
        match (t, self.peek()) {
            (Expect::RightParen, lexer::Type::RightParen) => Ok(()),
            (expectation, actuality) => Err(ParseError::UnexpectedToken(expectation, actuality)),
        }
    }
}

enum BinaryCategory {
    Equality,
    Relational,
    Shift,
    Additive,
    Multiplicative,
}

#[derive(Debug)]
pub enum Expect {
    RightParen,
}

impl lexer::Type {
    fn into(self, category: BinaryCategory) -> Option<ast::BinaryOp> {
        use BinaryCategory::*;
        match (category, self) {
            (Equality, lexer::Type::BangEqual) => Some(ast::BinaryOp::NotEquals),
            (Equality, lexer::Type::EqualEqual) => Some(ast::BinaryOp::Equals),
            (Relational, lexer::Type::Less) => Some(ast::BinaryOp::LessThan),
            (Relational, lexer::Type::LessEqual) => Some(ast::BinaryOp::LessThanOrEqual),
            (Relational, lexer::Type::Greater) => Some(ast::BinaryOp::GreaterThan),
            (Relational, lexer::Type::GreaterEqual) => Some(ast::BinaryOp::GreaterThanOrEqual),
            (Shift, lexer::Type::LeftShift) => Some(ast::BinaryOp::LeftBitShift),
            (Shift, lexer::Type::RightShift) => Some(ast::BinaryOp::RightBitShift),
            (Additive, lexer::Type::Plus) => Some(ast::BinaryOp::Sum),
            (Additive, lexer::Type::Minus) => Some(ast::BinaryOp::Difference),
            (Multiplicative, lexer::Type::Star) => Some(ast::BinaryOp::Product),
            (Multiplicative, lexer::Type::Slash) => Some(ast::BinaryOp::Quotient),
            (Multiplicative, lexer::Type::Percent) => Some(ast::BinaryOp::Remainder),
            _ => None,
        }
    }
}

impl Parser {
    fn new(tokens: Vec<lexer::Token>) -> Parser {
        Parser {
            input: ParserInput { tokens, curent: 0 },
        }
    }

    fn parse_equality(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_relational()?;

        while let Some(op) = self.input.peek().into(BinaryCategory::Equality) {
            self.input.advance();
            let right = self.parse_relational()?;
            expr = ast::Expression::BinaryOp {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(right),
            }
        }

        Ok(expr)
    }

    fn parse_relational(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_shift()?;

        while let Some(op) = self.input.peek().into(BinaryCategory::Relational) {
            self.input.advance();
            let right = self.parse_shift()?;
            expr = ast::Expression::BinaryOp {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(right),
            }
        }

        Ok(expr)
    }

    fn parse_shift(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_additive()?;

        while let Some(op) = self.input.peek().into(BinaryCategory::Shift) {
            self.input.advance();
            let right = self.parse_additive()?;
            expr = ast::Expression::BinaryOp {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(right),
            }
        }

        Ok(expr)
    }

    fn parse_additive(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_multiplicative()?;

        while let Some(op) = self.input.peek().into(BinaryCategory::Additive) {
            self.input.advance();
            let right = self.parse_multiplicative()?;
            expr = ast::Expression::BinaryOp {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(right),
            }
        }

        Ok(expr)
    }

    fn parse_multiplicative(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_unary()?;

        while let Some(op) = self.input.peek().into(BinaryCategory::Multiplicative) {
            self.input.advance();
            let right = self.parse_cast()?;
            expr = ast::Expression::BinaryOp {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(right),
            }
        }

        Ok(expr)
    }

    fn parse_cast(&mut self) -> ParseResult<ast::Expression> {
        match self.input.peek() {
            lexer::Type::LeftParen => {
                self.input.advance();
                let type_name = self.parse_typename()?;
                self.input.expect(Expect::RightParen)?;
                let expr = self.parse_cast()?;
                todo!("Casting");
            }
            _ => self.parse_unary(),
        }
    }

    fn parse_typename(&mut self) -> ParseResult<()> {
        todo!()
    }

    fn parse_unary(&self) -> ParseResult<ast::Expression> {
        todo!()
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
