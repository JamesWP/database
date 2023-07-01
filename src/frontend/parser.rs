use proptest::strategy::W;

use crate::frontend::lexer::lex;

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
        self.tokens[self.curent].tipe()
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
    PrimaryExpression,
    Identifier,
}

impl lexer::Type {
    fn as_binary(self, category: BinaryCategory) -> Option<ast::BinaryOp> {
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

    fn as_unary(self) -> Option<ast::UnaryOp> {
        match self {
            lexer::Type::Plus => Some(ast::UnaryOp::Plus),
            lexer::Type::Bang => Some(ast::UnaryOp::Negate),
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

    fn parse_expression(&mut self) -> ParseResult<ast::Expression> {
        self.parse_equality()
    }

    fn parse_equality(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_relational()?;

        while let Some(op) = self.input.peek().as_binary(BinaryCategory::Equality) {
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

        while let Some(op) = self.input.peek().as_binary(BinaryCategory::Relational) {
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

        while let Some(op) = self.input.peek().as_binary(BinaryCategory::Shift) {
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

        while let Some(op) = self.input.peek().as_binary(BinaryCategory::Additive) {
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

        while let Some(op) = self.input.peek().as_binary(BinaryCategory::Multiplicative) {
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

    fn parse_unary(&mut self) -> ParseResult<ast::Expression> {
        if let Some(op) = self.input.peek().as_unary() {
            self.input.advance();
            let expr = self.parse_cast()?;
            Ok(ast::Expression::UnaryOp {
                op,
                expression: Box::new(expr),
            })
        } else {
            self.parse_postfix()
        }
    }

    fn parse_postfix(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_primary()?;
        loop {
            match self.input.peek() {
                lexer::Type::Dot => {
                    self.input.advance();
                    let identifier = self.parse_identifier()?;
                    expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(Box::new(expr), identifier));
                },
                lexer::Type::LeftParen => todo!(),
                _ => { return Ok(expr); }
            }
        }
    }
    fn parse_identifier(&mut self) -> ParseResult<String> {
        match self.input.peek() {
            lexer::Type::Identifier(id) => {
                self.input.advance();
                Ok(id)
            }
            t => Err(ParseError::UnexpectedToken(Expect::Identifier, t))
        }
    }

    fn parse_primary(&mut self) -> ParseResult<ast::Expression> {
        match self.input.peek() {
            lexer::Type::Identifier(id) => {
                self.input.advance();
                Ok(ast::Expression::Value(ast::ScalarValue::Identifier(id)))
            }
            lexer::Type::IntegerNumber(value) => {
                self.input.advance();
                Ok(ast::Expression::Value(ast::ScalarValue::IntegerNumber(value)))
            }
            lexer::Type::FloatingPointNumber(value) => {
                self.input.advance();
                Ok(ast::Expression::Value(ast::ScalarValue::FloatingNumber(value)))
            }
            lexer::Type::LeftParen => {
                self.input.advance();
                let expr = self.parse_expression()?;
                self.input.expect(Expect::RightParen)?;

                Ok(expr)
            }
            t => Err(ParseError::UnexpectedToken(Expect::PrimaryExpression, t))
           
        }
    }
}

pub fn parse(tokens: Vec<lexer::Token>) -> ParseResult<ast::Statement> {
    todo!()
}

#[cfg(test)]
mod test {
    use crate::frontend::{lexer::lex, parser::parse, parser::Parser, parser::ParserInput};

    #[test]
    fn test() {
        // let input = "select t.col, t.othercol+1, finalcol*2 from tablename as t where col=1 and finalcol>0 limit 23;";
        let input = "t.othercol+1==44+10";
        let output = lex(input);

        let mut p = Parser {
            input: ParserInput { tokens: output, curent: 0 }
        };
        let expr = p.parse_expression();

        let expr = expr.unwrap();

        println!("Expr: {:#?}", expr);

    }
}
