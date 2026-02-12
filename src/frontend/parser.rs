use super::{ast, lexer};

/// Parse a SQL string into an AST Statement
pub fn parse(sql: &str) -> Result<ast::Statement, ParseError> {
    let tokens = lexer::lex(sql);
    let mut parser = Parser {
        input: ParserInput { tokens, curent: 0 },
    };
    parser.parse_statement()
}

#[derive(Debug)]
pub enum ParseError {
    #[allow(dead_code)]
    UnexpectedToken(Expect, lexer::Type),
}

struct ParserInput {
    tokens: Vec<lexer::Token>,
    curent: usize,
}

struct Parser {
    input: ParserInput,
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
            (Expect::RightParen, lexer::Type::RightParen) => {
                self.advance();
                Ok(())
            }
            (Expect::LeftParen, lexer::Type::LeftParen) => {
                self.advance();
                Ok(())
            }
            (Expect::From, lexer::Type::From) => {
                self.advance();
                Ok(())
            }
            (Expect::Select, lexer::Type::Select) => {
                self.advance();
                Ok(())
            }
            (Expect::Create, lexer::Type::Create) => {
                self.advance();
                Ok(())
            }
            (Expect::Table, lexer::Type::Table) => {
                self.advance();
                Ok(())
            }
            (Expect::Insert, lexer::Type::Insert) => {
                self.advance();
                Ok(())
            }
            (Expect::Into, lexer::Type::Into) => {
                self.advance();
                Ok(())
            }
            (Expect::Values, lexer::Type::Values) => {
                self.advance();
                Ok(())
            }
            // These expectations are not used with `.expect`
            (Expect::PrimaryExpression, _) => panic!("Not implemented"),
            (Expect::Identifier, _) => panic!("Not implemented"),

            // This is an error, we required a token and we didnt find it
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
    LogicalOr,
    LogicalAnd,
    InclusiveOr,
    ExclusiveOr,
    And,
}

#[derive(Debug)]
pub enum Expect {
    LeftParen,
    RightParen,
    PrimaryExpression,
    Identifier,
    From,
    Select,
    Create,
    Table,
    Insert,
    Into,
    Values,
}

impl lexer::Type {
    fn as_binary(self, category: BinaryCategory) -> Option<ast::BinaryOp> {
        use BinaryCategory::*;
        match (category, self) {
            (LogicalOr, lexer::Type::Or) => Some(ast::BinaryOp::Or),
            (LogicalAnd, lexer::Type::And) => Some(ast::BinaryOp::And),
            (InclusiveOr, lexer::Type::Pipe) => Some(ast::BinaryOp::BinaryOr),
            (ExclusiveOr, lexer::Type::Caret) => Some(ast::BinaryOp::BinaryExclusiveOr),
            (And, lexer::Type::Amp) => Some(ast::BinaryOp::BinaryAnd),
            (Equality, lexer::Type::BangEqual) => Some(ast::BinaryOp::NotEquals),
            (Equality, lexer::Type::EqualEqual) => Some(ast::BinaryOp::Equals),
            (Equality, lexer::Type::Equal) => Some(ast::BinaryOp::Equals),
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

/// Parser for statement types
impl Parser {
    pub(crate) fn parse_statement(&mut self) -> ParseResult<ast::Statement> {
        match self.input.peek() {
            lexer::Type::Select => Ok(ast::Statement::Select(self.parse_select_statement()?)),
            lexer::Type::Create => Ok(ast::Statement::CreateTable(
                self.parse_create_table_statement()?,
            )),
            lexer::Type::Insert => Ok(ast::Statement::Insert(self.parse_insert_statement()?)),
            _ => todo!(),
        }
    }

    fn parse_insert_statement(&mut self) -> ParseResult<ast::InsertStatement> {
        self.input.expect(Expect::Insert)?;
        self.input.expect(Expect::Into)?;
        let table_name = self.parse_identifier()?;

        // Optional column list appears before VALUES keyword.
        // We distinguish it from the VALUES rows by checking for the VALUES keyword:
        // if the next token is not VALUES, it must be a column list in parens.
        let columns = match self.input.peek() {
            lexer::Type::Values => None,
            lexer::Type::LeftParen => {
                self.input.expect(Expect::LeftParen)?;
                let mut cols = vec![self.parse_identifier()?];
                while let lexer::Type::Comma = self.input.peek() {
                    self.input.advance();
                    cols.push(self.parse_identifier()?);
                }
                self.input.expect(Expect::RightParen)?;
                Some(cols)
            }
            t => return Err(ParseError::UnexpectedToken(Expect::Values, t)),
        };

        self.input.expect(Expect::Values)?;

        // Parse one or more value rows: (expr, ...) [, (expr, ...)]
        let mut values = vec![self.parse_value_row()?];
        while let lexer::Type::Comma = self.input.peek() {
            self.input.advance();
            values.push(self.parse_value_row()?);
        }

        Ok(ast::InsertStatement {
            table_name,
            columns,
            values,
        })
    }

    fn parse_value_row(&mut self) -> ParseResult<Vec<ast::Expression>> {
        self.input.expect(Expect::LeftParen)?;
        let mut exprs = Vec::new();
        exprs.push(self.parse_expression()?);
        while let lexer::Type::Comma = self.input.peek() {
            self.input.advance();
            exprs.push(self.parse_expression()?);
        }
        self.input.expect(Expect::RightParen)?;
        Ok(exprs)
    }

    fn parse_create_table_statement(&mut self) -> ParseResult<ast::CreateTableStatement> {
        self.input.expect(Expect::Create)?;
        self.input.expect(Expect::Table)?;
        let table_name = self.parse_identifier()?;
        self.input.expect(Expect::LeftParen)?;

        let mut columns = Vec::new();
        columns.push(self.parse_column_def()?);
        while let lexer::Type::Comma = self.input.peek() {
            self.input.advance();
            columns.push(self.parse_column_def()?);
        }

        self.input.expect(Expect::RightParen)?;

        Ok(ast::CreateTableStatement {
            table_name,
            columns,
        })
    }

    fn parse_column_def(&mut self) -> ParseResult<ast::ColumnDef> {
        let name = self.parse_identifier()?;
        let type_name = self.parse_optional_data_type();
        Ok(ast::ColumnDef { name, type_name })
    }

    fn parse_optional_data_type(&mut self) -> Option<ast::DataType> {
        match self.input.peek() {
            lexer::Type::Integer => {
                self.input.advance();
                Some(ast::DataType::Integer)
            }
            lexer::Type::Text => {
                self.input.advance();
                Some(ast::DataType::Text)
            }
            lexer::Type::Real => {
                self.input.advance();
                Some(ast::DataType::Real)
            }
            lexer::Type::Blob => {
                self.input.advance();
                Some(ast::DataType::Blob)
            }
            _ => None,
        }
    }

    fn parse_column_expressions(&mut self) -> ParseResult<Vec<ast::ColumnExpression>> {
        let mut exprs = Vec::new();

        // Parse first column expression (could be * or a regular expression)
        if let lexer::Type::Star = self.input.peek() {
            self.input.advance();
            exprs.push(ast::ColumnExpression::Wildcard);
        } else {
            let expr = self.parse_named_column_expression()?;
            exprs.push(expr);
        }

        // Continue parsing additional column expressions
        loop {
            match self.input.peek() {
                lexer::Type::Comma => {
                    self.input.advance();
                    // Check for * in subsequent positions
                    if let lexer::Type::Star = self.input.peek() {
                        self.input.advance();
                        exprs.push(ast::ColumnExpression::Wildcard);
                    } else {
                        let expr = self.parse_named_column_expression()?;
                        exprs.push(expr);
                    }
                }
                _ => {
                    return Ok(exprs);
                }
            }
        }
    }

    fn parse_named_column_expression(&mut self) -> ParseResult<ast::ColumnExpression> {
        let expr = self.parse_column_expression()?;
        match self.input.peek() {
            lexer::Type::As => {
                self.input.advance();
                let name = self.parse_identifier()?;

                Ok(ast::ColumnExpression::Named {
                    name,
                    expression: Box::new(expr),
                })
            }
            _ => Ok(ast::ColumnExpression::Anonyomous(Box::new(expr))),
        }
    }

    fn parse_named_tuple_source(&mut self) -> ParseResult<ast::NamedTupleSource> {
        let source = self.parse_tuple_source()?;

        match self.input.peek() {
            lexer::Type::As => {
                self.input.advance();
                let alias = self.parse_identifier()?;

                Ok(ast::NamedTupleSource::Named { alias, source })
            }
            _ => Ok(ast::NamedTupleSource::Anonyomous(source)),
        }
    }

    fn parse_tuple_source(&mut self) -> ParseResult<ast::TupleSource> {
        match self.input.peek() {
            lexer::Type::LeftParen => {
                self.input.advance();
                let statement = self.parse_select_statement()?;
                Ok(ast::TupleSource::Subquery(Box::new(statement)))
            }
            _ => {
                let name = self.parse_table_name()?;
                Ok(ast::TupleSource::Table(name))
            }
        }
    }

    fn parse_table_name(&mut self) -> ParseResult<String> {
        self.parse_identifier()
    }

    fn parse_select_statement(&mut self) -> ParseResult<ast::SelectStatement> {
        self.input.expect(Expect::Select)?;
        let columns = self.parse_column_expressions()?;

        self.input.expect(Expect::From)?;

        let from = self.parse_named_tuple_source()?;

        let filter = match self.input.peek() {
            lexer::Type::Where => {
                self.input.advance();
                Some(self.parse_filter_expression()?)
            }
            _ => None,
        };

        let limit = match self.input.peek() {
            lexer::Type::Limit => {
                self.input.advance();
                Some(self.parse_limit_expression()?)
            }
            _ => None,
        };

        Ok(ast::SelectStatement {
            columns,
            from,
            filter,
            limit,
        })
    }
}

/// Parser for expression types
impl Parser {
    fn parse_column_expression(&mut self) -> ParseResult<ast::Expression> {
        self.parse_expression()
    }

    fn parse_filter_expression(&mut self) -> ParseResult<ast::Expression> {
        self.parse_expression()
    }

    fn parse_limit_expression(&mut self) -> ParseResult<ast::Expression> {
        self.parse_expression()
    }

    fn parse_expression(&mut self) -> ParseResult<ast::Expression> {
        self.parse_logical_or()
    }

    fn parse_logical_or(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_logical_and()?;

        while let Some(op) = self.input.peek().as_binary(BinaryCategory::LogicalOr) {
            self.input.advance();
            let right = self.parse_logical_and()?;
            expr = ast::Expression::BinaryOp {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(right),
            }
        }

        Ok(expr)
    }

    fn parse_logical_and(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_inclusive_or()?;

        while let Some(op) = self.input.peek().as_binary(BinaryCategory::LogicalAnd) {
            self.input.advance();
            let right = self.parse_inclusive_or()?;
            expr = ast::Expression::BinaryOp {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(right),
            }
        }

        Ok(expr)
    }

    fn parse_inclusive_or(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_exclusive_or()?;

        while let Some(op) = self.input.peek().as_binary(BinaryCategory::InclusiveOr) {
            self.input.advance();
            let right = self.parse_exclusive_or()?;
            expr = ast::Expression::BinaryOp {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(right),
            }
        }

        Ok(expr)
    }

    fn parse_exclusive_or(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_and()?;

        while let Some(op) = self.input.peek().as_binary(BinaryCategory::ExclusiveOr) {
            self.input.advance();
            let right = self.parse_and()?;
            expr = ast::Expression::BinaryOp {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(right),
            }
        }

        Ok(expr)
    }
    fn parse_and(&mut self) -> ParseResult<ast::Expression> {
        let mut expr = self.parse_equality()?;

        while let Some(op) = self.input.peek().as_binary(BinaryCategory::And) {
            self.input.advance();
            let right = self.parse_equality()?;
            expr = ast::Expression::BinaryOp {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(right),
            }
        }

        Ok(expr)
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
                let _type_name = self.parse_typename()?;
                self.input.expect(Expect::RightParen)?;
                let _expr = self.parse_cast()?;
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
                    expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
                        Box::new(expr),
                        identifier,
                    ));
                }
                lexer::Type::LeftParen => todo!(),
                _ => {
                    return Ok(expr);
                }
            }
        }
    }
    fn parse_identifier(&mut self) -> ParseResult<String> {
        match self.input.peek() {
            lexer::Type::Identifier(id) => {
                self.input.advance();
                Ok(id)
            }
            t => Err(ParseError::UnexpectedToken(Expect::Identifier, t)),
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
                Ok(ast::Expression::Value(ast::ScalarValue::IntegerNumber(
                    value,
                )))
            }
            lexer::Type::FloatingPointNumber(value) => {
                self.input.advance();
                Ok(ast::Expression::Value(ast::ScalarValue::FloatingNumber(
                    value,
                )))
            }
            lexer::Type::String(s) => {
                self.input.advance();
                Ok(ast::Expression::Value(ast::ScalarValue::StringLiteral(s)))
            }
            lexer::Type::Null => {
                self.input.advance();
                Ok(ast::Expression::Value(ast::ScalarValue::Null))
            }
            lexer::Type::LeftParen => {
                self.input.advance();
                let expr = self.parse_expression()?;
                self.input.expect(Expect::RightParen)?;

                Ok(expr)
            }
            t => Err(ParseError::UnexpectedToken(Expect::PrimaryExpression, t)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::parse;
    use crate::frontend::ast;

    #[test]
    fn test_parse_select() {
        let input = "select t.col as ben, t.othercol+1, finalcol*2 from tablename as t where col=1 and finalcol>0 limit 23;";
        let statement = parse(input).unwrap();
        println!("Statement: {:#?}", statement);
    }

    #[test]
    fn test_parse_create_table_with_types() {
        let stmt = parse("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
        match stmt {
            ast::Statement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "users");
                assert_eq!(ct.columns.len(), 3);
                assert_eq!(ct.columns[0].name, "id");
                assert_eq!(ct.columns[0].type_name, Some(ast::DataType::Integer));
                assert_eq!(ct.columns[1].name, "name");
                assert_eq!(ct.columns[1].type_name, Some(ast::DataType::Text));
                assert_eq!(ct.columns[2].name, "age");
                assert_eq!(ct.columns[2].type_name, Some(ast::DataType::Integer));
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_create_table_without_types() {
        let stmt = parse("CREATE TABLE test (col1, col2)").unwrap();
        match stmt {
            ast::Statement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "test");
                assert_eq!(ct.columns.len(), 2);
                assert_eq!(ct.columns[0].name, "col1");
                assert_eq!(ct.columns[0].type_name, None);
                assert_eq!(ct.columns[1].name, "col2");
                assert_eq!(ct.columns[1].type_name, None);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_create_table_mixed_types() {
        let stmt = parse("CREATE TABLE mixed (id INTEGER, label, score REAL, data BLOB)").unwrap();
        match stmt {
            ast::Statement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "mixed");
                assert_eq!(ct.columns.len(), 4);
                assert_eq!(ct.columns[0].type_name, Some(ast::DataType::Integer));
                assert_eq!(ct.columns[1].type_name, None);
                assert_eq!(ct.columns[2].type_name, Some(ast::DataType::Real));
                assert_eq!(ct.columns[3].type_name, Some(ast::DataType::Blob));
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_string_literal() {
        let stmt = parse("SELECT 'hello' FROM users").unwrap();
        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                match &select.columns[0] {
                    ast::ColumnExpression::Anonyomous(expr) => match expr.as_ref() {
                        ast::Expression::Value(ast::ScalarValue::StringLiteral(s)) => {
                            assert_eq!(s, "hello");
                        }
                        other => panic!("Expected StringLiteral, got {:?}", other),
                    },
                    other => panic!("Expected Anonymous, got {:?}", other),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_create_table_single_column() {
        let stmt = parse("CREATE TABLE simple (value INTEGER)").unwrap();
        match stmt {
            ast::Statement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "simple");
                assert_eq!(ct.columns.len(), 1);
                assert_eq!(ct.columns[0].name, "value");
                assert_eq!(ct.columns[0].type_name, Some(ast::DataType::Integer));
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_insert_without_columns() {
        let stmt = parse("INSERT INTO users VALUES (1, 'alice', 30)").unwrap();
        match stmt {
            ast::Statement::Insert(ins) => {
                assert_eq!(ins.table_name, "users");
                assert!(ins.columns.is_none());
                assert_eq!(ins.values.len(), 1);
                assert_eq!(ins.values[0].len(), 3);
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_with_columns() {
        let stmt = parse("INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)").unwrap();
        match stmt {
            ast::Statement::Insert(ins) => {
                assert_eq!(ins.table_name, "users");
                assert_eq!(
                    ins.columns,
                    Some(vec![
                        "id".to_string(),
                        "name".to_string(),
                        "age".to_string()
                    ])
                );
                assert_eq!(ins.values.len(), 1);
                assert_eq!(ins.values[0].len(), 3);
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_multiple_rows() {
        let stmt = parse("INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25)").unwrap();
        match stmt {
            ast::Statement::Insert(ins) => {
                assert_eq!(ins.table_name, "users");
                assert!(ins.columns.is_none());
                assert_eq!(ins.values.len(), 2);
                assert_eq!(ins.values[0].len(), 3);
                assert_eq!(ins.values[1].len(), 3);
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_select_star() {
        let stmt = parse("SELECT * FROM users").unwrap();
        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                match &select.columns[0] {
                    ast::ColumnExpression::Wildcard => {
                        // Success - wildcard parsed correctly
                    }
                    other => panic!("Expected Wildcard, got {:?}", other),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_select_star_with_expr() {
        let stmt = parse("SELECT *, 1 FROM users").unwrap();
        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.columns.len(), 2);
                // First should be wildcard
                assert!(matches!(select.columns[0], ast::ColumnExpression::Wildcard));
                // Second should be the literal 1
                match &select.columns[1] {
                    ast::ColumnExpression::Anonyomous(expr) => match expr.as_ref() {
                        ast::Expression::Value(ast::ScalarValue::IntegerNumber(1)) => {}
                        other => panic!("Expected integer 1, got {:?}", other),
                    },
                    other => panic!("Expected Anonymous expression, got {:?}", other),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_select_expr_star() {
        let stmt = parse("SELECT 999, * FROM users").unwrap();
        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.columns.len(), 2);
                // First should be literal 999
                match &select.columns[0] {
                    ast::ColumnExpression::Anonyomous(expr) => match expr.as_ref() {
                        ast::Expression::Value(ast::ScalarValue::IntegerNumber(999)) => {}
                        other => panic!("Expected integer 999, got {:?}", other),
                    },
                    other => panic!("Expected Anonymous expression, got {:?}", other),
                }
                // Second should be wildcard
                assert!(matches!(select.columns[1], ast::ColumnExpression::Wildcard));
            }
            _ => panic!("Expected Select statement"),
        }
    }
}
