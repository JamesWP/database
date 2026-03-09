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
    SyntaxError(String),
}

impl ParseError {
    fn syntax(msg: &str) -> Self {
        ParseError::SyntaxError(msg.to_string())
    }
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
            (Expect::Index, lexer::Type::Index) => {
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
            (Expect::Update, lexer::Type::Update) => {
                self.advance();
                Ok(())
            }
            (Expect::Set, lexer::Type::Set) => {
                self.advance();
                Ok(())
            }
            (Expect::Delete, lexer::Type::Delete) => {
                self.advance();
                Ok(())
            }
            (Expect::Drop, lexer::Type::Drop) => {
                self.advance();
                Ok(())
            }
            (Expect::By, lexer::Type::By) => {
                self.advance();
                Ok(())
            }
            (Expect::Null, lexer::Type::Null) => {
                self.advance();
                Ok(())
            }
            (Expect::Join, lexer::Type::Join) => {
                self.advance();
                Ok(())
            }
            (Expect::On, lexer::Type::On) => {
                self.advance();
                Ok(())
            }
            (Expect::Key, lexer::Type::Key) => {
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
    Index,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Drop,
    By,
    Null,
    Join,
    On,
    Key,
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
            (Equality, lexer::Type::Like) => Some(ast::BinaryOp::Like),
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
        if let lexer::Type::Explain = self.input.peek() {
            self.input.advance(); // consume EXPLAIN
            let inner = self.parse_statement()?;
            if matches!(inner, ast::Statement::Explain(_)) {
                return Err(ParseError::syntax("cannot nest EXPLAIN"));
            }
            return Ok(ast::Statement::Explain(Box::new(inner)));
        }
        match self.input.peek() {
            lexer::Type::Select => Ok(ast::Statement::Select(self.parse_select_statement()?)),
            lexer::Type::Create => {
                self.input.advance(); // consume CREATE
                match self.input.peek() {
                    lexer::Type::Table => Ok(ast::Statement::CreateTable(
                        self.parse_create_table_statement_after_create()?,
                    )),
                    lexer::Type::Index => Ok(ast::Statement::CreateIndex(
                        self.parse_create_index_statement()?,
                    )),
                    t => Err(ParseError::UnexpectedToken(Expect::Table, t)),
                }
            }
            lexer::Type::Insert => Ok(ast::Statement::Insert(self.parse_insert_statement()?)),
            lexer::Type::Update => Ok(ast::Statement::Update(self.parse_update_statement()?)),
            lexer::Type::Delete => Ok(ast::Statement::Delete(self.parse_delete_statement()?)),
            lexer::Type::Drop => Ok(ast::Statement::Drop(self.parse_drop_table_statement()?)),
            _ => todo!(),
        }
    }

    fn parse_insert_statement(&mut self) -> ParseResult<ast::InsertStatement> {
        self.input.expect(Expect::Insert)?;
        self.input.expect(Expect::Into)?;
        let table_name = self.parse_identifier()?;

        // Optional column list appears before VALUES/SELECT keyword.
        let columns = match self.input.peek() {
            lexer::Type::Values | lexer::Type::Select => None,
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

        let source = match self.input.peek() {
            lexer::Type::Values => {
                self.input.advance();
                let mut values = vec![self.parse_value_row()?];
                while let lexer::Type::Comma = self.input.peek() {
                    self.input.advance();
                    values.push(self.parse_value_row()?);
                }
                ast::InsertSource::Values(values)
            }
            lexer::Type::Select => {
                let select = self.parse_select_statement()?;
                ast::InsertSource::Query(Box::new(select))
            }
            t => return Err(ParseError::UnexpectedToken(Expect::Values, t)),
        };

        Ok(ast::InsertStatement {
            table_name,
            columns,
            source,
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

    fn parse_update_statement(&mut self) -> ParseResult<ast::UpdateStatement> {
        self.input.expect(Expect::Update)?;
        let table_name = self.parse_identifier()?;
        self.input.expect(Expect::Set)?;

        // Parse SET assignments: col1=expr1, col2=expr2, ...
        let mut assignments = Vec::new();
        loop {
            let column_name = self.parse_identifier()?;
            // Expect '=' token
            if !matches!(self.input.peek(), lexer::Type::Equal) {
                return Err(ParseError::UnexpectedToken(
                    Expect::PrimaryExpression, // Reuse this for '='
                    self.input.peek(),
                ));
            }
            self.input.advance();
            let expr = self.parse_expression()?;
            assignments.push((column_name, expr));

            // Check for more assignments
            if matches!(self.input.peek(), lexer::Type::Comma) {
                self.input.advance();
            } else {
                break;
            }
        }

        // Optional WHERE clause
        let filter = if matches!(self.input.peek(), lexer::Type::Where) {
            self.input.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(ast::UpdateStatement {
            table_name,
            assignments,
            filter,
        })
    }

    fn parse_delete_statement(&mut self) -> ParseResult<ast::DeleteStatement> {
        self.input.expect(Expect::Delete)?;
        self.input.expect(Expect::From)?;
        let table_name = self.parse_identifier()?;

        // Optional WHERE clause
        let filter = if matches!(self.input.peek(), lexer::Type::Where) {
            self.input.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(ast::DeleteStatement { table_name, filter })
    }

    fn parse_drop_table_statement(&mut self) -> ParseResult<ast::DropTableStatement> {
        self.input.expect(Expect::Drop)?;
        self.input.expect(Expect::Table)?;
        let table_name = self.parse_identifier()?;

        Ok(ast::DropTableStatement { table_name })
    }

    fn parse_create_table_statement_after_create(
        &mut self,
    ) -> ParseResult<ast::CreateTableStatement> {
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

    fn parse_create_index_statement(&mut self) -> ParseResult<ast::CreateIndexStatement> {
        // CREATE INDEX idx_name ON table_name(col1, col2, ...)
        // (CREATE already consumed by caller)
        self.input.expect(Expect::Index)?;
        let index_name = self.parse_identifier()?;
        self.input.expect(Expect::On)?;
        let table_name = self.parse_identifier()?;
        self.input.expect(Expect::LeftParen)?;
        let mut column_names = vec![self.parse_identifier()?];
        while let lexer::Type::Comma = self.input.peek() {
            self.input.advance();
            column_names.push(self.parse_identifier()?);
        }
        self.input.expect(Expect::RightParen)?;
        Ok(ast::CreateIndexStatement {
            index_name,
            table_name,
            column_names,
        })
    }

    fn parse_column_def(&mut self) -> ParseResult<ast::ColumnDef> {
        let name = self.parse_identifier()?;
        let type_name = self.parse_optional_data_type();
        let constraints = self.parse_column_constraints();
        Ok(ast::ColumnDef {
            name,
            type_name,
            constraints,
        })
    }

    fn parse_column_constraints(&mut self) -> Vec<ast::ColumnConstraint> {
        let mut cs = Vec::new();
        loop {
            match self.input.peek() {
                lexer::Type::Primary => {
                    self.input.advance();
                    self.input.expect(Expect::Key).ok();
                    cs.push(ast::ColumnConstraint::PrimaryKey);
                }
                lexer::Type::Unique => {
                    self.input.advance();
                    cs.push(ast::ColumnConstraint::Unique);
                }
                lexer::Type::Not => {
                    self.input.advance();
                    self.input.expect(Expect::Null).ok();
                    cs.push(ast::ColumnConstraint::NotNull);
                }
                _ => break,
            }
        }
        cs
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
            // Implicit alias: bare identifier that is not a keyword
            lexer::Type::Identifier(_) => {
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
        let distinct = match self.input.peek() {
            lexer::Type::Distinct => {
                self.input.advance();
                true
            }
            _ => false,
        };
        let columns = self.parse_column_expressions()?;

        self.input.expect(Expect::From)?;

        let from = self.parse_named_tuple_source()?;

        let mut joins = Vec::new();
        loop {
            match self.input.peek() {
                lexer::Type::Inner => {
                    self.input.advance(); // consume INNER
                    self.input.expect(Expect::Join)?;
                    let table = self.parse_named_tuple_source()?;
                    self.input.expect(Expect::On)?;
                    let on_condition = self.parse_expression()?;
                    joins.push(ast::JoinClause {
                        table,
                        on_condition,
                    });
                }
                lexer::Type::Join => {
                    self.input.advance(); // consume JOIN
                    let table = self.parse_named_tuple_source()?;
                    self.input.expect(Expect::On)?;
                    let on_condition = self.parse_expression()?;
                    joins.push(ast::JoinClause {
                        table,
                        on_condition,
                    });
                }
                _ => break,
            }
        }

        let filter = match self.input.peek() {
            lexer::Type::Where => {
                self.input.advance();
                Some(self.parse_filter_expression()?)
            }
            _ => None,
        };

        let group_by = match self.input.peek() {
            lexer::Type::Group => {
                self.input.advance();
                self.input.expect(Expect::By)?;
                Some(self.parse_group_by_expressions()?)
            }
            _ => None,
        };

        let having = match self.input.peek() {
            lexer::Type::Having => {
                self.input.advance();
                Some(self.parse_filter_expression()?)
            }
            _ => None,
        };

        let order_by = match self.input.peek() {
            lexer::Type::Order => {
                self.input.advance();
                self.input.expect(Expect::By)?;
                Some(self.parse_order_by_clauses()?)
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
            distinct,
            columns,
            from,
            joins,
            filter,
            limit,
            order_by,
            group_by,
            having,
        })
    }

    fn parse_order_by_clauses(&mut self) -> ParseResult<Vec<ast::OrderByClause>> {
        let mut clauses = Vec::new();

        loop {
            let expression = self.parse_expression()?;
            let direction = match self.input.peek() {
                lexer::Type::Asc => {
                    self.input.advance();
                    ast::OrderDirection::Asc
                }
                lexer::Type::Desc => {
                    self.input.advance();
                    ast::OrderDirection::Desc
                }
                _ => ast::OrderDirection::Asc, // Default to ASC
            };

            clauses.push(ast::OrderByClause {
                expression,
                direction,
            });

            // Check for comma (more sort keys)
            if matches!(self.input.peek(), lexer::Type::Comma) {
                self.input.advance();
            } else {
                break;
            }
        }

        Ok(clauses)
    }

    fn parse_group_by_expressions(&mut self) -> ParseResult<Vec<ast::Expression>> {
        let mut expressions = Vec::new();

        loop {
            expressions.push(self.parse_expression()?);

            // Check for comma (more group expressions)
            if matches!(self.input.peek(), lexer::Type::Comma) {
                self.input.advance();
            } else {
                break;
            }
        }

        Ok(expressions)
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

        // Check for IS [NOT] NULL
        if matches!(self.input.peek(), lexer::Type::Is) {
            self.input.advance(); // consume IS
            if matches!(self.input.peek(), lexer::Type::Not) {
                self.input.advance(); // consume NOT
                self.input.expect(Expect::Null)?;
                expr = ast::Expression::UnaryOp {
                    op: ast::UnaryOp::IsNotNull,
                    expression: Box::new(expr),
                };
            } else {
                self.input.expect(Expect::Null)?;
                expr = ast::Expression::UnaryOp {
                    op: ast::UnaryOp::IsNull,
                    expression: Box::new(expr),
                };
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
                // Check if this is a function call (identifier followed by '(')
                if matches!(self.input.peek(), lexer::Type::LeftParen) {
                    self.input.advance(); // consume '('

                    // Parse argument list
                    let mut args = Vec::new();

                    // Special case: COUNT(*) - the * is not an expression
                    if id.to_uppercase() == "COUNT"
                        && matches!(self.input.peek(), lexer::Type::Star)
                    {
                        self.input.advance(); // consume '*'
                        self.input.expect(Expect::RightParen)?;
                        // COUNT(*) has empty args to distinguish from COUNT(expr)
                        return Ok(ast::Expression::FunctionCall {
                            name: "COUNT".to_string(),
                            args: vec![],
                        });
                    }

                    // Check for empty argument list
                    if !matches!(self.input.peek(), lexer::Type::RightParen) {
                        loop {
                            args.push(self.parse_expression()?);

                            if matches!(self.input.peek(), lexer::Type::Comma) {
                                self.input.advance(); // consume ','
                            } else {
                                break;
                            }
                        }
                    }

                    self.input.expect(Expect::RightParen)?;

                    Ok(ast::Expression::FunctionCall { name: id, args })
                } else {
                    Ok(ast::Expression::Value(ast::ScalarValue::Identifier(id)))
                }
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
                match ins.source {
                    ast::InsertSource::Values(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].len(), 3);
                    }
                    _ => panic!("Expected Values source"),
                }
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
                match ins.source {
                    ast::InsertSource::Values(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].len(), 3);
                    }
                    _ => panic!("Expected Values source"),
                }
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
                match ins.source {
                    ast::InsertSource::Values(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0].len(), 3);
                        assert_eq!(rows[1].len(), 3);
                    }
                    _ => panic!("Expected Values source"),
                }
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_float_literal() {
        // Test INSERT with float value into REAL column
        let stmt = parse("INSERT INTO sprocket VALUES (4.5)").unwrap();
        match stmt {
            ast::Statement::Insert(ins) => {
                assert_eq!(ins.table_name, "sprocket");
                assert!(ins.columns.is_none());
                match ins.source {
                    ast::InsertSource::Values(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].len(), 1);
                        match &rows[0][0] {
                            ast::Expression::Value(ast::ScalarValue::FloatingNumber(n)) => {
                                assert_eq!(*n, 4.5);
                            }
                            other => panic!("Expected FloatingNumber(4.5), got {:?}", other),
                        }
                    }
                    _ => panic!("Expected Values source"),
                }
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_mixed_types() {
        // Test INSERT with mix of integers, floats, strings
        let stmt = parse("INSERT INTO sprocket VALUES (1, 4.5, 'test', .5, 5., 1e-3)").unwrap();
        match stmt {
            ast::Statement::Insert(ins) => match ins.source {
                ast::InsertSource::Values(rows) => {
                    assert_eq!(rows[0].len(), 6);
                    match &rows[0][0] {
                        ast::Expression::Value(ast::ScalarValue::IntegerNumber(n)) => {
                            assert_eq!(*n, 1);
                        }
                        other => panic!("Expected IntegerNumber(1), got {:?}", other),
                    }
                    match &rows[0][1] {
                        ast::Expression::Value(ast::ScalarValue::FloatingNumber(n)) => {
                            assert_eq!(*n, 4.5);
                        }
                        other => panic!("Expected FloatingNumber(4.5), got {:?}", other),
                    }
                    match &rows[0][2] {
                        ast::Expression::Value(ast::ScalarValue::StringLiteral(s)) => {
                            assert_eq!(s, "test");
                        }
                        other => panic!("Expected StringLiteral('test'), got {:?}", other),
                    }
                }
                _ => panic!("Expected Values source"),
            },
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_select() {
        let stmt = parse("INSERT INTO t2 SELECT id, name FROM t1").unwrap();
        match stmt {
            ast::Statement::Insert(ins) => {
                assert_eq!(ins.table_name, "t2");
                assert!(matches!(ins.source, ast::InsertSource::Query(_)));
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_values_still_works() {
        let stmt = parse("INSERT INTO t VALUES (1, 'x')").unwrap();
        match stmt {
            ast::Statement::Insert(ins) => {
                assert!(matches!(ins.source, ast::InsertSource::Values(_)));
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

    #[test]
    fn test_parse_create_index() {
        let sql = "CREATE INDEX idx_age ON users(age)";
        let stmt = parse(sql).unwrap();
        match stmt {
            ast::Statement::CreateIndex(ci) => {
                assert_eq!(ci.index_name, "idx_age");
                assert_eq!(ci.table_name, "users");
                assert_eq!(ci.column_names, vec!["age"]);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }

    #[test]
    fn test_parse_create_index_multi_column() {
        let sql = "CREATE INDEX idx_name_age ON users(last_name, first_name, age)";
        let stmt = parse(sql).unwrap();
        match stmt {
            ast::Statement::CreateIndex(ci) => {
                assert_eq!(ci.index_name, "idx_name_age");
                assert_eq!(ci.table_name, "users");
                assert_eq!(ci.column_names, vec!["last_name", "first_name", "age"]);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }

    #[test]
    fn test_parse_join() {
        // Parse "SELECT a.x, b.y FROM alpha AS a JOIN beta AS b ON a.id = b.a_id"
        let stmt =
            parse("SELECT a.x, b.y FROM alpha AS a JOIN beta AS b ON a.id = b.a_id").unwrap();
        match stmt {
            ast::Statement::Select(select) => {
                // Should have 1 join
                assert_eq!(select.joins.len(), 1);

                // Check the join table
                match &select.joins[0].table {
                    ast::NamedTupleSource::Named { alias, source } => {
                        assert_eq!(alias, "b");
                        match source {
                            ast::TupleSource::Table(name) => assert_eq!(name, "beta"),
                            _ => panic!("Expected Table source"),
                        }
                    }
                    _ => panic!("Expected Named tuple source"),
                }

                // Check ON condition is a binary operation
                assert!(matches!(
                    select.joins[0].on_condition,
                    ast::Expression::BinaryOp { .. }
                ));
            }
            _ => panic!("Expected Select statement"),
        }

        // Test INNER JOIN keyword variant
        let stmt2 = parse("SELECT a.x FROM t1 AS a INNER JOIN t2 AS b ON a.id = b.id").unwrap();
        match stmt2 {
            ast::Statement::Select(select) => {
                assert_eq!(select.joins.len(), 1);
            }
            _ => panic!("Expected Select statement"),
        }

        // Test query without joins (existing queries should be unaffected)
        let stmt3 = parse("SELECT x FROM t1 WHERE x > 1").unwrap();
        match stmt3 {
            ast::Statement::Select(select) => {
                assert_eq!(select.joins.len(), 0);
            }
            _ => panic!("Expected Select statement"),
        }

        // Test implicit alias (no AS keyword) on both FROM table and JOIN table
        let stmt4 = parse("SELECT * FROM users u JOIN orders o ON o.order_id = u.id").unwrap();
        match stmt4 {
            ast::Statement::Select(select) => {
                assert!(matches!(
                    &select.from,
                    ast::NamedTupleSource::Named { alias, .. } if alias == "u"
                ));
                assert_eq!(select.joins.len(), 1);
                assert!(matches!(
                    &select.joins[0].table,
                    ast::NamedTupleSource::Named { alias, .. } if alias == "o"
                ));
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_having_clause() {
        let stmt = parse("SELECT dept, COUNT(*) FROM t GROUP BY dept HAVING COUNT(*) > 3").unwrap();
        match stmt {
            ast::Statement::Select(sel) => {
                assert!(sel.group_by.is_some());
                assert!(sel.having.is_some());
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_select_without_having_is_none() {
        let stmt = parse("SELECT dept FROM t GROUP BY dept").unwrap();
        match stmt {
            ast::Statement::Select(sel) => {
                assert!(sel.having.is_none());
            }
            _ => panic!("Expected Select statement"),
        }
    }

    /// SQL reserved keywords that must not be used as identifiers in tests.
    const RESERVED_KEYWORDS: &[&str] = &[
        "select", "from", "where", "insert", "into", "values", "create", "table", "index", "on",
        "drop", "update", "delete", "set", "order", "by", "limit", "distinct", "inner", "join",
        "group", "having", "as", "and", "or", "not", "is", "null", "in", "like", "between", "asc",
        "desc", "integer", "text", "float", "boolean", "blob", "count", "sum", "avg", "min", "max",
    ];

    fn is_reserved(s: &str) -> bool {
        RESERVED_KEYWORDS.contains(&s.to_ascii_lowercase().as_str())
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(50))]

        /// Generate random valid SELECT and INSERT SQL strings and verify that
        /// the parser either succeeds or fails gracefully (no panics, no UB).
        #[test]
        fn test_parse_select_no_panic(
            table in "[a-z][a-z0-9_]{0,8}",
            col1 in "[a-z][a-z0-9_]{0,8}",
            col2 in "[a-z][a-z0-9_]{0,8}",
            limit in 1i64..1000,
        ) {
            proptest::prop_assume!(!is_reserved(&table));
            proptest::prop_assume!(!is_reserved(&col1));
            proptest::prop_assume!(!is_reserved(&col2));

            // Basic SELECT — must parse successfully
            let sql = format!("SELECT {col1} FROM {table}");
            proptest::prop_assert!(parse(&sql).is_ok(), "Failed to parse: {sql}");

            // SELECT with LIMIT
            let sql = format!("SELECT {col1} FROM {table} LIMIT {limit}");
            proptest::prop_assert!(parse(&sql).is_ok(), "Failed to parse: {sql}");

            // SELECT with WHERE
            let sql = format!("SELECT {col1} FROM {table} WHERE {col2} = {limit}");
            proptest::prop_assert!(parse(&sql).is_ok(), "Failed to parse: {sql}");

            // SELECT DISTINCT
            let sql = format!("SELECT DISTINCT {col1} FROM {table}");
            proptest::prop_assert!(parse(&sql).is_ok(), "Failed to parse: {sql}");

            // INSERT
            let sql = format!("INSERT INTO {table} VALUES ({limit}, {limit})");
            proptest::prop_assert!(parse(&sql).is_ok(), "Failed to parse: {sql}");
        }

        /// Verify that CREATE TABLE / CREATE INDEX round-trip through the parser
        /// without panicking for valid identifiers and data types.
        #[test]
        fn test_parse_ddl_no_panic(
            table in "[a-z][a-z0-9_]{0,8}",
            col in "[a-z][a-z0-9_]{0,8}",
            idx in "[a-z][a-z0-9_]{0,8}",
        ) {
            proptest::prop_assume!(!is_reserved(&table));
            proptest::prop_assume!(!is_reserved(&col));
            proptest::prop_assume!(!is_reserved(&idx));

            let sql = format!("CREATE TABLE {table} ({col} INTEGER)");
            proptest::prop_assert!(parse(&sql).is_ok(), "Failed to parse: {sql}");

            let sql = format!("CREATE INDEX {idx} ON {table} ({col})");
            proptest::prop_assert!(parse(&sql).is_ok(), "Failed to parse: {sql}");
        }
    }

    #[test]
    fn test_parse_explain_select() {
        let stmt = parse("EXPLAIN SELECT id FROM users WHERE age = 30").unwrap();
        assert!(matches!(stmt, ast::Statement::Explain(_)));
        if let ast::Statement::Explain(inner) = stmt {
            assert!(matches!(*inner, ast::Statement::Select(_)));
        }
    }

    #[test]
    fn test_parse_explain_insert() {
        let stmt = parse("EXPLAIN INSERT INTO users VALUES (1, 'alice', 30)").unwrap();
        assert!(matches!(stmt, ast::Statement::Explain(_)));
    }

    #[test]
    fn test_parse_explain_nested_error() {
        assert!(parse("EXPLAIN EXPLAIN SELECT 1").is_err());
    }

    #[test]
    fn test_parse_primary_key_constraint() {
        let stmt = parse("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        let ct = match stmt {
            ast::Statement::CreateTable(ct) => ct,
            _ => panic!("Expected CreateTable"),
        };
        assert!(ct.columns[0]
            .constraints
            .contains(&ast::ColumnConstraint::PrimaryKey));
        assert!(ct.columns[1].constraints.is_empty());
    }

    #[test]
    fn test_parse_unique_constraint() {
        let stmt = parse("CREATE TABLE t (email TEXT UNIQUE)").unwrap();
        let ct = match stmt {
            ast::Statement::CreateTable(ct) => ct,
            _ => panic!("Expected CreateTable"),
        };
        assert!(ct.columns[0]
            .constraints
            .contains(&ast::ColumnConstraint::Unique));
    }
}
