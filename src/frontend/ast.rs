#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
}

#[derive(Debug)]
pub struct SelectStatement {
    pub columns: Vec<ColumnExpression>,
    pub from: NamedTupleSource,
    pub filter: Option<Expression>,
    pub limit: Option<Expression>,
}

#[derive(Debug)]
pub enum ColumnExpression {
    Named {
        name: String,
        expression: Box<Expression>,
    },
    Anonyomous(Box<Expression>),
}

pub struct ColumnReference {
    pub table: String,
    pub name: String,
}

#[derive(Debug)]
pub enum ScalarValue {
    IntegerNumber(i64),
    FloatingNumber(f64),
    Identifier(String),
    MultiPartIdentifier(Box<Expression>, String),
}

#[derive(Debug)]
pub enum UnaryOp {
    Plus,
    Negate,
}

#[derive(Debug)]
pub enum BinaryOp {
    Sum,
    Difference,
    Product,
    Quotient,
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    LeftBitShift,
    RightBitShift,
    Remainder,
    Or,
    And,
    BinaryOr,
    BinaryExclusiveOr,
    BinaryAnd,
}

#[derive(Debug)]
pub enum Expression {
    UnaryOp {
        op: UnaryOp,
        expression: Box<Expression>,
    },
    BinaryOp {
        op: BinaryOp,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
    Value(ScalarValue),
}

#[derive(Debug)]
pub enum NamedTupleSource {
    Named { alias: String, source: TupleSource },
    Anonyomous(TupleSource),
}

#[derive(Debug)]
pub enum TupleSource {
    Table(String),
    Subquery(Box<SelectStatement>),
}

impl Expression {
    pub fn get_column_references(&self) -> Vec<ColumnReference> {
        match self {
            // select abc
            Expression::Value(ScalarValue::Identifier(ident)) => {
                vec![ColumnReference {
                    table: "".to_string(),
                    name: ident.clone(),
                }]
            }
            // select def.abc
            Expression::Value(ScalarValue::MultiPartIdentifier(expression, name)) => {
                // inner expression holds the table name. i.e. def
                let mut column_references = expression.get_column_references();

                // We only handle two part identifiers for now
                assert!(column_references.len() == 1);
                let mut column_reference = column_references.pop().unwrap();

                // inner will have no table specified, only a name (the name of the table)
                assert!(column_reference.table == "");

                // outer expression holds the column name. i.e. abc
                column_reference.table = column_reference.name;
                column_reference.name = name.clone();

                vec![column_reference]
            }
            Expression::Value(ScalarValue::FloatingNumber(_)) => vec![],
            Expression::Value(ScalarValue::IntegerNumber(_)) => vec![],
            Expression::UnaryOp { expression, .. } => expression.get_column_references(),
            Expression::BinaryOp { lhs, rhs, .. } => {
                let mut lhs = lhs.get_column_references();
                let mut rhs = rhs.get_column_references();

                lhs.append(&mut rhs);

                lhs
            }
        }
    }
}
