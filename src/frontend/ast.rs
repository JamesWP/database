#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
}

#[derive(Debug)]
pub struct SelectStatement {
    columns: Vec<ColumnExpression>,
    from: NamedTupleSource,
    filter: Option<Expression>,
    limit: Option<Expression>,
}

#[derive(Debug)]
enum ColumnExpression {
    Named {
        name: String,
        expression: Box<Expression>,
    },
    Anonyomous(Box<Expression>),
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
enum NamedTupleSource {
    Named { alias: String, source: TupleSource },
    Anonyomous(TupleSource),
}

#[derive(Debug)]
enum TupleSource {
    Table(String),
    Subquery(Box<SelectStatement>),
}
