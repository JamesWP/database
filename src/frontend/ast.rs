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
enum Value {
    Number(i64),
    String(String),
}

#[derive(Debug)]
enum UnaryOp {
    Plus,
    Negate,
}

#[derive(Debug)]
enum BinaryOp {
    Sum,
    Difference,
    Product,
    Quotient,
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    LeftBitShift,
    RightBitShift,
}

#[derive(Debug)]
enum Expression {
    UnaryOp {
        op: UnaryOp,
        expression: Box<Expression>,
    },
    BinaryOp {
        op: BinaryOp,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
    Value(Value),
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
