enum Statement {
    Select(SelectStatement),
}

struct SelectStatement {
    columns: Vec<ColumnExpression>,
    from: NamedTupleSource,
    filter: Option<Expression>,
    limit: Option<Expression>,
}

enum ColumnExpression {
    Named {
        name: String,
        expression: Box<Expression>,
    },
    Anonyomous(Box<Expression>),
}

enum Value {
    Number(i64),
    String(String),
}

enum UnaryOp {
    Plus,
    Negate,
}

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

enum NamedTupleSource {
    Named { alias: String, source: TupleSource },
    Anonyomous(TupleSource),
}

enum TupleSource {
    Table(String),
    Subquery(Box<SelectStatement>),
}
