
use crate::frontend::ast::{
    ColumnExpression, ColumnReference, Expression, NamedTupleSource, Statement, TupleSource,
};

pub fn get_expression(columns_expression: ColumnExpression) -> Expression {
    match columns_expression {
        ColumnExpression::Named { expression, .. } => *expression,
        ColumnExpression::Anonyomous(expression) => *expression,
    }
}

pub fn plan(statement: Statement, schema: &schema::Schema) -> Node {
    match statement {
        Statement::Select(select) => {
            let expressions: Vec<Expression> =
                select.columns.into_iter().map(get_expression).collect();
            let column_references: Vec<ColumnReference> = expressions
                .iter()
                .map(|e| e.get_column_references())
                .flatten()
                .collect();

            let source = match select.from {
                NamedTupleSource::Named { source, .. } => match source {
                    TupleSource::Table(name, ..) => {
                        let columns_from_this_table = column_references
                            .iter()
                            .filter(|c| c.table == name)
                            .collect();
                        let columns = schema
                            .get_table(&name)
                            .get_column_indexes(columns_from_this_table);

                        Box::new(Node::TableScan(TableScanNode { name, columns }))
                    }
                    _ => todo!(),
                },
                _ => todo!(),
            };

            Node::Select(SelectNode {
                touple_source: source,
                columns: expressions,
            })
        }
    }
}

#[derive(Debug)]
pub struct TableScanNode {
    name: String,        // Name of table to read from
    columns: Vec<usize>, // Columns indexes to produce
}

#[derive(Debug)]
pub struct SelectNode {
    touple_source: Box<Node>,
    columns: Vec<Expression>,
}

#[derive(Debug)]
pub enum Node {
    TableScan(TableScanNode),
    Select(SelectNode),
}

mod schema {
    use crate::frontend::ast::ColumnReference;

    pub struct Schema {
        pub(crate) tables: Vec<Table>,
    }

    pub struct Table {
        pub(crate) name: String,
        pub(crate) columns: Vec<Column>,
    }

    pub struct Column {
        pub(crate) name: String,
    }

    impl Schema {
        pub fn get_table(&self, name: &str) -> &Table {
            self.tables.iter().find(|t| t.name == name).unwrap()
        }
    }

    impl Table {
        pub fn get_column_indexes(&self, columns: Vec<&ColumnReference>) -> Vec<usize> {
            columns
                .iter()
                .map(|c| {
                    self.columns
                        .iter()
                        .position(|c2| c2.name == c.name)
                        .unwrap()
                })
                .collect()
        }
    }
}

#[cfg(test)]
mod test {
    use crate::frontend::ast::{
        ColumnExpression, Expression, NamedTupleSource, ScalarValue, SelectStatement, Statement,
        TupleSource,
    };
    use crate::planner::{
        plan,
        schema::{Column, Schema, Table},
    };

    #[test]
    fn test_simple_plan() {
        let schema = Schema {
            tables: vec![Table {
                name: "test".to_string(),
                columns: vec![
                    Column {
                        name: "a".to_string(),
                    },
                    Column {
                        name: "b".to_string(),
                    },
                    Column {
                        name: "c".to_string(),
                    },
                ],
            }],
        };

        let statement = Statement::Select(SelectStatement {
            columns: vec![
                ColumnExpression::Named {
                    expression: Box::new(Expression::Value(ScalarValue::Identifier(
                        "a".to_string(),
                    ))),
                    name: "a".to_string(),
                },
                ColumnExpression::Named {
                    expression: Box::new(Expression::Value(ScalarValue::Identifier(
                        "b".to_string(),
                    ))),
                    name: "b".to_string(),
                },
            ],
            from: NamedTupleSource::Named {
                alias: "test".to_string(),
                source: TupleSource::Table("test".to_string()),
            },
            filter: None,
            limit: None,
        });

        let p = plan(statement, &schema);

        println!("Plan: {:?}", p);
    }
}
