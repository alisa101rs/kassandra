use serde::Serialize;

use crate::{
    cql,
    cql::{
        execution::Executor,
        literal::Literal,
        schema::{keyspace::Strategy, TableSchema},
    },
    frame::response::{
        error::Error,
        event::{SchemaChangeEvent, SchemaChangeType},
        result::{QueryResult, SchemaChange},
    },
};

#[derive(Debug, Clone, Serialize)]
pub enum AlterSchema {
    Keyspace {
        name: String,
        ignore_existence: bool,
        replication: Strategy,
    },
    Table {
        keyspace: String,
        name: String,
        ignore_existence: bool,
        schema: TableSchema,
        options: Vec<(String, Literal)>,
    },
}

impl<E: cql::Engine> Executor<E> for AlterSchema {
    fn execute(self: Box<Self>, engine: &mut E) -> Result<QueryResult, Error> {
        let change = match *self {
            Self::Keyspace {
                name,
                replication,
                ignore_existence,
            } => {
                let _ = engine.create_keyspace(name.clone(), ignore_existence, replication)?;

                SchemaChange {
                    event: SchemaChangeEvent::KeyspaceChange {
                        change_type: SchemaChangeType::Created,
                        keyspace_name: name,
                    },
                }
            }
            AlterSchema::Table {
                keyspace,
                name,
                ignore_existence,
                schema,
                options,
            } => {
                let _ = engine.create_table(
                    keyspace.clone(),
                    name.clone(),
                    ignore_existence,
                    schema,
                    options,
                )?;

                SchemaChange {
                    event: SchemaChangeEvent::TableChange {
                        change_type: SchemaChangeType::Created,
                        keyspace_name: keyspace,
                        object_name: name,
                    },
                }
            }
        };

        Ok(QueryResult::SchemaChange(change))
    }
}
