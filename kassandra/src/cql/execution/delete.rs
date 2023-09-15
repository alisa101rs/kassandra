use serde::Serialize;

use crate::{
    cql,
    cql::{execution::Executor, value::CqlValue},
    frame::response::{error::Error, result::QueryResult},
};

#[derive(Debug, Clone, Serialize)]
pub struct DeleteNode {
    pub keyspace: String,
    pub table: String,
    pub partition_key: CqlValue,
    pub clustering_key: CqlValue,
}

impl<E: cql::Engine> Executor<E> for DeleteNode {
    fn execute(self: Box<Self>, engine: &mut E) -> Result<QueryResult, Error> {
        engine.delete(
            &self.keyspace,
            &self.table,
            self.partition_key,
            self.clustering_key,
        )?;

        Ok(QueryResult::Void)
    }
}
