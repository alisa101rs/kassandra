use serde::Serialize;

use crate::{
    cql::{
        self,
        execution::Executor,
        value::{ClusteringKeyValue, CqlValue, PartitionKeyValue},
    },
    frame::response::{error::Error, result::QueryResult},
};

#[derive(Debug, Clone, Serialize)]
pub struct InsertNode {
    pub keyspace: String,
    pub table: String,
    pub partition_key: PartitionKeyValue,
    pub clustering_key: ClusteringKeyValue,
    pub values: Vec<(String, CqlValue)>,
}

impl<E: cql::Engine> Executor<E> for InsertNode {
    fn execute(self: Box<Self>, engine: &mut E) -> Result<QueryResult, Error> {
        engine.insert(
            &self.keyspace,
            &self.table,
            self.partition_key,
            self.clustering_key,
            self.values,
        )?;

        Ok(QueryResult::Void)
    }
}
