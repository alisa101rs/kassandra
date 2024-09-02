use serde::Serialize;

use crate::{
    cql::{
        self,
        execution::Executor,
        value::{ClusteringKeyValue, PartitionKeyValue},
    },
    frame::response::{error::Error, result::QueryResult},
};

#[derive(Debug, Clone, Serialize)]
pub struct DeleteNode {
    pub keyspace: String,
    pub table: String,
    pub partition_key: PartitionKeyValue,
    pub clustering_key: ClusteringKeyValue,
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
