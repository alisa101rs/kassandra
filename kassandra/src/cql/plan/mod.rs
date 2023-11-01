use derive_more::Display;
use planner::Planner;
use serde::Serialize;

use crate::{
    cql,
    cql::{
        execution::{AlterSchema, DeleteNode, Executor, InsertNode, ScanNode, SelectNode},
        query::QueryString,
        schema::Catalog,
    },
    frame::{
        request::query_params::QueryParameters,
        response::{
            error::Error,
            result::{PreparedMetadata, QueryResult, ResultMetadata},
        },
    },
};

mod data_reader;
mod planner;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Display)]
pub enum Aggregate {
    #[display(fmt = "JSON")]
    Json,
}

#[derive(Debug, Clone, Serialize)]
pub enum Plan {
    Aggregate {
        source: Box<Plan>,
        aggregate: Aggregate,
    },
    Select(SelectNode),
    Scan(ScanNode),
    Insert(InsertNode),
    Delete(DeleteNode),
    AlterSchema(AlterSchema),
}

impl Plan {
    pub fn build(
        statement: QueryString,
        parameters: QueryParameters<'_>,
        use_keyspace: Option<String>,
        catalog: &mut impl Catalog,
    ) -> Result<Plan, Error> {
        Planner::new(catalog, use_keyspace).build(statement, parameters)
    }

    pub fn prepare(
        statement: QueryString,
        use_keyspace: Option<String>,
        catalog: &mut impl Catalog,
    ) -> Result<(PreparedMetadata, ResultMetadata), Error> {
        Planner::new(catalog, use_keyspace).prepare(statement)
    }

    pub fn execute<E: cql::Engine + 'static>(self, engine: &mut E) -> Result<QueryResult, Error> {
        <dyn Executor<E>>::build(self).execute(engine)
    }
}
