use std::{collections::HashMap, fmt};

use crate::{
    cql,
    cql::{
        plan::{Aggregate, Plan},
        value::CqlValue,
    },
    frame::response::{
        error::Error,
        result::{ColumnSpec, QueryResult},
    },
};

mod delete;
mod insert;
mod json;
mod scan;
mod schema;
mod select;

pub use self::{
    delete::DeleteNode,
    insert::InsertNode,
    json::JsonNode,
    scan::ScanNode,
    schema::AlterSchema,
    select::{PagingState, SelectNode},
};

pub trait Executor<E: cql::Engine>: fmt::Debug {
    fn execute(self: Box<Self>, engine: &mut E) -> Result<QueryResult, Error>;
}

fn filter(row: Vec<(String, CqlValue)>, metadata: &[ColumnSpec]) -> Vec<Option<CqlValue>> {
    let mut lookup: HashMap<String, CqlValue> = row.into_iter().collect();

    metadata
        .iter()
        .map(|it| &it.name)
        .map(|column| lookup.remove(column))
        .collect()
}

impl<E: cql::Engine + 'static> dyn Executor<E> {
    pub fn build(plan: Plan) -> Box<dyn Executor<E>> {
        match plan {
            Plan::Select(s) => Box::new(s),
            Plan::AlterSchema(s) => Box::new(s),
            Plan::Insert(i) => Box::new(i),
            Plan::Scan(s) => Box::new(s),
            Plan::Delete(d) => Box::new(d),
            Plan::Aggregate {
                aggregate: Aggregate::Json,
                source,
            } => Box::new(JsonNode(Self::build(*source))),
        }
    }
}
