use std::collections::HashMap;

use crate::{
    cql,
    cql::value::CqlValue,
    frame::response::{
        error::Error,
        result::{ColumnSpec, QueryResult},
    },
};

mod delete;
mod insert;
mod scan;
mod schema;
mod select;

pub use self::{
    delete::DeleteNode,
    insert::InsertNode,
    scan::ScanNode,
    schema::AlterSchema,
    select::{PagingState, SelectNode},
};

pub trait Executor<E: cql::Engine> {
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
