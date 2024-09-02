use std::fmt;

pub use selector::{ColumnSelector, ColumnsSelector, Transform};

use crate::{
    cql,
    cql::plan::{Aggregate, Plan},
    frame::response::{error::Error, result::QueryResult},
};

mod delete;
mod insert;
mod json;
mod scan;
mod schema;
mod select;
pub(crate) mod selector;

pub use self::{
    delete::DeleteNode, insert::InsertNode, json::JsonNode, scan::ScanNode, schema::AlterSchema,
    select::SelectNode,
};

pub trait Executor<E: cql::Engine>: fmt::Debug {
    fn execute(self: Box<Self>, engine: &mut E) -> Result<QueryResult, Error>;
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
