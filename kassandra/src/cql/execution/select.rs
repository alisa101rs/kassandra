use std::ops::RangeInclusive;

use serde::Serialize;
use tracing::{instrument, Level};

use crate::{
    cql,
    cql::{
        execution::{selector, ColumnsSelector, Executor},
        value::CqlValue,
    },
    frame::response::{
        error::Error,
        result::{QueryResult, ResultMetadata, Row, Rows},
    },
};

#[derive(Debug, Clone, Serialize)]
pub struct SelectNode {
    pub keyspace: String,
    pub table: String,
    pub partition_key: CqlValue,
    pub clustering_key: RangeInclusive<CqlValue>,
    pub selector: ColumnsSelector,
    pub metadata: ResultMetadata,
    pub limit: Option<usize>,
    pub state: PagingState,
}

#[derive(Debug, Clone, Serialize)]
pub struct PagingState {
    pub row: Option<CqlValue>,
    pub remaining: usize,
}

impl<E: cql::Engine> Executor<E> for SelectNode {
    #[instrument(level = Level::TRACE, skip(engine), err)]
    fn execute(self: Box<Self>, engine: &mut E) -> Result<QueryResult, Error> {
        let range = if let Some(row) = self.state.row {
            let (start, end) = self.clustering_key.into_inner();
            std::cmp::max(row, start)..=end
        } else {
            self.clustering_key
        };

        let scan = engine.read(&self.keyspace, &self.table, &self.partition_key, range)?;

        let mut rows = vec![];
        for row in scan {
            rows.push(Row {
                columns: selector::filter(row, &self.selector),
            })
        }

        let rows = Rows {
            metadata: self.metadata,
            rows,
        };

        Ok(QueryResult::Rows(rows))
    }
}
