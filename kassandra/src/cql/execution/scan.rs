use std::ops::Range;

use serde::Serialize;
use tracing::{instrument, Level};

use crate::{
    cql,
    cql::execution::Executor,
    frame::response::{
        error::Error,
        result::{QueryResult, ResultMetadata, Row, Rows},
    },
};

#[derive(Debug, Clone, Serialize)]
pub struct ScanNode {
    pub keyspace: String,
    pub table: String,
    pub metadata: ResultMetadata,
    pub range: Range<usize>,
}

impl<E: cql::Engine> Executor<E> for ScanNode {
    #[instrument(level = Level::TRACE, skip(engine), err)]
    fn execute(self: Box<Self>, engine: &mut E) -> Result<QueryResult, Error> {
        let scan = engine.scan(&self.keyspace, &self.table, self.range)?;

        let mut rows = vec![];
        for row in scan {
            rows.push(Row {
                columns: super::filter(row, &self.metadata.col_specs),
            })
        }

        let rows = Rows {
            metadata: self.metadata,
            rows,
        };

        Ok(QueryResult::Rows(rows))
    }
}
