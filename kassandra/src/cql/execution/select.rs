use bytes::{Bytes, BytesMut};
use serde::Serialize;
use tracing::{instrument, Level};

use crate::{
    cql::{
        self,
        execution::{selector, ColumnsSelector, Executor},
        value::{ClusteringKeyValue, ClusteringKeyValueRange, PartitionKeyValue},
    },
    frame::{
        response::{
            error::Error,
            result::{QueryResult, ResultMetadata, Row, Rows},
        },
        value::PagingState,
    },
};

#[derive(Debug, Clone, Serialize)]
pub struct SelectNode {
    pub keyspace: String,
    pub table: String,
    pub partition_key: PartitionKeyValue,
    pub clustering_range: ClusteringKeyValueRange,
    pub selector: ColumnsSelector,
    pub metadata: ResultMetadata,
    pub limit: usize,
    pub result_page_size: usize,
}

impl<E: cql::Engine> Executor<E> for SelectNode {
    #[instrument(level = Level::TRACE, skip(engine), err)]
    fn execute(self: Box<Self>, engine: &mut E) -> Result<QueryResult, Error> {
        let mut scan = engine
            .read(
                &self.keyspace,
                &self.table,
                &self.partition_key,
                self.clustering_range,
            )?
            .take(self.limit);

        let mut rows = vec![];

        let last_row = loop {
            let Some(next_entry) = scan.next() else {
                break None;
            };
            if rows.len() >= self.result_page_size {
                break Some(next_entry);
            };
            rows.push(Row {
                columns: selector::filter(next_entry.row, &self.selector),
            });
        };

        drop(scan);

        let metadata = if let Some(row) = last_row {
            let state = PagingState::new(
                None,
                Some(encode_row_marker(&row.clustering)),
                self.limit - rows.len(),
                1,
            );

            ResultMetadata {
                paging_state: Some(state),
                ..self.metadata
            }
        } else {
            self.metadata
        };

        let rows = Rows { metadata, rows };

        Ok(QueryResult::Rows(rows))
    }
}

fn encode_row_marker(value: &ClusteringKeyValue) -> Bytes {
    use crate::frame::write;
    let mut buf = BytesMut::new();
    write::clustering_value(&mut buf, value);

    buf.freeze()
}
