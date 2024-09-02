use std::ops::RangeBounds;

use bytes::{Bytes, BytesMut};
use serde::Serialize;
use tracing::{instrument, Level};

use crate::{
    cql::{
        self,
        execution::{
            selector::{self, ColumnsSelector},
            Executor,
        },
        value::{
            ClusteringKeyValue, ClusteringKeyValueRange, PartitionKeyValue, PartitionKeyValueRange,
        },
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
pub struct ScanNode {
    pub keyspace: String,
    pub table: String,
    pub selector: ColumnsSelector,
    pub metadata: ResultMetadata,
    pub clustering_key_start: ClusteringKeyValueRange,
    pub partition_range: PartitionKeyValueRange,
    pub limit: usize,
    pub result_page_size: usize,
}

impl<E: cql::Engine> Executor<E> for ScanNode {
    #[instrument(level = Level::TRACE, skip(engine), err)]
    fn execute(self: Box<Self>, engine: &mut E) -> Result<QueryResult, Error> {
        let mut scan = engine
            .scan(&self.keyspace, &self.table, self.partition_range)?
            .take(self.limit);

        let mut rows = vec![];
        let mut first_partition = None;

        let last_row_entry = loop {
            let Some(next_entry) = scan.next() else {
                break None;
            };
            if rows.len() >= self.result_page_size {
                break Some(next_entry);
            };
            if first_partition.is_none() {
                first_partition = Some(next_entry.partition.clone());
            }

            if Some(&next_entry.partition) == first_partition.as_ref()
                && !self.clustering_key_start.contains(&next_entry.clustering)
            {
                continue;
            }

            rows.push(Row {
                columns: selector::filter(next_entry.row, &self.selector),
            });
        };

        drop(scan);

        let metadata = if let Some(last_row_entry) = last_row_entry {
            let state = PagingState::new(
                Some(encode_partition_key(&last_row_entry.partition)),
                Some(encode_row_marker(&last_row_entry.clustering)),
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

fn encode_partition_key(value: &PartitionKeyValue) -> Bytes {
    use crate::frame::write;
    let mut buf = BytesMut::new();
    write::partition_value(&mut buf, value);

    buf.freeze()
}
