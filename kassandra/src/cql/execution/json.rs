use std::collections::BTreeMap;

use serde::Serialize;
use tracing::{instrument, Level};

use crate::{
    cql,
    cql::{column::ColumnType, execution::Executor, value::CqlValue},
    frame::response::{
        error::Error,
        result::{ColumnSpec, QueryResult, ResultMetadata, Row, Rows},
    },
    snapshot::ValueSnapshot,
};

#[derive(Debug, Clone, Serialize)]
pub struct JsonNode<N: ?Sized>(pub Box<N>);

impl<E: cql::Engine, N: Executor<E> + ?Sized> Executor<E> for JsonNode<N> {
    #[instrument(level = Level::TRACE, skip(engine), err)]
    fn execute(self: Box<Self>, engine: &mut E) -> Result<QueryResult, Error> {
        let Rows {
            metadata:
                ResultMetadata {
                    global_spec,
                    paging_state,
                    col_specs,
                    ..
                },
            rows,
        } = match self.0.execute(engine)? {
            QueryResult::Rows(rows) => rows,
            other => return Ok(other),
        };

        let metadata = ResultMetadata {
            global_spec,
            paging_state,
            col_specs: vec![ColumnSpec {
                table_spec: None,
                name: "json".to_string(),
                typ: ColumnType::Text,
            }],
        };

        let rows = rows
            .into_iter()
            .map(|row| {
                let serialized =
                    serialize_columns(col_specs.iter().map(|it| &it.name), row.columns.into_iter());

                Row {
                    columns: vec![Some(CqlValue::Text(serialized))],
                }
            })
            .collect();

        Ok(QueryResult::Rows(Rows { metadata, rows }))
    }
}

fn serialize_columns<'a>(
    columns: impl Iterator<Item = &'a String>,
    values: impl Iterator<Item = Option<CqlValue>>,
) -> String {
    let mut map = BTreeMap::new();

    for (column, value) in columns.zip(values) {
        map.insert(column, value.map(ValueSnapshot::from));
    }

    serde_json::to_string(&map).expect("row to be serializable")
}
