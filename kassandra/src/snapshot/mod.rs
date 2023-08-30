use std::collections::BTreeMap;

use serde::Serialize;

use crate::{
    snapshot::value::ValueSnapshot,
    storage::{keyspace::Keyspace, table::Table},
};

mod value;

#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct DataSnapshots(pub BTreeMap<String, KeyspaceSnapshot>);

impl DataSnapshots {
    pub fn from_keyspaces<'a>(
        keyspaces: impl IntoIterator<Item = (&'a String, &'a Keyspace)>,
    ) -> Self {
        Self(
            keyspaces
                .into_iter()
                .filter(|(name, _)| name.as_str() != "system" && name.as_str() != "system_schema")
                .map(|(name, keyspace)| (name.clone(), keyspace.into()))
                .collect(),
        )
    }
}

#[derive(Debug, Serialize)]
pub struct KeyspaceSnapshot {
    pub tables: BTreeMap<String, TableDataSnapshot>,
}

impl<'a> From<&'a Keyspace> for KeyspaceSnapshot {
    fn from(value: &'a Keyspace) -> Self {
        Self {
            tables: value
                .tables
                .iter()
                .filter(|(_, table)| !table.data.is_empty())
                .map(|(key, table)| (key.clone(), table.into()))
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TableDataSnapshot {
    pub rows: Vec<Row>,
}

impl<'a> From<&'a Table> for TableDataSnapshot {
    fn from(value: &'a Table) -> Self {
        let mut rows = Vec::new();

        for (partition_key, entries) in value.data.iter() {
            for (clustering_key, data) in entries {
                let partition_key = partition_key.clone().into();
                let clustering_key = clustering_key.clone().into();

                let row = Row {
                    partition_key,
                    clustering_key,
                    data: data
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone().map(ValueSnapshot::from)))
                        .collect(),
                };

                rows.push(row);
            }
        }

        Self { rows }
    }
}

#[derive(Debug, Serialize)]
pub struct Row {
    pub partition_key: ValueSnapshot,
    pub clustering_key: ValueSnapshot,
    pub data: BTreeMap<String, Option<ValueSnapshot>>,
}
