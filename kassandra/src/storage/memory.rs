use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeBounds,
};

use eyre::eyre;
use serde::{Deserialize, Serialize};

use super::RowEntry;
use crate::{
    cql::value::{ClusteringKeyValue, CqlValue, PartitionKeyValue},
    snapshot::DataSnapshots,
};

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct Memory {
    pub(crate) data: HashMap<String, Keyspace>,
}

pub(crate) type Keyspace = HashMap<String, Table>;
pub(crate) type Table = BTreeMap<PartitionKeyValue, BTreeMap<ClusteringKeyValue, RowValues>>;
pub(crate) type RowValues = BTreeMap<String, CqlValue>;

impl Memory {
    pub fn snapshot(&self) -> DataSnapshots {
        DataSnapshots::from_keyspaces(self.data.iter())
    }
}

impl super::Storage for Memory {
    type RowIterator<'a> = std::collections::btree_map::Iter<'a, String, CqlValue>;

    fn create_keyspace(&mut self, keyspace: &str) -> eyre::Result<()> {
        self.data.insert(keyspace.to_owned(), Default::default());
        Ok(())
    }

    fn create_table(&mut self, keyspace: &str, table: &str) -> eyre::Result<()> {
        self.data
            .get_mut(keyspace)
            .ok_or(eyre!("Keyspace does not exist"))?
            .insert(table.to_owned(), Default::default());
        Ok(())
    }

    fn write(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: PartitionKeyValue,
        clustering_key: ClusteringKeyValue,
        values: impl Iterator<Item = (String, CqlValue)>,
    ) -> eyre::Result<()> {
        let table = self
            .data
            .entry(keyspace.to_owned())
            .or_default()
            .entry(table.to_owned())
            .or_default();

        table
            .entry(partition_key)
            .or_default()
            .entry(clustering_key)
            .or_default()
            .extend(values);

        Ok(())
    }

    fn delete(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: &PartitionKeyValue,
        clustering_key: &ClusteringKeyValue,
    ) -> eyre::Result<()> {
        let table = self
            .data
            .get_mut(keyspace)
            .ok_or(eyre!("Keyspace does not exist"))?
            .get_mut(table)
            .ok_or(eyre!("Table does not exist"))?;

        match clustering_key {
            ClusteringKeyValue::Empty => {
                table.remove(partition_key);
            }
            other => {
                let Some(partition) = table.get_mut(partition_key) else {
                    return Ok(());
                };

                partition.remove(other);
            }
        }

        Ok(())
    }

    fn read<'a, 'b: 'a>(
        &'a mut self,
        keyspace: &str,
        table: &str,
        partition_key: &'b PartitionKeyValue,
        range: impl RangeBounds<ClusteringKeyValue> + Clone + 'static,
    ) -> eyre::Result<Box<dyn Iterator<Item = RowEntry<'a, Self::RowIterator<'a>>> + 'a>> {
        let partition = self
            .data
            .entry(keyspace.to_owned())
            .or_default()
            .entry(table.to_owned())
            .or_default()
            .get(partition_key);
        let iter = partition.into_iter().flat_map(move |partition_entry| {
            partition_entry
                .range(range.clone())
                .map(move |(clustering_key, row)| RowEntry {
                    row: row.iter(),
                    partition: partition_key,
                    clustering: clustering_key,
                })
        });
        Ok(Box::new(iter))
    }

    fn scan(
        &mut self,
        keyspace: &str,
        table: &str,
        range: impl RangeBounds<PartitionKeyValue> + Clone + 'static,
    ) -> eyre::Result<Box<dyn Iterator<Item = RowEntry<'_, Self::RowIterator<'_>>> + '_>> {
        let table = self
            .data
            .entry(keyspace.to_owned())
            .or_default()
            .entry(table.to_owned())
            .or_default();

        let iter = table.range(range).flat_map(|(partition_key, values)| {
            values.iter().map(|(clustering_key, row)| RowEntry {
                partition: partition_key,
                clustering: clustering_key,
                row: row.iter(),
            })
        });

        Ok(Box::new(iter))
    }
}
