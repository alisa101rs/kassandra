use std::{
    collections::{BTreeMap, Bound, HashMap},
    ops::{RangeBounds},
};

use eyre::eyre;
use serde::{Deserialize, Serialize};

use crate::{cql::value::CqlValue, snapshot::DataSnapshots};

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Memory {
    pub(crate) data: HashMap<String, Keyspace>,
}

pub(crate) type Keyspace = HashMap<String, Table>;
pub(crate) type Table = BTreeMap<CqlValue, BTreeMap<CqlValue, RowValues>>;
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
        partition_key: CqlValue,
        clustering_key: CqlValue,
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
        partition_key: &CqlValue,
        clustering_key: &CqlValue,
    ) -> eyre::Result<()> {
        let table = self
            .data
            .get_mut(keyspace)
            .ok_or(eyre!("Keyspace does not exist"))?
            .get_mut(table)
            .ok_or(eyre!("Table does not exist"))?;

        match clustering_key {
            CqlValue::Empty => {
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

    fn read(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: &CqlValue,
        range: impl RangeBounds<CqlValue> + Clone + 'static,
    ) -> eyre::Result<impl Iterator<Item = Self::RowIterator<'_>>> {
        let partition = self
            .data
            .entry(keyspace.to_owned())
            .or_default()
            .entry(table.to_owned())
            .or_default()
            .get(partition_key);
        Ok(partition
            .into_iter()
            .flat_map(move |p| p.range(range.clone()).map(|(_k, v)| v.iter())))
    }

    fn scan(
        &mut self,
        keyspace: &str,
        table: &str,
        range: impl RangeBounds<usize> + Clone + 'static,
    ) -> eyre::Result<impl Iterator<Item = Self::RowIterator<'_>>> {
        let table = self
            .data
            .entry(keyspace.to_owned())
            .or_default()
            .entry(table.to_owned())
            .or_default();

        let skip = match range.start_bound() {
            Bound::Included(&i) => i,
            Bound::Excluded(&i) => i + 1,
            Bound::Unbounded => 0,
        };

        let take = match range.end_bound() {
            Bound::Included(&i) => i - skip,
            Bound::Excluded(&i) => i - 1 - skip,
            Bound::Unbounded => usize::MAX,
        };

        Ok(table
            .iter()
            .flat_map(|(_key, values)| values.iter().map(|(_, row)| row.iter()))
            .skip(skip)
            .take(take))
    }
}
