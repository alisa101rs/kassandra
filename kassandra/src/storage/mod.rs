// pub mod keyspace;
// pub mod system;
// pub mod table;

pub mod memory;

pub type Entries = Vec<(String, CqlValue)>;

use std::ops::RangeBounds;

use crate::cql::value::{ClusteringKeyValue, CqlValue, PartitionKeyValue};

pub struct RowEntry<'a, I: 'a> {
    pub partition: &'a PartitionKeyValue,
    pub clustering: &'a ClusteringKeyValue,
    pub row: I,
}

pub trait Storage: std::fmt::Debug + Send + 'static {
    type RowIterator<'a>: Iterator<Item = (&'a String, &'a CqlValue)>
    where
        Self: 'a;

    fn create_keyspace(&mut self, keyspace: &str) -> eyre::Result<()>;
    fn create_table(&mut self, keyspace: &str, table: &str) -> eyre::Result<()>;

    fn write(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: PartitionKeyValue,
        clustering_key: ClusteringKeyValue,
        values: impl Iterator<Item = (String, CqlValue)>,
    ) -> eyre::Result<()>;

    fn delete(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: &PartitionKeyValue,
        clustering_key: &ClusteringKeyValue,
    ) -> eyre::Result<()>;

    fn read<'a, 'b: 'a>(
        &'a mut self,
        keyspace: &str,
        table: &str,
        partition_key: &'b PartitionKeyValue,
        range: impl RangeBounds<ClusteringKeyValue> + Clone + 'static,
    ) -> eyre::Result<Box<dyn Iterator<Item = RowEntry<Self::RowIterator<'a>>> + 'a>>;

    fn scan(
        &mut self,
        keyspace: &str,
        table: &str,
        range: impl RangeBounds<PartitionKeyValue> + Clone + 'static,
    ) -> eyre::Result<Box<dyn Iterator<Item = RowEntry<Self::RowIterator<'_>>> + '_>>;
}
