use std::{collections::BTreeMap, ops::RangeBounds};

use super::value::{ClusteringKeyValue, PartitionKeyValue};
use crate::{
    cql::{query_cache::QueryCache, schema::Catalog, value::CqlValue},
    frame::response::error::Error,
};

pub mod kv;

pub type RowsIterator<'a> = Box<dyn Iterator<Item = RowEntry> + 'a>;

pub struct RowEntry {
    pub partition: PartitionKeyValue,
    pub clustering: ClusteringKeyValue,
    pub row: BTreeMap<String, CqlValue>,
}

pub trait Engine: Catalog + QueryCache + 'static {
    fn insert(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: PartitionKeyValue,
        clustering_key: ClusteringKeyValue,
        values: Vec<(String, CqlValue)>,
    ) -> Result<(), Error>;

    fn delete(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: PartitionKeyValue,
        clustering_key: ClusteringKeyValue,
    ) -> Result<(), Error>;

    fn read<'a>(
        &'a mut self,
        keyspace: &'a str,
        table: &'a str,
        partition_key: &'a PartitionKeyValue,
        clustering_range: impl RangeBounds<ClusteringKeyValue> + Clone + 'static,
    ) -> Result<RowsIterator<'a>, Error>;

    fn scan<'a>(
        &'a mut self,
        keyspace: &'a str,
        table: &'a str,
        range: impl RangeBounds<PartitionKeyValue> + Clone + 'static,
    ) -> Result<RowsIterator<'a>, Error>;
}
