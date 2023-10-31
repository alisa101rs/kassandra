// pub mod keyspace;
// pub mod system;
// pub mod table;

pub mod memory;

pub type Entries = Vec<(String, CqlValue)>;

use std::ops::RangeBounds;

use crate::cql::value::CqlValue;

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
        partition_key: CqlValue,
        clustering_key: CqlValue,
        values: impl Iterator<Item = (String, CqlValue)>,
    ) -> eyre::Result<()>;

    fn delete(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: &CqlValue,
        clustering_key: &CqlValue,
    ) -> eyre::Result<()>;

    fn read(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: &CqlValue,
        range: impl RangeBounds<CqlValue> + Clone + 'static,
    ) -> eyre::Result<Box<dyn Iterator<Item = Self::RowIterator<'_>> + '_>>;

    fn scan(
        &mut self,
        keyspace: &str,
        table: &str,
        range: impl RangeBounds<usize> + Clone + 'static,
    ) -> eyre::Result<Box<dyn Iterator<Item = Self::RowIterator<'_>> + '_>>;
}
