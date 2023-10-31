use std::ops::RangeBounds;

use crate::{
    cql::{query_cache::QueryCache, schema::Catalog, value::CqlValue},
    frame::response::error::Error,
};

pub mod kv;

pub type RowsIterator<'a> = Box<dyn Iterator<Item = Vec<(String, CqlValue)>> + 'a>;

pub trait Engine: Catalog + QueryCache + 'static {
    fn insert(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: CqlValue,
        clustering_key: CqlValue,
        values: Vec<(String, CqlValue)>,
    ) -> Result<(), Error>;

    fn delete(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: CqlValue,
        clustering_key: CqlValue,
    ) -> Result<(), Error>;

    fn read<'a>(
        &'a mut self,
        keyspace: &'a str,
        table: &'a str,
        partition_key: &'a CqlValue,
        clustering_range: impl RangeBounds<CqlValue> + Clone + 'static,
    ) -> Result<RowsIterator<'a>, Error>;

    fn scan<'a>(
        &'a mut self,
        keyspace: &'a str,
        table: &'a str,
        range: impl RangeBounds<usize> + Clone + 'static,
    ) -> Result<RowsIterator<'a>, Error>;
}
