use std::ops::RangeBounds;

use serde::{Deserialize, Serialize};

use crate::{
    cql,
    cql::{
        engine::RowsIterator,
        literal::Literal,
        query::QueryString,
        query_cache::PersistedQueryCache,
        schema::{
            keyspace::{Keyspace, Strategy},
            PersistedSchema, Table, TableSchema,
        },
        value::CqlValue,
    },
    error::DbError,
    frame::response::{error::Error, event::SchemaChangeEvent},
    storage::Storage,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct KvEngine<S: Storage> {
    pub data: S,
    schema: PersistedSchema,
    query_cache: PersistedQueryCache,
}

impl<S: Storage + Default> Default for KvEngine<S> {
    fn default() -> Self {
        let mut storage = Self {
            data: S::default(),
            schema: PersistedSchema::default(),
            query_cache: PersistedQueryCache::default(),
        };

        PersistedSchema::persist_system_schema(&mut storage.data);

        storage
    }
}

impl<S: Storage> cql::Catalog for KvEngine<S> {
    fn create_keyspace(
        &mut self,
        keyspace: String,
        ignore_existence: bool,
        replication: Strategy,
    ) -> Result<&Keyspace, DbError> {
        self.schema
            .create_keyspace(&mut self.data, keyspace, ignore_existence, replication)
    }

    fn create_table(
        &mut self,
        keyspace: String,
        table: String,
        ignore_existence: bool,
        schema: TableSchema,
        options: Vec<(String, Literal)>,
    ) -> Result<&Table, DbError> {
        self.schema.create_table(
            &mut self.data,
            keyspace,
            table,
            ignore_existence,
            schema,
            options,
        )
    }

    fn create_type(
        &mut self,
        _keyspace: Option<String>,
        _table: String,
        _columns: Vec<(String, String)>,
    ) -> Result<SchemaChangeEvent, DbError> {
        todo!()
    }

    fn get_table(&self, keyspace: &str, table: &str) -> Option<&TableSchema> {
        self.schema.get_table(keyspace, table)
    }
}

impl<S: Storage> cql::QueryCache for KvEngine<S> {
    fn store(&mut self, id: u128, query: QueryString) -> Result<(), DbError> {
        self.query_cache.store(id, query, &mut self.data)
    }

    fn retrieve(&mut self, id: u128) -> Result<Option<QueryString>, DbError> {
        self.query_cache.retrieve(id, &self.data)
    }
}

impl<S: Storage> cql::Engine for KvEngine<S> {
    fn insert(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: CqlValue,
        clustering_key: CqlValue,
        values: Vec<(String, CqlValue)>,
    ) -> Result<(), Error> {
        self.data
            .write(
                keyspace,
                table,
                partition_key,
                clustering_key,
                values.into_iter(),
            )
            .map_err(|e| Error::new(DbError::Invalid, format!("{e}")))
    }

    fn delete(
        &mut self,
        keyspace: &str,
        table: &str,
        partition_key: CqlValue,
        clustering_key: CqlValue,
    ) -> Result<(), Error> {
        self.data
            .delete(keyspace, table, &partition_key, &clustering_key)
            .map_err(|e| Error::new(DbError::Invalid, format!("{e}")))
    }

    fn read<'a>(
        &'a mut self,
        keyspace: &'a str,
        table: &'a str,
        partition_key: &'a CqlValue,
        clustering_range: impl RangeBounds<CqlValue> + Clone + 'static,
    ) -> Result<RowsIterator<'a>, Error> {
        let scan = self
            .data
            .read(keyspace, table, partition_key, clustering_range)
            .map_err(|e| Error::new(DbError::Invalid, format!("{e}")))?;

        Ok(Box::new(scan.map(|row| {
            row.map(|(k, v)| (k.clone(), v.clone())).collect()
        })))
    }

    fn scan<'a>(
        &'a mut self,
        keyspace: &'a str,
        table: &'a str,
        range: impl RangeBounds<usize> + Clone + 'static,
    ) -> Result<RowsIterator<'a>, Error> {
        let scan = self
            .data
            .scan(keyspace, table, range)
            .map_err(|e| Error::new(DbError::Invalid, format!("{e}")))?;

        Ok(Box::new(scan.map(|row| {
            row.map(|(k, v)| (k.clone(), v.clone())).collect()
        })))
    }
}
