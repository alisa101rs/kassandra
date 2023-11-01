pub mod column;
pub mod keyspace;
pub mod persisted;
pub mod system;
pub mod table;

use std::collections::{btree_map::Entry, BTreeMap};

use derive_more::{Deref, DerefMut};
use serde::{Deserialize, Serialize};

pub use self::{
    column::{Column, ColumnKind, ColumnType},
    persisted::PersistedSchema,
    table::{PrimaryKey, Table, TableSchema},
};
use crate::{
    cql::{
        literal::Literal,
        schema::{
            keyspace::{Keyspace, Strategy},
            system::{system_keyspace, system_schema_keyspace},
        },
    },
    error::DbError,
    frame::response::event::SchemaChangeEvent,
};

pub trait Catalog {
    fn create_keyspace(
        &mut self,
        keyspace: String,
        ignore_existence: bool,
        replication: Strategy,
    ) -> Result<&Keyspace, DbError>;

    fn create_table(
        &mut self,
        keyspace: String,
        table: String,
        ignore_existence: bool,
        schema: TableSchema,
        options: Vec<(String, Literal)>,
    ) -> Result<&Table, DbError>;

    fn create_type(
        &mut self,
        keyspace: Option<String>,
        table: String,
        columns: Vec<(String, String)>,
    ) -> Result<SchemaChangeEvent, DbError>;

    fn get_table(&self, keyspace: &str, table: &str) -> Option<&TableSchema>;
}

#[derive(Debug, Clone, Serialize, Deserialize, Deref, DerefMut)]
#[serde(transparent)]
pub struct Schema(pub BTreeMap<String, Keyspace>);

impl Default for Schema {
    fn default() -> Self {
        Self(
            [system_keyspace(), system_schema_keyspace()]
                .into_iter()
                .collect(),
        )
    }
}

impl Catalog for Schema {
    fn create_keyspace(
        &mut self,
        keyspace: String,
        ignore_existence: bool,
        strategy: Strategy,
    ) -> Result<&Keyspace, DbError> {
        match self.0.entry(keyspace) {
            Entry::Occupied(occupied) if ignore_existence => Ok(&*occupied.into_mut()),
            Entry::Occupied(occupied) => Err(DbError::AlreadyExists {
                keyspace: occupied.key().clone(),
                table: "".to_string(),
            }),
            Entry::Vacant(vacant) => {
                let name = vacant.key().clone();
                let ks = vacant.insert(Keyspace {
                    name,
                    strategy,
                    tables: Default::default(),
                    user_defined_types: Default::default(),
                });

                Ok(&*ks)
            }
        }
    }

    fn create_table(
        &mut self,
        keyspace: String,
        table: String,
        ignore_existence: bool,
        schema: TableSchema,
        _options: Vec<(String, Literal)>,
    ) -> Result<&Table, DbError> {
        let ks = self.0.get_mut(&keyspace).ok_or(DbError::Invalid)?;

        match ks.tables.entry(table.clone()) {
            Entry::Occupied(occupied) if ignore_existence => Ok(&*occupied.into_mut()),
            Entry::Occupied(_) => Err(DbError::AlreadyExists { keyspace, table }),
            Entry::Vacant(vacant) => {
                let table = vacant.insert(Table {
                    keyspace,
                    name: table,
                    schema,
                });

                Ok(&*table)
            }
        }
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
        self.0.get(keyspace)?.tables.get(table).map(|it| &it.schema)
    }
}

impl<'a, C: Catalog> Catalog for &'a mut C {
    fn create_keyspace(
        &mut self,
        keyspace: String,
        ignore_existence: bool,
        replication: Strategy,
    ) -> Result<&Keyspace, DbError> {
        (*self).create_keyspace(keyspace, ignore_existence, replication)
    }

    fn create_table(
        &mut self,
        keyspace: String,
        table: String,
        ignore_existence: bool,
        schema: TableSchema,
        options: Vec<(String, Literal)>,
    ) -> Result<&Table, DbError> {
        (*self).create_table(keyspace, table, ignore_existence, schema, options)
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
        (**self).get_table(keyspace, table)
    }
}
