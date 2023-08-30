use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    cql::{column::ColumnType, query::QueryString},
    error::DbError,
    frame::{
        request::query::Query,
        response::{
            error::Error,
            event::{SchemaChangeEvent, SchemaChangeType},
            result::{PreparedMetadata, QueryResult, ResultMetadata, SchemaChange},
        },
    },
    storage::table::Table,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Keyspace {
    pub name: String,
    pub strategy: Strategy,
    pub tables: HashMap<String, Table>,
    pub user_defined_types: HashMap<String, UserDefinedType>,
}

impl Keyspace {
    pub fn create_table(&mut self, query: Query<'_>) -> Result<QueryResult, DbError> {
        let QueryString::CreateTable { table, ignore_existence, ..} = &query.query else {
            unreachable!()
        };

        if self.tables.contains_key(table) {
            return if *ignore_existence {
                Ok(QueryResult::Void)
            } else {
                Err(DbError::AlreadyExists {
                    keyspace: self.name.clone(),
                    table: table.clone(),
                })
            };
        }

        let event = SchemaChangeEvent::TableChange {
            change_type: SchemaChangeType::Created,
            keyspace_name: self.name.clone(),
            object_name: table.clone(),
        };

        self.tables.insert(table.clone(), Table::create(query)?);

        Ok(QueryResult::SchemaChange(SchemaChange { event }))
    }

    pub fn insert(&mut self, query: Query<'_>) -> Result<QueryResult, Error> {
        let QueryString::Insert { table, .. } = &query.query else {
            unreachable!()
        };

        let Some(table) = self.tables.get_mut(table) else {
            Err(Error::new(DbError::Invalid, format!("unconfigured table `{table}`")))?
        };

        table.insert(query)
    }

    pub fn select(&mut self, query: Query<'_>) -> Result<QueryResult, Error> {
        let QueryString::Select { table, .. } = &query.query else {
            unreachable!()
        };

        let Some(table) = self.tables.get(table) else {
            Err(Error::new(DbError::Invalid, format!("unconfigured table `{table}`")))?
        };

        table.select(query)
    }

    pub fn delete(&mut self, query: Query<'_>) -> Result<QueryResult, Error> {
        let QueryString::Delete { table, .. } = &query.query else {
            unreachable!()
        };

        let Some(table) = self.tables.get_mut(table) else {
            Err(Error::new(DbError::Invalid, format!("unconfigured table `{table}`")))?
        };

        table.delete(query)
    }

    pub(crate) fn prepare(
        &mut self,
        query: QueryString,
    ) -> Result<(PreparedMetadata, ResultMetadata), Error> {
        let table = query.table().unwrap();

        let Some(table) = self.tables.get_mut(table) else {
            Err(Error::new(DbError::Invalid, format!("unconfigured table `{table}`")))?
        };

        table.prepare(query)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum Strategy {
    SimpleStrategy {
        replication_factor: usize,
    },
    NetworkTopologyStrategy {
        // Replication factors of datacenters with given names
        datacenter_repfactors: HashMap<String, usize>,
    },
    LocalStrategy, // replication_factor == 1
    Other {
        name: String,
        data: HashMap<String, String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserDefinedType {
    pub name: String,
    pub keyspace: String,
    pub field_types: Vec<(String, ColumnType)>,
}
