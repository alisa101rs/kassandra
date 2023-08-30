use std::collections::HashMap;

use bytes::Bytes;
use eyre::Result;
use serde::Serialize;
use tracing::{instrument, Level};

use crate::{
    cql::query::QueryString,
    error::DbError,
    frame::{
        request::{
            batch::{Batch, BatchStatement},
            execute::Execute,
            query::Query,
        },
        response::{
            error::Error,
            event::{SchemaChangeEvent, SchemaChangeType},
            result::{Prepared, QueryResult, SchemaChange, SetKeyspace},
        },
    },
    snapshot::DataSnapshots,
    storage::{
        keyspace::{Keyspace, Strategy},
        system::{system_keyspace, system_schema_keyspace},
    },
};

#[derive(Debug, Clone, Serialize)]
pub struct Kassandra {
    use_keyspace: Option<String>,
    keyspace: HashMap<String, Keyspace>,
    #[serde(skip)]
    prepared: HashMap<u128, QueryString>,
}

impl Default for Kassandra {
    fn default() -> Self {
        Self::new()
    }
}

impl Kassandra {
    pub fn new() -> Self {
        let keyspace = [system_keyspace(), system_schema_keyspace()]
            .into_iter()
            .collect();

        Self {
            use_keyspace: None,
            prepared: HashMap::new(),
            keyspace,
        }
    }

    pub fn load_state<'a>(data: impl IntoIterator<Item = &'a [u8]>) -> Result<Self> {
        let mut kassandra = Self::new();

        for piece in data {
            let k = ron::de::from_bytes::<HashMap<String, Keyspace>>(piece)?;
            kassandra.keyspace.extend(k.into_iter());
        }

        Ok(kassandra)
    }

    #[instrument(level = Level::TRACE, skip(self), err, ret)]
    pub fn process(&mut self, query: Query<'_>) -> Result<QueryResult, Error> {
        match &query.query {
            QueryString::Use { keyspace, .. } => {
                self.use_keyspace = Some(keyspace.clone());

                Ok(QueryResult::SetKeyspace(SetKeyspace {
                    keyspace_name: keyspace.to_owned(),
                }))
            }
            QueryString::Select { keyspace, .. } => {
                let Some(keyspace) = keyspace.as_deref().or(self.use_keyspace.as_deref()) else {
                    Err(Error::new(DbError::Invalid, "keyspace must be specified"))?
                };
                let Some(keyspace) = self.keyspace.get_mut(keyspace) else {
                    Err(Error::new(DbError::Invalid, "keyspace does not exist"))?
                };
                Ok(keyspace.select(query)?)
            }
            QueryString::Insert { keyspace, .. } => {
                let Some(keyspace) = keyspace.as_deref().or(self.use_keyspace.as_deref()) else {
                    Err(Error::new(DbError::Invalid, "keyspace must be specified"))?
                };
                let Some(keyspace) = self.keyspace.get_mut(keyspace) else {
                    Err(Error::new(DbError::Invalid, "keyspace does not exist"))?
                };
                Ok(keyspace.insert(query)?)
            }
            QueryString::Delete { keyspace, .. } => {
                let Some(keyspace) = keyspace.as_deref().or(self.use_keyspace.as_deref()) else {
                    Err(Error::new(DbError::Invalid, "keyspace must be specified"))?
                };
                let Some(keyspace) = self.keyspace.get_mut(keyspace) else {
                    Err(Error::new(DbError::Invalid, "keyspace does not exist"))?
                };
                Ok(keyspace.delete(query)?)
            }

            QueryString::CreateKeyspace { .. } => Ok(self.create_keyspace(query)?),
            QueryString::CreateTable { keyspace, .. } => {
                let Some(keyspace) = keyspace.as_deref().or(self.use_keyspace.as_deref()) else {
                    Err(DbError::Invalid)?
                };
                let Some(keyspace) = self.keyspace.get_mut(keyspace) else {
                    Err(DbError::Invalid)?
                };

                Ok(keyspace.create_table(query)?)
            }
            QueryString::CreateType { .. } => {
                unimplemented!()
            }
        }
    }

    #[instrument(level = Level::TRACE, skip(self), err, ret)]
    pub fn execute(&mut self, execute: Execute<'_>) -> Result<QueryResult, Error> {
        let Execute {
            id,
            consistency,
            flags,
            data,
            page_size,
            paging_state,
            serial_consistency,
            default_timestamp,
        } = execute;
        let parsed_id = u128::from_be_bytes(id.try_into().unwrap());
        let Some(query) = self.prepared.get(&parsed_id).cloned() else {
            return Err(Error::new(
                DbError::Unprepared {
                    statement_id: Bytes::copy_from_slice(id)
                },
                "unprepared query"
            ))
        };

        let query = Query {
            query,
            raw_query: "",
            consistency,
            flags,
            data,
            page_size,
            paging_state,
            serial_consistency,
            default_timestamp,
        };

        self.process(query)
    }

    #[instrument(level = Level::TRACE, skip(self), err, ret)]
    pub fn process_batch(&mut self, batch: Batch<'_>) -> Result<QueryResult, Error> {
        let Batch {
            batch_type: _,
            consistency: _,
            serial_consistency: _,
            timestamp: _,
            values,
        } = batch;

        for query in values {
            match query {
                BatchStatement::Query(query) => {
                    let QueryResult::Void = self.process(query)? else {
                        unimplemented!("should be only void")
                    };
                }
                BatchStatement::Prepared(execute) => {
                    let QueryResult::Void = self.execute(execute)? else {
                        unimplemented!("should be only void")
                    };
                }
            }
        }

        Ok(QueryResult::Void)
    }

    fn create_keyspace(&mut self, query: Query<'_>) -> Result<QueryResult, DbError> {
        let QueryString::CreateKeyspace { keyspace,ignore_existence,  ..} = query.query else {
            unreachable!()
        };
        if self.keyspace.contains_key(&keyspace) {
            return if ignore_existence {
                Ok(QueryResult::Void)
            } else {
                Err(DbError::AlreadyExists {
                    keyspace,
                    table: "".to_string(),
                })
            };
        }

        self.keyspace.insert(
            keyspace.clone(),
            Keyspace {
                name: keyspace.clone(),
                strategy: Strategy::LocalStrategy,
                tables: Default::default(),
                user_defined_types: Default::default(),
            },
        );

        let event = SchemaChangeEvent::KeyspaceChange {
            change_type: SchemaChangeType::Created,
            keyspace_name: keyspace,
        };

        Ok(QueryResult::SchemaChange(SchemaChange { event }))
    }

    pub fn save_state(&self) -> Vec<u8> {
        ron::ser::to_string_pretty(&self.keyspace, Default::default())
            .unwrap()
            .into_bytes()
    }

    pub fn data_snapshot(&self) -> DataSnapshots {
        DataSnapshots::from_keyspaces(&self.keyspace)
    }

    #[instrument(level = Level::TRACE, skip(self), err, ret)]
    pub fn prepare(&mut self, query: QueryString) -> Result<QueryResult, Error> {
        self.prepare_with_id(query, ulid::Ulid::new().0)
    }

    pub fn prepare_with_id(&mut self, query: QueryString, id: u128) -> Result<QueryResult, Error> {
        if !matches!(
            query,
            QueryString::Insert { .. } | QueryString::Delete { .. } | QueryString::Select { .. }
        ) {
            return Err(Error::new(
                DbError::Invalid,
                "Only INSERT, DELETE or SELECT can be prepared",
            ));
        }
        let Some(keyspace) = query.keyspace().or(self.use_keyspace.as_deref()) else {
            Err(Error::new(DbError::Invalid, "keyspace must be specified"))?
        };
        let Some(keyspace) = self.keyspace.get_mut(keyspace) else {
            Err(Error::new(DbError::Invalid, "keyspace does not exist"))?
        };
        let (prepared_metadata, result_metadata) = keyspace.prepare(query.clone())?;

        self.prepared.insert(id, query);

        let prepared = Prepared {
            id,
            prepared_metadata,
            result_metadata,
        };

        Ok(QueryResult::Prepared(prepared))
    }

    pub fn use_keyspace(&mut self, ks: impl Into<String>) {
        self.use_keyspace = Some(ks.into());
    }
}
