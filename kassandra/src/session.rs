use std::net::IpAddr;

use bytes::Bytes;
use tracing::{instrument, Level};
use uuid::uuid;

use crate::{
    cql::{
        self,
        engine::kv::KvEngine,
        execution::InsertNode,
        plan::Plan,
        query::QueryString,
        value::{ClusteringKeyValue, CqlValue, PartitionKeyValue},
    },
    error::DbError,
    frame::{
        request::{
            batch::{Batch, BatchStatement},
            execute::Execute,
            query::Query,
            QueryFlags, QueryParameters,
        },
        response::{
            error::Error,
            result::{Prepared, QueryResult, SetKeyspace},
        },
    },
    snapshot::DataSnapshots,
    storage::memory::{self, Memory},
};

#[derive(Debug, Clone)]
pub struct KassandraSession<E: cql::Engine = KvEngine<Memory>> {
    use_keyspace: Option<String>,
    engine: E,
}

impl<E: cql::Engine + Default> Default for KassandraSession<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: cql::Engine + Default> KassandraSession<E> {
    pub fn new() -> Self {
        let mut engine = Default::default();
        init_session()
            .execute(&mut engine)
            .expect("Could not init session");
        Self {
            engine,
            use_keyspace: None,
        }
    }
}

impl<E: cql::Engine> KassandraSession<E> {
    #[instrument(level = Level::TRACE, skip(self), fields(operation = query.query.name(), target = query.query.target()) err, ret)]
    pub fn process(&mut self, query: Query) -> Result<QueryResult, Error> {
        match query.query {
            QueryString::Use { keyspace } => {
                self.use_keyspace(&keyspace);
                Ok(QueryResult::SetKeyspace(SetKeyspace {
                    keyspace_name: keyspace.to_owned(),
                }))
            }
            other => {
                let plan = Plan::build(
                    other,
                    query.parameters,
                    self.use_keyspace.clone(),
                    &mut self.engine,
                )?;
                tracing::trace!(?plan, "Built a plan");

                plan.execute(&mut self.engine)
            }
        }
    }

    #[instrument(level = Level::TRACE, skip(self), err, ret)]
    pub fn execute(&mut self, execute: Execute<'_>) -> Result<QueryResult, Error> {
        let id = u128::from_be_bytes(
            execute
                .id
                .try_into()
                .map_err(|_| Error::new(DbError::Invalid, "Invalid id for prepared query"))?,
        );
        let Some(query) = self.engine.retrieve(id)? else {
            return Err(Error::new(
                DbError::Unprepared {
                    statement_id: Bytes::copy_from_slice(execute.id),
                },
                "Unprepared query id",
            ));
        };

        self.process(Query {
            query,
            raw_query: "",
            parameters: execute.parameters,
        })
    }

    #[instrument(level = Level::TRACE, skip(self), err, ret)]
    pub fn process_batch(&mut self, batch: Batch<'_>) -> Result<QueryResult, Error> {
        for statement in batch.statements {
            let (query, values) = match statement {
                BatchStatement::Query { query, values, .. } => (query, values),
                BatchStatement::Prepared { id, values, .. } => {
                    let parsed_id = u128::from_be_bytes(id.try_into().map_err(|_| {
                        Error::new(DbError::Invalid, "Invalid id for prepared query")
                    })?);
                    let Some(query) = self.engine.retrieve(parsed_id)? else {
                        return Err(Error::new(
                            DbError::Unprepared {
                                statement_id: Bytes::copy_from_slice(id),
                            },
                            "Unprepared query id",
                        ));
                    };
                    (query, values)
                }
            };

            self.process(Query {
                query,
                raw_query: "",
                parameters: QueryParameters {
                    consistency: batch.consistency,
                    flags: QueryFlags::VALUES,
                    data: values,
                    result_page_size: None,
                    paging_state: None,
                    serial_consistency: batch.serial_consistency,
                    default_timestamp: batch.timestamp,
                },
            })?;
        }

        Ok(QueryResult::Void)
    }

    #[instrument(level = Level::TRACE, skip(self), err, ret)]
    pub fn prepare(&mut self, query: QueryString) -> Result<QueryResult, Error> {
        self.prepare_with_id(query, ulid::Ulid::new().0)
    }

    #[instrument(level = Level::TRACE, skip(self), err, ret)]
    pub fn prepare_with_id(&mut self, query: QueryString, id: u128) -> Result<QueryResult, Error> {
        let (prepared_metadata, result_metadata) =
            Plan::prepare(query.clone(), self.use_keyspace.clone(), &mut self.engine)?;

        self.engine.store(id, query)?;

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

impl KassandraSession<KvEngine<memory::Memory>> {
    pub fn load_state(data: &[u8]) -> eyre::Result<Self> {
        let engine = ron::de::from_bytes(data)?;

        Ok(Self {
            use_keyspace: None,
            engine,
        })
    }

    pub fn save_state(&self) -> Vec<u8> {
        ron::ser::to_string_pretty(&self.engine, Default::default())
            .unwrap()
            .into_bytes()
    }

    pub fn data_snapshot(&self) -> DataSnapshots {
        self.engine.data.snapshot()
    }
}

fn init_session() -> Plan {
    Plan::Insert(InsertNode {
        keyspace: "system".to_string(),
        table: "local".to_string(),
        partition_key: PartitionKeyValue::Simple(CqlValue::Text("local".to_owned())),
        clustering_key: ClusteringKeyValue::Empty,
        values: vec![
            ("key".to_owned(), "local".to_owned().into()),
            ("bootstrapped".to_owned(), "COMPLETED".to_owned().into()),
            (
                "broadcast_address".to_owned(),
                CqlValue::Inet(IpAddr::from([127, 0, 0, 1])),
            ),
            ("cluster_name".to_owned(), "Test Cluster".to_owned().into()),
            ("data_center".to_owned(), "datacenter1".to_owned().into()),
            ("gossip_generation".to_owned(), CqlValue::Int(1683509222)),
            (
                "listen_address".to_owned(),
                CqlValue::Inet(IpAddr::from([127, 0, 0, 1])),
            ),
            ("native_protocol_version".to_owned(), "4".to_owned().into()),
            ("rack".to_owned(), "rack".to_owned().into()),
            ("release_version".to_owned(), "3.0.0".to_owned().into()),
            ("cql_version".to_owned(), "4.1.0".to_owned().into()),
            (
                "host_id".to_owned(),
                CqlValue::Uuid(uuid! {"aa1f1ae0-469d-4abf-ae3f-ecb7a17132fe"}),
            ),
            (
                "schema_version".to_owned(),
                CqlValue::Uuid(uuid! {"0b1c3252-f787-4099-8594-157323b71789"}),
            ),
            (
                "rpc_address".to_owned(),
                CqlValue::Inet(IpAddr::from([127, 0, 0, 1])),
            ),
            (
                "tokens".to_owned(),
                CqlValue::Set(vec!["hello".to_owned().into()]),
            ),
        ],
    })
}
