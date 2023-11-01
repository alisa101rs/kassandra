use serde::{Deserialize, Serialize};

use crate::{
    cql::{
        column::ColumnKind,
        literal::Literal,
        schema::{
            keyspace::{Keyspace, Strategy},
            system::{system_keyspace, system_schema_keyspace},
            Schema, Table, TableSchema,
        },
        value::CqlValue,
        Catalog,
    },
    error::DbError,
    frame::response::event::SchemaChangeEvent,
    storage,
};

#[derive(Debug, Clone, Serialize, Default, Deserialize)]
pub struct PersistedSchema {
    pub schema: Schema,
}

impl PersistedSchema {
    fn insert_keyspace(
        storage: &mut impl storage::Storage,
        keyspace: &Keyspace,
    ) -> Result<(), DbError> {
        let pk: CqlValue = keyspace.name.clone().into();
        let class: &'static str = keyspace.strategy.clone().into();
        let replication = [
            ("class".to_owned().into(), class.to_owned().into()),
            (
                "replication_factor".to_owned().into(),
                "1".to_owned().into(),
            ),
        ]
        .into_iter()
        .collect::<Vec<(CqlValue, CqlValue)>>();

        storage
            .write(
                "system_schema",
                "keyspaces",
                pk.clone(),
                CqlValue::Empty,
                [
                    ("keyspace_name".to_owned(), pk),
                    ("durable_writes".to_owned(), CqlValue::Boolean(true)),
                    ("replication".to_owned(), CqlValue::Map(replication)),
                ]
                .into_iter(),
            )
            .map_err(|_| DbError::Invalid)?;

        Ok(())
    }
    fn insert_table(storage: &mut impl storage::Storage, table: &Table) -> Result<(), DbError> {
        let pk: CqlValue = table.keyspace.clone().into();
        let ck: CqlValue = table.name.clone().into();

        storage
            .write(
                "system_schema",
                "tables",
                pk.clone(),
                ck.clone(),
                [
                    ("keyspace_name".to_owned(), pk),
                    ("table_name".to_owned(), ck),
                    ("allow_auto_snapshot".to_owned(), CqlValue::Boolean(false)),
                    ("incremental_backups".to_owned(), CqlValue::Boolean(false)),
                    ("cdc".to_owned(), CqlValue::Boolean(false)),
                ]
                .into_iter(),
            )
            .map_err(|_| DbError::Invalid)?;

        Ok(())
    }

    fn insert_columns(storage: &mut impl storage::Storage, table: &Table) -> Result<(), DbError> {
        let pk: CqlValue = table.keyspace.clone().into();

        let mut partition_order = -1;
        let mut clustering_order = -1;
        for (column_name, column_spec) in table.schema.columns.iter() {
            let name: CqlValue = column_name.clone().into();
            let ck: CqlValue = CqlValue::Tuple(vec![table.name.clone().into(), name.clone()]);

            let order = match column_spec.kind {
                ColumnKind::Regular => -1,
                ColumnKind::Static => -1,
                ColumnKind::Clustering => {
                    clustering_order += 1;

                    clustering_order
                }
                ColumnKind::PartitionKey => {
                    partition_order += 1;

                    partition_order
                }
            };

            storage
                .write(
                    "system_schema",
                    "columns",
                    pk.clone(),
                    ck.clone(),
                    [
                        ("keyspace_name".to_owned(), pk.clone()),
                        ("table_name".to_owned(), table.name.clone().into()),
                        ("column_name".to_owned(), name),
                        ("clustering_order".to_owned(), "none".to_owned().into()),
                        (
                            "column_name_bytes".to_owned(),
                            CqlValue::Blob(column_name.as_bytes().to_owned()),
                        ),
                        (
                            "kind".to_owned(),
                            CqlValue::Text(column_spec.kind.to_string()),
                        ),
                        ("position".to_owned(), CqlValue::Int(order as _)),
                        ("type".to_owned(), column_spec.ty.into_cql().unwrap().into()),
                    ]
                    .into_iter(),
                )
                .map_err(|_| DbError::Invalid)?;
        }

        Ok(())
    }

    pub(crate) fn persist_system_schema(storage: &mut impl storage::Storage) {
        for (_, keyspace) in [system_keyspace(), system_schema_keyspace()] {
            Self::insert_keyspace(storage, &keyspace).expect("system keyspace not to fail");
            for table in keyspace.tables.values() {
                Self::insert_table(storage, table).expect("system tables not to fail");
                Self::insert_columns(storage, table).expect("system table not to fail");
            }
        }
    }
}

impl PersistedSchema {
    pub(crate) fn create_keyspace(
        &mut self,
        storage: &mut impl storage::Storage,
        keyspace: String,
        ignore_existence: bool,
        replication: Strategy,
    ) -> Result<&Keyspace, DbError> {
        let ks = self
            .schema
            .create_keyspace(keyspace, ignore_existence, replication)?;
        Self::insert_keyspace(storage, ks)?;

        Ok(ks)
    }

    pub(crate) fn create_table(
        &mut self,
        storage: &mut impl storage::Storage,
        keyspace: String,
        table: String,
        ignore_existence: bool,
        schema: TableSchema,
        options: Vec<(String, Literal)>,
    ) -> Result<&Table, DbError> {
        let table = self
            .schema
            .create_table(keyspace, table, ignore_existence, schema, options)?;
        Self::insert_table(storage, table)?;
        Self::insert_columns(storage, table)?;

        Ok(table)
    }

    #[allow(dead_code)]
    fn create_type(
        &mut self,
        _storage: &mut impl storage::Storage,
        _keyspace: Option<String>,
        _table: String,
        _columns: Vec<(String, String)>,
    ) -> Result<SchemaChangeEvent, DbError> {
        todo!()
    }

    pub fn get_table(&self, keyspace: &str, table: &str) -> Option<&TableSchema> {
        self.schema.get_table(keyspace, table)
    }
}
