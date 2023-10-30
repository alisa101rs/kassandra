use std::collections::HashMap;

use serde::Serialize;

use crate::{
    cql,
    cql::{
        column,
        execution::{
            AlterSchema, DeleteNode, Executor, InsertNode, JsonNode, PagingState, ScanNode,
            SelectNode,
        },
        query::{
            CreateKeyspaceQuery, CreateTableQuery, DeleteQuery, InsertQuery, QueryString,
            QueryValue, SelectExpression, SelectQuery,
        },
        schema::{keyspace::Strategy, Catalog, Column, ColumnKind, PrimaryKey, TableSchema},
        types::PreCqlType,
        value::CqlValue,
    },
    error::DbError,
    frame::{
        request::query_params::QueryParameters,
        response::{
            error::Error,
            result::{
                ColumnSpec, PartitionKeyIndex, PreparedMetadata, QueryResult, ResultMetadata,
                TableSpec,
            },
        },
    },
};

mod data_reader;

#[derive(Debug, Clone, Serialize)]
pub enum Plan {
    Select(SelectNode),
    SelectJson(JsonNode<SelectNode>),
    Scan(ScanNode),
    ScanJson(JsonNode<ScanNode>),
    Insert(InsertNode),
    Delete(DeleteNode),
    AlterSchema(AlterSchema),
}

impl Plan {
    pub fn build(
        statement: QueryString,
        parameters: QueryParameters<'_>,
        use_keyspace: Option<String>,
        catalog: &mut impl Catalog,
    ) -> Result<Plan, Error> {
        Planner {
            catalog,
            use_keyspace,
        }
        .build(statement, parameters)
    }

    pub fn prepare(
        statement: QueryString,
        use_keyspace: Option<String>,
        catalog: &mut impl Catalog,
    ) -> Result<(PreparedMetadata, ResultMetadata), Error> {
        Planner {
            catalog,
            use_keyspace,
        }
        .prepare(statement)
    }

    pub fn execute(self, engine: &mut impl cql::Engine) -> Result<QueryResult, Error> {
        match self {
            Plan::Select(s) => Box::new(s).execute(engine),
            Plan::SelectJson(s) => Box::new(s).execute(engine),
            Plan::AlterSchema(s) => Box::new(s).execute(engine),
            Plan::Insert(i) => Box::new(i).execute(engine),
            Plan::Scan(s) => Box::new(s).execute(engine),
            Plan::ScanJson(s) => Box::new(s).execute(engine),
            Plan::Delete(d) => Box::new(d).execute(engine),
        }
    }
}

pub struct Planner<C: Catalog> {
    catalog: C,
    use_keyspace: Option<String>,
}

impl<C: Catalog> Planner<C> {
    pub fn build(
        &mut self,
        statement: QueryString,
        parameters: QueryParameters<'_>,
    ) -> Result<Plan, Error> {
        match statement {
            QueryString::Select(select) if !select.r#where.is_empty() => {
                self.select(select, parameters)
            }

            QueryString::Select(select) => self.scan(select),
            QueryString::Insert(insert) => self.insert(insert, parameters),
            QueryString::Delete(delete) if delete.columns.is_empty() => {
                self.delete(delete, parameters)
            }
            QueryString::Delete(delete) => self.delete_columns(delete, parameters),
            QueryString::Use { .. } => unimplemented!(),
            QueryString::CreateKeyspace(create) => self.create_keyspace(create),
            QueryString::CreateTable(create) => self.create_table(create),
            QueryString::CreateType { .. } => unimplemented!(),
        }
    }

    pub fn prepare(
        &mut self,
        statement: QueryString,
    ) -> Result<(PreparedMetadata, ResultMetadata), Error> {
        match statement {
            QueryString::Select(select) => self.prepare_select(select),
            QueryString::Insert(insert) => self.prepare_insert(insert),
            QueryString::Delete(delete) if delete.columns.is_empty() => self.prepare_delete(delete),

            _ => Err(Error::new(
                DbError::Invalid,
                "Can't prepare this type of query",
            )),
        }
    }

    fn insert(&mut self, insert: InsertQuery, parameters: QueryParameters) -> Result<Plan, Error> {
        let InsertQuery {
            keyspace,
            table,
            columns,
            values,
        } = insert;
        let keyspace = keyspace
            .or(self.use_keyspace.clone())
            .ok_or(Error::new(DbError::Invalid, "Keyspace is not specified"))?;

        if values.len() != columns.len() {
            return Err(Error::new(
                DbError::SyntaxError,
                "Missmatch the amount of columns and values",
            ));
        }

        let schema = self.catalog.get_table(&keyspace, &table).ok_or(Error::new(
            DbError::Invalid,
            "Keyspace or table does nor exist",
        ))?;

        let values = data_reader::DataPayload::read(
            schema,
            columns.into_iter().zip(values),
            parameters.data,
        )?;

        let partition_key = values.get_partition_key()?;
        let clustering_key = values.get_clustering_key()?;

        let values = values
            .raw
            .into_iter()
            .filter_map(|(k, v)| Some((k, v?)))
            .collect();

        let insert = InsertNode {
            keyspace,
            table,
            partition_key,
            clustering_key,
            values,
        };

        Ok(Plan::Insert(insert))
    }

    fn prepare_insert(
        &mut self,
        insert: InsertQuery,
    ) -> Result<(PreparedMetadata, ResultMetadata), Error> {
        let InsertQuery {
            keyspace,
            table,
            columns,
            values,
        } = insert;
        let keyspace = keyspace
            .or(self.use_keyspace.clone())
            .ok_or(Error::new(DbError::Invalid, "Keyspace is not specified"))?;

        if values.len() != columns.len() {
            return Err(Error::new(
                DbError::SyntaxError,
                "Missmatch the amount of columns and values",
            ));
        }

        let schema = self.catalog.get_table(&keyspace, &table).ok_or(Error::new(
            DbError::Invalid,
            "Keyspace or table does nor exist",
        ))?;

        let prepared_metadata = prepared_metadata(
            &keyspace,
            &table,
            schema,
            columns.into_iter().zip(values.into_iter()),
        )?;

        let result_metadata = ResultMetadata::empty();

        Ok((prepared_metadata, result_metadata))
    }

    fn delete_columns(
        &mut self,
        delete: DeleteQuery,
        parameters: QueryParameters,
    ) -> Result<Plan, Error> {
        assert!(
            !delete.columns.is_empty(),
            "Other method should be called for that"
        );

        let keyspace = delete
            .keyspace
            .or(self.use_keyspace.clone())
            .ok_or(Error::new(DbError::Invalid, "Keyspace is not specified"))?;
        let schema = self
            .catalog
            .get_table(&keyspace, &delete.table)
            .ok_or(Error::new(
                DbError::Invalid,
                "Keyspace or table does nor exist",
            ))?;

        let values = data_reader::DataPayload::read(
            schema,
            delete.r#where.statements.into_iter(),
            parameters.data,
        )?;

        let partition_key = values.get_partition_key()?;
        let clustering_key = values.get_clustering_key().unwrap_or(CqlValue::Empty);
        let mut values = vec![];
        for column in delete.columns {
            if schema.columns.get(&column).is_none() {
                return Err(Error::new(
                    DbError::Invalid,
                    format!("Unknown column `{column}`"),
                ));
            }
            values.push((column, CqlValue::Empty));
        }

        Ok(Plan::Insert(InsertNode {
            keyspace,
            table: delete.table,
            partition_key,
            clustering_key,
            values,
        }))
    }

    fn delete(&mut self, delete: DeleteQuery, parameters: QueryParameters) -> Result<Plan, Error> {
        assert!(
            delete.columns.is_empty(),
            "Other method should be called for that"
        );

        let keyspace = delete
            .keyspace
            .or(self.use_keyspace.clone())
            .ok_or(Error::new(DbError::Invalid, "Keyspace is not specified"))?;
        let schema = self
            .catalog
            .get_table(&keyspace, &delete.table)
            .ok_or(Error::new(
                DbError::Invalid,
                "Keyspace or table does nor exist",
            ))?;

        let values = data_reader::DataPayload::read(
            schema,
            delete.r#where.statements.into_iter(),
            parameters.data,
        )?;

        let partition_key = values.get_partition_key()?;
        let clustering_key = values.get_clustering_key().unwrap_or(CqlValue::Empty);

        Ok(Plan::Delete(DeleteNode {
            keyspace,
            table: delete.table,
            partition_key,
            clustering_key,
        }))
    }

    fn prepare_delete(
        &mut self,
        delete: DeleteQuery,
    ) -> Result<(PreparedMetadata, ResultMetadata), Error> {
        assert!(delete.columns.is_empty(), "Other method should be called");
        let DeleteQuery {
            keyspace,
            table,
            r#where,
            ..
        } = delete;

        let keyspace = keyspace
            .or(self.use_keyspace.clone())
            .ok_or(Error::new(DbError::Invalid, "Keyspace is not specified"))?;

        let schema = self.catalog.get_table(&keyspace, &table).ok_or(Error::new(
            DbError::Invalid,
            "Keyspace or table does nor exist",
        ))?;

        let prepared_metadata =
            prepared_metadata(&keyspace, &table, schema, r#where.statements.into_iter())?;

        let result_metadata = ResultMetadata::empty();

        Ok((prepared_metadata, result_metadata))
    }

    fn create_keyspace(&mut self, create: CreateKeyspaceQuery) -> Result<Plan, Error> {
        // todo parse replication literal

        Ok(Plan::AlterSchema(AlterSchema::Keyspace {
            name: create.keyspace,
            ignore_existence: create.ignore_existence,
            replication: Strategy::LocalStrategy,
        }))
    }

    fn create_table(&mut self, create: CreateTableQuery) -> Result<Plan, Error> {
        let CreateTableQuery {
            keyspace,
            table,
            ignore_existence,
            columns,
            partition_keys,
            clustering_keys,
            options,
        } = create;
        let keyspace = keyspace
            .or(self.use_keyspace.clone())
            .ok_or(Error::new(DbError::Invalid, "Keyspace is not specified"))?;

        Ok(Plan::AlterSchema(AlterSchema::Table {
            keyspace,
            name: table,
            ignore_existence,
            schema: create_table_schema(columns, partition_keys, clustering_keys),
            options,
        }))
    }

    fn select(&mut self, select: SelectQuery, parameters: QueryParameters) -> Result<Plan, Error> {
        let SelectQuery {
            keyspace,
            table,
            columns,
            r#where,
            ..
        } = select;

        let keyspace = keyspace
            .or(self.use_keyspace.clone())
            .ok_or(Error::new(DbError::Invalid, "Keyspace is not specified"))?;

        let schema = self.catalog.get_table(&keyspace, &table).ok_or(Error::new(
            DbError::Invalid,
            "Keyspace or table does nor exist",
        ))?;

        let values = data_reader::DataPayload::read(
            schema,
            r#where.statements.into_iter(),
            parameters.data,
        )?;

        let partition_key = values.get_partition_key()?;
        let clustering_key = values.get_clustering_key_range()?;

        let metadata = metadata(&keyspace, &table, schema, &columns)?;

        let node = SelectNode {
            keyspace,
            table,
            partition_key,
            clustering_key,
            metadata,
            limit: None,
            state: PagingState {
                row: None,
                remaining: 0,
            },
        };
        if select.json {
            Ok(Plan::SelectJson(JsonNode(Box::new(node))))
        } else {
            Ok(Plan::Select(node))
        }
    }

    fn prepare_select(
        &mut self,
        select: SelectQuery,
    ) -> Result<(PreparedMetadata, ResultMetadata), Error> {
        let SelectQuery {
            keyspace,
            table,
            columns,
            r#where,
            ..
        } = select;

        let keyspace = keyspace
            .or(self.use_keyspace.clone())
            .ok_or(Error::new(DbError::Invalid, "Keyspace is not specified"))?;

        let schema = self.catalog.get_table(&keyspace, &table).ok_or(Error::new(
            DbError::Invalid,
            "Keyspace or table does nor exist",
        ))?;

        let metadata = metadata(&keyspace, &table, schema, &columns)?;
        let prepared_metadata =
            prepared_metadata(&keyspace, &table, schema, r#where.statements.into_iter())?;

        Ok((prepared_metadata, metadata))
    }

    fn scan(&mut self, select: SelectQuery) -> Result<Plan, Error> {
        let SelectQuery {
            keyspace,
            table,
            columns,
            ..
        } = select;

        let keyspace = keyspace
            .or(self.use_keyspace.clone())
            .ok_or(Error::new(DbError::Invalid, "Keyspace is not specified"))?;

        let schema = self.catalog.get_table(&keyspace, &table).ok_or(Error::new(
            DbError::Invalid,
            "Keyspace or table does nor exist",
        ))?;

        let metadata = metadata(&keyspace, &table, schema, &columns)?;

        let node = ScanNode {
            keyspace,
            table,
            metadata,
            range: 0..500,
        };

        if select.json {
            Ok(Plan::ScanJson(JsonNode(Box::new(node))))
        } else {
            Ok(Plan::Scan(node))
        }
    }
}

fn metadata(
    keyspace: &str,
    table: &str,
    schema: &TableSchema,
    columns: &SelectExpression,
) -> Result<ResultMetadata, DbError> {
    let columns = match &columns {
        SelectExpression::All => schema.columns.iter().collect::<Vec<_>>(),
        SelectExpression::Columns(columns) => {
            let mut c = vec![];
            for column_name in columns {
                let Some(column) = schema.columns.get(column_name) else {
                    Err(DbError::Invalid)?
                };
                c.push((column_name, column));
            }
            c
        }
    };

    Ok(ResultMetadata {
        col_count: columns.len(),
        global_spec: Some(TableSpec {
            ks_name: keyspace.to_owned(),
            table_name: table.to_owned(),
        }),
        paging_state: None,
        col_specs: columns
            .into_iter()
            .map(|(name, c)| ColumnSpec::new(name.clone(), c.ty.clone()))
            .collect(),
    })
}

fn prepared_metadata(
    keyspace: &str,
    table: &str,
    schema: &TableSchema,
    r#where: impl Iterator<Item = (String, QueryValue)>,
) -> Result<PreparedMetadata, Error> {
    let mut pk_indexes = vec![];
    let mut col_specs = vec![];

    for (seq, (column, value)) in r#where.enumerate() {
        match value {
            QueryValue::Blankslate => {
                if let Some(index) = schema.partition_key.into_iter().position(|p| p == &column) {
                    pk_indexes.push(PartitionKeyIndex {
                        index: seq as _,
                        sequence: index as _,
                    });
                }
            }
            QueryValue::Literal(_) => {}
        }

        let Some(column_spec) = schema.columns.get(&column) else {
            return Err(Error::new(
                DbError::Invalid,
                format!("unknown column `{column}`"),
            ));
        };

        col_specs.push(ColumnSpec::new(column, column_spec.ty.clone()));
    }

    Ok(PreparedMetadata {
        pk_indexes,
        global_spec: Some(TableSpec {
            ks_name: keyspace.to_owned(),
            table_name: table.to_owned(),
        }),
        col_specs,
    })
}

fn create_table_schema(
    columns: Vec<(String, PreCqlType)>,
    partition_keys: Vec<String>,
    clustering_keys: Vec<String>,
) -> TableSchema {
    let mut columns_res = HashMap::default();

    for (column_name, column_type) in columns {
        let kind = if partition_keys.contains(&column_name) {
            ColumnKind::PartitionKey
        } else if clustering_keys.contains(&column_name) {
            ColumnKind::Clustering
        } else {
            ColumnKind::Regular
        };
        let ty = column::map_pre_type(column_type);

        columns_res.insert(column_name, Column { ty, kind });
    }

    TableSchema {
        columns: columns_res,
        partition_key: PrimaryKey::from_definition(partition_keys),
        clustering_key: PrimaryKey::from_definition(clustering_keys),
        partitioner: None,
    }
}
