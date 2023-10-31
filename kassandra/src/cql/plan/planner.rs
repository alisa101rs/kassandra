use std::collections::BTreeMap;

use crate::{
    cql::{
        column,
        column::{Column, ColumnKind},
        execution,
        execution::{
            selector::{ColumnsSelector, Transform},
            AlterSchema, DeleteNode, InsertNode, PagingState, ScanNode, SelectNode,
        },
        functions::CqlFunction,
        plan::{data_reader, Aggregate, Plan},
        query,
        query::{
            CreateKeyspaceQuery, CreateTableQuery, DeleteQuery, InsertQuery, QueryString,
            QueryValue, SelectExpression, SelectQuery,
        },
        schema::{keyspace::Strategy, PrimaryKey, TableSchema},
        types::PreCqlType,
        value::CqlValue,
        Catalog,
    },
    error::DbError,
    frame::{
        request::QueryParameters,
        response::{
            error::Error,
            result::{ColumnSpec, PartitionKeyIndex, PreparedMetadata, ResultMetadata, TableSpec},
        },
    },
};

pub struct Planner<C: Catalog> {
    catalog: C,
    use_keyspace: Option<String>,
}

impl<C: Catalog> Planner<C> {
    pub fn new(catalog: C, use_keyspace: Option<String>) -> Self {
        Self {
            catalog,
            use_keyspace,
        }
    }
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
                "Mismatch the amount of columns and values",
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
        let selector = columns_selector(schema, columns)?;

        let node = SelectNode {
            keyspace,
            table,
            partition_key,
            selector,
            clustering_key,
            metadata,
            limit: None,
            state: PagingState {
                row: None,
                remaining: 0,
            },
        };
        if select.json {
            Ok(Plan::Aggregate {
                source: Box::new(Plan::Select(node)),
                aggregate: Aggregate::Json,
            })
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
        let selector = columns_selector(schema, columns)?;

        let node = ScanNode {
            keyspace,
            table,
            metadata,
            selector,
            range: 0..500,
        };

        if select.json {
            Ok(Plan::Aggregate {
                source: Box::new(Plan::Scan(node)),
                aggregate: Aggregate::Json,
            })
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
    let global_spec = Some(TableSpec {
        ks_name: keyspace.to_owned(),
        table_name: table.to_owned(),
    });
    let col_specs = match &columns {
        SelectExpression::All => schema
            .columns
            .iter()
            .map(|(name, c)| ColumnSpec::new(name.clone(), c.ty.clone()))
            .collect(),
        SelectExpression::Columns(columns) => columns
            .iter()
            .map(|it| resolve_column_spec(schema, it))
            .collect::<Result<Vec<_>, _>>()?,
    };

    Ok(ResultMetadata {
        global_spec,
        paging_state: None,
        col_specs,
    })
}

fn resolve_column_spec(
    schema: &TableSchema,
    selector: &query::ColumnSelector,
) -> Result<ColumnSpec, DbError> {
    let Some(column) = schema.columns.get(&selector.name) else {
        // Unknown column
        return Err(DbError::Invalid);
    };
    let name = selector.alias.as_ref().unwrap_or(&selector.name).clone();
    let ty = selector
        .function
        .map(|it| it.return_type(&column.ty))
        .unwrap_or_else(|| column.ty.clone());

    Ok(ColumnSpec::new(name, ty))
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
    let mut columns_res = BTreeMap::default();

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

fn columns_selector(
    schema: &TableSchema,
    selector: query::SelectExpression,
) -> Result<ColumnsSelector, DbError> {
    Ok(ColumnsSelector(match selector {
        SelectExpression::All => schema
            .columns.keys().map(|name| execution::ColumnSelector {
                name: name.clone(),
                transform: Transform::Identity,
            })
            .collect(),
        SelectExpression::Columns(columns) => columns
            .iter()
            .map(|column| {
                let transform = match column.function {
                    None => Transform::Identity,
                    Some(CqlFunction::ToJson) => Transform::ToJson,
                    Some(_) => return Err(DbError::Invalid),
                };
                Ok(execution::ColumnSelector {
                    name: column.name.clone(),
                    transform,
                })
            })
            .collect::<Result<_, _>>()?,
    }))
}
