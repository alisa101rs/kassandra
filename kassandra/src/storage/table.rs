use std::{collections::BTreeMap, hash::BuildHasherDefault, ops::RangeInclusive};

use seahash::SeaHasher;
use serde::{Deserialize, Serialize};

use crate::{
    cql::{
        column::map_pre_type,
        query::{QueryString, QueryValue, SelectExpression},
        value::CqlValue,
    },
    error::DbError,
    frame::{
        request::query::Query,
        response::{
            error::Error,
            result::{
                ColumnSpec, PartitionKeyIndex, PreparedMetadata, QueryResult, ResultMetadata, Row,
                Rows, TableSpec,
            },
        },
    },
    storage::schema::{Column, ColumnKind, PrimaryKey, TableSchema},
};

type HashMap<K, V> = std::collections::HashMap<K, V, BuildHasherDefault<SeaHasher>>;

type RowValues = HashMap<String, Option<CqlValue>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    pub keyspace: String,
    pub name: String,
    pub schema: TableSchema,
    pub data: HashMap<CqlValue, BTreeMap<CqlValue, RowValues>>,
}

impl Table {
    pub fn create(query: Query<'_>) -> Result<Self, DbError> {
        let QueryString::CreateTable {
            keyspace: Some(keyspace),
            table,
            ignore_existence: _,
            columns,
            partition_keys,
            clustering_keys,
            options: _,
        } = query.query else {
            unreachable!()
        };

        let mut columns_res = std::collections::HashMap::default();

        for (column_name, column_type) in columns {
            let kind = if partition_keys.contains(&column_name) {
                ColumnKind::PartitionKey
            } else if clustering_keys.contains(&column_name) {
                ColumnKind::Clustering
            } else {
                ColumnKind::Regular
            };
            let ty = map_pre_type(column_type);

            columns_res.insert(column_name, Column { ty, kind });
        }

        let schema = TableSchema {
            columns: columns_res,
            partition_key: PrimaryKey::from_definition(partition_keys),
            clustering_key: PrimaryKey::from_definition(clustering_keys),
            partitioner: None,
        };

        Ok(Self {
            keyspace,
            name: table,
            schema,
            data: Default::default(),
        })
    }

    pub fn insert(&mut self, query: Query<'_>) -> Result<QueryResult, Error> {
        tracing::trace!(?query, "Query");
        let QueryString::Insert {
            columns, values, ..
        } = query.query else {
            unreachable!()
        };

        let data_payload = DataPayload::read(
            &self.schema,
            columns.into_iter().zip(values),
            query.data.into_iter(),
        )?;

        let partition_key_value = data_payload.get_partition_key()?;
        let clustering_key = data_payload.get_clustering_key()?;

        self.data
            .entry(partition_key_value)
            .or_default()
            .entry(clustering_key)
            .or_default()
            .extend(data_payload.raw.into_iter());

        Ok(QueryResult::Void)
    }

    pub fn delete(&mut self, query: Query<'_>) -> Result<QueryResult, Error> {
        let QueryString::Delete {
            keyspace:_,
            table:_,
            columns,
            values,
        } = query.query else {
            unreachable!()
        };

        let mut data_payload = DataPayload::read(
            &self.schema,
            columns.into_iter().zip(values),
            query.data.into_iter(),
        )?;

        let partition_key_value = data_payload.remove_partition_key()?;
        let clustering_key_range = data_payload.remove_clustering_key_range()?;

        let empty = if let Some(rows) = self.data.get_mut(&partition_key_value) {
            rows.retain(|k, _| !clustering_key_range.contains(k));
            rows.is_empty()
        } else {
            false
        };

        if empty {
            self.data.remove(&partition_key_value);
        }

        Ok(QueryResult::Void)
    }

    pub fn select(&self, query: Query<'_>) -> Result<QueryResult, Error> {
        let QueryString::Select {
            keyspace:_,
            table:_,
            columns,
            r#where: closure
        } = query.query else {
            unreachable!()
        };

        let columns = match &columns {
            SelectExpression::All => self.schema.columns.iter().collect::<Vec<_>>(),
            SelectExpression::Columns(columns) => {
                let mut c = vec![];
                for column_name in columns {
                    let Some(column) = self.schema.columns.get(column_name) else {
                        Err(DbError::Invalid)?
                    };
                    c.push((column_name, column));
                }
                c
            }
        };

        let rows = match closure {
            None => self.get_all(&columns),
            Some(closure) => {
                let mut data_payload = DataPayload::read(
                    &self.schema,
                    closure.statements.into_iter(),
                    query.data.into_iter(),
                )?;

                self.get(&columns, &mut data_payload)?
            }
        };

        let rows = Rows {
            metadata: ResultMetadata {
                col_count: columns.len(),
                global_spec: Some(TableSpec {
                    ks_name: self.keyspace.clone(),
                    table_name: self.name.clone(),
                }),
                paging_state: None,
                col_specs: columns
                    .into_iter()
                    .map(|(name, c)| ColumnSpec::new(name.clone(), c.ty.clone()))
                    .collect(),
            },
            rows,
        };

        let res = QueryResult::Rows(rows);
        Ok(res)
    }

    fn get_all(&self, columns: &[(&String, &Column)]) -> Vec<Row> {
        let mut rows = vec![];

        for values in self.data.values() {
            for value in values.values() {
                let mut row = Row::new();
                for &(c, _) in columns {
                    row.push(value.get(c).and_then(|it| it.clone()));
                }

                rows.push(row);
            }
        }

        rows
    }

    fn get(
        &self,
        columns: &[(&String, &Column)],
        data_payload: &mut DataPayload<'_>,
    ) -> Result<Vec<Row>, Error> {
        let partition_key_value = data_payload.remove_partition_key()?;
        let clustering_key_range = data_payload.remove_clustering_key_range()?;

        let Some(values) = self.data.get(&partition_key_value) else {
            return Ok(vec![])
        };

        let mut rows = vec![];
        for (_, row_values) in values.range(clustering_key_range) {
            let mut row = Row::new();
            for &(c, _) in columns {
                row.push(row_values.get(c).and_then(|it| it.clone()));
            }

            rows.push(row);
        }

        Ok(rows)
    }

    pub fn prepare(
        &mut self,
        query: QueryString,
    ) -> Result<(PreparedMetadata, ResultMetadata), Error> {
        let global_spec = Some(TableSpec {
            ks_name: self.keyspace.clone(),
            table_name: self.name.clone(),
        });

        let (prepared, result) = match query {
            QueryString::Select {
                columns,
                r#where: closure,
                ..
            } => {
                let (pk_indexes, col_specs) = match closure {
                    Some(r#where) => {
                        let mut pk_indexes = vec![];
                        let mut col_specs = vec![];

                        for (seq, (column, value)) in r#where.statements.into_iter().enumerate() {
                            match value {
                                QueryValue::Literal(_) => {
                                    unimplemented!()
                                }
                                QueryValue::Blankslate => {
                                    if let Some(index) = self
                                        .schema
                                        .partition_key
                                        .into_iter()
                                        .position(|p| p == &column)
                                    {
                                        pk_indexes.push(PartitionKeyIndex {
                                            index: seq as _,
                                            sequence: index as _,
                                        });
                                    }

                                    let Some(column_spec) = self.schema.columns.get(&column) else {
                                        return Err(Error::new(
                                            DbError::Invalid,
                                            format!("unknown column `{column}`"),
                                        ))
                                    };

                                    col_specs.push(ColumnSpec::new(column, column_spec.ty.clone()));
                                }
                            }
                        }

                        (pk_indexes, col_specs)
                    }
                    None => (vec![], vec![]),
                };

                let prepared_metadata = PreparedMetadata {
                    global_spec: global_spec.clone(),
                    pk_indexes,
                    col_specs,
                };

                let col_specs = match columns {
                    SelectExpression::All => self
                        .schema
                        .columns
                        .iter()
                        .map(|(name, c)| ColumnSpec::new(name.clone(), c.ty.clone()))
                        .collect(),
                    SelectExpression::Columns(columns) => columns
                        .into_iter()
                        .map::<Result<_, Error>, _>(|name| {
                            let c = self.schema.columns.get(&name).ok_or_else(|| {
                                Error::new(DbError::Invalid, format!("unknown column `{name}`"))
                            })?;
                            Ok(ColumnSpec::new(name, c.ty.clone()))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                };

                let result_metadata = ResultMetadata {
                    col_count: col_specs.len(),
                    global_spec,
                    paging_state: None,
                    col_specs,
                };

                (prepared_metadata, result_metadata)
            }
            QueryString::Insert { columns, .. } => {
                let mut pk_indexes = vec![];
                let mut col_specs = vec![];

                for (seq, column) in columns.iter().enumerate() {
                    let spec = self.schema.columns.get(column).ok_or_else(|| {
                        Error::new(DbError::Invalid, format!("unknown column `{column}`"))
                    })?;

                    if let Some(index) = self
                        .schema
                        .partition_key
                        .into_iter()
                        .position(|p| p == column)
                    {
                        pk_indexes.push(PartitionKeyIndex {
                            index: index as _,
                            sequence: seq as _,
                        });
                    }

                    col_specs.push(ColumnSpec::new(column.clone(), spec.ty.clone()));
                }

                let prepared_metadata = PreparedMetadata {
                    global_spec: global_spec.clone(),
                    pk_indexes,
                    col_specs,
                };

                let col_specs = columns
                    .into_iter()
                    .map::<Result<_, Error>, _>(|name| {
                        let c = self.schema.columns.get(&name).ok_or_else(|| {
                            Error::new(DbError::Invalid, format!("unknown column `{name}`"))
                        })?;
                        Ok(ColumnSpec::new(name, c.ty.clone()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let result_metadata = ResultMetadata {
                    col_count: col_specs.len(),
                    global_spec,
                    paging_state: None,
                    col_specs,
                };

                (prepared_metadata, result_metadata)
            }
            QueryString::Delete { columns, .. } => {
                let mut pk_indexes = vec![];
                let mut col_specs = vec![];

                for (seq, column) in columns.iter().enumerate() {
                    let spec = self
                        .schema
                        .columns
                        .get(column)
                        .ok_or_else(|| Error::new(DbError::Invalid, "unknown column"))?;

                    if let Some(index) = self
                        .schema
                        .partition_key
                        .into_iter()
                        .position(|p| p == column)
                    {
                        pk_indexes.push(PartitionKeyIndex {
                            index: index as _,
                            sequence: seq as _,
                        });
                    }

                    col_specs.push(ColumnSpec::new(column.clone(), spec.ty.clone()));
                }

                let prepared_metadata = PreparedMetadata {
                    global_spec: global_spec.clone(),
                    pk_indexes,
                    col_specs,
                };

                let col_specs = columns
                    .into_iter()
                    .map::<Result<_, Error>, _>(|name| {
                        let c = self
                            .schema
                            .columns
                            .get(&name)
                            .ok_or_else(|| Error::new(DbError::Invalid, "unknown column"))?;
                        Ok(ColumnSpec::new(name, c.ty.clone()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let result_metadata = ResultMetadata {
                    col_count: col_specs.len(),
                    global_spec,
                    paging_state: None,
                    col_specs,
                };

                (prepared_metadata, result_metadata)
            }
            _ => unimplemented!(),
        };

        Ok((prepared, result))
    }
}

struct DataPayload<'a> {
    schema: &'a TableSchema,
    raw: HashMap<String, Option<CqlValue>>,
}

impl<'a> DataPayload<'a> {
    fn read(
        schema: &'a TableSchema,
        columns: impl Iterator<Item = (String, QueryValue)> + 'a,
        data: impl IntoIterator<Item = Option<&'a [u8]>> + 'a,
    ) -> Result<Self, Error> {
        Ok(Self {
            schema,
            raw: schema
                .parse_values(columns, data)
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_partition_key(&self) -> Result<CqlValue, Error> {
        Ok(match &self.schema.partition_key {
            PrimaryKey::Empty => unreachable!("Can't have empty primary key"),
            PrimaryKey::Simple(key) => {
                self.raw
                    .get(key)
                    .ok_or(DbError::Invalid)? // partition key must be present
                    .as_ref()
                    .ok_or(DbError::Invalid)? // partition key value must not be null
                    .clone()
            }
            PrimaryKey::Composite(keys) => {
                let mut values = vec![];
                for key in keys {
                    let value = self
                        .raw
                        .get(key)
                        .ok_or(DbError::Invalid)? // partition key must be present
                        .as_ref()
                        .ok_or(DbError::Invalid)?; // partition key value must not be null

                    values.push(value.clone());
                }
                CqlValue::Tuple(values)
            }
        })
    }

    fn remove_partition_key(&mut self) -> Result<CqlValue, Error> {
        Ok(match &self.schema.partition_key {
            PrimaryKey::Empty => unreachable!("Can't have empty primary key"),
            PrimaryKey::Simple(key) => {
                self.raw.remove(key).flatten().ok_or(DbError::Invalid)? // partition key must be present
            }
            PrimaryKey::Composite(keys) => {
                let mut values = vec![];
                for key in keys {
                    let value = self.raw.remove(key).flatten().ok_or(DbError::Invalid)?; // partition key must be present

                    values.push(value);
                }
                CqlValue::Tuple(values)
            }
        })
    }

    fn get_clustering_key(&self) -> Result<CqlValue, Error> {
        Ok(match &self.schema.clustering_key {
            PrimaryKey::Empty => CqlValue::Empty,
            PrimaryKey::Simple(key) => self
                .raw
                .get(key)
                .ok_or(DbError::Invalid)?
                .as_ref()
                .cloned()
                .unwrap_or(CqlValue::Empty),
            PrimaryKey::Composite(keys) => {
                let mut values = vec![];

                for key in keys {
                    let value = self
                        .raw
                        .get(key)
                        .ok_or(DbError::Invalid)? // clustering key must be present
                        .clone()
                        .unwrap_or(CqlValue::Empty);

                    values.push(value.clone());
                }

                CqlValue::Tuple(values)
            }
        })
    }

    fn remove_clustering_key_range(&mut self) -> Result<RangeInclusive<CqlValue>, Error> {
        let range = match &self.schema.clustering_key {
            PrimaryKey::Empty => CqlValue::Empty..=CqlValue::Empty,
            PrimaryKey::Simple(key) => {
                let value = self.raw.remove(key).flatten().unwrap_or(CqlValue::Empty);

                if !self.raw.is_empty() {
                    return Err(Error::new(DbError::Invalid, "something went wrong"));
                }

                value..=CqlValue::Empty
            }
            PrimaryKey::Composite(keys) => {
                let mut values = vec![];
                for key in keys {
                    let Some(value) = self.raw.remove(key) else {
                        break
                    };

                    let value = value.ok_or(Error::new(
                        DbError::Invalid,
                        "clustering key value must not be null",
                    ))?;

                    values.push(value);
                }
                if !self.raw.is_empty() {
                    return Err(Error::new(DbError::Invalid, "something went wrong"));
                }
                let upper = {
                    let mut v = values.clone();

                    for _ in 0..(self.schema.clustering_key.len() - values.len()) {
                        v.push(CqlValue::Empty)
                    }

                    v
                };

                CqlValue::Tuple(values)..=CqlValue::Tuple(upper)
            }
        };

        Ok(range)
    }
}
