use std::collections::HashMap;

use crate::{
    cql::{
        query::QueryValue,
        schema::{PrimaryKey, TableSchema},
        value::{
            deserialize_value, map_lit, ClusteringKeyValue, ClusteringKeyValueRange, CqlValue,
            PartitionKeyValue,
        },
    },
    error::DbError,
    frame::{response::error::Error, value::FrameValue},
};

pub struct DataPayload<'a> {
    schema: &'a TableSchema,
    pub raw: HashMap<String, Option<CqlValue>>,
}

impl<'a> DataPayload<'a> {
    pub fn read(
        schema: &'a TableSchema,
        columns: impl Iterator<Item = (String, QueryValue)> + 'a,
        data: impl IntoIterator<Item = FrameValue<'a>> + 'a,
    ) -> Result<Self, Error> {
        Ok(Self {
            schema,
            raw: parse_values(schema, columns, data).collect::<Result<_, _>>()?,
        })
    }

    pub fn get_partition_key(&self) -> Result<PartitionKeyValue, Error> {
        Ok(match &self.schema.partition_key {
            PrimaryKey::Empty => unreachable!("Can't have empty primary key"),
            PrimaryKey::Simple(key) => {
                PartitionKeyValue::Simple(
                    self.raw
                        .get(key)
                        .ok_or(DbError::Invalid)? // partition key must be present
                        .as_ref()
                        .ok_or(DbError::Invalid)? // partition key value must not be null
                        .clone(),
                )
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
                PartitionKeyValue::Composite(values)
            }
        })
    }

    pub fn get_clustering_key(&self) -> Result<ClusteringKeyValue, Error> {
        Ok(match &self.schema.clustering_key {
            PrimaryKey::Empty => ClusteringKeyValue::Empty,
            PrimaryKey::Simple(key) => self
                .raw
                .get(key)
                .ok_or(DbError::Invalid)?
                .as_ref()
                .cloned()
                .into(),
            PrimaryKey::Composite(keys) => {
                let mut values = vec![];

                for key in keys {
                    let value = self
                        .raw
                        .get(key)
                        .ok_or(DbError::Invalid)? // clustering key must be present
                        .clone();

                    values.push(value);
                }

                ClusteringKeyValue::Composite(values)
            }
        })
    }

    pub fn get_clustering_key_range(&self) -> Result<ClusteringKeyValueRange, Error> {
        let range = match &self.schema.clustering_key {
            PrimaryKey::Empty => (..).into(),
            PrimaryKey::Simple(key) => {
                if let Some(value) = self.raw.get(key).and_then(Clone::clone) {
                    (ClusteringKeyValue::Simple(Some(value))..).into()
                } else {
                    (..).into()
                }
            }
            PrimaryKey::Composite(keys) => {
                let mut values = vec![];
                for key in keys {
                    let Some(value) = self.raw.get(key).cloned() else {
                        break;
                    };

                    let value = value.ok_or(Error::new(
                        DbError::Invalid,
                        "clustering key value must not be null",
                    ))?;

                    values.push(Some(value));
                }

                let upper = {
                    let mut v = values.clone();

                    for _ in 0..(self.schema.clustering_key.count() - values.len()) {
                        v.push(Some(CqlValue::Empty))
                    }

                    v
                };

                (ClusteringKeyValue::Composite(values)..ClusteringKeyValue::Composite(upper)).into()
            }
        };

        Ok(range)
    }
}

fn parse_values<'a>(
    schema: &'a TableSchema,
    c: impl Iterator<Item = (String, QueryValue)> + 'a,
    data: impl IntoIterator<Item = FrameValue<'a>> + 'a,
) -> impl Iterator<Item = Result<(String, Option<CqlValue>), Error>> + 'a {
    ParsedValuesIter {
        schema,
        inputs: c,
        data: data.into_iter(),
    }
}

struct ParsedValuesIter<'a, I, V> {
    schema: &'a TableSchema,
    inputs: I,
    data: V,
}

impl<'a, I, V> Iterator for ParsedValuesIter<'a, I, V>
where
    I: Iterator<Item = (String, QueryValue)>,
    V: Iterator<Item = FrameValue<'a>>,
{
    type Item = Result<(String, Option<CqlValue>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (column, value) = self.inputs.next()?;

            let Some(schema) = self.schema.columns.get(&column) else {
                return Some(Err(Error::new(
                    DbError::Invalid,
                    format!("unknown column `{column}`"),
                )));
            };

            let value = match value {
                QueryValue::Literal(lit) => map_lit(&schema.ty, lit).map(Some),
                QueryValue::Blankslate => {
                    let Some(next_value) = self.data.next() else {
                        return Some(Err(Error::new(
                            DbError::Invalid,
                            "Missing required blankslate value",
                        )));
                    };

                    match next_value {
                        FrameValue::NotSet => continue,
                        FrameValue::Null => Ok(None),
                        FrameValue::Some(value) => deserialize_value(value, &schema.ty).map(Some),
                    }
                }
            };

            return match value {
                Ok(value) => Some(Ok((column, value))),
                Err(er) => Some(Err(er)),
            };
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inputs.size_hint()
    }
}
