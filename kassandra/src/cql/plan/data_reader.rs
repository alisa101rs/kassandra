use std::{collections::HashMap, ops::RangeInclusive};

use crate::{
    cql::{
        query::QueryValue,
        schema::{PrimaryKey, TableSchema},
        value::{deserialize_value, map_lit, CqlValue},
    },
    error::DbError,
    frame::response::error::Error,
};

pub struct DataPayload<'a> {
    schema: &'a TableSchema,
    pub raw: HashMap<String, Option<CqlValue>>,
}

impl<'a> DataPayload<'a> {
    pub fn read(
        schema: &'a TableSchema,
        columns: impl Iterator<Item = (String, QueryValue)> + 'a,
        data: impl IntoIterator<Item = Option<&'a [u8]>> + 'a,
    ) -> Result<Self, Error> {
        Ok(Self {
            schema,
            raw: parse_values(schema, columns, data).collect::<Result<_, _>>()?,
        })
    }

    pub fn get_partition_key(&self) -> Result<CqlValue, Error> {
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

    pub fn get_clustering_key(&self) -> Result<CqlValue, Error> {
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

    pub fn get_clustering_key_range(&self) -> Result<RangeInclusive<CqlValue>, Error> {
        let range = match &self.schema.clustering_key {
            PrimaryKey::Empty => CqlValue::Empty..=CqlValue::Empty,
            PrimaryKey::Simple(key) => {
                let value = self
                    .raw
                    .get(key)
                    .and_then(Clone::clone)
                    .unwrap_or(CqlValue::Empty);

                value..=CqlValue::Empty
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

                    values.push(value);
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

fn parse_values<'a>(
    schema: &'a TableSchema,
    c: impl Iterator<Item = (String, QueryValue)> + 'a,
    data: impl IntoIterator<Item = Option<&'a [u8]>> + 'a,
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
    V: Iterator<Item = Option<&'a [u8]>>,
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
                        None => continue,
                        Some(&[]) => Ok(None),
                        Some(value) => deserialize_value(value, &schema.ty).map(Some),
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
