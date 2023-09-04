use std::{collections::HashMap, slice};

use serde::{Deserialize, Serialize};

use crate::{
    cql::{
        column::ColumnType,
        query::QueryValue,
        value::{deserialize_value, map_lit, CqlValue},
    },
    error::DbError,
    frame::response::error::Error,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub columns: HashMap<String, Column>,
    pub partition_key: PrimaryKey,
    pub clustering_key: PrimaryKey,
    pub partitioner: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrimaryKey {
    Empty,
    Simple(String),
    Composite(Vec<String>),
}

impl PrimaryKey {
    pub fn len(&self) -> usize {
        match self {
            PrimaryKey::Empty => 0,
            PrimaryKey::Simple(_) => 1,
            PrimaryKey::Composite(v) => v.len(),
        }
    }
    pub fn from_definition(mut names: Vec<String>) -> PrimaryKey {
        match names.len() {
            0 => PrimaryKey::Empty,
            1 => PrimaryKey::Simple(names.pop().unwrap()),
            _ => PrimaryKey::Composite(names),
        }
    }
}

impl<'a> IntoIterator for &'a PrimaryKey {
    type Item = &'a String;
    type IntoIter = slice::Iter<'a, String>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            PrimaryKey::Empty => slice::Iter::default(),
            PrimaryKey::Simple(v) => slice::from_ref(v).iter(),
            PrimaryKey::Composite(v) => v.iter(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub ty: ColumnType,
    pub kind: ColumnKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnKind {
    Regular,
    Static,
    Clustering,
    PartitionKey,
}

impl TableSchema {
    pub fn parse_values<'a>(
        &'a self,
        c: impl Iterator<Item = (String, QueryValue)> + 'a,
        data: impl IntoIterator<Item = Option<&'a [u8]>> + 'a,
    ) -> impl Iterator<Item = Result<(String, Option<CqlValue>), Error>> + 'a {
        ParsedValuesIter {
            schema: self,
            inputs: c,
            data: data.into_iter(),
        }
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
                return Some(Err(Error::new(DbError::Invalid, format!("unknown column `{column}`"))))
            };

            let value = match value {
                QueryValue::Literal(lit) => map_lit(&schema.ty, lit).map(Some),
                QueryValue::Blankslate => {
                    let Some(next_value) = self.data.next() else {
                        return Some(Err(Error::new(DbError::Invalid, "Missing required blankslate value")))
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
