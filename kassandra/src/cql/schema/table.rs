use std::{collections::HashMap, slice};

use serde::{Deserialize, Serialize};

use crate::cql::schema::Column;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    pub keyspace: String,
    pub name: String,
    pub schema: TableSchema,
}

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
    pub fn count(&self) -> usize {
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
