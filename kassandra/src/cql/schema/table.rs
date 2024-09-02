use std::slice;

use indexmap::map::IndexMap;
use serde::{Deserialize, Serialize};

use super::ColumnType;
use crate::cql::schema::Column;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    pub keyspace: String,
    pub name: String,
    pub schema: TableSchema,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub columns: IndexMap<String, Column>,
    pub partition_key: PrimaryKey,
    pub clustering_key: PrimaryKey,
    pub partitioner: Option<String>,
}

impl TableSchema {
    pub fn clustering_key_column(&self) -> PrimaryKeyColumn {
        PrimaryKeyColumn::new(self.clustering_key.into_iter(), &self.columns)
    }
    pub fn partition_key_column(&self) -> PrimaryKeyColumn {
        PrimaryKeyColumn::new(self.partition_key.into_iter(), &self.columns)
    }
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrimaryKeyColumn {
    Empty,
    Simple(ColumnType),
    Composite(Vec<ColumnType>),
}

impl PrimaryKeyColumn {
    fn new<'a>(
        mut names: impl Iterator<Item = &'a String>,
        def: &'a IndexMap<String, Column>,
    ) -> Self {
        let Some(first) = names.next() else {
            return Self::Empty;
        };
        let first = def.get(first).cloned().unwrap().ty;

        let Some(next) = names.next() else {
            return Self::Simple(first);
        };

        let tail = std::iter::once(next)
            .chain(names)
            .map(|it| def.get(it).unwrap().clone().ty);

        Self::Composite(std::iter::once(first).chain(tail).collect())
    }

    pub fn size(&self) -> usize {
        match self {
            PrimaryKeyColumn::Empty => 0,
            PrimaryKeyColumn::Simple(_) => 1,
            PrimaryKeyColumn::Composite(v) => v.len(),
        }
    }
}

impl<'a> IntoIterator for &'a PrimaryKeyColumn {
    type Item = &'a ColumnType;
    type IntoIter = slice::Iter<'a, ColumnType>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            PrimaryKeyColumn::Empty => slice::Iter::default(),
            PrimaryKeyColumn::Simple(v) => slice::from_ref(v).iter(),
            PrimaryKeyColumn::Composite(v) => v.iter(),
        }
    }
}
