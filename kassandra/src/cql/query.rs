use std::fmt;

use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::cql::{functions::CqlFunction, literal::Literal, types::PreCqlType};

#[derive(Debug, Clone, Serialize, Deserialize, Display, From)]
pub enum QueryString {
    #[display(fmt = "{}", "_0")]
    Select(SelectQuery),
    #[display(fmt = "{}", "_0")]
    Insert(InsertQuery),
    #[display(fmt = "{}", "_0")]
    Delete(DeleteQuery),
    #[display(fmt = "USE {}", "keyspace")]
    Use { keyspace: String },
    #[display(fmt = "{}", "_0")]
    CreateKeyspace(CreateKeyspaceQuery),
    #[display(fmt = "{}", "_0")]
    CreateTable(CreateTableQuery),
    #[display(fmt = "{}", "_0")]
    CreateType(CreateTypeQuery),
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
#[display(
    fmt = "SELECT {} FROM {}.{} WHERE {}",
    "columns",
    "keyspace.as_deref().unwrap_or_default()",
    "table",
    "r#where"
)]
pub struct SelectQuery {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: SelectExpression,
    pub r#where: WhereClosure,
    pub limit: Option<usize>,
    pub json: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
#[display(
    fmt = "INSERT INTO {}.{} ({}) VALUES({})",
    "keyspace.as_deref().unwrap_or_default()",
    "table",
    "columns.join(\", \")",
    "values.iter().map(|it| it.to_string()).collect::<Vec<_>>().join(\", \")"
)]
pub struct InsertQuery {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<QueryValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
#[display(
    fmt = "DELETE {} FROM {}.{} WHERE {}",
    "columns.join(\", \")",
    "keyspace.as_deref().unwrap_or_default()",
    "table",
    "r#where"
)]
pub struct DeleteQuery {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub r#where: WhereClosure,
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
#[display(fmt = "CREATE KEYSPACE {}", "keyspace")]
pub struct CreateKeyspaceQuery {
    pub keyspace: String,
    pub ignore_existence: bool,
    pub replication: Literal,
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
#[display(
    fmt = "CREATE TABLE {}.{}",
    "keyspace.as_deref().unwrap_or_default()",
    "table"
)]
pub struct CreateTableQuery {
    pub keyspace: Option<String>,
    pub table: String,
    pub ignore_existence: bool,
    pub columns: Vec<(String, PreCqlType)>,
    pub partition_keys: Vec<String>,
    pub clustering_keys: Vec<String>,
    pub options: Vec<(String, Literal)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
#[display(
    fmt = "CREATE TYPE {}.{}",
    "keyspace.as_deref().unwrap_or_default()",
    "name"
)]
pub struct CreateTypeQuery {
    pub keyspace: Option<String>,
    pub name: String,
    pub columns: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectExpression {
    All,
    Columns(Vec<ColumnSelector>),
}

impl fmt::Display for SelectExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let columns = match self {
            SelectExpression::All => return write!(f, "*"),
            SelectExpression::Columns(c) => c,
        };
        for (column, last) in columns.iter().zip(
            std::iter::repeat(false)
                .take(columns.len() - 1)
                .chain(Some(true)),
        ) {
            write!(f, "{}", column)?;
            if !last {
                write!(f, ",")?
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialOrd, PartialEq, Ord, Eq)]
pub struct ColumnSelector {
    pub name: String,
    pub alias: Option<String>,
    pub function: Option<CqlFunction>,
}

impl fmt::Display for ColumnSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(function) = &self.function {
            write!(f, "{function}({})", self.name)?;
        } else {
            write!(f, "{}", self.name)?;
        }
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WhereClosure {
    pub statements: Vec<(String, QueryValue)>,
}

impl WhereClosure {
    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }
}

impl fmt::Display for WhereClosure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self.statements.iter().peekable();
        while let Some((name, value)) = iter.next() {
            write!(f, "{name} = {value}")?;
            if iter.peek().is_some() {
                write!(f, " AND ")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
pub enum QueryValue {
    #[display(fmt = "{}", "_0")]
    Literal(Literal),
    #[display(fmt = "?")]
    Blankslate,
}
