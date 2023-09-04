use std::fmt;

use serde::Serialize;

use crate::cql::{literal::Literal, types::PreCqlType};

#[derive(Debug, Clone, Serialize)]
pub enum QueryString {
    Select {
        keyspace: Option<String>,
        table: String,
        columns: SelectExpression,
        r#where: Option<WhereClosure>,
    },
    Insert {
        keyspace: Option<String>,
        table: String,
        columns: Vec<String>,
        values: Vec<QueryValue>,
    },
    Delete {
        keyspace: Option<String>,
        table: String,
        columns: Vec<String>,
        values: Vec<QueryValue>,
    },
    Use {
        keyspace: String,
    },
    CreateKeyspace {
        keyspace: String,
        ignore_existence: bool,
        replication: Literal,
    },
    CreateTable {
        keyspace: Option<String>,
        table: String,
        ignore_existence: bool,
        columns: Vec<(String, PreCqlType)>,
        partition_keys: Vec<String>,
        clustering_keys: Vec<String>,
        options: Vec<(String, Literal)>,
    },
    CreateType {
        keyspace: Option<String>,
        table: String,
        columns: Vec<(String, String)>,
    },
}

impl QueryString {
    pub fn keyspace(&self) -> Option<&str> {
        match self {
            QueryString::Select { keyspace, .. } => keyspace.as_deref(),
            QueryString::Insert { keyspace, .. } => keyspace.as_deref(),
            QueryString::Delete { keyspace, .. } => keyspace.as_deref(),
            QueryString::Use { keyspace, .. } => Some(keyspace),
            QueryString::CreateKeyspace { keyspace, .. } => Some(keyspace),
            QueryString::CreateTable { keyspace, .. } => keyspace.as_deref(),
            QueryString::CreateType { keyspace, .. } => keyspace.as_deref(),
        }
    }

    pub fn table(&self) -> Option<&str> {
        match self {
            QueryString::Select { table, .. } => Some(table),
            QueryString::Insert { table, .. } => Some(table),
            QueryString::Delete { table, .. } => Some(table),
            QueryString::CreateTable { table, .. } => Some(table),
            QueryString::CreateType { table, .. } => Some(table),
            _ => None,
        }
    }

    pub fn encode(&self) -> String {
        match self {
            QueryString::Select { .. } => "SELECT ".to_string(),
            QueryString::Insert { .. } => String::new(),
            QueryString::Delete { .. } => String::new(),
            _ => unimplemented!(),
        }
    }
}

impl fmt::Display for QueryString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryString::Select {
                keyspace,
                table,
                columns,
                r#where: closure,
            } => {
                write!(f, "SELECT {} FROM ", columns)?;
                if let Some(keyspace) = keyspace {
                    write!(f, "{}.", keyspace)?;
                }
                write!(f, "{}", table)?;
                if let Some(closure) = closure {
                    write!(f, " WHERE {}", closure)?;
                }
                Ok(())
            }
            QueryString::Insert {
                keyspace,
                table,
                columns,
                values,
            } => {
                write!(f, "INSERT INTO ")?;
                if let Some(keyspace) = keyspace {
                    write!(f, "{}.", keyspace)?;
                }
                write!(f, "{} ({}) VALUES (", table, columns.join(", "),)?;

                for value in values {
                    write!(f, "{}, ", value)?;
                }
                write!(f, ")")?;

                Ok(())
            }
            QueryString::Delete {
                keyspace,
                table,
                columns,
                values,
            } => {
                write!(f, "DELETE FROM")?;
                if let Some(keyspace) = keyspace {
                    write!(f, "{}.", keyspace)?;
                }
                write!(f, "{} WHERE ", table,)?;
                for (k, v) in columns.iter().zip(values.iter()) {
                    write!(f, "{} = {}, ", k, v)?;
                }
                Ok(())
            }
            QueryString::Use { keyspace } => write!(f, "USE {keyspace}"),
            _ => write!(f, "unimplemented debug string"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum SelectExpression {
    All,
    Columns(Vec<String>),
}

impl fmt::Display for SelectExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectExpression::All => write!(f, "*"),
            SelectExpression::Columns(columns) => write!(f, "{}", columns.join(", ")),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WhereClosure {
    pub statements: Vec<(String, QueryValue)>,
}

impl fmt::Display for WhereClosure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (name, value) in &self.statements {
            write!(f, "{} = {}", name, value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum QueryValue {
    Literal(Literal),
    Blankslate,
}

impl fmt::Display for QueryValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryValue::Literal(l) => write!(f, "{l}"),
            QueryValue::Blankslate => write!(f, "?"),
        }
    }
}
