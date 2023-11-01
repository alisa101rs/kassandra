use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Literal {
    String(String),
    Number(i64),
    Float(f64),
    Uuid(Uuid),
    List(Vec<Literal>),
    Map(BTreeMap<String, Literal>),
    Bool(bool),
    Null,
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::String(v) => write!(f, "'{v}'"),
            Literal::Number(n) => n.fmt(f),
            Literal::Float(v) => v.fmt(f),
            Literal::List(values) => {
                write!(f, "[")?;
                for value in values {
                    write!(f, "{}, ", value)?;
                }
                write!(f, "]")?;
                Ok(())
            }
            Literal::Map(m) => {
                write!(f, "{{")?;
                for (k, v) in m {
                    write!(f, "{}: {}, ", k, v)?;
                }
                write!(f, "}}")?;
                Ok(())
            }
            Literal::Bool(b) => b.fmt(f),
            Literal::Null => write!(f, "null"),
            Literal::Uuid(u) => u.fmt(f),
        }
    }
}
