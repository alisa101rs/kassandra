use std::{collections::HashMap, fmt};

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub enum Literal {
    String(String),
    Number(i64),
    Float(f64),
    List(Vec<Literal>),
    Map(HashMap<String, Literal>),
    Bool(bool),
    Null,
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::String(v) => v.fmt(f),
            Literal::Number(n) => n.fmt(f),
            Literal::Float(v) => v.fmt(f),
            Literal::List(values) => {
                for value in values {
                    write!(f, "{}, ", value)?;
                }
                Ok(())
            }
            Literal::Map(m) => {
                for (k, v) in m {
                    write!(f, "{} = {}, ", k, v)?;
                }
                Ok(())
            }
            Literal::Bool(b) => b.fmt(f),
            Literal::Null => write!(f, "null"),
        }
    }
}
