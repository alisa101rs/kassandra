use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::cql::column::ColumnType;

#[derive(
    Debug, Copy, Clone, Serialize, Deserialize, Display, PartialOrd, PartialEq, Eq, Ord, Hash,
)]
pub enum CqlFunction {
    #[display(fmt = "toJson")]
    ToJson,
    #[display(fmt = "fromJson")]
    FromJson,
}

impl CqlFunction {
    pub fn return_type(&self, _input: &ColumnType) -> ColumnType {
        match self {
            CqlFunction::ToJson | CqlFunction::FromJson => ColumnType::Text,
        }
    }
}
