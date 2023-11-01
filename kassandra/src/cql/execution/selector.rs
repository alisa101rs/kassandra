use std::collections::BTreeMap;

use serde::Serialize;

use crate::{cql::value::CqlValue, snapshot::ValueSnapshot};

#[derive(Debug, Clone, Serialize)]
#[serde(transparent)]
pub struct ColumnsSelector(pub Vec<ColumnSelector>);

#[derive(Debug, Clone, Serialize)]
pub struct ColumnSelector {
    pub name: String,
    pub transform: Transform,
}

pub fn filter(
    mut row: BTreeMap<String, CqlValue>,
    selector: &ColumnsSelector,
) -> Vec<Option<CqlValue>> {
    selector
        .0
        .iter()
        .map(|it| {
            let column = row.remove(&it.name)?;
            it.transform.transform(column)
        })
        .collect()
}

#[derive(Debug, Copy, Clone, Serialize)]
pub enum Transform {
    Identity,
    ToJson,
}

impl Transform {
    fn transform(&self, input: CqlValue) -> Option<CqlValue> {
        match self {
            Transform::Identity => Some(input),
            Transform::ToJson => {
                let t = ValueSnapshot::from(input);
                let json = serde_json::to_string(&t).expect("to be serializable");
                Some(CqlValue::Text(json))
            }
        }
    }
}
