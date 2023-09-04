use std::net::IpAddr;

use bigdecimal::BigDecimal;
use derive_more::From;
use num_bigint::BigInt;
use serde::Serialize;
use uuid::Uuid;

use crate::cql::value::{CqlDuration, CqlValue};

#[derive(Clone, Debug, PartialEq, Serialize, PartialOrd, From)]
#[serde(untagged)]
pub enum ValueSnapshot {
    #[from(ignore)]
    Ascii(String),
    Boolean(bool),
    #[serde(with = "serde_bytes")]
    Blob(Vec<u8>),
    #[from(ignore)]
    Counter(i64),
    Decimal(BigDecimal),
    #[from(ignore)]
    Date(u32),
    #[from(ignore)]
    Double(f64),
    Duration(CqlDuration),
    #[from(ignore)]
    Float(f32),
    Int(i32),
    BigInt(i64),
    Text(String),
    #[from(ignore)]
    Timestamp(String),
    Inet(IpAddr),
    List(Vec<ValueSnapshot>),
    Map(Vec<(ValueSnapshot, ValueSnapshot)>),
    #[from(ignore)]
    Set(Vec<ValueSnapshot>),
    UserDefinedType {
        keyspace: String,
        type_name: String,
        fields: Vec<(String, Option<ValueSnapshot>)>,
    },
    SmallInt(i16),
    TinyInt(i8),
    #[from(ignore)]
    Time(i64),
    #[from(ignore)]
    Timeuuid(Uuid),
    #[from(ignore)]
    Tuple(Vec<ValueSnapshot>),
    Uuid(Uuid),
    Varint(BigInt),
    #[from(types(()))]
    Empty,
}

impl From<CqlValue> for ValueSnapshot {
    fn from(value: CqlValue) -> Self {
        match value {
            CqlValue::Ascii(v) => ValueSnapshot::Ascii(v),
            CqlValue::Boolean(v) => ValueSnapshot::Boolean(v),
            CqlValue::Blob(v) => ValueSnapshot::Blob(v),
            CqlValue::Counter(v) => ValueSnapshot::Counter(v),
            CqlValue::Decimal(v) => ValueSnapshot::Decimal(v),
            CqlValue::Date(v) => ValueSnapshot::Date(v),
            CqlValue::Double(v) => ValueSnapshot::Double(f64::from_be_bytes(v.to_be_bytes())),
            CqlValue::Duration(v) => ValueSnapshot::Duration(v),
            CqlValue::Float(v) => ValueSnapshot::Float(f32::from_be_bytes(v.to_be_bytes())),
            CqlValue::Int(v) => ValueSnapshot::Int(v),
            CqlValue::BigInt(v) => ValueSnapshot::BigInt(v),
            CqlValue::Text(v) => ValueSnapshot::Text(v),
            CqlValue::Timestamp(v) => ValueSnapshot::Timestamp(
                chrono::NaiveDateTime::from_timestamp_millis(v)
                    .unwrap()
                    .and_utc()
                    .to_rfc3339(),
            ),
            CqlValue::Inet(v) => ValueSnapshot::Inet(v),
            CqlValue::List(v) => {
                ValueSnapshot::List(v.into_iter().map(ValueSnapshot::from).collect())
            }
            CqlValue::Map(v) => ValueSnapshot::Map(
                v.into_iter()
                    .map(|(k, v)| (ValueSnapshot::from(k), ValueSnapshot::from(v)))
                    .collect(),
            ),
            CqlValue::Set(v) => {
                ValueSnapshot::Set(v.into_iter().map(ValueSnapshot::from).collect())
            }
            CqlValue::SmallInt(v) => ValueSnapshot::SmallInt(v),
            CqlValue::TinyInt(v) => ValueSnapshot::TinyInt(v),
            CqlValue::Time(v) => ValueSnapshot::Time(v),
            CqlValue::Timeuuid(v) => ValueSnapshot::Timeuuid(v),
            CqlValue::Tuple(v) => {
                ValueSnapshot::Tuple(v.into_iter().map(ValueSnapshot::from).collect())
            }
            CqlValue::Uuid(v) => ValueSnapshot::Uuid(v),
            CqlValue::Varint(v) => ValueSnapshot::Varint(v),
            CqlValue::UserDefinedType {
                keyspace,
                type_name,
                fields,
            } => ValueSnapshot::UserDefinedType {
                keyspace,
                type_name,
                fields: fields
                    .into_iter()
                    .map(|(n, v)| (n, v.map(ValueSnapshot::from)))
                    .collect(),
            },
            CqlValue::Empty => ValueSnapshot::Empty,
        }
    }
}
