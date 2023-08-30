use serde::{Deserialize, Serialize};

use crate::cql::types::{NativeType, PreCqlType};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Custom(String),
    Ascii,
    Boolean,
    Blob,
    Counter,
    Date,
    Decimal,
    Double,
    Duration,
    Float,
    Int,
    BigInt,
    Text,
    Timestamp,
    Inet,
    List(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
    Set(Box<ColumnType>),
    UserDefinedType {
        type_name: String,
        keyspace: String,
        field_types: Vec<(String, ColumnType)>,
    },
    SmallInt,
    TinyInt,
    Time,
    Timeuuid,
    Tuple(Vec<ColumnType>),
    Uuid,
    Varint,
}

pub fn map_pre_type(pre: PreCqlType) -> ColumnType {
    match pre {
        PreCqlType::Native(NativeType::Ascii) => ColumnType::Ascii,
        PreCqlType::Native(NativeType::Boolean) => ColumnType::Boolean,
        PreCqlType::Native(NativeType::Blob) => ColumnType::Blob,
        PreCqlType::Native(NativeType::Counter) => ColumnType::Counter,
        PreCqlType::Native(NativeType::Date) => ColumnType::Date,
        PreCqlType::Native(NativeType::Decimal) => ColumnType::Decimal,
        PreCqlType::Native(NativeType::Double) => ColumnType::Double,
        PreCqlType::Native(NativeType::Duration) => ColumnType::Duration,
        PreCqlType::Native(NativeType::Float) => ColumnType::Float,
        PreCqlType::Native(NativeType::Int) => ColumnType::Int,
        PreCqlType::Native(NativeType::BigInt) => ColumnType::BigInt,
        PreCqlType::Native(NativeType::Text) => ColumnType::Text,
        PreCqlType::Native(NativeType::Timestamp) => ColumnType::Timestamp,
        PreCqlType::Native(NativeType::Inet) => ColumnType::Inet,
        PreCqlType::Native(NativeType::SmallInt) => ColumnType::SmallInt,
        PreCqlType::Native(NativeType::TinyInt) => ColumnType::TinyInt,
        PreCqlType::Native(NativeType::Time) => ColumnType::Time,
        PreCqlType::Native(NativeType::Timeuuid) => ColumnType::Timeuuid,
        PreCqlType::Native(NativeType::Uuid) => ColumnType::Uuid,
        PreCqlType::Native(NativeType::Varint) => ColumnType::Varint,
        PreCqlType::List { frozen: _, item } => ColumnType::List(Box::new(map_pre_type(*item))),
        PreCqlType::Set { frozen: _, item } => ColumnType::Set(Box::new(map_pre_type(*item))),
        PreCqlType::Map {
            frozen: _,
            key,
            value,
        } => ColumnType::Map(Box::new(map_pre_type(*key)), Box::new(map_pre_type(*value))),
        PreCqlType::Tuple(_) => unimplemented!(),
        PreCqlType::UserDefinedType { .. } => unimplemented!(),
    }
}
