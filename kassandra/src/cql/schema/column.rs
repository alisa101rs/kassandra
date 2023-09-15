use derive_more::Display;
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

impl ColumnType {
    pub fn into_cql(&self) -> Option<String> {
        Some(match self {
            ColumnType::Custom(_) => unimplemented!(),
            ColumnType::Ascii => "ascii".to_owned(),
            ColumnType::Boolean => "bool".to_owned(),
            ColumnType::Blob => "blob".to_owned(),
            ColumnType::Counter => unimplemented!(),
            ColumnType::Date => "date".to_owned(),
            ColumnType::Decimal => "bigint".to_owned(),
            ColumnType::Double => "double".to_owned(),
            ColumnType::Duration => "timestamp".to_owned(),
            ColumnType::Float => "float".to_owned(),
            ColumnType::Int => "int".to_owned(),
            ColumnType::BigInt => "bigint".to_owned(),
            ColumnType::Text => "text".to_owned(),
            ColumnType::Timestamp => "timestamp".to_owned(),
            ColumnType::Inet => "inet".to_owned(),
            ColumnType::List(l) => format!("list<{}>", l.into_cql()?),
            ColumnType::Map(k, v) => format!("map<{}, {}>", k.into_cql()?, v.into_cql()?),
            ColumnType::Set(i) => format!("set<{}>", i.into_cql()?),
            ColumnType::UserDefinedType { .. } => unimplemented!(),
            ColumnType::SmallInt => "smallint".to_owned(),
            ColumnType::TinyInt => "timyint".to_owned(),
            ColumnType::Time => "time".to_owned(),
            ColumnType::Timeuuid => "timeuuid".to_owned(),
            ColumnType::Tuple(_i) => unimplemented!(),
            ColumnType::Uuid => "uuid".to_owned(),
            ColumnType::Varint => "varint".to_owned(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub ty: ColumnType,
    pub kind: ColumnKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Display)]
pub enum ColumnKind {
    #[display(fmt = "regular")]
    Regular,
    #[display(fmt = "static")]
    Static,
    #[display(fmt = "clustering")]
    Clustering,
    #[display(fmt = "partition_key")]
    PartitionKey,
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
