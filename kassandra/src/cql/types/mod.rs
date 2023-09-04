use serde::Serialize;
use strum_macros::EnumString;

pub mod literal;
pub mod value;

#[derive(Clone, Debug, PartialEq, Eq, EnumString, Serialize)]
#[strum(serialize_all = "lowercase")]
pub enum NativeType {
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
    SmallInt,
    TinyInt,
    Time,
    Timeuuid,
    Uuid,
    Varint,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum PreCqlType {
    Native(NativeType),
    List {
        item: Box<PreCqlType>,
        frozen: bool,
    },
    Map {
        key: Box<PreCqlType>,
        value: Box<PreCqlType>,
        frozen: bool,
    },
    Set {
        item: Box<PreCqlType>,
        frozen: bool,
    },
    Tuple(Vec<PreCqlType>),
    UserDefinedType {
        frozen: bool,
        name: String,
    },
}

impl PreCqlType {
    pub fn freeze(mut self) -> PreCqlType {
        match self {
            PreCqlType::List { ref mut frozen, .. }
            | PreCqlType::Set { ref mut frozen, .. }
            | PreCqlType::Map { ref mut frozen, .. }
            | PreCqlType::UserDefinedType { ref mut frozen, .. } => {
                *frozen = true;
            }
            _ => {}
        }

        self
    }
}
