use std::{
    hash::{Hash, Hasher},
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    ops::RangeBounds,
    str::FromStr,
};

use bigdecimal::BigDecimal;
use derive_more::From;
use eyre::Result;
use nom::number::complete::{be_f32, be_f64, be_i32, be_i64, be_u128, be_u32};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    cql::{column::ColumnType, literal::Literal},
    error::DbError,
    frame::{parse, response::error::Error},
};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord, From)]
pub enum CqlValue {
    #[from(ignore)]
    Tuple(Vec<CqlValue>),
    #[from(ignore)]
    Ascii(String),
    Boolean(bool),
    Blob(Vec<u8>),
    #[from(ignore)]
    Counter(i64),
    Decimal(BigDecimal),
    /// Days since -5877641-06-23 i.e. 2^31 days before unix epoch
    /// Can be converted to chrono::NaiveDate (-262145-1-1 to 262143-12-31) using as_date
    #[from(ignore)]
    Date(u32),
    #[from(ignore)]
    Double(u64),
    Duration(CqlDuration),

    #[from(ignore)]
    Float(u32),
    Int(i32),
    BigInt(i64),
    Text(String),
    /// Milliseconds since unix epoch
    #[from(ignore)]
    Timestamp(i64),
    Inet(IpAddr),
    List(Vec<CqlValue>),
    Map(Vec<(CqlValue, CqlValue)>),
    #[from(ignore)]
    Set(Vec<CqlValue>),
    UserDefinedType {
        keyspace: String,
        type_name: String,
        /// Order of `fields` vector must match the order of fields as defined in the UDT. The
        /// driver does not check it by itself, so incorrect data will be written if the order is
        /// wrong.
        fields: Vec<(String, Option<CqlValue>)>,
    },
    SmallInt(i16),
    TinyInt(i8),
    /// Nanoseconds since midnight
    #[from(ignore)]
    Time(i64),
    #[from(ignore)]
    Timeuuid(Uuid),
    Uuid(Uuid),
    Varint(BigInt),
    #[default]
    #[from(types(()))]
    Empty,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct CqlDuration {
    pub months: i32,
    pub days: i32,
    pub nanoseconds: i64,
}

impl Hash for CqlValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            CqlValue::Ascii(value) => {
                value.hash(state);
            }
            CqlValue::Boolean(value) => {
                value.hash(state);
            }
            CqlValue::Blob(value) => {
                value.hash(state);
            }
            CqlValue::Counter(value) => {
                value.hash(state);
            }
            CqlValue::Decimal(value) => {
                value.hash(state);
            }
            CqlValue::Date(value) => {
                value.hash(state);
            }
            CqlValue::Double(value) => {
                value.hash(state);
            }
            CqlValue::Duration(_value) => {
                unimplemented!()
            }
            CqlValue::Empty => {}
            CqlValue::Float(value) => {
                value.hash(state);
            }
            CqlValue::Int(value) => {
                value.hash(state);
            }
            CqlValue::BigInt(value) => {
                value.hash(state);
            }
            CqlValue::Text(value) => {
                value.hash(state);
            }
            CqlValue::Timestamp(value) => {
                value.hash(state);
            }
            CqlValue::Inet(value) => {
                value.hash(state);
            }
            CqlValue::List(value) => {
                value.hash(state);
            }
            CqlValue::Map(value) => {
                value.hash(state);
            }
            CqlValue::Set(value) => {
                value.hash(state);
            }
            CqlValue::UserDefinedType { fields, .. } => {
                fields.hash(state);
            }
            CqlValue::SmallInt(value) => {
                value.hash(state);
            }
            CqlValue::TinyInt(value) => {
                value.hash(state);
            }
            CqlValue::Time(value) => {
                value.hash(state);
            }
            CqlValue::Timeuuid(value) => {
                value.hash(state);
            }
            CqlValue::Tuple(value) => {
                value.hash(state);
            }
            CqlValue::Uuid(value) => {
                value.hash(state);
            }
            CqlValue::Varint(value) => {
                value.hash(state);
            }
        }
    }
}

pub fn opt_deserialize_value<'a>(
    data: &'a [u8],
    col: &ColumnType,
) -> Result<(&'a [u8], Option<CqlValue>), Error> {
    let (rest, data) = parse::bytes_opt(data)?;
    Ok((rest, data.map(|it| deserialize_value(it, col)).transpose()?))
}

pub fn deserialize_value(data: &[u8], col: &ColumnType) -> Result<CqlValue, Error> {
    match col {
        ColumnType::Custom(_) => {
            unimplemented!()
        }
        ColumnType::Ascii => {
            let ascii_str = std::str::from_utf8(data).unwrap().to_string();
            Ok(CqlValue::Ascii(ascii_str))
        }
        ColumnType::Boolean => {
            let boolean = data[0] != 0;
            Ok(CqlValue::Boolean(boolean))
        }
        ColumnType::Blob => Ok(CqlValue::Blob(data.to_vec())),
        ColumnType::Counter => {
            let (_, counter) = be_i64::<_, nom::error::Error<_>>(data)?;
            Ok(CqlValue::Counter(counter))
        }
        ColumnType::Date => {
            let (_, date) = be_u32::<_, nom::error::Error<_>>(data)?;
            Ok(CqlValue::Date(date))
        }
        ColumnType::Decimal => {
            todo!()
        }
        ColumnType::Double => {
            let (_, double) = be_f64::<_, nom::error::Error<_>>(data)?;
            Ok(CqlValue::Double(double.to_bits()))
        }
        ColumnType::Duration => {
            todo!()
        }
        ColumnType::Float => {
            let (_, float) = be_f32::<_, nom::error::Error<_>>(data)?;
            Ok(CqlValue::Float(float.to_bits()))
        }
        ColumnType::Int => {
            let (_, v) = be_i32::<_, nom::error::Error<_>>(data)?;
            Ok(CqlValue::Int(v))
        }
        ColumnType::BigInt => {
            let (_, v) = be_i64::<_, nom::error::Error<_>>(data)?;
            Ok(CqlValue::BigInt(v))
        }
        ColumnType::Text => {
            let s = String::from_utf8_lossy(data).into();
            Ok(CqlValue::Text(s))
        }
        ColumnType::Timestamp => {
            let (_, timestamp) = be_i64::<_, nom::error::Error<_>>(data)?;

            Ok(CqlValue::Timestamp(timestamp))
        }
        ColumnType::Inet => {
            let (_, ip) = match data.len() {
                4 => {
                    let (data, a) = be_u32::<_, nom::error::Error<_>>(data)?;
                    (data, IpAddr::V4(Ipv4Addr::from(a)))
                }
                16 => {
                    let (data, a) = be_u128::<_, nom::error::Error<_>>(data)?;
                    (data, IpAddr::V6(Ipv6Addr::from(a)))
                }
                n => {
                    return Err(Error::new(
                        DbError::ProtocolError,
                        format!("Invalid value passed for `inet` type. Expected 4 or 16, got {n}"),
                    ))
                }
            };
            Ok(CqlValue::Inet(ip))
        }
        ColumnType::List(ref inner_type) => {
            let (mut data, elements_count) = be_u32::<_, nom::error::Error<_>>(data)?;
            let mut list = Vec::new();

            for _ in 0..elements_count {
                let (d, value) = opt_deserialize_value(data, inner_type)?;
                if let Some(it) = value {
                    list.push(it)
                }
                data = d;
            }

            Ok(CqlValue::List(list))
        }
        ColumnType::Map(ref key_type, ref value_type) => {
            let (mut data, pairs_count) = be_u32::<_, nom::error::Error<_>>(data)?;
            let mut map = Vec::new();

            for _ in 0..pairs_count {
                let (d, key) = opt_deserialize_value(data, key_type)?;
                let (d, value) = opt_deserialize_value(d, value_type)?;
                data = d;

                if let Some((key, value)) = Option::zip(key, value) {
                    map.push((key, value));
                }
            }

            Ok(CqlValue::Map(map))
        }
        ColumnType::Set(ref inner_type) => {
            let (data, elements_count) = be_u32::<_, nom::error::Error<_>>(data)?;
            let mut set = Vec::new();

            let mut data = data;
            for _ in 0..elements_count {
                let (d, value) = opt_deserialize_value(data, inner_type)?;
                if let Some(it) = value {
                    set.push(it)
                }
                data = d;
            }

            Ok(CqlValue::Set(set))
        }
        ColumnType::UserDefinedType { .. } => {
            todo!()
        }
        ColumnType::SmallInt => {
            todo!()
        }
        ColumnType::TinyInt => {
            todo!()
        }
        ColumnType::Time => {
            todo!()
        }
        ColumnType::Timeuuid => {
            todo!()
        }
        ColumnType::Tuple(types) => {
            let mut result = vec![];
            let mut rest = data;
            let mut value;
            for ty in types {
                (rest, value) = parse::bytes_opt(rest)?;
                let Some(value) = value else {
                    result.push(CqlValue::Empty);
                    continue;
                };
                let (_, value) = opt_deserialize_value(value, ty)?;
                result.push(value.unwrap_or_default());
            }

            Ok(CqlValue::Tuple(result))
        }
        ColumnType::Uuid => {
            let (_, v) = be_u128::<_, nom::error::Error<_>>(data)?;
            let v = Uuid::from_u128(v);
            Ok(CqlValue::Uuid(v))
        }
        ColumnType::Varint => {
            todo!()
        }
    }
}

pub fn map_lit(col: &ColumnType, lit: Literal) -> Result<CqlValue, Error> {
    match (col, lit) {
        (_, Literal::Null) => Ok(CqlValue::Empty),
        (ColumnType::Text, Literal::String(v)) => Ok(CqlValue::Text(v)),
        (ColumnType::BigInt, Literal::Number(n)) => Ok(CqlValue::BigInt(n)),
        (ColumnType::Int, Literal::Number(n)) => Ok(CqlValue::Int(n as _)),
        (ColumnType::Inet, Literal::String(v)) => {
            let addr = IpAddr::from_str(&v).map_err(|err| {
                tracing::error!(value = ?v, ?err, "Could not parse inet literal");
                Error::new(DbError::Invalid, "invalid literal for inet")
            })?;

            Ok(CqlValue::Inet(addr))
        }
        (ColumnType::Uuid, Literal::String(v)) => {
            let uuid = Uuid::from_str(&v).map_err(|err| {
                tracing::error!(value = ?v, ?err, "Could not parse uuid literal");
                Error::new(DbError::Invalid, "invalid literal for uuid")
            })?;

            Ok(CqlValue::Uuid(uuid))
        }
        (ColumnType::Uuid, Literal::Uuid(uuid)) => Ok(CqlValue::Uuid(uuid)),
        (ColumnType::Set(item_ty), Literal::List(literals)) => Ok(CqlValue::Set(
            literals
                .into_iter()
                .map(|item| map_lit(item_ty, item))
                .collect::<Result<_, _>>()?,
        )),
        (ColumnType::Map(key, value_ty), Literal::Map(map)) if **key == ColumnType::Text => {
            Ok(CqlValue::Map(
                map.into_iter()
                    .map(|(k, value)| {
                        let value = map_lit(value_ty, value)?;
                        Ok((CqlValue::Text(k), value))
                    })
                    .collect::<Result<_, Error>>()?,
            ))
        }
        (ty, lit) => {
            tracing::error!(?ty, ?lit, "Not implemented for pair");
            Err(Error::new(
                DbError::Invalid,
                format!("invalid literal: {lit:?} for ty: {ty:?}"),
            ))?
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord, From)]
pub enum ClusteringKeyValue {
    Simple(Option<CqlValue>),
    Composite(Vec<Option<CqlValue>>),

    #[default]
    #[from(types(()))]
    Empty,
}

impl From<CqlValue> for ClusteringKeyValue {
    fn from(value: CqlValue) -> Self {
        match value {
            CqlValue::Empty => Self::Empty,
            CqlValue::Tuple(values) => Self::Composite(values.into_iter().map(Some).collect()),
            _ => Self::Simple(Some(value)),
        }
    }
}

impl<'a> IntoIterator for &'a ClusteringKeyValue {
    type Item = &'a Option<CqlValue>;

    type IntoIter = std::slice::Iter<'a, Option<CqlValue>>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            ClusteringKeyValue::Simple(v) => std::slice::from_ref(v).iter(),
            ClusteringKeyValue::Composite(vs) => vs.as_slice().iter(),
            ClusteringKeyValue::Empty => [].iter(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusteringKeyValueRange {
    Full,
    From(ClusteringKeyValue),
    To(ClusteringKeyValue),
    Range(ClusteringKeyValue, ClusteringKeyValue),
}

impl ClusteringKeyValueRange {
    pub fn from(self, left: ClusteringKeyValue) -> Self {
        match self {
            Self::Full => Self::From(left),
            Self::From(old) if old < left => Self::From(left),
            Self::From(_) => self,
            Self::To(right) => Self::Range(left, right),
            Self::Range(old, right) if old < left => Self::Range(left, right),
            Self::Range(_, _) => self,
        }
    }
}

impl RangeBounds<ClusteringKeyValue> for ClusteringKeyValueRange {
    fn start_bound(&self) -> std::ops::Bound<&ClusteringKeyValue> {
        match self {
            ClusteringKeyValueRange::Full => std::ops::Bound::Unbounded,
            ClusteringKeyValueRange::From(v) => std::ops::Bound::Included(v),
            ClusteringKeyValueRange::To(_) => std::ops::Bound::Unbounded,
            ClusteringKeyValueRange::Range(v, _) => std::ops::Bound::Included(v),
        }
    }

    fn end_bound(&self) -> std::ops::Bound<&ClusteringKeyValue> {
        match self {
            ClusteringKeyValueRange::Full => std::ops::Bound::Unbounded,
            ClusteringKeyValueRange::From(_) => std::ops::Bound::Unbounded,
            ClusteringKeyValueRange::To(v) => std::ops::Bound::Included(v),
            ClusteringKeyValueRange::Range(_, v) => std::ops::Bound::Included(v),
        }
    }
}

impl From<std::ops::Range<ClusteringKeyValue>> for ClusteringKeyValueRange {
    fn from(value: std::ops::Range<ClusteringKeyValue>) -> Self {
        ClusteringKeyValueRange::Range(value.start, value.end)
    }
}

impl From<std::ops::RangeFull> for ClusteringKeyValueRange {
    fn from(_: std::ops::RangeFull) -> Self {
        ClusteringKeyValueRange::Full
    }
}

impl From<std::ops::RangeFrom<ClusteringKeyValue>> for ClusteringKeyValueRange {
    fn from(value: std::ops::RangeFrom<ClusteringKeyValue>) -> Self {
        Self::From(value.start)
    }
}

impl From<std::ops::RangeToInclusive<ClusteringKeyValue>> for ClusteringKeyValueRange {
    fn from(value: std::ops::RangeToInclusive<ClusteringKeyValue>) -> Self {
        Self::To(value.end)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum PartitionKeyValue {
    Simple(CqlValue),
    Composite(Vec<CqlValue>),

    Empty,
}

impl From<CqlValue> for PartitionKeyValue {
    fn from(value: CqlValue) -> Self {
        match value {
            CqlValue::Empty => Self::Empty,
            CqlValue::Tuple(values) => Self::Composite(values),
            _ => Self::Simple(value),
        }
    }
}

impl<'a> IntoIterator for &'a PartitionKeyValue {
    type Item = &'a CqlValue;

    type IntoIter = std::slice::Iter<'a, CqlValue>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            PartitionKeyValue::Simple(v) => std::slice::from_ref(v).iter(),
            PartitionKeyValue::Composite(vs) => vs.as_slice().iter(),
            PartitionKeyValue::Empty => [].iter(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionKeyValueRange {
    Full,
    From(PartitionKeyValue),
    To(PartitionKeyValue),
    Range(PartitionKeyValue, PartitionKeyValue),
}

impl RangeBounds<PartitionKeyValue> for PartitionKeyValueRange {
    fn start_bound(&self) -> std::ops::Bound<&PartitionKeyValue> {
        match self {
            PartitionKeyValueRange::Full => std::ops::Bound::Unbounded,
            PartitionKeyValueRange::From(v) => std::ops::Bound::Included(v),
            PartitionKeyValueRange::To(_) => std::ops::Bound::Unbounded,
            PartitionKeyValueRange::Range(v, _) => std::ops::Bound::Included(v),
        }
    }

    fn end_bound(&self) -> std::ops::Bound<&PartitionKeyValue> {
        match self {
            PartitionKeyValueRange::Full => std::ops::Bound::Unbounded,
            PartitionKeyValueRange::From(_) => std::ops::Bound::Unbounded,
            PartitionKeyValueRange::To(v) => std::ops::Bound::Included(v),
            PartitionKeyValueRange::Range(_, v) => std::ops::Bound::Included(v),
        }
    }
}

impl From<std::ops::Range<PartitionKeyValue>> for PartitionKeyValueRange {
    fn from(value: std::ops::Range<PartitionKeyValue>) -> Self {
        PartitionKeyValueRange::Range(value.start, value.end)
    }
}

impl From<std::ops::RangeFull> for PartitionKeyValueRange {
    fn from(_: std::ops::RangeFull) -> Self {
        PartitionKeyValueRange::Full
    }
}

impl From<std::ops::RangeFrom<PartitionKeyValue>> for PartitionKeyValueRange {
    fn from(value: std::ops::RangeFrom<PartitionKeyValue>) -> Self {
        Self::From(value.start)
    }
}

impl From<std::ops::RangeToInclusive<PartitionKeyValue>> for PartitionKeyValueRange {
    fn from(value: std::ops::RangeToInclusive<PartitionKeyValue>) -> Self {
        Self::To(value.end)
    }
}

#[cfg(test)]
mod tests {
    use super::CqlValue;
    use crate::cql::value::PartitionKeyValue;

    #[test]
    fn test_composite_value_ranges() {
        let range = CqlValue::Tuple(vec![1.into(), 2.into()])
            ..=CqlValue::Tuple(vec![1.into(), 2.into(), CqlValue::Empty]);

        assert!(range.contains(&CqlValue::Tuple(vec![1.into(), 2.into(), 3.into()])));
        assert!(!range.contains(&CqlValue::Tuple(vec![1.into(), 3.into(), 3.into()])));
        assert!(!range.contains(&CqlValue::Tuple(vec![
            1.into(),
            CqlValue::Empty,
            CqlValue::Empty
        ])))
    }

    #[test]
    fn test_simple_value_ranges() {
        let range = PartitionKeyValue::Simple(3i32.into())..;

        assert!(range.contains(&CqlValue::Int(4).into()));
        assert!(!range.contains(&CqlValue::Int(2).into()));
    }
}
