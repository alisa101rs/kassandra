use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

use integer_encoding::VarIntReader;
use nom::{
    bytes::complete::take,
    combinator::map,
    error::{self, Error, ErrorKind},
    number::complete::{self, be_f32, be_f64, be_i32, be_i64, be_u128, be_u32},
    sequence::pair,
    IResult,
};
use uuid::Uuid;

use crate::{
    cql::{
        schema::{ColumnType, PrimaryKeyColumn},
        value::{ClusteringKeyValue, CqlValue, PartitionKeyValue},
    },
    frame::{
        consistency::{Consistency, LegacyConsistency, SerialConsistency},
        value::FrameValue,
    },
};

pub fn short_string(input: &[u8]) -> IResult<&[u8], &str> {
    let (rest, n) = complete::be_u16(input)?;
    let (rest, bytes) = take(n as usize)(rest)?;
    let s = std::str::from_utf8(bytes).unwrap();
    Ok((rest, s))
}

pub fn long_string(input: &[u8]) -> IResult<&[u8], &str> {
    let (rest, n) = complete::be_u32(input)?;
    let (rest, bytes) = take(n as usize)(rest)?;
    let s = std::str::from_utf8(bytes).unwrap();
    Ok((rest, s))
}

pub fn short_string_list(input: &[u8]) -> IResult<&[u8], Vec<&str>> {
    let (rest, len) = complete::be_i16(input)?;
    nom::multi::count(short_string, len as usize)(rest)
}

pub fn string_multimap(input: &[u8]) -> IResult<&[u8], HashMap<&str, Vec<&str>>> {
    let (rest, len) = complete::be_i16(input)?;

    map(
        nom::multi::count(pair(short_string, short_string_list), len as usize),
        |it| it.into_iter().collect(),
    )(rest)
}

pub fn string_map(input: &[u8]) -> IResult<&[u8], HashMap<&str, &str>> {
    let (rest, len) = complete::be_i16(input)?;

    map(
        nom::multi::count(pair(short_string, short_string), len as usize),
        |it| it.into_iter().collect(),
    )(rest)
}

pub fn bytes_opt(input: &[u8]) -> IResult<&[u8], Option<&[u8]>> {
    let (rest, len) = complete::be_i32(input)?;
    if len < 0 {
        return Ok((rest, None));
    }
    let (rest, bytes) = take(len as usize)(rest)?;
    Ok((rest, Some(bytes)))
}

pub fn value(input: &[u8]) -> IResult<&[u8], FrameValue> {
    let (rest, len) = complete::be_i32(input)?;
    match len {
        -1 => Ok((rest, FrameValue::Null)),
        -2 => Ok((rest, FrameValue::NotSet)),
        _ if len < -2 => Err(nom::Err::Failure(error::Error::new(
            input,
            ErrorKind::NonEmpty,
        ))),
        _ => {
            let (rest, bytes) = take(len as usize)(rest)?;
            Ok((rest, FrameValue::Some(bytes)))
        }
    }
}

pub fn short_bytes(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (rest, len) = complete::be_i16(input)?;
    let (rest, bytes) = take(len as usize)(rest)?;
    Ok((rest, bytes))
}

pub fn bytes_with_vint(input: &[u8]) -> IResult<&[u8], Option<&[u8]>> {
    let (rest, len) = unsigned_vint(input)?;

    if len == 0 {
        return Ok((rest, None));
    }

    map(take(len as usize), Some)(rest)
}

pub fn consistency(input: &[u8]) -> IResult<&[u8], LegacyConsistency> {
    let (rest, raw) = complete::be_i16(input)?;
    let parsed = match Consistency::try_from(raw) {
        Ok(c) => LegacyConsistency::Regular(c),
        Err(_) => {
            let parsed_serial =
                SerialConsistency::try_from(raw).expect("could not parsed serial consistency");
            LegacyConsistency::Serial(parsed_serial)
        }
    };
    Ok((rest, parsed))
}

pub fn unsigned_vint(mut input: &[u8]) -> IResult<&[u8], u64> {
    let int = input
        .read_varint()
        .map_err(|_| nom::Err::Error(Error::new(input, ErrorKind::LengthValue)))?;

    Ok((input, int))
}

fn cql_value_without_size<'a>(data: &'a [u8], col: &ColumnType) -> IResult<&'a [u8], CqlValue> {
    match col {
        ColumnType::Custom(_) => {
            unimplemented!()
        }
        ColumnType::Ascii => {
            let (rest, size) = unsigned_vint(data)?;
            let (rest, slice) = take(size as usize)(rest)?;
            let ascii_str = std::str::from_utf8(slice).unwrap().to_string();
            Ok((rest, CqlValue::Ascii(ascii_str)))
        }
        ColumnType::Boolean => {
            let (rest, b) = take(1_usize)(data)?;
            Ok((rest, CqlValue::Boolean(b[0] != 0)))
        }
        ColumnType::Blob => {
            let (rest, size) = unsigned_vint(data)?;
            let (rest, slice) = take(size as usize)(rest)?;

            Ok((rest, CqlValue::Blob(slice.to_owned())))
        }
        ColumnType::Counter => {
            let (rest, counter) = be_i64::<_, nom::error::Error<_>>(data)?;
            Ok((rest, CqlValue::Counter(counter)))
        }
        ColumnType::Date => {
            let (rest, date) = be_u32::<_, nom::error::Error<_>>(data)?;
            Ok((rest, CqlValue::Date(date)))
        }
        ColumnType::Decimal => {
            todo!()
        }
        ColumnType::Double => {
            let (rest, double) = be_f64::<_, nom::error::Error<_>>(data)?;
            Ok((rest, CqlValue::Double(double.to_bits())))
        }
        ColumnType::Duration => {
            todo!()
        }
        ColumnType::Float => {
            let (rest, float) = be_f32::<_, nom::error::Error<_>>(data)?;
            Ok((rest, CqlValue::Float(float.to_bits())))
        }
        ColumnType::Int => {
            let (rest, v) = be_i32::<_, nom::error::Error<_>>(data)?;
            Ok((rest, CqlValue::Int(v)))
        }
        ColumnType::BigInt => {
            let (rest, v) = be_i64::<_, nom::error::Error<_>>(data)?;
            Ok((rest, CqlValue::BigInt(v)))
        }
        ColumnType::Text => {
            let (rest, size) = unsigned_vint(data)?;
            let (rest, slice) = take(size as usize)(rest)?;
            let s = std::str::from_utf8(slice).unwrap().to_string();
            Ok((rest, CqlValue::Text(s)))
        }
        ColumnType::Timestamp => {
            let (rest, timestamp) = be_i64::<_, nom::error::Error<_>>(data)?;

            Ok((rest, CqlValue::Timestamp(timestamp)))
        }
        ColumnType::Inet => {
            let (rest, size) = unsigned_vint(data)?;
            let (rest, ip) = match size {
                4 => {
                    let (rest, a) = be_u32::<_, nom::error::Error<_>>(rest)?;
                    (rest, IpAddr::V4(Ipv4Addr::from(a)))
                }
                16 => {
                    let (rest, a) = be_u128::<_, nom::error::Error<_>>(rest)?;
                    (rest, IpAddr::V6(Ipv6Addr::from(a)))
                }
                _ => {
                    return Err(nom::Err::Error(nom::error::Error::new(
                        rest,
                        nom::error::ErrorKind::Tag,
                    )))
                }
            };
            Ok((rest, CqlValue::Inet(ip)))
        }
        ColumnType::List(_) => {
            todo!()
        }
        ColumnType::Map(_, _) => {
            todo!()
        }
        ColumnType::Set(_) => {
            todo!()
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
        ColumnType::Tuple(_) => {
            todo!()
        }
        ColumnType::Uuid => {
            let (rest, v) = be_u128::<_, nom::error::Error<_>>(data)?;
            let v = Uuid::from_u128(v);
            Ok((rest, CqlValue::Uuid(v)))
        }
        ColumnType::Varint => {
            todo!()
        }
    }
}

pub fn clustering_key<'a>(
    input: &'a [u8],
    ty: &PrimaryKeyColumn,
) -> IResult<&'a [u8], ClusteringKeyValue> {
    fn is_null(header: u64, offset: usize) -> bool {
        let mask = 1 << ((offset as u64 * 2) + 1);
        header & mask != 0
    }
    fn is_empty(header: u64, offset: usize) -> bool {
        let mask = 1 << (offset as u64 * 2);
        header & mask != 0
    }

    let mut rest = input;
    let mut offset = 0;
    let size = ty.size();

    let mut columns = ty.into_iter();
    let mut values = Vec::with_capacity(1);

    while offset < size {
        let (r, header) = unsigned_vint(rest)?;
        rest = r;
        for column in (&mut columns).take(32) {
            offset += 1;
            if is_null(header, offset - 1) {
                values.push(None);
                continue;
            }
            if is_empty(header, offset - 1) {
                values.push(Some(CqlValue::Empty));
                continue;
            }
            let (r, v) = cql_value_without_size(rest, column)?;
            rest = r;
            values.push(Some(v));
        }
    }

    let result = match ty {
        PrimaryKeyColumn::Empty => ClusteringKeyValue::Empty,
        PrimaryKeyColumn::Simple(_) => ClusteringKeyValue::Simple(values.pop().unwrap()),
        PrimaryKeyColumn::Composite(_) => ClusteringKeyValue::Composite(values),
    };

    Ok((rest, result))
}

pub fn partition_key<'a>(
    input: &'a [u8],
    ty: &PrimaryKeyColumn,
) -> IResult<&'a [u8], PartitionKeyValue> {
    let mut types = ty.into_iter();

    let first = types
        .next()
        .expect("PrimaryKeyColumn can't be composite of 0 values");
    let (rest, first) = cql_value_without_size(input, first)?;

    if types.as_slice().is_empty() {
        return Ok((rest, PartitionKeyValue::Simple(first)));
    }

    let mut rest = rest;
    let mut result = vec![first];
    let mut v;

    for ty in types {
        (rest, v) = cql_value_without_size(rest, ty)?;
        result.push(v);
    }

    Ok((rest, PartitionKeyValue::Composite(result)))
}

#[cfg(test)]
mod tests {
    use crate::cql::{
        schema::{ColumnType, PrimaryKeyColumn},
        value::{ClusteringKeyValue, CqlValue},
    };

    #[test]
    fn test_clustering_value() -> eyre::Result<()> {
        let data: &[u8] = b"\0\x03998\x011";
        let ty = PrimaryKeyColumn::Composite(vec![ColumnType::Ascii, ColumnType::Ascii]);

        let (_, v) = super::clustering_key(data, &ty)?;

        assert_eq!(
            v,
            ClusteringKeyValue::Composite(vec![
                Some(CqlValue::Ascii("998".to_owned())),
                Some(CqlValue::Ascii("1".to_owned())),
            ])
        );

        Ok(())
    }
}
