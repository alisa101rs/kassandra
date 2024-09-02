use std::{collections::HashMap, net::IpAddr};

use bytes::{BufMut, Bytes, BytesMut};
use nom::AsBytes;

use crate::{
    cql::{
        column::ColumnType,
        value::{ClusteringKeyValue, CqlValue, PartitionKeyValue},
    },
    frame::consistency::LegacyConsistency,
};

pub(crate) fn string_multimap(buf: &mut impl BufMut, value: &HashMap<String, Vec<String>>) {
    buf.put_u16(value.len() as u16);

    for (k, v) in value {
        string(buf, k);
        string_list(buf, v);
    }
}

pub(crate) fn string_map(buf: &mut impl BufMut, value: &HashMap<String, String>) {
    buf.put_u16(value.len() as u16);

    for (k, v) in value {
        string(buf, k);
        string(buf, v);
    }
}

pub(crate) fn string_list(buf: &mut impl BufMut, value: &[String]) {
    buf.put_u16(value.len() as u16);
    for v in value {
        string(buf, v);
    }
}

pub(crate) fn string(buf: &mut impl BufMut, value: &str) {
    buf.put_u16(value.len() as u16);
    buf.put_slice(value.as_bytes());
}

pub(crate) fn long_string(buf: &mut impl BufMut, value: &str) {
    buf.put_u32(value.len() as _);
    buf.put_slice(value.as_bytes());
}

pub(crate) fn short_bytes(buf: &mut impl BufMut, value: &[u8]) {
    buf.put_u16(value.len() as u16);
    buf.put_slice(value.as_bytes());
}

pub(crate) fn bytes(buf: &mut impl BufMut, value: &[u8]) {
    buf.put_u32(value.len() as u32);
    buf.put_slice(value.as_bytes());
}

pub(crate) fn r#type(buf: &mut impl BufMut, value: &ColumnType) {
    match value {
        ColumnType::Custom(n) => {
            buf.put_u16(0x0000);
            string(buf, n);
        }
        ColumnType::Ascii => {
            buf.put_u16(0x0001);
        }
        ColumnType::BigInt => {
            buf.put_u16(0x0002);
        }
        ColumnType::Blob => {
            buf.put_u16(0x0003);
        }
        ColumnType::Boolean => {
            buf.put_u16(0x0004);
        }
        ColumnType::Counter => {
            buf.put_u16(0x0005);
        }
        ColumnType::Decimal => {
            buf.put_u16(0x0006);
        }
        ColumnType::Double => {
            buf.put_u16(0x0007);
        }
        ColumnType::Float => {
            buf.put_u16(0x0008);
        }
        ColumnType::Int => {
            buf.put_u16(0x0009);
        }
        ColumnType::Timestamp => {
            buf.put_u16(0x000B);
        }
        ColumnType::Uuid => {
            buf.put_u16(0x000C);
        }
        ColumnType::Text => {
            buf.put_u16(0x000D);
        }
        ColumnType::Varint => {
            buf.put_u16(0x000E);
        }
        ColumnType::Timeuuid => {
            buf.put_u16(0x000F);
        }
        ColumnType::Inet => {
            buf.put_u16(0x0010);
        }
        ColumnType::Date => {
            buf.put_u16(0x0011);
        }
        ColumnType::Time => {
            buf.put_u16(0x0012);
        }
        ColumnType::SmallInt => {
            buf.put_u16(0x0013);
        }
        ColumnType::TinyInt => {
            buf.put_u16(0x0014);
        }
        ColumnType::Duration => {
            buf.put_u16(0x0015);
        }
        ColumnType::List(i) => {
            buf.put_u16(0x0020);
            r#type(buf, i);
        }
        ColumnType::Map(k, v) => {
            buf.put_u16(0x0021);
            r#type(buf, k);
            r#type(buf, v);
        }
        ColumnType::Set(i) => {
            buf.put_u16(0x0022);
            r#type(buf, i);
        }
        ColumnType::UserDefinedType { .. } => {
            buf.put_u16(0x0030);
            unimplemented!()
        }
        ColumnType::Tuple(_) => {
            buf.put_u16(0x0031);
            unimplemented!()
        }
    }
}

pub(crate) fn opt_cql_value(buf: &mut impl BufMut, value: Option<&CqlValue>) {
    let Some(value) = value else {
        buf.put_i32(-1);
        return;
    };

    match value {
        CqlValue::Ascii(v) => {
            long_string(buf, v);
        }
        CqlValue::Boolean(b) => {
            buf.put_u32(1);
            buf.put_u8(*b as u8);
        }
        CqlValue::Blob(v) => {
            bytes(buf, v.as_slice());
        }
        CqlValue::Counter(i) => {
            bytes(buf, &i.to_be_bytes());
        }
        CqlValue::Decimal(_i) => {
            unimplemented!()
        }
        CqlValue::Date(i) => {
            bytes(buf, &i.to_be_bytes());
        }
        CqlValue::Double(f) => {
            bytes(buf, &f.to_be_bytes());
        }
        CqlValue::Duration(_i) => {
            unimplemented!()
        }
        CqlValue::Empty => {
            buf.put_u32(0);
        }
        CqlValue::Float(v) => {
            bytes(buf, &v.to_be_bytes());
        }
        CqlValue::Int(v) => {
            bytes(buf, &v.to_be_bytes());
        }
        CqlValue::BigInt(v) => {
            bytes(buf, &v.to_be_bytes());
        }
        CqlValue::Text(t) => {
            long_string(buf, t);
        }
        CqlValue::Timestamp(v) => {
            bytes(buf, &v.to_be_bytes());
        }
        CqlValue::Inet(v) => match v {
            IpAddr::V4(ip) => {
                buf.put_i32(4);
                buf.put_slice(&ip.octets())
            }
            IpAddr::V6(ip) => {
                buf.put_i32(16);
                buf.put_slice(&ip.octets());
            }
        },
        CqlValue::List(list) => {
            let mut bytes = vec![];
            for v in list {
                opt_cql_value(&mut bytes, Some(v));
            }
            buf.put_u32(4 + bytes.len() as u32);
            buf.put_u32(list.len() as _);
            buf.put_slice(bytes.as_slice());
        }
        CqlValue::Map(map) => {
            let mut bytes = vec![];
            for (k, v) in map {
                opt_cql_value(&mut bytes, Some(k));
                opt_cql_value(&mut bytes, Some(v));
            }
            buf.put_u32(4 + bytes.len() as u32);
            buf.put_u32(map.len() as _);
            buf.put_slice(bytes.as_slice());
        }
        CqlValue::Set(list) => {
            let mut bytes = vec![];
            for v in list {
                opt_cql_value(&mut bytes, Some(v));
            }
            buf.put_u32(4 + bytes.len() as u32);
            buf.put_u32(list.len() as _);
            buf.put_slice(bytes.as_slice());
        }
        CqlValue::UserDefinedType { .. } => {
            unimplemented!()
        }
        CqlValue::SmallInt(i) => {
            bytes(buf, &i.to_be_bytes());
        }
        CqlValue::TinyInt(i) => {
            bytes(buf, &i.to_be_bytes());
        }
        CqlValue::Time(i) => {
            bytes(buf, &i.to_be_bytes());
        }
        CqlValue::Timeuuid(u) => {
            bytes(buf, &u.as_u128().to_be_bytes());
        }
        CqlValue::Tuple(values) => {
            for v in values {
                if v == &CqlValue::Empty {
                    buf.put_i32(-1);
                    continue;
                }
                let mut value = BytesMut::new();
                opt_cql_value(&mut value, Some(v));
                bytes(buf, value.as_bytes());
            }
        }
        CqlValue::Uuid(u) => {
            bytes(buf, &u.as_u128().to_be_bytes());
        }
        CqlValue::Varint(_) => {
            unimplemented!()
        }
    }
}

fn cql_value_without_size(buf: &mut impl BufMut, value: &CqlValue) {
    match value {
        CqlValue::Ascii(v) => {
            unsigned_varint(buf, v.len() as _);
            buf.put_slice(v.as_bytes());
        }
        CqlValue::Boolean(b) => {
            buf.put_u8(*b as u8);
        }
        CqlValue::Blob(v) => {
            unsigned_varint(buf, v.len() as _);
            buf.put_slice(v.as_bytes());
        }
        CqlValue::Counter(i) => {
            buf.put_slice(&i.to_be_bytes());
        }
        CqlValue::Decimal(_i) => {
            unimplemented!()
        }
        CqlValue::Date(i) => {
            buf.put_slice(&i.to_be_bytes());
        }
        CqlValue::Double(f) => {
            buf.put_slice(&f.to_be_bytes());
        }
        CqlValue::Duration(_i) => {
            unimplemented!()
        }
        CqlValue::Empty => {}
        CqlValue::Float(v) => {
            buf.put_slice(&v.to_be_bytes());
        }
        CqlValue::Int(v) => {
            buf.put_slice(&v.to_be_bytes());
        }
        CqlValue::BigInt(v) => {
            buf.put_slice(&v.to_be_bytes());
        }
        CqlValue::Text(t) => {
            unsigned_varint(buf, t.len() as _);
            buf.put_slice(t.as_bytes());
        }
        CqlValue::Timestamp(v) => {
            buf.put_slice(&v.to_be_bytes());
        }
        CqlValue::Inet(v) => match v {
            IpAddr::V4(ip) => {
                buf.put_u8(4);
                buf.put_slice(&ip.octets())
            }
            IpAddr::V6(ip) => {
                buf.put_u8(16);
                buf.put_slice(&ip.octets());
            }
        },
        CqlValue::List(list) | CqlValue::Set(list) => {
            let mut bytes = vec![];
            for v in list {
                cql_value_without_size(&mut bytes, v);
            }
            unsigned_varint(buf, 4 + bytes.len() as u64);
            unsigned_varint(buf, list.len() as _);
            buf.put_slice(bytes.as_slice());
        }
        CqlValue::Map(map) => {
            let mut bytes = vec![];
            for (k, v) in map {
                cql_value_without_size(&mut bytes, k);
                cql_value_without_size(&mut bytes, v);
            }
            unsigned_varint(buf, 4 + bytes.len() as u64);
            unsigned_varint(buf, map.len() as _);
            buf.put_slice(bytes.as_slice());
        }

        CqlValue::SmallInt(i) => {
            buf.put_slice(&i.to_be_bytes());
        }
        CqlValue::TinyInt(i) => {
            buf.put_slice(&i.to_be_bytes());
        }
        CqlValue::Time(i) => {
            buf.put_slice(&i.to_be_bytes());
        }
        CqlValue::Timeuuid(u) | CqlValue::Uuid(u) => {
            buf.put_slice(&u.as_u128().to_be_bytes());
        }
        CqlValue::Tuple(values) => {
            for v in values {
                if v == &CqlValue::Empty {
                    buf.put_i8(-1);
                    continue;
                }
                cql_value_without_size(buf, v);
            }
        }
        CqlValue::UserDefinedType { .. } => {
            unimplemented!()
        }
        CqlValue::Varint(_) => {
            unimplemented!()
        }
    }
}

pub(crate) fn clustering_value(buf: &mut impl BufMut, key: &ClusteringKeyValue) {
    fn make_header<'a>(
        buf: &mut impl BufMut,
        values: &mut impl Iterator<Item = &'a Option<CqlValue>>,
        offset: usize,
        limit: usize,
    ) {
        let mut header: u64 = 0;
        let mut i = offset;
        for value in values.take(limit) {
            header |= match value {
                None => 1 << ((i * 2) + 1),
                Some(CqlValue::Empty) => 1 << (i * 2),
                _ => 0,
            };
            i += 1;
        }
        unsigned_varint(buf, header);
    }

    let size = match key {
        ClusteringKeyValue::Simple(_) => 1,
        ClusteringKeyValue::Composite(v) => v.len(),
        ClusteringKeyValue::Empty => return,
    };
    let mut offset = 0;
    let mut values = key.into_iter();
    let mut header_values = key.into_iter();

    while offset < size {
        make_header(buf, &mut header_values, offset, 32);

        for value in (&mut values).take(32) {
            offset += 1;
            let value = match value {
                None | Some(CqlValue::Empty) => {
                    continue;
                }
                Some(v) => v,
            };
            cql_value_without_size(buf, value);
        }
    }
}

pub(crate) fn partition_value(buf: &mut impl BufMut, key: &PartitionKeyValue) {
    for v in key {
        cql_value_without_size(buf, v);
    }
}

pub(crate) fn consistency(buf: &mut impl BufMut, consistency: &LegacyConsistency) {
    match *consistency {
        LegacyConsistency::Regular(r) => buf.put_i16(r.into()),
        LegacyConsistency::Serial(s) => buf.put_i16(s.into()),
    }
}

pub(crate) fn unsigned_varint(buf: &mut impl BufMut, value: u64) {
    use integer_encoding::VarIntWriter;

    buf.writer().write_varint(value).unwrap();
}

pub(crate) fn opt_buffer_varint(buf: &mut impl BufMut, value: Option<&Bytes>) {
    let Some(value) = value else {
        unsigned_varint(buf, 0);
        return;
    };

    unsigned_varint(buf, value.len() as u64);
    buf.put_slice(value.as_bytes());
}

#[cfg(test)]
mod tests {
    use crate::cql::value::{ClusteringKeyValue, CqlValue};

    #[test]
    fn serialize_clustering_value() {
        let value = ClusteringKeyValue::Composite(vec![
            Some(CqlValue::Ascii("998".to_owned())),
            Some(CqlValue::Ascii("1".to_owned())),
        ]);
        let mut buf = vec![];
        super::clustering_value(&mut buf, &value);

        assert_eq!(buf, b"\0\x03998\x011");
    }
}
