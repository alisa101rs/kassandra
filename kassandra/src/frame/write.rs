use std::{collections::HashMap, net::IpAddr};

use bytes::BufMut;
use nom::AsBytes;

use crate::{
    cql::{column::ColumnType, value::CqlValue},
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

    //let mut data = Vec::new();

    {
        //let buf = &mut data;
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
            CqlValue::Tuple(_) => {
                unimplemented!()
            }
            CqlValue::Uuid(u) => {
                bytes(buf, &u.as_u128().to_be_bytes());
            }
            CqlValue::Varint(_) => {
                unimplemented!()
            }
        }
    }

    //bytes(buf, data.as_slice());
}

pub(crate) fn consistency(buf: &mut impl BufMut, consistency: &LegacyConsistency) {
    match *consistency {
        LegacyConsistency::Regular(r) => buf.put_i16(r.into()),
        LegacyConsistency::Serial(s) => buf.put_i16(s.into()),
    }
}
