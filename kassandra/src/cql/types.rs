use std::str::FromStr;

use nom::{
    bytes::{complete::tag, streaming::take_while},
    character::is_alphanumeric,
    error::ErrorKind,
    multi::separated_list1,
    sequence::terminated,
    IResult,
};
use serde::Serialize;
use strum_macros::EnumString;

use crate::parse::{identifier, ws};

type ParseResult<'a, T> = IResult<&'a str, T, nom::error::Error<&'a str>>;

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

pub fn parse_cql_type(p: &str) -> ParseResult<PreCqlType> {
    if let Ok((_rest, _)) = tag::<_, _, nom::error::Error<_>>("frozen<")(p) {
        let (p, inner_type) = parse_cql_type(p)?;
        let frozen_type = freeze_type(inner_type);
        Ok((p, frozen_type))
    } else if let Ok((p, _)) = tag::<_, _, nom::error::Error<_>>("map<")(p) {
        let (p, key) = terminated(parse_cql_type, ws(tag(",")))(p)?;
        let (p, value) = parse_cql_type(p)?;
        let (p, _) = tag(">")(p)?;

        let typ = PreCqlType::Map {
            frozen: false,
            key: Box::new(key),
            value: Box::new(value),
        };

        Ok((p, typ))
    } else if let Ok((p, _)) = tag::<_, _, nom::error::Error<_>>("list<")(p) {
        let (p, inner_type) = parse_cql_type(p)?;
        let (p, _) = tag(">")(p)?;

        let typ = PreCqlType::List {
            frozen: false,
            item: Box::new(inner_type),
        };

        Ok((p, typ))
    } else if let Ok((p, _)) = tag::<_, _, nom::error::Error<_>>("set<")(p) {
        let (p, inner_type) = parse_cql_type(p)?;
        let (p, _) = tag(">")(p)?;

        let typ = PreCqlType::Set {
            frozen: false,
            item: Box::new(inner_type),
        };

        Ok((p, typ))
    } else if let Ok((p, _)) = tag::<_, _, nom::error::Error<_>>("tuple<")(p) {
        let (p, types) = separated_list1(ws(tag(",")), parse_cql_type)(p)?;
        let (p, _) = tag(">")(p)?;
        Ok((p, PreCqlType::Tuple(types)))
    } else if let Ok((p, typ)) = parse_native_type(p) {
        Ok((p, PreCqlType::Native(typ)))
    } else if let Ok((name, p)) = parse_user_defined_type(p) {
        let typ = PreCqlType::UserDefinedType {
            frozen: false,
            name: name.to_string(),
        };
        Ok((p, typ))
    } else {
        // Err(p.error(ParseErrorCause::Other("invalid cql type")))
        panic!("invalid cql type")
    }
}

fn parse_native_type(p: &str) -> ParseResult<NativeType> {
    let (p, tok) = identifier(p)?;
    let typ = NativeType::from_str(&tok)
        .map_err(|_| nom::Err::Error(nom::error::make_error(p, ErrorKind::Tag)))?;
    Ok((p, typ))
}

fn parse_user_defined_type(p: &str) -> ParseResult<&str> {
    // Java identifiers allow letters, underscores and dollar signs at any position
    // and digits in non-first position. Dots are accepted here because the names
    // are usually fully qualified.
    let (p, tok) = take_while(|c| is_alphanumeric(c as u8) || c == '.' || c == '_' || c == '$')(p)?;

    if tok.is_empty() {
        return Err(nom::Err::Error(nom::error::make_error(p, ErrorKind::Tag)));
    }
    Ok((p, tok))
}

fn freeze_type(mut type_: PreCqlType) -> PreCqlType {
    match type_ {
        PreCqlType::List { ref mut frozen, .. }
        | PreCqlType::Set { ref mut frozen, .. }
        | PreCqlType::Map { ref mut frozen, .. }
        | PreCqlType::UserDefinedType { ref mut frozen, .. } => {
            *frozen = true;
        }
        _ => {}
    }

    type_
}
