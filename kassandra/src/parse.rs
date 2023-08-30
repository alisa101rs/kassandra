use std::collections::HashMap;

use nom::{
    branch::alt,
    bytes::complete::{tag, take},
    character::complete::{alpha1, alphanumeric1, multispace0},
    combinator::{map, opt, recognize},
    error,
    error::{ErrorKind, ParseError},
    multi::{many0_count, separated_list1},
    number::complete,
    sequence::{delimited, pair},
    IResult,
};

use crate::frame::consistency::{Consistency, LegacyConsistency, SerialConsistency};

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

pub fn value(input: &[u8]) -> IResult<&[u8], Option<&[u8]>> {
    let (rest, len) = complete::be_i32(input)?;
    match len {
        -1 => Ok((rest, Some(&[]))),
        -2 => Ok((rest, None)),
        _ if len < -2 => Err(nom::Err::Failure(error::Error::new(
            input,
            ErrorKind::NonEmpty,
        ))),
        _ => {
            let (rest, bytes) = take(len as usize)(rest)?;
            Ok((rest, Some(bytes)))
        }
    }
}

pub fn short_bytes(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (rest, len) = complete::be_i16(input)?;
    let (rest, bytes) = take(len as usize)(rest)?;
    Ok((rest, bytes))
}

pub fn identifier(input: &str) -> IResult<&str, String> {
    let ident = recognize(pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    ));

    map(ident, |it: &str| it.to_lowercase())(input)
}

pub fn cassandra_type(input: &str) -> IResult<&str, String> {
    let ident = pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    );
    let generics = opt(delimited(
        tag("<"),
        separated_list1(ws(tag(",")), ident),
        tag(">"),
    ));
    let ident = pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    );
    map(recognize(pair(ident, generics)), |it: &str| it.to_owned())(input)
}

pub fn ws<'a, F: 'a, O, E: ParseError<&'a str>>(
    inner: F,
) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
    F: FnMut(&'a str) -> IResult<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
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
