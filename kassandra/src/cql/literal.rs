use std::{collections::HashMap, fmt};

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until},
    character::complete::multispace0,
    combinator::map,
    multi::separated_list0,
    sequence::{delimited, separated_pair, terminated},
    IResult,
};
use serde::Serialize;

use crate::parse::ws;

#[derive(Debug, Clone, Serialize)]
pub enum Literal {
    String(String),
    Number(i64),
    Float(f64),
    List(Vec<Literal>),
    Map(HashMap<String, Literal>),
    Bool(bool),
    Null,
}

impl Literal {
    pub fn parse(input: &str) -> IResult<&str, Self> {
        alt((
            null_literal,
            map_literal,
            string_literal,
            number_literal,
            float_literal,
            list_literal,
        ))(input)
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::String(v) => v.fmt(f),
            Literal::Number(n) => n.fmt(f),
            Literal::Float(v) => v.fmt(f),
            Literal::List(values) => {
                for value in values {
                    write!(f, "{}, ", value)?;
                }
                Ok(())
            }
            Literal::Map(m) => {
                for (k, v) in m {
                    write!(f, "{} = {}, ", k, v)?;
                }
                Ok(())
            }
            Literal::Bool(b) => b.fmt(f),
            Literal::Null => write!(f, "null"),
        }
    }
}

fn string_literal(input: &str) -> IResult<&str, Literal> {
    map(
        delimited(tag("'"), take_until("'"), tag("'")),
        |it: &str| Literal::String(it.to_owned()),
    )(input)
}

fn number_literal(input: &str) -> IResult<&str, Literal> {
    map(nom::character::complete::i64, Literal::Number)(input)
}

fn null_literal(input: &str) -> IResult<&str, Literal> {
    map(tag_no_case("null"), |_| Literal::Null)(input)
}

fn float_literal(input: &str) -> IResult<&str, Literal> {
    map(nom::number::complete::double, Literal::Float)(input)
}

fn list_literal(input: &str) -> IResult<&str, Literal> {
    let values = separated_list0(ws(tag(",")), ws(Literal::parse));
    map(delimited(ws(tag("[")), values, ws(tag("]"))), Literal::List)(input)
}

fn map_literal(input: &str) -> IResult<&str, Literal> {
    let quoted_string = delimited(tag("'"), take_until("'"), tag("'"));
    let value = separated_pair(ws(quoted_string), tag(":"), ws(Literal::parse));

    let values = separated_list0(terminated(tag(","), multispace0), value);

    map(
        delimited(tag("{"), values, tag("}")),
        |it: Vec<(&str, Literal)>| {
            Literal::Map(
                it.into_iter()
                    .map(|(key, value)| (key.to_owned(), value))
                    .collect(),
            )
        },
    )(input)
}

#[cfg(test)]
mod tests {
    use super::map_literal;

    #[test]
    fn test_map() {
        let v = "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
        let (_, m) = map_literal(v).unwrap();
        println!("{m:?}");
    }
}
