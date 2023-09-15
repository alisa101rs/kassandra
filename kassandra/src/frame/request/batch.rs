use nom::{
    combinator::map,
    multi::count,
    number::complete::{be_i16, be_i64, be_u16, be_u8},
};
use num_enum::TryFromPrimitive;

use crate::{
    cql::{parser, query::QueryString},
    error::DbError,
    frame::{
        consistency::{Consistency, SerialConsistency},
        parse,
        response::error::Error,
    },
};

#[derive(Debug, Clone)]
pub struct Batch<'a> {
    pub batch_type: BatchType,
    pub consistency: Consistency,
    pub serial_consistency: Option<SerialConsistency>,
    pub timestamp: Option<i64>,
    pub statements: Vec<BatchStatement<'a>>,
}

/// The type of a batch.
#[derive(Debug, Clone, Copy, TryFromPrimitive)]
#[repr(u8)]
pub enum BatchType {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
}

#[derive(Debug, Clone)]
pub enum BatchStatement<'a> {
    Query {
        query: QueryString,
        raw_query: &'a str,
        values: Vec<Option<&'a [u8]>>,
    },
    Prepared {
        id: &'a [u8],
        values: Vec<Option<&'a [u8]>>,
    },
}

impl<'a> Batch<'a> {
    pub fn deserialize(input: &'a [u8]) -> Result<Self, Error> {
        let (rest, ty) = be_u8::<_, nom::error::Error<_>>(input)?;
        let batch_type = BatchType::try_from(ty).map_err(|_| DbError::ProtocolError)?;
        let (mut rest, queries_count) = be_u16::<_, nom::error::Error<_>>(rest)?;

        let mut statements = vec![];
        for _ in 0..queries_count {
            let (r, kind) = be_u8::<_, nom::error::Error<_>>(rest)?;

            let values_parser = |r: &'a [u8]| {
                let (r, values_count) = be_u16::<_, nom::error::Error<_>>(r)?;
                count(parse::value, values_count as usize)(r)
            };
            match kind {
                0 => {
                    let (r, query_string) = parse::long_string(r)?;
                    let query = parser::query(query_string)?;

                    let (r, values) = values_parser(r)?;
                    rest = r;

                    let query = BatchStatement::Query {
                        query,
                        raw_query: query_string,
                        values,
                    };
                    statements.push(query);
                }
                1 => {
                    let (r, id) = parse::short_bytes(r)?;
                    let (r, values) = values_parser(r)?;
                    rest = r;

                    let execute = BatchStatement::Prepared { id, values };

                    statements.push(execute)
                }
                _ => unreachable!(),
            }
        }

        let (rest, consistency) = map(be_i16::<_, nom::error::Error<_>>, |it| {
            Consistency::try_from(it).unwrap()
        })(rest)?;
        let (rest, flags) = be_u8::<_, nom::error::Error<_>>(rest)?;

        if flags & 0x40 != 0 {
            return Err(Error::new(
                DbError::ProtocolError,
                "Batch query with NAMES_FOR_VALUES flag is un-implementable",
            ));
        }

        let (rest, serial_consistency) = if flags & 0x10 != 0 {
            map(be_i16::<_, nom::error::Error<_>>, |f| {
                SerialConsistency::try_from(f).ok()
            })(rest)?
        } else {
            (rest, None)
        };

        let (rest, timestamp) = if flags & 0x20 != 0 {
            map(be_i64::<_, nom::error::Error<_>>, Some)(rest)?
        } else {
            (rest, None)
        };

        if !rest.is_empty() {
            tracing::warn!(?rest, "Batch wasn't parsed till the end");
        }

        Ok(Batch {
            batch_type,
            consistency,
            serial_consistency,
            timestamp,
            statements,
        })
    }
}
