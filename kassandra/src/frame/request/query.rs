use eyre::Result;

use crate::{
    cql::{parser, query::QueryString},
    error::DbError,
    frame::{
        consistency::{Consistency, SerialConsistency},
        parse,
        request::{query_params::QueryParameters, QueryFlags},
        response::error::Error,
    },
};

#[derive(Debug, Clone)]
pub struct Query<'a> {
    pub query: QueryString,
    pub raw_query: &'a str,
    pub consistency: Consistency,
    pub flags: QueryFlags,
    pub data: Vec<Option<&'a [u8]>>,
    pub page_size: Option<usize>,
    pub paging_state: Option<&'a [u8]>,
    pub serial_consistency: Option<SerialConsistency>,
    pub default_timestamp: Option<i64>,
}

impl<'a> Query<'a> {
    pub fn simple(input: &'a str) -> Result<Self, Error> {
        Ok(Self {
            query: parser::query(input)?,
            raw_query: input,
            consistency: Consistency::LocalOne,
            flags: QueryFlags::empty(),
            data: vec![],
            page_size: None,
            paging_state: None,
            serial_consistency: None,
            default_timestamp: None,
        })
    }

    pub fn parse(input: &'a [u8]) -> Result<Self, Error> {
        let (rest, raw_query) = parse::long_string(input)?;
        let query = parser::query(raw_query).map_err(|_| {
            Error::new(
                DbError::SyntaxError,
                format!("Could not parse query: {raw_query}"),
            )
        })?;

        let QueryParameters {
            consistency,
            flags,
            data,
            page_size,
            paging_state,
            serial_consistency,
            default_timestamp,
        } = QueryParameters::parse(rest)?;

        Ok(Self {
            query,
            raw_query,
            consistency,
            flags,
            data,
            page_size,
            paging_state,
            serial_consistency,
            default_timestamp,
        })
    }
}
