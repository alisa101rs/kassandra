use eyre::Result;

use crate::{
    cql::{parser, query::QueryString},
    error::DbError,
    frame::{parse, request::query_params::QueryParameters, response::error::Error},
};

#[derive(Debug, Clone)]
pub struct Query<'a> {
    pub query: QueryString,
    pub raw_query: &'a str,
    pub parameters: QueryParameters<'a>,
}

impl<'a> Query<'a> {
    pub fn simple(input: &'a str) -> Result<Self, Error> {
        Ok(Self {
            query: parser::query(input)?,
            raw_query: input,
            parameters: Default::default(),
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

        let parameters = QueryParameters::parse(rest)?;

        Ok(Self {
            query,
            raw_query,
            parameters,
        })
    }
}
