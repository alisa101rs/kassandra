use eyre::Result;

use crate::{
    cql::{parser, query::QueryString},
    error::DbError,
    frame::{parse, request::query_params::QueryParameters, response::error::Error, FrameFlags},
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

    pub fn parse(input: &'a [u8], flags: FrameFlags) -> Result<Self, Error> {
        let (rest, raw_query) = parse::long_string(input)?;
        let query = parser::query(raw_query).map_err(|_| {
            Error::new(
                DbError::SyntaxError,
                format!("Could not parse query: {raw_query}"),
            )
        })?;

        let parameters = QueryParameters::parse(rest, flags)?;

        Ok(Self {
            query,
            raw_query,
            parameters,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::{request::query::Query, FrameFlags};

    #[test]
    fn test_select_1() {
        let data: &[u8] = b"\0\0\0.select * from system.local where key = 'local'\0\x01\0\0\0$\0\0\x13\x88\0\x06\x08\xd3\xa0\xc0K\xe9";
        let q = Query::parse(data, FrameFlags::empty());
        assert!(q.is_ok());
    }
}
