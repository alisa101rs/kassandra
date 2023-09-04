use crate::{
    cql::{parser, query::QueryString},
    error::DbError,
    frame::{parse, response::error::Error},
};

pub fn parse(data: &[u8]) -> Result<QueryString, Error> {
    let (rest, raw_query) = parse::long_string(data)?;
    let query = parser::query(raw_query).map_err(|_| {
        Error::new(
            DbError::SyntaxError,
            format!("Could not parse query: {raw_query}"),
        )
    })?;
    if !rest.is_empty() {
        return Err(Error::new(DbError::Invalid, "Data contains ".to_string()));
    }

    Ok(query)
}
