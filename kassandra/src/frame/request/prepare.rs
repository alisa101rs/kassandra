use crate::{cql::query::QueryString, error::DbError, frame::response::error::Error, parse};

pub fn parse(data: &[u8]) -> Result<QueryString, Error> {
    let (rest, raw_query) = parse::long_string(data)?;
    let query = QueryString::parse(raw_query).map_err(|_| {
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
