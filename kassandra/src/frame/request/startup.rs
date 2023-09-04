use std::collections::HashMap;

use nom::AsBytes;

use crate::{
    error::DbError,
    frame::{parse, response::error::Error},
};

pub type Parameters = HashMap<String, String>;

pub(crate) fn deserialize(input: &[u8]) -> Result<Parameters, Error> {
    let (_, map) = parse::string_map(input.as_bytes())
        .map_err(|_| Error::new(DbError::ProtocolError, "Invalid Startup message body"))?;

    Ok(map
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect())
}
