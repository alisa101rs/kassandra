use crate::frame::{
    parse, request::query_params::QueryParameters, response::error::Error, FrameFlags,
};

#[derive(Debug, Clone)]
pub struct Execute<'a> {
    pub id: &'a [u8],
    pub parameters: QueryParameters<'a>,
}

impl<'a> Execute<'a> {
    pub fn parse(data: &'a [u8], flags: FrameFlags) -> Result<Execute<'a>, Error> {
        let (rest, id) = parse::short_bytes(data)?;

        let parameters = QueryParameters::parse(rest, flags)?;

        Ok(Self { id, parameters })
    }
}
