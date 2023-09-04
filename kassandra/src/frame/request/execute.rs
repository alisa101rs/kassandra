use crate::frame::{
    consistency::{Consistency, SerialConsistency},
    parse,
    request::{query_params::QueryParameters, QueryFlags},
    response::error::Error,
};

#[derive(Debug, Clone)]
pub struct Execute<'a> {
    pub id: &'a [u8],
    pub consistency: Consistency,
    pub flags: QueryFlags,
    pub data: Vec<Option<&'a [u8]>>,
    pub page_size: Option<usize>,
    pub paging_state: Option<&'a [u8]>,
    pub serial_consistency: Option<SerialConsistency>,
    pub default_timestamp: Option<i64>,
}

impl<'a> Execute<'a> {
    pub fn parse(data: &'a [u8]) -> Result<Execute<'a>, Error> {
        let (rest, id) = parse::short_bytes(data)?;

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
            id,
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
