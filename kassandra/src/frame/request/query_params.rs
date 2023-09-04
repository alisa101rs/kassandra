use bitflags::bitflags;
use eyre::Result;
use nom::{combinator::map, multi::count, number::complete, sequence::pair};

use crate::{
    error::DbError,
    frame::{
        consistency::{Consistency, SerialConsistency},
        parse,
        response::error::Error,
    },
};

pub struct QueryParameters<'a> {
    pub consistency: Consistency,
    pub flags: QueryFlags,
    pub data: Vec<Option<&'a [u8]>>,
    pub page_size: Option<usize>,
    pub paging_state: Option<&'a [u8]>,
    pub serial_consistency: Option<SerialConsistency>,
    pub default_timestamp: Option<i64>,
}

bitflags! {
    pub struct QueryFlags: u8 {
        const VALUES                    = 0b0000001;
        const SKIP_METADATA             = 0b0000010;
        const PAGE_SIZE                 = 0b0000100;
        const WITH_PAGING_STATE         = 0b0001000;
        const WITH_SERIAL_CONSISTENCY   = 0b0010000;
        const WITH_DEFAULT_TIMESTAMP    = 0b0100000;
        const WITH_NAMES_FOR_VALUES     = 0b1000000;
    }
}

impl<'a> QueryParameters<'a> {
    pub fn parse(input: &'a [u8]) -> Result<Self, Error> {
        let (rest, consistency) = complete::be_i16::<_, nom::error::Error<_>>(input)?;
        let consistency = Consistency::try_from(consistency).map_err(|_| DbError::ProtocolError)?;
        let (rest, flags) = complete::be_u8::<_, nom::error::Error<_>>(rest)?;
        let flags = QueryFlags::from_bits(flags).ok_or(DbError::ProtocolError)?;

        let (rest, data) = if flags.contains(QueryFlags::VALUES) {
            let (rest, num_values) = complete::be_u16::<_, nom::error::Error<_>>(rest)?;

            let (rest, values) = if flags.contains(QueryFlags::WITH_NAMES_FOR_VALUES) {
                count(
                    map(pair(parse::short_string, parse::value), |(_, value)| value),
                    num_values as usize,
                )(rest)?
            } else {
                count(parse::value, num_values as usize)(rest)?
            };

            (rest, values)
        } else {
            (rest, vec![])
        };

        let (rest, page_size) = if flags.contains(QueryFlags::PAGE_SIZE) {
            map(complete::be_u32::<_, nom::error::Error<_>>, |it| {
                Some(it as _)
            })(rest)?
        } else {
            (rest, None)
        };

        let (rest, paging_state) = if flags.contains(QueryFlags::WITH_PAGING_STATE) {
            parse::bytes_opt(rest)?
        } else {
            (rest, None)
        };
        let (rest, serial_consistency) = if flags.contains(QueryFlags::WITH_SERIAL_CONSISTENCY) {
            let (rest, raw) = complete::be_i16::<_, nom::error::Error<_>>(rest)?;
            (rest, SerialConsistency::try_from(raw).ok())
        } else {
            (rest, None)
        };
        let (rest, default_timestamp) = if flags.contains(QueryFlags::WITH_DEFAULT_TIMESTAMP) {
            map(complete::be_i64::<_, nom::error::Error<_>>, Some)(rest)?
        } else {
            (rest, None)
        };

        if !rest.is_empty() {
            tracing::warn!(?rest, "Buf is not empty, probably was parsed incorrectly");
        }

        Ok(Self {
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
