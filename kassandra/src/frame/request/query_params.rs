use bitflags::bitflags;
use eyre::Result;
use integer_encoding::VarIntReader;
use nom::{
    bytes::complete::take, combinator::map, multi::count, number::complete, sequence::pair, IResult,
};

use crate::{
    error::DbError,
    frame::{
        consistency::{Consistency, SerialConsistency},
        parse,
        response::error::Error,
    },
};

#[derive(Debug, Clone)]
pub struct QueryParameters<'a> {
    pub consistency: Consistency,
    pub flags: QueryFlags,
    pub data: Vec<Option<&'a [u8]>>,
    pub page_size: Option<usize>,
    pub paging_state: Option<PagingState<'a>>,
    pub serial_consistency: Option<SerialConsistency>,
    pub default_timestamp: Option<i64>,
}

impl Default for QueryParameters<'static> {
    fn default() -> Self {
        Self {
            consistency: Consistency::LocalOne,
            flags: QueryFlags::empty(),
            data: vec![],
            page_size: None,
            paging_state: None,
            serial_consistency: None,
            default_timestamp: None,
        }
    }
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

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PagingState<'a> {
    partition_key: Option<&'a [u8]>,
    row_mark: Option<&'a [u8]>,
    remaining: usize,
    remaining_in_partition: usize,
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
            let (rest, encoded_paging_state) = parse::bytes_opt(rest)?;

            if let Some(encoded_paging_state) = encoded_paging_state {
                let (_, state) = parse_paging_state(encoded_paging_state).map_err(|er| {
                    tracing::error!(?er, "Could not parse paging state");
                    er
                })?;
                (rest, Some(state))
            } else {
                (rest, None)
            }
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

fn parse_paging_state(input: &[u8]) -> IResult<&[u8], PagingState<'_>> {
    fn unsigned_vint(mut input: &[u8]) -> IResult<&[u8], u32> {
        let int = input.read_varint().map_err(|_| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::IsNot))
        })?;
        Ok((input, int))
    }
    fn bytes_with_vint(input: &[u8]) -> IResult<&[u8], Option<&[u8]>> {
        let (rest, len) = unsigned_vint(input)?;

        if len <= 0 {
            return Ok((input, None));
        }

        map(take(len as usize), Some)(rest)
    }

    let (rest, partition_key) = bytes_with_vint(input)?;
    let (rest, row_mark) = bytes_with_vint(rest)?;
    let (rest, remaining) = map(unsigned_vint, |it| it as _)(rest)?;
    let (rest, remaining_in_partition) = map(unsigned_vint, |it| it as _)(rest)?;

    Ok((
        rest,
        PagingState {
            partition_key,
            row_mark,
            remaining,
            remaining_in_partition,
        },
    ))
}
