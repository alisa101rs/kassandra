use bitflags::bitflags;

use crate::{
    error::DbError,
    frame::{
        consistency::{Consistency, SerialConsistency},
        response::error::Error,
        value::FrameValue,
        FrameFlags,
    },
};

#[derive(Debug, Clone)]
pub struct QueryParameters<'a> {
    pub consistency: Consistency,
    pub flags: QueryFlags,
    pub data: Vec<FrameValue<'a>>,
    pub result_page_size: Option<usize>,
    pub paging_state: Option<PagingState<'a>>,
    pub serial_consistency: SerialConsistency,
    pub default_timestamp: Option<i64>,
}

impl Default for QueryParameters<'static> {
    fn default() -> Self {
        Self {
            consistency: Consistency::LocalOne,
            flags: QueryFlags::empty(),
            data: vec![],
            result_page_size: None,
            paging_state: None,
            serial_consistency: SerialConsistency::Serial,
            default_timestamp: None,
        }
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    pub fn parse(input: &'a [u8], _flags: FrameFlags) -> Result<Self, Error> {
        parse::query_parameters(input)
            .map(|(_r, it)| it)
            .map_err(|er| {
                Error::new(
                    DbError::ProtocolError,
                    format!("Could not parse query parameters: {er}"),
                )
            })
    }
}

mod parse {

    use integer_encoding::VarIntReader;
    use nom::{
        branch::alt,
        bytes::complete::{tag, take},
        combinator::{map, recognize},
        error::{Error, ErrorKind},
        multi::count,
        number::complete,
        sequence::pair,
        IResult,
    };

    use crate::frame::{
        consistency::{Consistency, SerialConsistency},
        parse,
        request::{query_params::PagingState, QueryFlags, QueryParameters},
        value::FrameValue,
    };

    ///  `<query_parameters>` must be:
    ///     `<consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]`
    pub fn query_parameters(input: &[u8]) -> IResult<&[u8], QueryParameters> {
        // <consistency> is the [consistency] level for the operation.
        let (rest, consistency) = complete::be_i16(input)?;
        let consistency = Consistency::try_from(consistency)
            .map_err(|_| nom::Err::Failure(Error::new(input, ErrorKind::Tag)))?;

        // <flags> is a [byte] whose bits define the options for this query and
        //       in particular influence what the remainder of the message contains.
        let (rest, flags) = complete::be_u8(rest)?;
        let flags = QueryFlags::from_bits(flags)
            .ok_or_else(|| nom::Err::Failure(Error::new(input, ErrorKind::Tag)))?;

        // QueryFlags::VALUES. If set, a [short] <n> followed by <n> [value]
        //    values are provided. Those values are used for bound variables in
        //    the query. Optionally, if the 0x40 flag is present, each value
        //    will be preceded by a [string] name, representing the name of
        //     the marker the value must be bound to.
        let (rest, data) = if flags.contains(QueryFlags::VALUES) {
            values(rest, flags.contains(QueryFlags::WITH_NAMES_FOR_VALUES))?
        } else {
            (rest, vec![])
        };

        // QueryFlags::PAGE_SIZE. If set, <result_page_size> is an [int]
        //     controlling the desired page size of the result (in CQL3 rows).
        let (rest, result_page_size) = if flags.contains(QueryFlags::PAGE_SIZE) {
            result_page_size(rest)?
        } else {
            (rest, None)
        };

        // QueryFlags::WITH_PAGING_STATE. If set, <paging_state> should be present.
        //     <paging_state> is a [bytes] value that should have been returned
        //     in a result set . The query will be executed but starting from a given paging state.
        let (rest, paging_state) = if flags.contains(QueryFlags::WITH_PAGING_STATE) {
            opt_paging_state(rest)?
        } else {
            (rest, None)
        };

        // QueryFlags::WITH_SERIAL_CONSISTENCY. If set, <serial_consistency> should be present.
        // <serial_consistency> is the [consistency] level for the serial phase of conditional updates.
        // That consistency can only be either SERIAL or LOCAL_SERIAL and if not present, it defaults to SERIAL.
        // This option will be ignored for anything else other than a conditional update/insert.
        let (rest, serial_consistency) = if flags.contains(QueryFlags::WITH_SERIAL_CONSISTENCY) {
            let (rest, raw) = recognize(alt((
                tag(0x0008i16.to_be_bytes()),
                tag(0x0009i16.to_be_bytes()),
            )))(rest)?;
            let number = i16::from_be_bytes(raw.try_into().unwrap());

            (
                rest,
                SerialConsistency::try_from(number).expect("to be a valid serial consistency"),
            )
        } else {
            (rest, SerialConsistency::Serial)
        };

        // QueryFlags::WITH_DEFAULT_TIMESTAMP. If set, <timestamp> should be present.
        //   <timestamp> is a [long] representing the default timestamp for the query
        //   in microseconds (negative values are forbidden). This will
        //   replace the server side assigned timestamp as default timestamp.
        //   Note that a timestamp in the query itself will still override
        //   this timestamp. This is entirely optional.
        let (rest, default_timestamp) = if flags.contains(QueryFlags::WITH_DEFAULT_TIMESTAMP) {
            map(complete::be_i64, Some)(rest)?
        } else {
            (rest, None)
        };

        Ok((
            rest,
            QueryParameters {
                consistency,
                flags,
                data,
                result_page_size,
                paging_state,
                serial_consistency,
                default_timestamp,
            },
        ))
    }

    fn values(rest: &[u8], with_names: bool) -> IResult<&[u8], Vec<FrameValue>> {
        let (rest, num_values) = complete::be_u16(rest)?;

        let (rest, values) = if with_names {
            count(
                map(pair(parse::short_string, parse::value), |(_, value)| value),
                num_values as usize,
            )(rest)?
        } else {
            count(parse::value, num_values as usize)(rest)?
        };

        Ok((rest, values))
    }

    fn result_page_size(rest: &[u8]) -> IResult<&[u8], Option<usize>> {
        map(complete::be_u32, |it| Some(it as _))(rest)
    }
    fn opt_paging_state(rest: &[u8]) -> IResult<&[u8], Option<PagingState>> {
        let (rest, encoded_paging_state) = parse::bytes_opt(rest)?;

        if let Some(encoded_paging_state) = encoded_paging_state {
            let (_, state) = paging_state(encoded_paging_state).map_err(|er| {
                tracing::error!(?er, "Could not parse paging state");
                er
            })?;
            Ok((rest, Some(state)))
        } else {
            Ok((rest, None))
        }
    }

    fn paging_state(input: &[u8]) -> IResult<&[u8], PagingState<'_>> {
        fn unsigned_vint(mut input: &[u8]) -> IResult<&[u8], u32> {
            let int = input
                .read_varint()
                .map_err(|_| nom::Err::Error(Error::new(input, ErrorKind::LengthValue)))?;
            Ok((input, int))
        }
        fn bytes_with_vint(input: &[u8]) -> IResult<&[u8], Option<&[u8]>> {
            let (rest, len) = unsigned_vint(input)?;

            if len == 0 {
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

    #[test]
    fn test_params_1() {
        let input: &[u8] = &[0u8, 1, 36, 0, 0, 19, 136, 0, 6, 8, 211, 160, 192, 75, 233];
        let (rest, _params) = query_parameters(input).unwrap();

        assert!(rest.is_empty());
    }
}
