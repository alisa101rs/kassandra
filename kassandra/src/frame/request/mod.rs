use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use eyre::eyre;
use nom::AsBytes;
use num_enum::TryFromPrimitive;
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    cql::query::QueryString,
    frame::{parse, request::batch::Batch, response::error::Error, write, FrameFlags, FrameParams},
};

pub mod batch;
pub mod execute;
mod prepare;
pub mod query;
mod startup;

pub(crate) mod query_params;

pub use query_params::QueryFlags;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(u8)]
pub enum RequestOpcode {
    Startup = 0x01,
    Options = 0x05,
    Query = 0x07,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Batch = 0x0D,
    AuthResponse = 0x0F,
}

#[derive(Debug)]
pub enum Request<'a> {
    StartUp(HashMap<String, String>),
    Options,
    Query(query::Query<'a>),
    Batch(Batch<'a>),
    Prepare(QueryString),
    Execute(execute::Execute<'a>),
    Register { events: Vec<String> },
    AuthResponse,
}

impl<'a> Request<'a> {
    pub fn opcode(&self) -> u8 {
        match self {
            Self::StartUp { .. } => 0x01,
            Self::Options { .. } => 0x05,
            Self::Query { .. } => 0x07,
            Self::Prepare { .. } => 0x09,
            Self::Execute { .. } => 0x0A,
            Self::Register { .. } => 0x0B,
            Self::Batch { .. } => 0x0D,
            Self::AuthResponse { .. } => 0x0F,
        }
    }

    pub fn serialize(&self, buf: &mut impl BufMut) -> eyre::Result<()> {
        match self {
            Self::Options => {}
            Self::StartUp(opts) => {
                write::string_map(buf, opts);
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    pub fn deserialize(opcode: RequestOpcode, data: &'a [u8]) -> Result<Self, Error> {
        let request = match opcode {
            RequestOpcode::Startup => Request::StartUp(startup::deserialize(data)?),
            RequestOpcode::Options => Request::Options,
            RequestOpcode::Query => Request::Query(query::Query::parse(data)?),
            RequestOpcode::Prepare => Request::Prepare(prepare::parse(data)?),
            RequestOpcode::Execute => Request::Execute(execute::Execute::parse(data)?),
            RequestOpcode::Register => {
                let (_, events) = parse::short_string_list(data)?;

                Request::Register {
                    events: events.into_iter().map(|it| it.to_owned()).collect(),
                }
            }
            RequestOpcode::Batch => Request::Batch(Batch::deserialize(data)?),
            RequestOpcode::AuthResponse => unimplemented!(),
        };

        Ok(request)
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct RequestFrameCodec;

impl<'a> Encoder<(Request<'a>, FrameParams)> for RequestFrameCodec {
    type Error = eyre::Report;

    fn encode(
        &mut self,
        (request, frame): (Request<'a>, FrameParams),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        dst.resize(9, 0);
        request.serialize(dst)?;

        let (mut header, data) = dst.split_at_mut(9);

        header.put_u8(0x04); // version
        header.put_u8(frame.flags.bits());
        header.put_i16(frame.stream);
        header.put_u8(request.opcode());
        header.put_u32(data.len() as _);

        debug_assert_eq!(header.len(), 0);

        Ok(())
    }
}

impl Encoder<(FrameParams, RequestOpcode, Bytes)> for RequestFrameCodec {
    type Error = eyre::Report;

    fn encode(
        &mut self,
        (frame, opcode, data): (FrameParams, RequestOpcode, Bytes),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        dst.put_u8(0x04); // version
        dst.put_u8(frame.flags.bits());
        dst.put_i16(frame.stream);
        dst.put_u8(opcode as _);
        dst.put_u32(data.len() as _);
        dst.put_slice(data.as_bytes());
        tracing::trace!(?frame, ?opcode, "Sent request frame");

        Ok(())
    }
}

impl Decoder for RequestFrameCodec {
    type Item = (FrameParams, RequestOpcode, Bytes);
    type Error = eyre::Report;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 9 {
            src.reserve(9 - src.len());
            return Ok(None);
        }

        let length = (&src[5..9]).get_u32() as usize;

        if src.len() < 9 + length {
            src.reserve(9 + length - src.len());

            return Ok(None);
        }

        let frame = FrameParams {
            version: src.get_u8(),
            flags: FrameFlags::from_bits(src.get_u8()).ok_or(eyre!("invalid flag"))?,
            stream: src.get_i16(),
        };

        if frame.version != 0x04 {
            tracing::warn!(?frame, "Frame version is not v4, ignore and read as v4");
        }

        if frame.flags.contains(FrameFlags::COMPRESSION) {
            Err(eyre!("Compression is not supported"))?;
        }

        let opcode = RequestOpcode::try_from(src.get_u8())?;
        let _ = src.get_u32() as usize;
        let body = src.split_to(length);

        tracing::trace!(?body, ?opcode, ?frame, "Received request frame");

        Ok(Some((frame, opcode, Bytes::from(body))))
    }
}
