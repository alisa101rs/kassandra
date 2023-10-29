use bytes::{Buf, BufMut, Bytes, BytesMut};
use eyre::{eyre, Result};
use nom::AsBytes;
use num_enum::TryFromPrimitive;
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    error::DbError,
    frame::{FrameFlags, FrameParams, ProtocolVersion},
};

pub mod authenticate;
pub mod error;
pub mod event;
pub mod result;
pub mod supported;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(u8)]
pub enum ResponseOpcode {
    Error = 0x00,
    Ready = 0x02,
    Authenticate = 0x03,
    Supported = 0x06,
    Result = 0x08,
    Event = 0x0C,
    AuthChallenge = 0x0E,
    AuthSuccess = 0x10,
}

#[derive(Debug)]
pub enum Response {
    Error(error::Error),
    Ready,
    Authenticate(authenticate::Authenticate),
    Supported(supported::Supported),
    Result(result::QueryResult),
    Event(event::Event),
    AuthChallenge(authenticate::AuthChallenge),
    AuthSuccess(authenticate::AuthSuccess),
}

impl Response {
    pub fn opcode(&self) -> u8 {
        match self {
            Self::Error { .. } => 0x00,
            Self::Ready { .. } => 0x02,
            Self::Authenticate { .. } => 0x03,
            Self::Supported { .. } => 0x06,
            Self::Result { .. } => 0x08,
            Self::Event { .. } => 0x0C,
            Self::AuthChallenge { .. } => 0x0E,
            Self::AuthSuccess { .. } => 0x10,
        }
    }

    pub fn options() -> Self {
        Response::Supported(supported::Supported {
            options: vec![
                ("CQL_VERSION".to_owned(), vec!["3.0.0".to_owned()]),
                ("COMPRESSION".to_owned(), vec![]),
                ("PROTOCOL_VERSIONS".to_owned(), vec!["4/v4".to_owned()]),
            ]
            .into_iter()
            .collect(),
        })
    }

    /// Sends ProtocolError response with a message that most drivers expect to receive
    /// in order to start downgrading versions
    pub fn unsupported_version() -> Self {
        Self::Error(error::Error::new(
            DbError::ProtocolError,
            "unsupported protocol version",
        ))
    }

    pub fn serialize(&self, buf: &mut impl BufMut, _flags: &mut FrameFlags) -> Result<()> {
        match self {
            Response::Supported(supported) => {
                supported.serialize(buf)?;
                Ok(())
            }
            Response::Ready => Ok(()),
            Response::Error(er) => {
                er.serialize(buf);
                Ok(())
            }
            Response::Authenticate(_) => {
                unimplemented!()
            }
            Response::Result(res) => {
                res.serialize(buf)?;
                Ok(())
            }
            Response::Event(_) => {
                unimplemented!()
            }
            Response::AuthChallenge(_) => {
                unimplemented!()
            }
            Response::AuthSuccess(_) => {
                unimplemented!()
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct ResponseFrameCodec;

impl Encoder<(Response, i16)> for ResponseFrameCodec {
    type Error = eyre::Report;

    fn encode(
        &mut self,
        (response, stream_id): (Response, i16),
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        let mut flags = FrameFlags::empty();
        dst.resize(9, 0);
        response.serialize(dst, &mut flags)?;

        let (mut header, data) = dst.split_at_mut(9);

        header.put_u8(0x84); // version
        header.put_u8(flags.bits());
        header.put_i16(stream_id);
        header.put_u8(response.opcode());
        header.put_u32(data.len() as _);

        debug_assert_eq!(header.len(), 0);

        Ok(())
    }
}

impl Encoder<(FrameParams, ResponseOpcode, Bytes)> for ResponseFrameCodec {
    type Error = eyre::Report;

    fn encode(
        &mut self,
        (frame, opcode, data): (FrameParams, ResponseOpcode, Bytes),
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        let FrameParams {
            version,
            flags,
            stream,
        } = frame;

        dst.put_u8(version.to_response()); // version
        dst.put_u8(flags.bits());
        dst.put_i16(stream);
        dst.put_u8(opcode as u8);
        dst.put_u32(data.len() as _);
        dst.put_slice(data.as_bytes());

        tracing::trace!(?opcode, ?frame, body = ?data, "Sent response frame");

        Ok(())
    }
}

impl Decoder for ResponseFrameCodec {
    type Item = (FrameParams, ResponseOpcode, Bytes);
    type Error = eyre::Report;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        if src.len() < 9 {
            src.reserve(9 - src.len());
            return Ok(None);
        }

        let length = (&src[5..9]).get_u32() as usize;

        if src.len() < 9 + length {
            src.reserve(9 + length - src.len());

            return Ok(None);
        }

        let version = ProtocolVersion::from_response(src.get_u8());
        let frame = FrameParams {
            version,
            flags: FrameFlags::from_bits(src.get_u8()).ok_or(eyre!("invalid flag"))?,
            stream: src.get_i16(),
        };

        if matches!(frame.version, ProtocolVersion::Unsupported(_)) {
            tracing::warn!(?frame, "Frame version is not v4, ignore and read as v4");
        }

        if frame.flags.contains(FrameFlags::COMPRESSION) {
            Err(eyre!("Compression is not supported"))?;
        }

        let opcode = ResponseOpcode::try_from(src.get_u8())?;
        let _ = src.get_u32() as usize;
        let body = src.split_to(length);

        tracing::trace!(?opcode, ?frame, ?body, "Received response frame");

        Ok(Some((frame, opcode, Bytes::from(body))))
    }
}
