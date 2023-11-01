use bitflags::bitflags;
use bytes::Bytes;
use futures::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, FramedRead, FramedWrite};

use crate::frame::{
    request::{Request, RequestFrameCodec, RequestOpcode},
    response::{Response, ResponseFrameCodec, ResponseOpcode},
};

pub mod consistency;
pub mod parse;
pub mod request;
pub mod response;
pub mod value;
pub mod write;

bitflags! {
    /// Frame Flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct FrameFlags: u8 {
        const COMPRESSION = 1 << 0;
        const TRACING = 1 << 1;
        const CUSTOM_PAYLOAD = 1 << 2;
        const WARNING = 1 << 3;
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ProtocolVersion {
    V4,
    Unsupported(u8),
}

impl ProtocolVersion {
    pub fn is_unsupported(&self) -> bool {
        matches!(self, Self::Unsupported(..))
    }
    pub fn from_request(value: u8) -> Self {
        match value {
            0x04 => Self::V4,

            x => Self::Unsupported(x),
        }
    }

    pub fn from_response(value: u8) -> Self {
        match value {
            0x84 => Self::V4,
            x => Self::Unsupported(x),
        }
    }

    pub fn to_request(&self) -> u8 {
        match self {
            ProtocolVersion::V4 => 0x04,
            &ProtocolVersion::Unsupported(x) => x,
        }
    }

    pub fn to_response(&self) -> u8 {
        match self {
            ProtocolVersion::V4 => 0x84,
            &ProtocolVersion::Unsupported(x) => x,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct FrameParams {
    pub version: ProtocolVersion,
    pub flags: FrameFlags,
    pub stream: i16,
}

pub fn request_stream<'a>(
    reader: impl AsyncRead + 'a,
) -> impl Stream<
    Item = Result<<RequestFrameCodec as Decoder>::Item, <RequestFrameCodec as Decoder>::Error>,
> + 'a {
    FramedRead::new(reader, RequestFrameCodec)
}

pub fn response_stream<'a>(
    reader: impl AsyncRead + 'a,
) -> impl Stream<
    Item = Result<<ResponseFrameCodec as Decoder>::Item, <ResponseFrameCodec as Decoder>::Error>,
> + 'a {
    FramedRead::new(reader, ResponseFrameCodec)
}

pub fn response_sink<'a>(
    writer: impl AsyncWrite + 'a,
) -> impl Sink<(Response, i16), Error = eyre::Report> + 'a {
    FramedWrite::new(writer, ResponseFrameCodec)
}

pub fn raw_response_sink<'a>(
    writer: impl AsyncWrite + 'a,
) -> impl Sink<(FrameParams, ResponseOpcode, Bytes), Error = eyre::Report> + 'a {
    FramedWrite::new(writer, ResponseFrameCodec)
}

pub fn request_sink<'a>(
    writer: impl AsyncWrite + 'a,
) -> impl Sink<(Request<'a>, FrameParams), Error = eyre::Report> + 'a {
    FramedWrite::new(writer, RequestFrameCodec)
}

pub fn raw_request_sink<'a>(
    writer: impl AsyncWrite + 'a,
) -> impl Sink<(FrameParams, RequestOpcode, Bytes), Error = eyre::Report> + 'a {
    FramedWrite::new(writer, RequestFrameCodec)
}
