use bytes::{BufMut, Bytes, BytesMut};
use nom::AsBytes;
use serde::Serialize;

use crate::frame::write;

#[derive(Debug, Clone)]
pub enum FrameValue<'a> {
    Some(&'a [u8]),
    Null,
    NotSet,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PagingState {
    pub partition_key: Option<Bytes>,
    pub row_mark: Option<Bytes>,
    pub remaining: usize,
    pub remaining_in_partition: usize,
}

impl PagingState {
    pub fn new(
        partition_key: Option<Bytes>,
        clustering_key: Option<Bytes>,
        remaining: usize,
        remaining_in_partition: usize,
    ) -> Self {
        Self {
            partition_key,
            row_mark: clustering_key,
            remaining,
            remaining_in_partition,
        }
    }

    pub fn encode(&self, dst: &mut impl BufMut) {
        let mut b = BytesMut::new();
        write::opt_buffer_varint(&mut b, self.partition_key.as_ref());
        write::opt_buffer_varint(&mut b, self.row_mark.as_ref());
        write::unsigned_varint(&mut b, self.remaining as _);
        write::unsigned_varint(&mut b, self.remaining_in_partition as _);

        write::bytes(dst, b.as_bytes());
    }
}
