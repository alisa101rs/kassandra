#[derive(Debug, Clone)]
pub enum FrameValue<'a> {
    Some(&'a [u8]),
    Null,
    NotSet,
}
