use derive_more::Display;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::Deserialize;

#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    TryFromPrimitive,
    Deserialize,
    Display,
    IntoPrimitive,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[repr(i16)]
pub enum Consistency {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    LocalOne = 0x000A,
}

#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    TryFromPrimitive,
    Deserialize,
    Display,
    IntoPrimitive,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[repr(i16)]
pub enum SerialConsistency {
    Serial = 0x0008,
    LocalSerial = 0x0009,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Display)]
pub enum LegacyConsistency {
    Regular(Consistency),
    Serial(SerialConsistency),
}
