use bytes::{BufMut, Bytes};
use nom::{
    number::complete::{be_i32, be_u8},
    AsBytes, IResult,
};
use thiserror::Error;

use crate::{error::DbError, frame::write, parse};

#[derive(Error, Debug, Clone)]
#[error("[{error}] {reason}")]
pub struct Error {
    pub error: DbError,
    pub reason: String,
}

impl Error {
    pub fn new(error: DbError, msg: impl ToString) -> Self {
        Self {
            error,
            reason: msg.to_string(),
        }
    }

    pub fn serialize(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.error.code());
        write::string(buf, &self.reason);

        match &self.error {
            DbError::AlreadyExists { keyspace, table } => {
                write::string(buf, keyspace);
                write::string(buf, table);
            }
            DbError::FunctionFailure {
                keyspace,
                function,
                arg_types,
            } => {
                write::string(buf, keyspace);
                write::string(buf, function);
                write::string_list(buf, arg_types);
            }
            DbError::Unavailable {
                consistency,
                required,
                alive,
            } => {
                write::consistency(buf, consistency);
                buf.put_i32(*required);
                buf.put_i32(*alive);
            }
            DbError::ReadTimeout {
                consistency,
                received,
                required,
                data_present,
            } => {
                write::consistency(buf, consistency);
                buf.put_i32(*received);
                buf.put_i32(*required);
                buf.put_u8(if *data_present { 1 } else { 0 });
            }
            DbError::WriteTimeout {
                consistency,
                received,
                required,
                write_type,
            } => {
                write::consistency(buf, consistency);
                buf.put_i32(*received);
                buf.put_i32(*required);
                write::string(buf, write_type.as_str());
            }
            DbError::ReadFailure {
                consistency,
                received,
                required,
                numfailures,
                data_present,
            } => {
                write::consistency(buf, consistency);
                buf.put_i32(*received);
                buf.put_i32(*required);
                buf.put_i32(*numfailures);
                buf.put_u8(if *data_present { 1 } else { 0 });
            }
            DbError::WriteFailure {
                consistency,
                received,
                required,
                numfailures,
                write_type,
            } => {
                write::consistency(buf, consistency);
                buf.put_i32(*received);
                buf.put_i32(*required);
                buf.put_i32(*numfailures);
                write::string(buf, write_type.as_str());
            }
            DbError::Unprepared { statement_id } => {
                write::short_bytes(buf, statement_id.as_bytes());
            }
            _ => {}
        }
    }

    pub fn deserialize(buf: &[u8]) -> IResult<&[u8], Self> {
        let (buf, code) = be_i32(buf)?;
        let (buf, reason) = parse::short_string(buf)?.to_owned();
        let reason = reason.to_owned();

        let (buf, error) = match code {
            0x0000 => (buf, DbError::ServerError),
            0x000A => (buf, DbError::ProtocolError),
            0x0100 => (buf, DbError::AuthenticationError),
            0x1000 => {
                let (buf, consistency) = parse::consistency(buf)?;
                let (buf, required) = be_i32(buf)?;
                let (buf, alive) = be_i32(buf)?;

                (
                    buf,
                    DbError::Unavailable {
                        consistency,
                        required,
                        alive,
                    },
                )
            }
            0x1001 => (buf, DbError::Overloaded),
            0x1002 => (buf, DbError::IsBootstrapping),
            0x1003 => (buf, DbError::TruncateError),
            0x1100 => {
                let (buf, consistency) = parse::consistency(buf)?;
                let (buf, received) = be_i32(buf)?;
                let (buf, required) = be_i32(buf)?;
                let (buf, write_type) = parse::short_string(buf)?;

                (
                    buf,
                    DbError::WriteTimeout {
                        consistency,
                        received,
                        required,
                        write_type: write_type.parse().unwrap(),
                    },
                )
            }
            0x1200 => {
                let (buf, consistency) = parse::consistency(buf)?;
                let (buf, received) = be_i32(buf)?;
                let (buf, required) = be_i32(buf)?;
                let (buf, data_present) = be_u8(buf)?;

                (
                    buf,
                    DbError::ReadTimeout {
                        consistency,
                        received,
                        required,
                        data_present: data_present != 0,
                    },
                )
            }
            0x1300 => {
                let (buf, consistency) = parse::consistency(buf)?;
                let (buf, received) = be_i32(buf)?;
                let (buf, required) = be_i32(buf)?;
                let (buf, numfailures) = be_i32(buf)?;
                let (buf, data_present) = be_u8(buf)?;

                (
                    buf,
                    DbError::ReadFailure {
                        consistency,
                        received,
                        required,
                        numfailures,
                        data_present: data_present != 0,
                    },
                )
            }
            0x1400 => {
                let (buf, keyspace) = parse::short_string(buf)?;
                let (buf, function) = parse::short_string(buf)?;
                let (buf, arg_types) = parse::short_string_list(buf)?;

                (
                    buf,
                    DbError::FunctionFailure {
                        keyspace: keyspace.to_owned(),
                        function: function.to_owned(),
                        arg_types: arg_types.into_iter().map(|it| it.to_owned()).collect(),
                    },
                )
            }
            0x1500 => {
                let (buf, consistency) = parse::consistency(buf)?;
                let (buf, received) = be_i32(buf)?;
                let (buf, required) = be_i32(buf)?;
                let (buf, numfailures) = be_i32(buf)?;
                let (buf, write_type) = parse::short_string(buf)?;

                (
                    buf,
                    DbError::WriteFailure {
                        consistency,
                        received,
                        required,
                        numfailures,
                        write_type: write_type.parse().unwrap(),
                    },
                )
            }
            0x2000 => (buf, DbError::SyntaxError),
            0x2100 => (buf, DbError::Unauthorized),
            0x2200 => (buf, DbError::Invalid),
            0x2300 => (buf, DbError::ConfigError),
            0x2400 => {
                let (buf, keyspace) = parse::short_string(buf)?;
                let (buf, table) = parse::short_string(buf)?;

                (
                    buf,
                    DbError::AlreadyExists {
                        keyspace: keyspace.to_owned(),
                        table: table.to_owned(),
                    },
                )
            }
            0x2500 => {
                let (buf, blob) = parse::short_bytes(buf)?;

                (
                    buf,
                    DbError::Unprepared {
                        statement_id: Bytes::from(blob.to_owned()),
                    },
                )
            }
            _ => (buf, DbError::Other(code)),
        };

        Ok((buf, Error { error, reason }))
    }
}

impl From<DbError> for Error {
    fn from(value: DbError) -> Self {
        let msg = format!("{value}");
        Error::new(value, msg)
    }
}

impl From<nom::Err<nom::error::Error<&str>>> for Error {
    fn from(value: nom::Err<nom::error::Error<&str>>) -> Self {
        tracing::error!(error = ?value, "Parsing error");
        Error::new(DbError::SyntaxError, value.to_string())
    }
}

impl From<nom::Err<nom::error::Error<&[u8]>>> for Error {
    fn from(value: nom::Err<nom::error::Error<&[u8]>>) -> Self {
        tracing::error!(error = ?value, "Parsing error");
        Error::new(DbError::ProtocolError, value.to_string())
    }
}
