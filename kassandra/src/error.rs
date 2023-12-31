use std::{convert::Infallible, str::FromStr};

use bytes::Bytes;
use derive_more::Display;
use thiserror::Error;

use crate::frame::consistency::LegacyConsistency;

/// An error sent from the database in response to a query
/// as described in the [specification](https://github.com/apache/cassandra/blob/5ed5e84613ef0e9664a774493db7d2604e3596e0/doc/native_protocol_v4.spec#L1029)\
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum DbError {
    /// The submitted query has a syntax error
    #[error("The submitted query has a syntax error")]
    SyntaxError,

    /// The query is syntactically correct but invalid
    #[error("The query is syntactically correct but invalid")]
    Invalid,

    /// Attempted to create a keyspace or a table that was already existing
    #[error(
        "Attempted to create a keyspace or a table that was already existing \
        (keyspace: {keyspace}, table: {table})"
    )]
    AlreadyExists {
        /// Created keyspace name or name of the keyspace in which table was created
        keyspace: String,
        /// Name of the table created, in case of keyspace creation it's an empty string
        table: String,
    },

    /// User defined function failed during execution
    #[error(
        "User defined function failed during execution \
        (keyspace: {keyspace}, function: {function}, arg_types: {arg_types:?})"
    )]
    FunctionFailure {
        /// Keyspace of the failed function
        keyspace: String,
        /// Name of the failed function
        function: String,
        /// Types of arguments passed to the function
        arg_types: Vec<String>,
    },

    /// Authentication failed - bad credentials
    #[error("Authentication failed - bad credentials")]
    AuthenticationError,

    /// The logged user doesn't have the right to perform the query
    #[error("The logged user doesn't have the right to perform the query")]
    Unauthorized,

    /// The query is invalid because of some configuration issue
    #[error("The query is invalid because of some configuration issue")]
    ConfigError,

    /// Not enough nodes are alive to satisfy required consistency level
    #[error(
        "Not enough nodes are alive to satisfy required consistency level \
        (consistency: {consistency}, required: {required}, alive: {alive})"
    )]
    Unavailable {
        /// Consistency level of the query
        consistency: LegacyConsistency,
        /// Number of nodes required to be alive to satisfy required consistency level
        required: i32,
        /// Found number of active nodes
        alive: i32,
    },

    /// The request cannot be processed because the coordinator node is overloaded
    #[error("The request cannot be processed because the coordinator node is overloaded")]
    Overloaded,

    /// The coordinator node is still bootstrapping
    #[error("The coordinator node is still bootstrapping")]
    IsBootstrapping,

    /// Error during truncate operation
    #[error("Error during truncate operation")]
    TruncateError,

    /// Not enough nodes responded to the read request in time to satisfy required consistency level
    #[error("Not enough nodes responded to the read request in time to satisfy required consistency level \
            (consistency: {consistency}, received: {received}, required: {required}, data_present: {data_present})")]
    ReadTimeout {
        /// Consistency level of the query
        consistency: LegacyConsistency,
        /// Number of nodes that responded to the read request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Replica that was asked for data has responded
        data_present: bool,
    },

    /// Not enough nodes responded to the write request in time to satisfy required consistency level
    #[error("Not enough nodes responded to the write request in time to satisfy required consistency level \
            (consistency: {consistency}, received: {received}, required: {required}, write_type: {write_type})")]
    WriteTimeout {
        /// Consistency level of the query
        consistency: LegacyConsistency,
        /// Number of nodes that responded to the write request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Type of write operation requested
        write_type: WriteType,
    },

    /// A non-timeout error during a read request
    #[error(
        "A non-timeout error during a read request \
        (consistency: {consistency}, received: {received}, required: {required}, \
        numfailures: {numfailures}, data_present: {data_present})"
    )]
    ReadFailure {
        /// Consistency level of the query
        consistency: LegacyConsistency,
        /// Number of nodes that responded to the read request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Number of nodes that experience a failure while executing the request
        numfailures: i32,
        /// Replica that was asked for data has responded
        data_present: bool,
    },

    /// A non-timeout error during a write request
    #[error(
        "A non-timeout error during a write request \
        (consistency: {consistency}, received: {received}, required: {required}, \
        numfailures: {numfailures}, write_type: {write_type}"
    )]
    WriteFailure {
        /// Consistency level of the query
        consistency: LegacyConsistency,
        /// Number of nodes that responded to the read request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Number of nodes that experience a failure while executing the request
        numfailures: i32,
        /// Type of write operation requested
        write_type: WriteType,
    },

    /// Tried to execute a prepared statement that is not prepared. Driver should prepare it again
    #[error(
    "Tried to execute a prepared statement that is not prepared. Driver should prepare it again"
    )]
    Unprepared {
        /// Statement id of the requested prepared query
        statement_id: Bytes,
    },

    /// Internal server error. This indicates a server-side bug
    #[error("Internal server error. This indicates a server-side bug")]
    ServerError,

    /// Invalid protocol message received from the driver
    #[error("Invalid protocol message received from the driver")]
    ProtocolError,

    /// Other error code not specified in the specification
    #[error("Other error not specified in the specification. Error code: {0}")]
    Other(i32),
}

impl DbError {
    pub fn code(&self) -> i32 {
        match self {
            DbError::ServerError => 0x0000,
            DbError::ProtocolError => 0x000A,
            DbError::AuthenticationError => 0x0100,
            DbError::Unavailable {
                consistency: _,
                required: _,
                alive: _,
            } => 0x1000,
            DbError::Overloaded => 0x1001,
            DbError::IsBootstrapping => 0x1002,
            DbError::TruncateError => 0x1003,
            DbError::WriteTimeout {
                consistency: _,
                received: _,
                required: _,
                write_type: _,
            } => 0x1100,
            DbError::ReadTimeout {
                consistency: _,
                received: _,
                required: _,
                data_present: _,
            } => 0x1200,
            DbError::ReadFailure {
                consistency: _,
                received: _,
                required: _,
                numfailures: _,
                data_present: _,
            } => 0x1300,
            DbError::FunctionFailure {
                keyspace: _,
                function: _,
                arg_types: _,
            } => 0x1400,
            DbError::WriteFailure {
                consistency: _,
                received: _,
                required: _,
                numfailures: _,
                write_type: _,
            } => 0x1500,
            DbError::SyntaxError => 0x2000,
            DbError::Unauthorized => 0x2100,
            DbError::Invalid => 0x2200,
            DbError::ConfigError => 0x2300,
            DbError::AlreadyExists {
                keyspace: _,
                table: _,
            } => 0x2400,
            DbError::Unprepared { statement_id: _ } => 0x2500,
            DbError::Other(code) => *code,
        }
    }
}

/// Type of write operation requested
#[derive(Debug, Clone, PartialEq, Eq, Display)]
pub enum WriteType {
    /// Non-batched non-counter write
    Simple,
    /// Logged batch write. If this type is received, it means the batch log has been successfully written
    /// (otherwise BatchLog type would be present)
    Batch,
    /// Unlogged batch. No batch log write has been attempted.
    UnloggedBatch,
    /// Counter write (batched or not)
    Counter,
    /// Timeout occurred during the write to the batch log when a logged batch was requested
    BatchLog,
    /// Timeout occurred during Compare And Set write/update
    Cas,
    /// Write involves VIEW update and failure to acquire local view(MV) lock for key within timeout
    View,
    /// Timeout occurred  when a cdc_total_space_in_mb is exceeded when doing a write to data tracked by cdc
    Cdc,
    /// Other type not specified in the specification
    Other(String),
}

impl WriteType {
    pub fn as_str(&self) -> &str {
        match self {
            WriteType::Simple => "SIMPLE",
            WriteType::Batch => "BATCH",
            WriteType::UnloggedBatch => "UNLOGGED_BATCH",
            WriteType::Counter => "COUNTER",
            WriteType::BatchLog => "BATCH_LOG",
            WriteType::Cas => "CAS",
            WriteType::View => "VIEW",
            WriteType::Cdc => "CDC",
            WriteType::Other(o) => o,
        }
    }
}

impl FromStr for WriteType {
    type Err = Infallible;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

/// Type of the operation rejected by rate limiting
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationType {
    Read,
    Write,
    Other(u8),
}
