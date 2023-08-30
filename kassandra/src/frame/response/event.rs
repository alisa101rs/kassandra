use std::net::SocketAddr;

use bytes::BufMut;

use crate::frame::write;

#[derive(Debug)]
pub enum Event {
    TopologyChange(TopologyChangeEvent),
    StatusChange(StatusChangeEvent),
    SchemaChange(SchemaChangeEvent),
}

#[derive(Debug)]
pub enum TopologyChangeEvent {
    NewNode(SocketAddr),
    RemovedNode(SocketAddr),
}

#[derive(Debug)]
pub enum StatusChangeEvent {
    Up(SocketAddr),
    Down(SocketAddr),
}

#[derive(Debug)]
pub enum SchemaChangeEvent {
    KeyspaceChange {
        change_type: SchemaChangeType,
        keyspace_name: String,
    },
    TableChange {
        change_type: SchemaChangeType,
        keyspace_name: String,
        object_name: String,
    },
    TypeChange {
        change_type: SchemaChangeType,
        keyspace_name: String,
        type_name: String,
    },
    FunctionChange {
        change_type: SchemaChangeType,
        keyspace_name: String,
        function_name: String,
        arguments: Vec<String>,
    },
    AggregateChange {
        change_type: SchemaChangeType,
        keyspace_name: String,
        aggregate_name: String,
        arguments: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaChangeType {
    Created,
    Updated,
    Dropped,
    Invalid,
}

impl SchemaChangeType {
    pub(crate) fn write(&self, buf: &mut impl BufMut) {
        match self {
            SchemaChangeType::Created => write::string(buf, "CREATED"),
            SchemaChangeType::Updated => write::string(buf, "UPDATED"),
            SchemaChangeType::Dropped => write::string(buf, "DROPPED"),
            SchemaChangeType::Invalid => write::string(buf, "INVALID"),
        }
    }
}
