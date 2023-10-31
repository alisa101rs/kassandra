use bitflags::bitflags;
use bytes::{BufMut, Bytes};
use nom::AsBytes;
use serde::Serialize;

use crate::{
    cql::{column::ColumnType, value::CqlValue},
    frame::{response::event::SchemaChangeEvent, write},
};

#[derive(Debug)]
pub enum QueryResult {
    Void,
    Rows(Rows),
    SetKeyspace(SetKeyspace),
    Prepared(Prepared),
    SchemaChange(SchemaChange),
}

impl QueryResult {
    pub fn serialize(&self, buf: &mut impl BufMut) -> eyre::Result<()> {
        match self {
            QueryResult::Void => {
                buf.put_i32(0x0001);
            }
            QueryResult::Rows(rows) => {
                buf.put_i32(0x0002);
                rows.serialize(buf);
            }
            QueryResult::SetKeyspace(set) => {
                buf.put_i32(0x0003);
                write::string(buf, &set.keyspace_name);
            }
            QueryResult::Prepared(prepared) => {
                buf.put_i32(0x0004);
                prepared.serialize(buf);
            }
            QueryResult::SchemaChange(schema) => {
                buf.put_i32(0x0005);
                schema.serialize(buf)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct SetKeyspace {
    pub keyspace_name: String,
}

#[derive(Debug)]
pub struct Prepared {
    pub id: u128,
    pub prepared_metadata: PreparedMetadata,
    pub result_metadata: ResultMetadata,
}

impl Prepared {
    pub fn serialize(&self, buf: &mut impl BufMut) {
        write::short_bytes(buf, &self.id.to_be_bytes());
        self.prepared_metadata.serialize(buf);
        self.result_metadata.serialize(buf);
    }
}

#[derive(Debug)]
pub struct SchemaChange {
    pub event: SchemaChangeEvent,
}

impl SchemaChange {
    pub fn serialize(&self, buf: &mut impl BufMut) -> eyre::Result<()> {
        match self.event {
            SchemaChangeEvent::KeyspaceChange {
                change_type,
                ref keyspace_name,
            } => {
                change_type.write(buf);
                write::string(buf, "KEYSPACE");
                write::string(buf, keyspace_name);
            }
            SchemaChangeEvent::TableChange {
                change_type,
                ref keyspace_name,
                ref object_name,
            } => {
                change_type.write(buf);
                write::string(buf, "TABLE");
                write::string(buf, keyspace_name);
                write::string(buf, object_name);
            }
            SchemaChangeEvent::TypeChange { .. } => {
                unimplemented!()
            }
            SchemaChangeEvent::FunctionChange { .. } => {
                unimplemented!()
            }
            SchemaChangeEvent::AggregateChange { .. } => {
                unimplemented!()
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TableSpec {
    pub ks_name: String,
    pub table_name: String,
}

impl TableSpec {
    fn serialize(&self, buf: &mut impl BufMut) {
        write::string(buf, &self.ks_name);
        write::string(buf, &self.table_name);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnSpec {
    pub table_spec: Option<TableSpec>,
    pub name: String,
    pub typ: ColumnType,
}

impl ColumnSpec {
    pub fn new(name: impl Into<String>, typ: ColumnType) -> Self {
        Self {
            table_spec: None,
            name: name.into(),
            typ,
        }
    }
}

bitflags! {
    pub struct ResultMetadataFlags: u32 {
        const GLOBAL_TABLES_SPEC = 0x1;
        const HAS_MORE_PAGES = 0x2;
        const NO_METADATA = 0x4;
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct ResultMetadata {
    pub global_spec: Option<TableSpec>,
    pub paging_state: Option<Bytes>,
    pub col_specs: Vec<ColumnSpec>,
}

impl ResultMetadata {
    pub fn empty() -> ResultMetadata {
        ResultMetadata::default()
    }

    pub fn serialize(&self, buf: &mut impl BufMut) {
        let mut flags = ResultMetadataFlags::empty();

        if self.paging_state.is_some() {
            flags |= ResultMetadataFlags::HAS_MORE_PAGES
        }

        if self.col_specs.is_empty() {
            flags |= ResultMetadataFlags::NO_METADATA;

            buf.put_u32(flags.bits());
            buf.put_u32(0);

            if let Some(bytes) = &self.paging_state {
                write::bytes(buf, bytes.as_bytes());
            }

            return;
        }

        if self.global_spec.is_some() {
            flags |= ResultMetadataFlags::GLOBAL_TABLES_SPEC;
        }

        buf.put_u32(flags.bits());
        buf.put_u32(self.col_specs.len() as u32);

        if let Some(bytes) = &self.paging_state {
            write::bytes(buf, bytes.as_bytes());
        }

        if let Some(spec) = &self.global_spec {
            spec.serialize(buf);
        }

        for spec in &self.col_specs {
            if self.global_spec.is_none() {
                spec.table_spec
                    .as_ref()
                    .expect("global spec was none but column spec is also none")
                    .serialize(buf);
            }

            write::string(buf, &spec.name);
            write::r#type(buf, &spec.typ);
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PartitionKeyIndex {
    /// index in the serialized values
    pub index: u16,
    /// sequence number in partition key
    pub sequence: u16,
}

#[derive(Debug, Clone)]
pub struct PreparedMetadata {
    /// bind markers count
    //pub col_count: usize,
    /// pk_indexes are sorted by `index` and can be reordered in partition key order
    /// using `sequence` field
    pub pk_indexes: Vec<PartitionKeyIndex>,
    pub global_spec: Option<TableSpec>,
    pub col_specs: Vec<ColumnSpec>,
}

impl PreparedMetadata {
    pub fn serialize(&self, buf: &mut impl BufMut) {
        let flag = if self.global_spec.is_some() {
            // Global_tables_spec
            1
        } else {
            0
        };

        buf.put_i32(flag);
        buf.put_u32(self.col_specs.len() as _);
        buf.put_u32(self.pk_indexes.len() as _);
        for index in &self.pk_indexes {
            buf.put_u16(index.index);
        }
        if let Some(spec) = &self.global_spec {
            spec.serialize(buf);
        }

        for spec in &self.col_specs {
            if self.global_spec.is_none() {
                spec.table_spec
                    .as_ref()
                    .expect("global spec was none but column spec is also none")
                    .serialize(buf);
            }

            write::string(buf, &spec.name);
            write::r#type(buf, &spec.typ);
        }
    }
}

#[derive(Debug, Default, PartialEq, Serialize)]
pub struct Row {
    pub columns: Vec<Option<CqlValue>>,
}

impl Row {
    pub fn new() -> Self {
        Self { columns: vec![] }
    }

    pub fn push(&mut self, v: Option<CqlValue>) {
        self.columns.push(v);
    }

    pub fn serialize(&self, buf: &mut impl BufMut) {
        for column in &self.columns {
            write::opt_cql_value(buf, column.as_ref());
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Rows {
    pub metadata: ResultMetadata,
    pub rows: Vec<Row>,
}

impl Rows {
    pub fn serialize(&self, buf: &mut impl BufMut) {
        self.metadata.serialize(buf);

        // rows_count serialization
        buf.put_u32(self.rows.len() as _);
        for row in &self.rows {
            row.serialize(buf);
        }
    }
}
