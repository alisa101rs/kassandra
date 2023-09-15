use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use strum::IntoStaticStr;

use crate::cql::schema::{ColumnType, Table};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Keyspace {
    pub name: String,
    pub strategy: Strategy,
    pub tables: HashMap<String, Table>,
    pub user_defined_types: HashMap<String, UserDefinedType>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, IntoStaticStr)]
#[allow(clippy::enum_variant_names)]
pub enum Strategy {
    SimpleStrategy {
        replication_factor: usize,
    },
    NetworkTopologyStrategy {
        // Replication factors of datacenters with given names
        datacenter_repfactors: HashMap<String, usize>,
    },
    LocalStrategy, // replication_factor == 1
    Other {
        name: String,
        data: HashMap<String, String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserDefinedType {
    pub name: String,
    pub keyspace: String,
    pub field_types: Vec<(String, ColumnType)>,
}
