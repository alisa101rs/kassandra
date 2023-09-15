use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{cql::query::QueryString, error::DbError, storage};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PersistedQueryCache {
    local: HashMap<u128, QueryString>,
}

impl PersistedQueryCache {
    pub fn store(
        &mut self,
        id: u128,
        query: QueryString,
        _storage: &mut impl storage::Storage,
    ) -> Result<(), DbError> {
        self.local.insert(id, query);
        // todo: insert in storage
        Ok(())
    }

    pub fn retrieve(
        &mut self,
        id: u128,
        _storage: &impl storage::Storage,
    ) -> Result<Option<QueryString>, DbError> {
        Ok(self.local.get(&id).cloned())
    }
}
