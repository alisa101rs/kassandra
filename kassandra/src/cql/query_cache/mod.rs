use crate::{cql::query::QueryString, error::DbError};

mod persisted;

pub use persisted::PersistedQueryCache;

pub trait QueryCache {
    fn store(&mut self, id: u128, query: QueryString) -> Result<(), DbError>;

    fn retrieve(&mut self, id: u128) -> Result<Option<QueryString>, DbError>;
}
