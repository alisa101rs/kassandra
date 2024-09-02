use kassandra::{
    cql::query::QueryString,
    frame::request::{batch::Batch, execute::Execute, query::Query},
    session::KassandraSession,
    snapshot::DataSnapshots,
};

#[derive(Clone)]
pub struct ReplayInterceptor {
    session: KassandraSession,
}

impl ReplayInterceptor {
    pub fn new(state: &KassandraSession) -> Self {
        Self {
            session: state.clone(),
        }
    }

    pub fn prepare_all(&mut self, prepare: impl Iterator<Item = (u128, QueryString)>) {
        for (id, query) in prepare {
            if let Err(error) = self.session.prepare_with_id(query.clone(), id) {
                tracing::error!(?error, query = query.to_string(), "Could not prepare query");
            }
        }
    }

    pub fn process(&mut self, query: Query<'_>) {
        if let Err(error) = self.session.process(query) {
            tracing::error!(?error, "Error while replaying query");
        }
    }

    pub fn execute(&mut self, query: Execute<'_>) {
        if let Err(error) = self.session.execute(query) {
            tracing::error!(?error, "Error while replaying prepared query");
        }
    }

    pub fn process_batch(&mut self, batch: Batch<'_>) {
        if let Err(error) = self.session.process_batch(batch) {
            tracing::error!(?error, "Error while replaying batch");
        }
    }

    pub fn snapshot(&self) -> DataSnapshots {
        self.session.data_snapshot()
    }
}
