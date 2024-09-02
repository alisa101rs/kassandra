use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use kassandra::{
    cql::query::QueryString,
    frame::{
        parse,
        request::{Request, RequestOpcode},
        response::ResponseOpcode,
    },
};
use parking_lot::Mutex;
use stable_eyre::eyre::{self, eyre};
use tokio::sync::broadcast::Receiver;

use super::{CassandraRequest, CassandraResponse};

#[derive(Clone)]
pub struct PreparedQueryTranslator {
    cache: Arc<Mutex<HashMap<u128, QueryString>>>,
}

impl PreparedQueryTranslator {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::default())),
        }
    }
    pub fn translate(&self, id: &[u8]) -> eyre::Result<QueryString> {
        let id = u128::from_be_bytes(id.try_into()?);
        let cache = self.cache.lock();

        cache
            .get(&id)
            .cloned()
            .ok_or(eyre!("Could not translate prepared query"))
    }

    pub fn insert(&self, id: u128, query: QueryString) {
        self.cache.lock().insert(id, query);
    }

    pub fn read_all(&self) -> impl Iterator<Item = (u128, QueryString)> {
        self.cache.lock().clone().into_iter()
    }
}

pub async fn translation_loop(
    translator: PreparedQueryTranslator,
    mut requests: Receiver<CassandraRequest>,
    mut responses: Receiver<CassandraResponse>,
) {
    // very bold assumption that they will come in order
    let mut in_preparation: VecDeque<(i16, QueryString)> = VecDeque::new();

    loop {
        tokio::select! {
            Ok((frame, opcode, body)) = requests.recv() => {
                if opcode != RequestOpcode::Prepare {
                    continue;
                }

                let Request::Prepare(query) = Request::deserialize(opcode, body.as_ref(), frame.flags).unwrap() else {
                    unreachable!("opcode was Prepare")
                };

                tracing::debug!(?query, "Intercepted preparation request");
                in_preparation.push_back((frame.stream, query));
            }
            Ok((frame, opcode, mut body)) = responses.recv() => {
                use bytes::buf::Buf;
                if opcode != ResponseOpcode::Result {
                    continue;
                }

                // Prepared
                if body.get_u32() != 0x0004 {
                    continue;
                }

                let (_, id) = parse::short_bytes(body.as_ref()).unwrap();
                let id = u128::from_be_bytes(id.try_into().unwrap());


                let Some(pos) = in_preparation.iter().position(|(s, _)| *s == frame.stream) else {
                    unimplemented!("Got Result::Prepared, without having Request::Prepare")
                };
                let (_, query) = in_preparation.swap_remove_front(pos).unwrap();


                tracing::debug!(?id, ?query, "Intercepted preparation result, associating with query");

                translator.insert(id, query);
            }
            else => {}
        }
    }
}
