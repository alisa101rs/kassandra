use std::{
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_util::{SinkExt, StreamExt};
pub use kassandra;
use kassandra::{
    error::DbError,
    frame::{
        request::Request,
        request_stream,
        response::{error::Error, Response},
        response_sink,
    },
    session::KassandraSession,
};
use tokio::{
    net::{TcpListener, TcpStream},
    select, task,
};

#[derive(Debug, Clone)]
pub struct KassandraTester {
    kassandra: Arc<Mutex<KassandraSession>>,
}

impl KassandraTester {
    pub fn new(kassandra: KassandraSession) -> Self {
        Self {
            kassandra: Arc::new(Mutex::new(kassandra)),
        }
    }

    pub async fn in_scope<F, Fut, E>(mut self, mut block: F) -> Result<KassandraSession, E>
    where
        F: FnMut(SocketAddr) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        select! {
            _ = self.serve(listener) => {},
            _ = block(addr) => {}
        }

        let kassandra = Arc::try_unwrap(self.kassandra)
            .expect("Some clients still alive")
            .into_inner()
            .expect("Mutex is poisoned");

        Ok(kassandra)
    }

    async fn serve(&mut self, listener: TcpListener) {
        let tasks = task::LocalSet::new();
        tasks
            .run_until(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        continue;
                    };
                    task::spawn_local(self.clone().client(stream));
                }
            })
            .await;
    }

    async fn client(mut self, mut stream: TcpStream) {
        let (mut read, mut write) = stream.split();
        let mut stream = request_stream(&mut read);
        let mut sink = response_sink(&mut write);

        while let Some(frame) = stream.next().await {
            match frame {
                Ok((frame, opcode, data)) => {
                    let request = match Request::deserialize(opcode, &data) {
                        Ok(req) => req,
                        Err(er) => {
                            tracing::error!(
                                ?er,
                                ?frame,
                                ?opcode,
                                data = ?data.as_ref(),
                                "Error trying deserialize request"
                            );
                            let _ = sink
                                .send((
                                    Response::Error(Error::new(
                                        DbError::ProtocolError,
                                        "Error parsing request",
                                    )),
                                    frame.stream,
                                ))
                                .await;
                            continue;
                        }
                    };

                    let response = self.request(request);
                    let _ = sink.send((response, frame.stream)).await;
                }
                Err(er) => {
                    tracing::error!(?er, "Could not read frame");
                    break;
                }
            }
        }
    }

    fn request(&mut self, request: Request) -> Response {
        match request {
            Request::StartUp(_options) => Response::Ready,
            Request::Options => Response::options(),
            Request::Query(query) => {
                let mut kass = self.kassandra.lock().unwrap();
                match kass.process(query) {
                    Ok(res) => Response::Result(res),
                    Err(er) => Response::Error(er),
                }
            }
            Request::Prepare(prep) => {
                let mut kass = self.kassandra.lock().unwrap();
                match kass.prepare(prep) {
                    Ok(res) => Response::Result(res),
                    Err(er) => Response::Error(er),
                }
            }
            Request::Execute(execute) => {
                let mut kass = self.kassandra.lock().unwrap();
                match kass.execute(execute) {
                    Ok(res) => Response::Result(res),
                    Err(er) => Response::Error(er),
                }
            }
            Request::Register { events: _ } => Response::Ready,
            Request::Batch(b) => {
                let mut kass = self.kassandra.lock().unwrap();
                match kass.process_batch(b) {
                    Ok(res) => Response::Result(res),
                    Err(er) => Response::Error(er),
                }
            }
            Request::AuthResponse => unimplemented!(),
        }
    }
}
