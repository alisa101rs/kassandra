use std::{
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use kassandra::{
    frame::{request::Request, request_stream, response::Response, response_sink},
    KassandraSession,
};
use stable_eyre::{eyre::Context, Result};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

mod logging;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Port to listen connections for
    #[arg(short, long, default_value_t = 9044)]
    port: u16,

    /// Preload state from path
    #[arg(short, long, default_value = "./kass.data.ron")]
    data: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    stable_eyre::install()?;
    logging::setup_telemetry("kassandra")?;
    let Args { port, data } = Args::parse();

    let state = std::fs::read(&data)
        .map(Some)
        .or_else(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(err)
            }
        })
        .context("reading state")?;

    let kassandra = state
        .map(|it| KassandraSession::load_state(&it))
        .transpose()?
        .unwrap_or(KassandraSession::new());
    let addr = format!("0.0.0.0:{port}");

    tracing::info!(%addr, "Starting kassandra node");
    let server = Server::new(kassandra);

    tokio::select! {
        _ = Server::serve(server.clone(), addr) => {},
        _ = tokio::signal::ctrl_c() => {
            tracing::info!(output.path = %data.display(), "Received SIG_TERM, saving state and closing server");
            let kassandra = server.kassandra.lock().unwrap();
            let state = kassandra.save_state();
            std::fs::write(&data, state).context("saving state")?;
        }
    }

    Ok(())
}

macro_rules! span {
    ($name: tt) => {
        tracing::info_span!(
            $name,
            error = Empty,
            db.system = "cassandra",
            span.kind = "server"
        )
    };
}

#[derive(Clone, Debug)]
struct Server {
    kassandra: Arc<Mutex<KassandraSession>>,
}

impl Server {
    fn new(kassandra: KassandraSession) -> Self {
        Self {
            kassandra: Arc::new(Mutex::new(kassandra)),
        }
    }

    async fn serve(self, addr: impl ToSocketAddrs) -> Result<()> {
        let listen = TcpListener::bind(addr).await?;

        loop {
            let Ok((stream, addr)) = listen.accept().await else {
                continue;
            };
            tracing::info!(%addr, "New client");

            tokio::task::spawn(self.clone().client(stream));
        }
    }

    async fn client(mut self, mut stream: TcpStream) -> Result<()> {
        let (mut read, mut write) = stream.split();
        let mut stream = request_stream(&mut read);
        let mut sink = response_sink(&mut write);
        while let Some(frame) = stream.next().await {
            match frame {
                Ok((frame, opcode, data)) => {
                    tracing::debug!(?frame, ?opcode, data.len = data.len(), "New message");
                    if frame.version.is_unsupported() {
                        sink.send((Response::unsupported_version(), frame.stream))
                            .await?;
                        continue;
                    }

                    let request = Request::deserialize(opcode, &data, frame.flags)?;
                    let response = self.request(request)?;
                    sink.send((response, frame.stream)).await?;
                }
                Err(er) => {
                    tracing::error!(?er, "Could not read frame");
                    break;
                }
            }
        }

        Ok(())
    }

    fn request(&mut self, request: Request) -> Result<Response> {
        use tracing::field::Empty;
        match request {
            Request::StartUp(options) => {
                let span = span!("StartUp");
                let _span = span.enter();
                tracing::trace!(?options, "Starting client");
                Ok(Response::Ready)
            }
            Request::Options => {
                let span = span!("Options");
                let _span = span.enter();
                Ok(Response::options())
            }
            Request::Query(query) => {
                let span = span!("Query");
                let _span = span.enter();
                let mut kass = self.kassandra.lock().unwrap();
                Ok(match kass.process(query) {
                    Ok(res) => Response::Result(res),
                    Err(er) => {
                        span.record("error", true);
                        Response::Error(er)
                    }
                })
            }
            Request::Prepare(q) => {
                let span = span!("Prepare");
                let _span = span.enter();
                let mut kass = self.kassandra.lock().unwrap();
                Ok(match kass.prepare(q) {
                    Ok(res) => Response::Result(res),
                    Err(er) => {
                        span.record("error", true);
                        Response::Error(er)
                    }
                })
            }
            Request::Execute(e) => {
                let span = span!("Execute");
                let _span = span.enter();
                let mut kass = self.kassandra.lock().unwrap();
                Ok(match kass.execute(e) {
                    Ok(res) => Response::Result(res),
                    Err(er) => {
                        span.record("error", true);
                        Response::Error(er)
                    }
                })
            }
            Request::Register { events } => {
                let span = span!("Register");
                let _span = span.enter();
                tracing::trace!(?events, "Client asked for events");

                Ok(Response::Ready)
            }
            Request::Batch(b) => {
                let span = span!("Batch");
                let _span = span.enter();
                let mut kass = self.kassandra.lock().unwrap();
                Ok(match kass.process_batch(b) {
                    Ok(res) => Response::Result(res),
                    Err(er) => {
                        span.record("error", true);
                        Response::Error(er)
                    }
                })
            }
            Request::AuthResponse => unimplemented!(),
        }
    }
}
