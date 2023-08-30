use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use kassandra::{
    frame::{request::Request, request_stream, response::Response, response_sink},
    kassandra::Kassandra,
};
use stable_eyre::Result;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tracing::instrument;

mod logging;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Preload state from path
    #[arg(short, long, default_value_t = 9044)]
    port: u16,

    /// Preload state from path
    #[arg(short, long)]
    data: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    stable_eyre::install()?;
    logging::setup_telemetry("kassandra")?;
    let Args { port, data } = Args::parse();
    let state = data.map(std::fs::read).transpose()?;
    let kassandra = Kassandra::load_state(state.as_deref()).unwrap();
    let addr = format!("0.0.0.0:{port}");

    tracing::info!(%addr, "Starting kassandra node");
    Server::new(kassandra).serve(addr).await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct Server {
    kassandra: Arc<Mutex<Kassandra>>,
}

impl Server {
    fn new(kassandra: Kassandra) -> Self {
        Self {
            kassandra: Arc::new(Mutex::new(kassandra)),
        }
    }

    async fn serve(&mut self, addr: impl ToSocketAddrs) -> Result<()> {
        let listen = TcpListener::bind(addr).await?;

        {
            let s = self.clone();
            tokio::spawn(async move {
                match tokio::signal::ctrl_c().await {
                    Ok(()) => {
                        let state = { s.kassandra.lock().unwrap().data_snapshot() };
                        let state = serde_yaml::to_string(&state).unwrap();

                        tokio::fs::write("kass.data.yaml", state).await.unwrap();
                        std::process::exit(0);
                    }
                    Err(err) => {
                        eprintln!("Unable to listen for shutdown signal: {}", err);
                    }
                }
            });
        }
        loop {
            let Ok((stream, addr)) = listen.accept().await else {
                continue
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
                    let request = Request::deserialize(opcode, &data)?;
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

    #[instrument(skip(self))]
    fn request(&mut self, request: Request) -> Result<Response> {
        match request {
            Request::StartUp(options) => {
                tracing::trace!(?options, "Starting client");
                // todo check supported options
                Ok(Response::Ready)
            }
            Request::Options => Ok(Response::options()),
            Request::Query(query) => {
                let mut kass = self.kassandra.lock().unwrap();
                Ok(match kass.process(query) {
                    Ok(res) => Response::Result(res),
                    Err(er) => Response::Error(er),
                })
            }
            Request::Prepare(q) => {
                let mut kass = self.kassandra.lock().unwrap();
                Ok(match kass.prepare(q) {
                    Ok(res) => Response::Result(res),
                    Err(er) => Response::Error(er),
                })
            }
            Request::Execute(e) => {
                let mut kass = self.kassandra.lock().unwrap();
                Ok(match kass.execute(e) {
                    Ok(res) => Response::Result(res),
                    Err(er) => Response::Error(er),
                })
            }
            Request::Register { events } => {
                tracing::trace!(?events, "Client asked for events");

                Ok(Response::Ready)
            }
            Request::Batch(b) => {
                let mut kass = self.kassandra.lock().unwrap();
                Ok(match kass.process_batch(b) {
                    Ok(res) => Response::Result(res),
                    Err(er) => Response::Error(er),
                })
            }
            Request::AuthResponse => unimplemented!(),
        }
    }
}
