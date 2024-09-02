use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
};

use broadcast_sink::BroadcastSink;
use bytes::Bytes;
use clap::Parser;
use futures::{Sink, Stream};
use futures_util::SinkExt;
use kassandra::{
    frame::{
        raw_request_sink, raw_response_sink,
        request::{batch::BatchStatement, Request, RequestOpcode},
        request_stream,
        response::ResponseOpcode,
        response_stream, FrameParams,
    },
    session::KassandraSession,
};
use replay::ReplayInterceptor;
use stable_eyre::eyre::{self, Context};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::broadcast::{self, Receiver},
};
use translator::PreparedQueryTranslator;

mod broadcast_sink;
mod logging;
mod replay;
mod translator;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Port to listen connections for
    #[arg(short, long, default_value_t = 9044)]
    port: u16,

    /// Port of upstream cassandra
    #[arg(short, long, default_value_t = 9042)]
    upstream: u16,

    /// Preload state from path
    #[arg(short, long)]
    data: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    stable_eyre::install()?;
    logging::setup_telemetry("kassandra-proxy")?;

    let Args {
        port,
        upstream,
        data,
    } = Args::parse();

    let CassandraSniffer {
        mut requests,
        mut responses,
        translator,
    } = CassandraSniffer::new(format!("127.0.0.1:{port}"), format!("127.0.0.1:{upstream}"))?;
    let session: KassandraSession = if let Some(data) = data {
        let content = std::fs::read(&data).wrap_err("while reading initial state file")?;
        KassandraSession::load_state(&content)?
    } else {
        KassandraSession::new()
    };
    let mut replay = ReplayInterceptor::new(&session);

    loop {
        let (frame, op, payload) = requests.recv().await?;
        tracing::info!(?frame, ?op, ?payload, "Request");
        let response = responses.recv().await?;
        tracing::info!(frame = ?response.0, op = ?response.1, payload = ?response.2, "Response");
        if op == RequestOpcode::Prepare {
            replay.prepare_all(translator.read_all());
            continue;
        }

        replay_request(&mut replay, &translator, (frame, op, payload));
    }
}

type CassandraRequest = (FrameParams, RequestOpcode, Bytes);
type CassandraResponse = (FrameParams, ResponseOpcode, Bytes);

struct CassandraSniffer {
    requests: Receiver<CassandraRequest>,
    #[allow(dead_code)]
    responses: Receiver<CassandraResponse>,
    translator: PreparedQueryTranslator,
}

impl CassandraSniffer {
    fn new<S1, S2>(addr: S1, upstream: S2) -> eyre::Result<Self>
    where
        S1: ToSocketAddrs,
        S2: ToSocketAddrs,
    {
        let addr = addr.to_socket_addrs()?.next().unwrap();
        let upstream = upstream.to_socket_addrs()?.next().unwrap();

        let (rq, requests) = broadcast::channel(256);
        let (rs, responses) = broadcast::channel(256);
        tokio::spawn(cassandra_proxy(
            addr,
            upstream,
            BroadcastSink::new(rq),
            BroadcastSink::new(rs),
        ));
        let translator = PreparedQueryTranslator::new();

        tokio::spawn(translator::translation_loop(
            translator.clone(),
            requests.resubscribe(),
            responses.resubscribe(),
        ));

        Ok(Self {
            requests,
            responses,
            translator,
        })
    }
}

async fn cassandra_proxy(
    addr: SocketAddr,
    upstream: SocketAddr,
    requests: impl Sink<CassandraRequest, Error = eyre::Report> + Unpin + Send + Clone + 'static,
    responses: impl Sink<CassandraResponse, Error = eyre::Report> + Unpin + Send + Clone + 'static,
) -> eyre::Result<()> {
    let tcp = TcpListener::bind(addr).await?;
    tracing::info!(addr = %tcp.local_addr().unwrap(), "Listening for cassandra clients");
    loop {
        let Ok((mut client, a)) = tcp.accept().await else {
            continue;
        };
        tracing::info!(address = ?a, "Got a cassandra connection");
        let requests = requests.clone();
        let responses = responses.clone();
        tokio::spawn(async move {
            let mut service = TcpStream::connect(upstream).await?;
            tracing::info!("Connected to upstream cassandra");

            let (mut up_stream, up_sink) = cassandra_client_stream_sink(client.split());
            let (mut down_stream, down_sink) = cassandra_server_stream_sink(service.split());
            let mut request_sink = down_sink.fanout(requests);
            let mut response_sink = up_sink.fanout(responses);
            let (x, y) = tokio::join!(
                request_sink.send_all(&mut up_stream),
                response_sink.send_all(&mut down_stream)
            );
            if let Err(er) = x {
                tracing::error!(?er, "Error during proxying cassandra requests")
            }
            if let Err(er) = y {
                tracing::error!(?er, "Error during proxying cassandra responses")
            }

            Ok::<(), eyre::Report>(())
        });
    }
}

fn cassandra_client_stream_sink<'a>(
    (read, write): (impl AsyncRead + 'a, impl AsyncWrite + 'a),
) -> (
    impl Stream<Item = Result<CassandraRequest, eyre::Report>> + 'a,
    impl Sink<CassandraResponse, Error = eyre::Report> + 'a,
) {
    (request_stream(read), raw_response_sink(write))
}

fn cassandra_server_stream_sink<'a>(
    (read, write): (impl AsyncRead + 'a, impl AsyncWrite + 'a),
) -> (
    impl Stream<Item = Result<CassandraResponse, eyre::Report>> + 'a,
    impl Sink<CassandraRequest, Error = eyre::Report> + 'a,
) {
    (response_stream(read), raw_request_sink(write))
}

fn replay_request(
    replay: &mut ReplayInterceptor,
    translator: &PreparedQueryTranslator,
    request: CassandraRequest,
) {
    let (frame, opcode, b) = request;
    let request = Request::deserialize(opcode, b.as_ref(), frame.flags).unwrap();

    let mut queries = vec![];
    match request {
        Request::Query(q) => {
            queries.push(q.query.to_string());
            replay.process(q);
        }
        Request::Execute(ex) => {
            let id = ex.id;
            let translated = translator.translate(id).ok();
            if let Some(q) = translated {
                queries.push(q.to_string())
            } else {
                tracing::warn!(query = ?ex, "Untranslated query")
            }
            replay.execute(ex);
        }
        Request::Batch(batch) => {
            for statement in &batch.statements {
                let translated = match statement {
                    BatchStatement::Prepared { id, .. } => translator.translate(id).ok(),
                    BatchStatement::Query { query, .. } => Some(query.clone()),
                };
                if let Some(q) = translated {
                    queries.push(q.to_string())
                } else {
                    tracing::warn!(query = ?statement, ?batch, "Untranslated query from the batch")
                }
            }
            replay.process_batch(batch);
        }
        _ => {}
    }

    tracing::info!(?queries, "Intercepted queries");
}
