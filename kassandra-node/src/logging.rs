use std::time::Duration;

use opentelemetry::{
    runtime::Tokio,
    sdk::{
        propagation::TraceContextPropagator,
        trace,
        trace::{RandomIdGenerator, Sampler},
        Resource,
    },
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use stable_eyre::eyre;
use tracing_error::ErrorLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

pub fn setup_telemetry(service: impl ToString) -> eyre::Result<()> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(std::io::stderr);

    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("debug"))?;

    let mut layers = vec![
        fmt_layer.boxed(),
        filter_layer.boxed(),
        ErrorLayer::default().boxed(),
    ];

    if let Ok(endpoint) = std::env::var("OTLP_EXPORTER_ENDPOINT") {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(endpoint)
                    .with_timeout(Duration::from_secs(3)),
            )
            .with_trace_config(
                trace::config()
                    .with_sampler(Sampler::AlwaysOn)
                    .with_id_generator(RandomIdGenerator::default())
                    .with_max_events_per_span(64)
                    .with_max_attributes_per_span(32)
                    .with_max_events_per_span(16)
                    .with_max_attributes_per_link(512)
                    .with_resource(Resource::new(vec![KeyValue::new(
                        "service.name",
                        service.to_string(),
                    )])),
            )
            .install_batch(Tokio)?;

        layers.push(tracing_opentelemetry::layer().with_tracer(tracer).boxed())
    }

    tracing_subscriber::registry().with(layers).try_init()?;

    Ok(())
}
