//! Live observation and optional HTTP composition.
use super::*;

pub(super) async fn observe(live: Live, capture: Option<CaptureConfig>) -> Result<(), Error> {
    let placement = live.ingestion.placement()?;
    let measurements = live.ingestion.settings()?;
    eprintln!(
        "ingestion: {placement:?}; detailed traces 1/{}",
        live.sample_every
    );
    let metadata = metadata(&live.metadata_url).await?;
    let markets: Vec<_> = live.markets.into_iter().map(MarketId).collect();
    let (origin, output) = runner::channel(&metadata, &markets, "live")?;
    let mut updates = output.subscribe();
    let mut feed = FeedConfig::public(markets);
    feed.read_buffer_bytes = live.ingestion.read_buffer_bytes as usize;
    feed.receive = live.ingestion.receive();
    feed.url = live.websocket_url;
    feed.delivery_sample_every =
        if live.ingestion.no_delivery_timing || !cfg!(feature = "delivery-traces") {
            0
        } else {
            live.sample_every
        };
    let config = RunConfig {
        measurements,
        feed,
        metadata,
        capture,
        receive_sidecar: live.ingestion.sidecar(),
        duration: (live.seconds != 0).then(|| Duration::from_secs(live.seconds)),
        depth: live.depth as usize,
        quantity: Quantity(live.quantity),
        sample_every: live.sample_every,
        inline_delivery: live.ingestion.delivery == "inline",
        queue_drain: usize::from(live.ingestion.queue_drain),
        report_on_ingestion: false,
    };
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = stop.send(true);
        }
    });
    let printer = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            match updates.has_changed() {
                Err(_) => break,
                Ok(false) => continue,
                Ok(true) => {}
            }
            let view = updates.borrow_and_update().clone();
            println!("{}", headless_json(&view).expect("report serialization"));
        }
    });
    let result = placement::run(
        placement,
        runner::live(config, origin, output.clone(), receiver),
    )
    .await;
    printer.abort();
    signal.abort();
    println!("{}", headless_json(&output.borrow())?);
    eprintln!("final state digest: {}", result?);
    Ok(())
}

fn headless_json(report: &runner::Published) -> Result<String, serde_json::Error> {
    let mut report = report.clone();
    report.view.latency.traces.clear();
    serde_json::to_string(&report)
}

pub(super) async fn serve(config: Serve) -> Result<(), Error> {
    let placement = config.ingestion.placement()?;
    let measurements = config.ingestion.settings()?;
    if config.ingestion.dedicated_ingestion && (config.fixture || config.replay.is_some()) {
        return Err("--dedicated-ingestion applies only to live observation".into());
    }
    eprintln!(
        "ingestion: {placement:?}; detailed traces 1/{}",
        config.sample_every
    );
    let replay = config
        .replay
        .as_ref()
        .map(|path| Replay::open(path, 1024 * 1024 * 1024))
        .transpose()?;
    let (metadata, markets, mode) = if config.fixture {
        (
            runner::offline::fixture_metadata()?,
            vec![MarketId(1)],
            "fixture replay",
        )
    } else if let Some(input) = replay.as_ref() {
        (
            input.manifest().metadata.clone(),
            input.manifest().markets.clone(),
            "capture replay",
        )
    } else {
        (
            metadata(&config.metadata_url).await?,
            config.markets.into_iter().map(MarketId).collect(),
            "live",
        )
    };
    let (origin, output) = runner::channel(&metadata, &markets, mode)?;
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let signal_stop = stop.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = signal_stop.send(true);
        }
    });
    let http = tokio::spawn(pax_demo::presentation::serve_with_settings(
        config.bind,
        origin,
        output.subscribe(),
        receiver.clone(),
        config
            .route_diagnostics
            .then(|| pax_demo::measurement::network::Config {
                endpoint: config.websocket_url.clone(),
                method: if config.route_method == "tcp" {
                    pax_demo::measurement::network::Method::Tcp
                } else {
                    pax_demo::measurement::network::Method::Udp
                },
            }),
        measurements,
    ));
    let source = tokio::spawn(async move {
        if config.fixture {
            runner::offline::fixture_with_settings(
                origin,
                output,
                receiver,
                Quantity(config.quantity),
                config.sample_every,
                measurements,
            )
            .await
        } else if let Some(replay) = replay {
            runner::offline::replay_with_settings(
                replay,
                origin,
                output,
                receiver,
                Quantity(config.quantity),
                config.sample_every,
                measurements,
            )
            .await
        } else {
            let mut feed = FeedConfig::public(markets);
            feed.read_buffer_bytes = config.ingestion.read_buffer_bytes as usize;
            feed.receive = config.ingestion.receive();
            feed.url = config.websocket_url;
            feed.delivery_sample_every =
                if config.ingestion.no_delivery_timing || !cfg!(feature = "delivery-traces") {
                    0
                } else {
                    config.sample_every
                };
            placement::run(
                placement,
                runner::live(
                    RunConfig {
                        measurements,
                        feed,
                        metadata,
                        capture: None,
                        receive_sidecar: config.ingestion.sidecar(),
                        duration: None,
                        depth: 20,
                        quantity: Quantity(config.quantity),
                        sample_every: config.sample_every,
                        inline_delivery: config.ingestion.delivery == "inline",
                        queue_drain: usize::from(config.ingestion.queue_drain),
                        report_on_ingestion: false,
                    },
                    origin,
                    output,
                    receiver,
                ),
            )
            .await
            .map(|_| ())
        }
    });
    let result = shutdown::supervise(http, source, stop, Duration::from_secs(5)).await;
    signal.abort();
    result
}
