//! HTTP owns no mutable books. Slow readers retain one immutable snapshot each.
use crate::{
    pipeline::ns,
    runner::{Error, Published},
};
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderValue, Response, header},
    response::Html,
    routing::get,
};
use bytes::Bytes;
use std::{net::SocketAddr, sync::Arc, time::Instant};
use tokio::sync::watch;

#[derive(Clone)]
struct App {
    display: crate::measurement::settings::Settings,
    latest: watch::Receiver<Bytes>,
    reports: watch::Receiver<Arc<Published>>,
    origin: Instant,
    route: watch::Receiver<Option<crate::measurement::network::Route>>,
}
/// Serve both demo pages and read-only APIs, until the shared stop signal.
/// Encoding runs on a separate coalescing task and is performed once per update.
pub async fn serve(
    bind: SocketAddr,
    origin: Instant,
    reports: watch::Receiver<Arc<Published>>,
    stop: watch::Receiver<bool>,
    route_endpoint: Option<crate::measurement::network::Config>,
) -> Result<(), Error> {
    serve_with_settings(
        bind,
        origin,
        reports,
        stop,
        route_endpoint,
        Default::default(),
    )
    .await
}
/// Serve with independent browser polling, browser measurement and route cadence.
/// Invalid settings fail before binding or starting probe tasks.
pub async fn serve_with_settings(
    bind: SocketAddr,
    origin: Instant,
    reports: watch::Receiver<Arc<Published>>,
    stop: watch::Receiver<bool>,
    route_endpoint: Option<crate::measurement::network::Config>,
    settings: crate::measurement::settings::Settings,
) -> Result<(), Error> {
    settings.validate()?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    serve_listener(listener, origin, reports, stop, route_endpoint, settings).await
}
/// Serve a caller-bound socket (including an ephemeral port) with the same routes
/// and shutdown contract as [`serve_with_settings`]. Binding errors belong to the caller.
pub async fn serve_listener(
    listener: tokio::net::TcpListener,
    origin: Instant,
    mut reports: watch::Receiver<Arc<Published>>,
    mut stop: watch::Receiver<bool>,
    route_endpoint: Option<crate::measurement::network::Config>,
    settings: crate::measurement::settings::Settings,
) -> Result<(), Error> {
    settings.validate()?;
    let mut publications = super::publication::Publications::default();
    let initial = encode(&reports.borrow(), origin, &mut publications)?;
    let (encoded, latest) = watch::channel(initial);
    let (route_tx, route_rx) = watch::channel(None);
    let diagnostic = route_endpoint
        .filter(|_| settings.route_seconds != 0)
        .map(|endpoint| {
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(settings.route_seconds));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    route_tx.send_replace(Some(
                        crate::measurement::network::probe_with_method(
                            &endpoint.endpoint,
                            endpoint.method,
                        )
                        .await,
                    ));
                }
            })
        });
    let app = App {
        display: crate::measurement::settings::Settings {
            route_seconds: if diagnostic.is_some() {
                settings.route_seconds
            } else {
                0
            },
            ..settings
        },
        latest,
        reports: reports.clone(),
        origin,
        route: route_rx,
    };
    let encoder = tokio::spawn(async move {
        while reports.changed().await.is_ok() {
            let report = reports.borrow_and_update().clone();
            match encode(&report, origin, &mut publications) {
                Ok(bytes) => {
                    encoded.send_replace(bytes);
                }
                Err(error) => eprintln!("presentation encoding: {error}"),
            }
        }
    });
    let router = Router::new()
        .route("/", get(orderbook))
        .route("/latency", get(latency))
        .route("/polling.mjs", get(polling_javascript))
        .route("/book.mjs", get(book_javascript))
        .route("/elements.mjs", get(elements_javascript))
        .route("/primary.mjs", get(primary_javascript))
        .route("/details.mjs", get(details_javascript))
        .route("/diagnostics.mjs", get(diagnostics_javascript))
        .route("/recovery.mjs", get(recovery_javascript))
        .route("/app.js", get(javascript))
        .route("/timing.mjs", get(timing))
        .route("/ingress.mjs", get(ingress))
        .route("/history.mjs", get(history))
        .route("/style.css", get(stylesheet))
        .route("/api/state", get(state))
        .route("/api/clock", get(clock))
        .route("/api/diagnostics", get(diagnostics))
        .route("/api/route", get(route))
        .route(
            "/favicon.ico",
            get(|| async { axum::http::StatusCode::NO_CONTENT }),
        )
        .with_state(app);
    eprintln!(
        "demo: http://{} (orderbook), /latency (timing)",
        listener.local_addr()?
    );
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            while !*stop.borrow() {
                if stop.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
    encoder.abort();
    if let Some(task) = diagnostic {
        task.abort();
    }
    result?;
    Ok(())
}
fn encode(
    report: &Published,
    origin: Instant,
    publications: &mut super::publication::Publications,
) -> Result<Bytes, serde_json::Error> {
    let started = Instant::now();
    let mut bytes = Vec::with_capacity(32_768);
    bytes.extend_from_slice(b"{\"state\":");
    serde_json::to_writer(&mut bytes, report)?;
    let elapsed = ns(started.elapsed());
    // Checkpoint after state encoding. The small timing annotation below is
    // outside encoding_ns and precedes the immutable watch publication.
    let ready = ns(origin.elapsed());
    bytes.extend_from_slice(b",\"first_publications\":");
    serde_json::to_writer(&mut bytes, publications.observe(report, elapsed, ready))?;
    // The state is encoded exactly once; the small wrapper records that cost.
    bytes.extend_from_slice(
        format!(",\"encoding_ns\":{elapsed},\"ready_ns\":{}}}", ready).as_bytes(),
    );
    Ok(Bytes::from(bytes))
}
async fn state(State(app): State<App>) -> Response<Body> {
    response("application/json", app.latest.borrow().clone())
}
async fn diagnostics(State(app): State<App>) -> Response<Body> {
    let status = app.reports.borrow().diagnostics.clone().snapshot();
    let mut value = serde_json::to_value(status).expect("diagnostic status serialization");
    value["display"] = serde_json::json!({"poll_ms":app.display.browser_poll_ms,"sampling":app.display.browser,"route_seconds":app.display.route_seconds});
    response(
        "application/json",
        Bytes::from(serde_json::to_vec(&value).expect("diagnostic status serialization")),
    )
}
async fn clock(State(app): State<App>) -> Response<Body> {
    let received = ns(app.origin.elapsed());
    let session = app.reports.borrow().session.clone();
    let value = serde_json::json!({"session":session, "received_ms":received as f64 / 1_000_000.0, "sent_ms":ns(app.origin.elapsed()) as f64 / 1_000_000.0});
    response("application/json", Bytes::from(value.to_string()))
}
async fn route(State(app): State<App>) -> Response<Body> {
    let value = app.route.borrow().clone();
    response(
        "application/json",
        Bytes::from(serde_json::to_vec(&value).expect("route serialization")),
    )
}
async fn orderbook(State(app): State<App>) -> Html<String> {
    let report = app.reports.borrow().clone();
    Html(super::book::page(&report))
}
async fn latency() -> Html<&'static str> {
    Html(include_str!("assets/latency.html"))
}
async fn javascript() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/app.js")),
    )
}
async fn recovery_javascript() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/recovery.mjs")),
    )
}
async fn stylesheet() -> Response<Body> {
    response(
        "text/css; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/style.css")),
    )
}
async fn timing() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/timing.mjs")),
    )
}
async fn ingress() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/ingress.mjs")),
    )
}
async fn history() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/history.mjs")),
    )
}
fn response(content_type: &'static str, bytes: Bytes) -> Response<Body> {
    let mut response = Response::new(Body::from(bytes));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

async fn polling_javascript() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/polling.mjs")),
    )
}

async fn book_javascript() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/book.mjs")),
    )
}

async fn elements_javascript() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/elements.mjs")),
    )
}

async fn primary_javascript() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/primary.mjs")),
    )
}

async fn details_javascript() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/details.mjs")),
    )
}

async fn diagnostics_javascript() -> Response<Body> {
    response(
        "text/javascript; charset=utf-8",
        Bytes::from_static(include_bytes!("assets/diagnostics.mjs")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn unread_response_does_not_hold_up_coalesced_publications() {
        let metadata = crate::runner::offline::fixture_metadata().unwrap();
        let (origin, reports) =
            crate::runner::channel(&metadata, &[pax_types::units::MarketId(1)], "fixture").unwrap();
        let (sender, latest) = watch::channel(Bytes::from_static(b"initial"));
        let (_route, route) = watch::channel(None);
        let app = App {
            display: Default::default(),
            origin,
            reports: reports.subscribe(),
            latest,
            route,
        };
        // Keep an HTTP body unread while thousands of producer updates replace
        // the latest value. Neither a body nor an idle reader owns the watch lock.
        let unread = state(State(app.clone())).await;
        for update in 0..10_000 {
            sender.send_replace(Bytes::from(update.to_string()));
        }
        let newest = state(State(app)).await;
        assert_eq!(
            axum::body::to_bytes(unread.into_body(), 32).await.unwrap(),
            "initial"
        );
        assert_eq!(
            axum::body::to_bytes(newest.into_body(), 32).await.unwrap(),
            "9999"
        );
    }
    #[test]
    fn encoded_snapshot_contains_exact_string_sequence_and_timing_metadata() {
        let metadata = crate::runner::offline::fixture_metadata().unwrap();
        let (origin, reports) =
            crate::runner::channel(&metadata, &[pax_types::units::MarketId(1)], "fixture").unwrap();
        let mut report = (**reports.borrow()).clone();
        report.view.markets[0].sequence = Some(u64::MAX.to_string());
        let bytes = encode(
            &report,
            origin,
            &mut super::super::publication::Publications::default(),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value["state"]["view"]["markets"][0]["sequence"],
            u64::MAX.to_string()
        );
        assert!(value["ready_ns"].is_u64());
        assert!(value["encoding_ns"].is_u64());
    }
}
