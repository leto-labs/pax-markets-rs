//! Offline isolated receive components. No network, handshake or sender encryption in timings.
use pax_client::test_support::fixture::Fixture;
use pax_lab::{
    Error,
    measurement::{TurnoverSummary, profiling::Gate},
};
#[path = "support/fixture_book.rs"]
mod book;
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[path = "protocol_components/framing.rs"]
mod framing;
#[path = "protocol_components/tls.rs"]
mod tls;
use clap::Parser;
use std::{hint::black_box, time::Instant};
use tls::{Api, Cipher};
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
struct Options {
    #[arg(long, default_value="all", value_parser=["all","tls","websocket","websocket-pending","websocket-pending-stream","initialization","calibration","empty"])]
    region: String,
    #[arg(long, default_value_t=8, value_parser=clap::value_parser!(u32).range(1..=1000))]
    repetitions: u32,
    #[arg(long, default_value_t=10000, value_parser=clap::value_parser!(u32).range(1..=1000000))]
    events: u32,
    #[arg(long, default_value_t=16384, value_parser=clap::value_parser!(u32).range(1..=1048576))]
    read_bytes: u32,
    #[arg(long, default_value_t=131072, value_parser=clap::value_parser!(u32).range(1024..=1048576))]
    read_buffer: u32,
    /// Verify and release decoded payloads before pending retries; pending region only.
    #[arg(long)]
    release_pending_plaintext: bool,
    #[arg(long, default_value_t=16384, value_parser=clap::value_parser!(u32).range(32..=16389))]
    tls_fragment: u32,
    #[arg(long, value_enum, value_delimiter = ',', default_value = "default")]
    tls_cipher: Vec<Cipher>,
    #[arg(long, value_enum, value_delimiter = ',', default_value = "buffered")]
    tls_api: Vec<Api>,
    /// Retained offline ancestry intervals; zero measures boundary work without retention.
    #[arg(long, default_value_t=256, value_parser=clap::value_parser!(u32).range(0..=4096))]
    tls_record_capacity: u32,
    #[arg(long)]
    fragmented: bool,
    #[arg(long, requires = "ack_fd")]
    control_fd: Option<u32>,
    #[arg(long, requires = "control_fd")]
    ack_fd: Option<u32>,
    #[arg(long)]
    smoke: bool,
    #[arg(long, hide = true)]
    test: bool,
    #[arg(long, hide = true)]
    bench: bool,
}
fn main() -> Result<(), Error> {
    let options = Options::parse();
    if options.release_pending_plaintext && options.region != "websocket-pending" {
        return Err("plaintext release requires the websocket-pending region".into());
    }
    if !matches!(options.region.as_str(), "all" | "tls")
        && (!matches!(options.tls_cipher.as_slice(), [Cipher::Default])
            || !matches!(options.tls_api.as_slice(), [Api::Buffered]))
    {
        return Err("explicit TLS selection requires the TLS component region".into());
    }
    if options.tls_record_capacity != 256
        && !options
            .tls_api
            .iter()
            .any(|api| matches!(api, Api::RecordBounded))
    {
        return Err("explicit record capacity requires the record-bounded TLS API".into());
    }
    let smoke = options.smoke || options.test;
    let count = if smoke { 101 } else { options.events as usize };
    let fixture = Fixture::synthetic(count, 20)?;
    let mut reference = book::new()?;
    for raw in &fixture.messages {
        book::apply(&mut reference, raw)?;
    }
    let expected = book::expected(&fixture)?;
    assert_eq!(book::verify(&reference, &fixture)?, expected);
    let mut gate = options
        .control_fd
        .zip(options.ack_fd)
        .map(|(c, a)| Gate::from_fds(c, a))
        .transpose()?;
    let frames: Vec<_> = fixture
        .messages
        .iter()
        .map(|raw| framing::encode(raw, options.fragmented))
        .collect();
    let plaintext: Vec<_> = frames.iter().map(Vec::as_slice).collect();
    let wire: Vec<_> = frames.concat();
    let repetitions = if smoke { 2 } else { options.repetitions };
    let regions: &[&str] = if options.region == "all" {
        &["empty", "tls", "websocket", "initialization"]
    } else {
        &[&options.region]
    };
    let regions: Vec<_> = regions
        .iter()
        .flat_map(|&region| {
            let ciphers = if region == "tls" {
                &options.tls_cipher[..]
            } else {
                &[Cipher::Default][..]
            };
            let apis = if region == "tls" {
                &options.tls_api[..]
            } else {
                &[Api::Buffered][..]
            };
            ciphers
                .iter()
                .flat_map(move |&cipher| apis.iter().map(move |&api| (region, cipher, api)))
        })
        .collect();
    for round in 1..=repetitions {
        for &(region, cipher, api) in &regions {
            let mut book = book::new()?;
            let mut samples = Vec::with_capacity(fixture.messages.len() * 2);
            let mut tls = if region == "tls" {
                Some(tls::Prepared::new(
                    api,
                    cipher,
                    &plaintext,
                    options.tls_fragment as usize,
                    options.tls_record_capacity as usize,
                )?)
            } else {
                None
            };
            // All output storage is initialized and bounded outside TLS timing.
            let mut decrypted: Vec<_> = frames.iter().map(|f| vec![0; f.len()]).collect();
            let mut ws = framing::parser(
                &wire,
                options.read_bytes as usize,
                options.read_buffer as usize,
                1048576,
            );
            let mut decoded = Vec::with_capacity(fixture.messages.len());
            let mut initialization = vec![0u8; options.read_buffer as usize];
            let mut controls = 0;
            let mut clock_rejections = 0;
            let mut calibration = pax_client::websocket::receive::clock::Calibration::system().ok();
            let clock_source = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos() as i128;
            if !matches!(region, "websocket-pending" | "websocket-pending-stream")
                && let Some(gate) = &mut gate
            {
                gate.set(true)?;
            }
            match region {
                "tls" => {
                    tls.as_mut().unwrap().measure(
                        &mut decrypted,
                        options.read_bytes as usize,
                        &mut samples,
                    )?;
                }
                "websocket" | "websocket-pending" => {
                    while decoded.len() < fixture.messages.len() {
                        let start = Instant::now();
                        let message = ws.read()?;
                        let elapsed = start.elapsed();
                        if region == "websocket" {
                            samples.push(elapsed);
                        }
                        match message {
                            Message::Text(text) => decoded.push(bytes::Bytes::from(text)),
                            Message::Ping(payload) => {
                                assert_eq!(payload.as_ref(), b"control");
                                controls += 1;
                            }
                            _ => return Err("unexpected component frame".into()),
                        }
                    }
                    if region == "websocket-pending" {
                        if options.release_pending_plaintext {
                            assert_eq!(decoded, fixture.messages, "assembled message identity");
                            for raw in &decoded {
                                book::apply(&mut book, raw)?;
                            }
                            assert_eq!(book::verify(&book, &fixture)?, expected);
                            // Release immutable aliases before timing so scratch
                            // can be reclaimed as in a consumer that drops texts.
                            decoded.clear();
                        }
                        if let Some(gate) = &mut gate {
                            gate.set(true)?;
                        }
                        for _ in &fixture.messages {
                            let start = Instant::now();
                            let result = ws.read();
                            let elapsed = start.elapsed();
                            if !matches!(result, Err(tokio_tungstenite::tungstenite::Error::Io(ref error)) if error.kind() == std::io::ErrorKind::WouldBlock)
                            {
                                return Err(
                                    "pending component produced data, EOF or another error".into(),
                                );
                            }
                            samples.push(elapsed);
                        }
                    }
                }
                "websocket-pending-stream" => {
                    for (raw, frame) in fixture.messages.iter().zip(&frames) {
                        ws.get_mut().allow_next(frame.len())?;
                        loop {
                            match ws.read()? {
                                Message::Text(text) => {
                                    assert_eq!(text.as_bytes(), raw.as_ref());
                                    book::apply(&mut book, text.as_bytes())?;
                                    break; // Drops the payload before the retry.
                                }
                                Message::Ping(payload) => {
                                    assert_eq!(payload.as_ref(), b"control");
                                    controls += 1;
                                }
                                _ => return Err("unexpected streaming component frame".into()),
                            }
                        }
                        if let Some(gate) = &mut gate {
                            gate.set(true)?;
                        }
                        let start = Instant::now();
                        let result = ws.read();
                        let elapsed = start.elapsed();
                        if let Some(gate) = &mut gate {
                            gate.set(false)?;
                        }
                        if !matches!(result, Err(tokio_tungstenite::tungstenite::Error::Io(ref error)) if error.kind() == std::io::ErrorKind::WouldBlock)
                        {
                            return Err(
                                "streaming component read ahead or changed retry outcome".into()
                            );
                        }
                        samples.push(elapsed);
                    }
                    assert!(ws.get_ref().input.is_empty());
                }
                "calibration" => {
                    for _ in &fixture.messages {
                        let start = Instant::now();
                        let result = pax_client::websocket::receive::clock::Calibration::system()
                            .and_then(|next| {
                                if let Some(model) = &mut calibration {
                                    model.update(next, std::time::Duration::from_micros(10))?;
                                } else {
                                    calibration = Some(next);
                                }
                                calibration.as_ref().unwrap().align(
                                    clock_source,
                                    Instant::now(),
                                    std::time::Duration::from_secs(2),
                                    100,
                                )
                            });
                        black_box(&result);
                        samples.push(start.elapsed());
                        clock_rejections += usize::from(result.is_err());
                    }
                }
                "initialization" => {
                    for _ in &fixture.messages {
                        let start = Instant::now();
                        // A warm destination baseline; no allocation/page faults included.
                        black_box(initialization.as_mut_slice()).fill(0);
                        samples.push(start.elapsed());
                    }
                }
                "empty" => {
                    for _ in &fixture.messages {
                        let start = Instant::now();
                        black_box(&initialization);
                        samples.push(start.elapsed());
                    }
                }
                _ => unreachable!(),
            }
            if let Some(gate) = &mut gate {
                gate.set(false)?;
            }
            if region == "tls" {
                assert_eq!(decrypted, frames, "authenticated plaintext identity");
            }
            if matches!(region, "websocket" | "websocket-pending")
                && !options.release_pending_plaintext
            {
                assert_eq!(decoded, fixture.messages, "assembled message identity");
                for raw in &decoded {
                    book::apply(&mut book, raw)?;
                }
            } else if region == "tls" {
                // Parse the actual authenticated output and apply it outside TLS timing.
                let authenticated = decrypted.concat();
                let mut parser = framing::parser(&authenticated, 65536, 131072, 1048576);
                let mut seen = 0;
                while seen < fixture.messages.len() {
                    match parser.read()? {
                        Message::Text(text) => {
                            assert_eq!(text.as_bytes(), fixture.messages[seen].as_ref());
                            book::apply(&mut book, text.as_bytes())?;
                            seen += 1;
                        }
                        Message::Ping(payload) => {
                            assert_eq!(payload.as_ref(), b"control");
                            controls += 1;
                        }
                        _ => return Err("unexpected authenticated component frame".into()),
                    }
                }
            }
            if matches!(
                region,
                "tls" | "websocket" | "websocket-pending" | "websocket-pending-stream"
            ) {
                assert_eq!(book::verify(&book, &fixture)?, expected);
                assert_eq!(
                    controls,
                    if options.fragmented {
                        fixture.messages.len()
                    } else {
                        0
                    }
                );
            }
            let stats = TurnoverSummary::from_samples(&mut samples).unwrap();
            let ancestry = tls
                .as_ref()
                .map(|t| {
                    t.ancestry(
                        cipher,
                        &plaintext,
                        options.tls_fragment as usize,
                        options.read_bytes as usize,
                    )
                })
                .transpose()?
                .flatten();
            let mut report = serde_json::json!({"kind":"protocol_component","region":region,"round":round,"build":pax_lab::build_info::current_build_info(),
                "cpu":std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s|s.lines().find(|l|l.starts_with("model name")).map(str::to_owned)),
                "target":env!("PAX_TARGET"),"rustflags":env!("PAX_RUSTFLAGS"),"allocator":if cfg!(feature="mimalloc"){"mimalloc"}else{"system"},"profiled":gate.is_some(),
                "input_digest":fixture.digest(),"book_digest":if matches!(region,"tls"|"websocket"|"websocket-pending"|"websocket-pending-stream"){Some(&expected)}else{None},
                "input_bytes":wire.len(),"events":fixture.events,"messages":fixture.messages.len(),"controls":controls,"clock_rejections":clock_rejections,
                "samples":stats.samples,"p50_ns":stats.p50.as_nanos(),"p95_ns":stats.p95.as_nanos(),"p99_ns":stats.p99.as_nanos(),
                "read_bytes":options.read_bytes,"read_buffer":options.read_buffer,"fragmented":options.fragmented,
                "websocket_fixture_io":if matches!(region,"websocket"|"websocket-pending"|"websocket-pending-stream"){Some(serde_json::json!({"successful_reads":ws.get_ref().reads,"read_bytes":wire.len()-ws.get_ref().input.len(),"write_bytes":ws.get_ref().writes,"scope":"whole offline Wire stream, including controls/read-ahead; not socket syscalls or TLS reads"}))}else{None},
                "pending_calls":if matches!(region,"websocket-pending"|"websocket-pending-stream"){Some(stats.samples)}else{None},
                "pending_plaintext_retention":if region=="websocket-pending-stream"{Some("each streaming message verified/applied/released before its retry")}else if region=="websocket-pending"{Some(if options.release_pending_plaintext{"verified then released before timing"}else{"whole untimed stream retained through timing"})}else{None},
                "component_population":if region=="websocket-pending-stream"{"one gated WouldBlock retry per untimed consumed message; no packet, TLS, scheduler or book work in timing"}else if region=="websocket-pending"{"bounded WouldBlock retries after untimed complete input; no arriving packet, TLS, scheduler or book commit in timing"}else if region=="tls" && matches!(api,Api::RecordBounded){"intrusive offline record-bounded TLS operations; not ordinary SDK receive performance"}else{"ordinary component operations; fragmented WebSocket calls include interleaved Ping returns"},
                "tls_fragment":options.tls_fragment,"requested_cipher":if region=="tls"{Some(cipher)}else{None},
                "crypto_provider":tls.as_ref().map(|_|if matches!(cipher,Cipher::Default){"process-default"}else{"aws-lc-rs"}),
                "tls_api":if region=="tls"{Some(api)}else{None},
                "diagnostic":ancestry.is_some(),
                "tls_ancestry":ancestry,
                "tls_input_arena_bytes":if region=="tls" && matches!(api,Api::Unbuffered){Some(pax_client::test_support::records::unbuffered::INPUT_BYTES)}else{None},
                "tls_api_limits":if region=="tls" && matches!(api,Api::Unbuffered){Some("one-direction fixed-record fixture; exposed output and Close refused; peer KeyUpdate decryption does not qualify deferred response delivery; owned plaintext remains; no network adapter or zero-copy claim")}else if region=="tls" && matches!(api,Api::RecordBounded){Some("intrusive record-limited input; exact fixture intervals and explicit retention loss; no TCP/hardware clock/frame/book ancestry claim")}else{None},
                "cipher":tls.as_ref().map(|t|t.metadata().0),"tls_version":tls.as_ref().map(|t|t.metadata().1),
                "tls_records":tls.as_ref().map(|t|t.metadata().2.iter().map(|b|b.records).sum::<usize>()),
                "ciphertext_bytes":tls.as_ref().map(|t|t.metadata().2.iter().map(|b|b.ciphertext.len()).sum::<usize>()),
                "included":"component operation and memory copying; gated regions also include sample storage, loops and gate tail; compare empty gate",
                "excluded":"handshake, sender encryption, fixtures, allocation of output arrays, JSON/book processing, identity validation and printing"});
            if report["book_digest"].is_string() {
                report["book_digest_schema"] = "snapshot-json-blake3-v1".into();
            } else {
                report["book_digest_schema"] = serde_json::Value::Null;
            }
            println!("{report}");
        }
    }
    Ok(())
}
