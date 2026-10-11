use super::*;

#[test]
fn runtime_workers_and_drain_have_finite_bounds() {
    for workers in ["0", "257"] {
        assert!(
            Cli::try_parse_from([
                "pax-demo",
                "--runtime-workers",
                workers,
                "serve",
                "--fixture"
            ])
            .is_err()
        );
    }
    for drain in ["0", "257"] {
        assert!(
            Cli::try_parse_from([
                "pax-demo",
                "observe",
                "--markets",
                "1",
                "--queue-drain",
                drain
            ])
            .is_err()
        );
    }
    assert!(
        Cli::try_parse_from([
            "pax-demo",
            "observe",
            "--markets",
            "1",
            "--runtime-workers",
            "2",
            "--delivery",
            "inline"
        ])
        .is_ok()
    );
}

#[tokio::test]
async fn inline_batching_is_rejected_before_metadata_access() {
    let Command::Observe(live) = Cli::try_parse_from([
        "pax-demo",
        "observe",
        "--markets",
        "1",
        "--metadata-url",
        "http://invalid.invalid",
        "--delivery",
        "inline",
        "--queue-drain",
        "8",
    ])
    .unwrap()
    .command
    else {
        panic!()
    };
    assert!(
        observe(live, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("requires --delivery queued")
    );
}

#[test]
fn cpu_requires_dedicated_ingestion_and_zero_sampling_disables_probes() {
    assert!(
        Cli::try_parse_from(["pax-demo", "serve", "--markets", "1", "--ingest-cpu", "0"]).is_err()
    );
    assert!(
        Cli::try_parse_from([
            "pax-demo",
            "observe",
            "--markets",
            "1",
            "--sample-every",
            "0"
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "pax-demo",
            "observe",
            "--markets",
            "1",
            "--dedicated-ingestion",
            "--sample-every",
            "8"
        ])
        .is_ok()
    );
}

#[tokio::test]
async fn invalid_placement_fails_before_metadata_access() {
    let id = usize::MAX.to_string();
    let Command::Serve(config) = Cli::try_parse_from([
        "pax-demo",
        "serve",
        "--markets",
        "1",
        "--metadata-url",
        "http://invalid.invalid",
        "--dedicated-ingestion",
        "--ingest-cpu",
        &id,
    ])
    .unwrap()
    .command
    else {
        panic!()
    };
    let error = serve(config).await.unwrap_err().to_string();
    assert!(error.contains("CPU"), "{error}");
    let Command::Serve(config) =
        Cli::try_parse_from(["pax-demo", "serve", "--fixture", "--dedicated-ingestion"])
            .unwrap()
            .command
    else {
        panic!()
    };
    assert!(
        serve(config)
            .await
            .unwrap_err()
            .to_string()
            .contains("only to live")
    );
}
