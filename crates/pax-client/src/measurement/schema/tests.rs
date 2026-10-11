use super::*;
fn event() -> Observation {
    let mut event = Observation::new(
        Boundary::CompleteMessageToBook,
        Population::Event,
        Clock::same(ClockDomain::ReceiverMonotonic),
        Coverage {
            eligible: Some(20),
            retained: 2,
            lost: Some(0),
            completed: true,
        },
    );
    event.id = Some(ObservationId {
        generation: 1,
        message: u64::MAX,
    });
    event.probes.push(Probe {
        domain: "decode".into(),
        rate: SampleRate::every(8),
    });
    event
}
#[test]
fn exact_identity_roundtrip_and_incompatible_event_refusal() {
    let expected = event();
    let decoded: Observation =
        serde_json::from_slice(&serde_json::to_vec(&expected).unwrap()).unwrap();
    assert_eq!(expected, decoded);
    expected.compatible_event(&decoded).unwrap();
    for change in 0..8 {
        let mut other = decoded.clone();
        match change {
            0 => other.id.as_mut().unwrap().generation += 1,
            1 => other.population = Population::Snapshot,
            2 => other.boundary = Boundary::NativeSoftwarePacketToBook,
            3 => other.clock.mapping = Mapping::Unavailable,
            4 => other.schema_version += 1,
            5 => other.probes[0].rate = SampleRate::ALL,
            6 => other.clock.end = ClockDomain::SourceMonotonic,
            _ => other.session.push_str("-different"),
        }
        assert!(expected.compatible_event(&other).is_err());
    }
}
#[test]
fn raw_hardware_cannot_be_promoted_and_loss_stays_explicit() {
    let mut sample = event();
    sample.boundary = Boundary::FirstHardwareReadWindowToBook;
    sample.clock = Clock {
        start: ClockDomain::Phc,
        end: ClockDomain::ReceiverMonotonic,
        mapping: Mapping::Unavailable,
    };
    sample.coverage.lost = None;
    sample.validate().unwrap();
    assert!(!sample.clock.qualified());
    sample.clock.mapping = Mapping::Aligned {
        uncertainty_ns: 4000,
    };
    sample.validate().unwrap();
    assert!(sample.clock.qualified());
    sample.clock.start = ClockDomain::DeviceRaw;
    assert!(sample.validate().is_err());
    sample.clock.start = ClockDomain::SourceMonotonic;
    assert!(sample.validate().is_err());
}
#[test]
fn matched_phases_require_full_additive_same_event() {
    assert_eq!(
        matched_percentages(10, &[Some(4), Some(6)]).unwrap(),
        [40., 60.]
    );
    for phases in [
        vec![Some(4), None],
        vec![Some(4), Some(7)],
        vec![Some(u64::MAX), Some(1)],
    ] {
        assert!(matched_percentages(10, &phases).is_err());
    }
}

#[test]
fn noncommits_and_unbounded_metadata_cannot_qualify_as_book_latency() {
    let mut value = event();
    for population in [
        Population::Rejected,
        Population::Buffered,
        Population::Duplicate,
        Population::Control,
    ] {
        value.population = population;
        value.boundary = Boundary::DecodeToBook;
        assert!(value.validate().is_err());
        value.boundary = Boundary::DecodeToDisposition;
        value.validate().unwrap();
    }
    value.probes.resize(33, value.probes[0].clone());
    assert!(value.validate().is_err());
    value.probes.truncate(1);
    value.session = "x".repeat(129);
    assert!(value.validate().is_err());
}

#[test]
fn an_ingress_domain_cannot_replace_the_declared_commit_clock() {
    let mut value = event();
    for (boundary, domain) in [
        (Boundary::FirstHardwareReadWindowToBook, ClockDomain::Phc),
        (
            Boundary::SoftwareReadBatchToBook,
            ClockDomain::KernelRealtime,
        ),
    ] {
        value.boundary = boundary;
        value.clock = Clock::same(domain);
        // A same-domain clock is valid in isolation, but book commit is observed
        // on the receiver monotonic clock, not this ingress clock.
        assert!(value.clock.qualified());
        assert!(value.validate().is_err());
        value.clock.end = ClockDomain::ReceiverMonotonic;
        value.clock.mapping = Mapping::Unavailable;
        value.validate().unwrap();
    }
    value.boundary = Boundary::SourcePreparationToAck;
    value.clock = Clock::same(ClockDomain::SourceMonotonic);
    value.validate().unwrap();
    value.clock.end = ClockDomain::ReceiverMonotonic;
    value.clock.mapping = Mapping::Unavailable;
    assert!(value.validate().is_err());
}
