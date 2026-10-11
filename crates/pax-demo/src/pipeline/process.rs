//! Ordered input transitions and optional processing observation.
use super::{CommitPoint, Pipeline, ProcessResult, ns};
use crate::{
    capture::{CaptureError, Record, RecordKind},
    measurement::{
        receive::ReceiveTrace,
        trace::{DeliveryStages, Trace, TraceId},
    },
};
use pax_book::{
    session::SessionOutcome,
    state::{BookError, Outcome, Phase},
};
use pax_client::websocket::{DeliveryTiming, receive::ReceiveTiming};
use pax_types::protocol::{self, Message};
use std::time::Instant;

impl Pipeline {
    /// Consume a raw input at its recorded receive offset. Queue delay is telemetry
    /// only and cannot change reconstruction or weighted metrics.
    pub fn process(
        &mut self,
        record: &Record,
        queue_ns: u64,
    ) -> Result<ProcessResult, CaptureError> {
        let origin = if cfg!(feature = "processing-traces")
            && self.probes.policy != crate::measurement::policy::ProcessingPolicy::OFF
        {
            Instant::now()
        } else {
            self.replay_origin
        };
        self.process_with_clock(record, origin, queue_ns, None, None, |_| {})
    }
    /// Consume a live input using its original complete-message receive instant.
    /// This includes caller dispatch and any intervening scheduling delay in the
    /// queue span. `received` and `record.offset_ns` must describe the same input.
    pub fn process_received(
        &mut self,
        record: &Record,
        received: Instant,
    ) -> Result<ProcessResult, CaptureError> {
        self.process_delivered(record, received, None)
    }
    /// Consume a live input with optional transport and consumer-delivery markers.
    pub fn process_delivered(
        &mut self,
        record: &Record,
        received: Instant,
        delivery: Option<DeliveryTiming>,
    ) -> Result<ProcessResult, CaptureError> {
        self.process_with_clock(record, received, 0, delivery, None, |_| {})
    }
    /// Consume optional receive diagnostics without changing canonical transitions.
    pub fn process_instrumented(
        &mut self,
        record: &Record,
        received: Instant,
        delivery: Option<DeliveryTiming>,
        receive: Option<ReceiveTiming>,
    ) -> Result<ProcessResult, CaptureError> {
        self.process_with_clock(record, received, 0, delivery, receive, |_| {})
    }
    /// Observe the same commit boundary independently of production telemetry.
    ///
    /// Intended for controlled experiments. The synchronous hook must not block;
    /// its cost can affect the next input. Ordinary calls compile with a no-op
    /// observer. Connection boundaries do not invoke the hook.
    pub fn process_observed(
        &mut self,
        record: &Record,
        received: Instant,
        delivery: Option<DeliveryTiming>,
        receive: Option<ReceiveTiming>,
        observer: impl FnMut(CommitPoint),
    ) -> Result<ProcessResult, CaptureError> {
        self.process_with_clock(record, received, 0, delivery, receive, observer)
    }
    /// Reserve optional receive storage before live processing. No default payload
    /// or processing trace allocation grows when diagnostics are disabled.
    pub fn reserve_receive_diagnostics(&mut self) {
        if self.receive_traces.is_empty() {
            self.receive_traces = crate::measurement::receive::prefaulted_window(2048);
        }
    }
    /// Latest retained receive trace, suitable for an independent diagnostic writer.
    pub fn latest_receive(&self) -> Option<ReceiveTrace> {
        self.receive_traces.back().copied()
    }
    #[allow(clippy::too_many_arguments)]
    fn process_with_clock(
        &mut self,
        record: &Record,
        queue_origin: Instant,
        queue_ns: u64,
        delivery: Option<DeliveryTiming>,
        receive: Option<ReceiveTiming>,
        mut observer: impl FnMut(CommitPoint),
    ) -> Result<ProcessResult, CaptureError> {
        let previous_callback = self.last_callback.take();
        self.advance(record.offset_ns)?;
        if record.generation < self.session.generation() {
            return Ok(ProcessResult::default());
        }
        match &record.kind {
            RecordKind::Connected => {
                if self.connected || record.generation <= self.session.generation() {
                    return Err(CaptureError::Invalid("connection generation order"));
                }
                self.probes
                    .restart_connection()
                    .map_err(CaptureError::Invalid)?;
                let states = &mut self.markets;
                self.session
                    .connect_observed(record.generation, |book| {
                        let state = states.get_mut(&book.market()).unwrap();
                        state.close_interval(book, record.offset_ns);
                        state.last_update_ns = None;
                        state.last_trace_id = None;
                    })
                    .map_err(|_| CaptureError::Invalid("book reset"))?;
                self.connected = true;
                Ok(ProcessResult::default())
            }
            RecordKind::Disconnected { reason, .. } => {
                if record.generation != self.session.generation() {
                    return Err(CaptureError::Invalid("boundary generation"));
                }
                self.connected = false;
                self.invalidate(reason);
                Ok(ProcessResult::default())
            }
            RecordKind::Message(bytes) => {
                if !self.connected || record.generation != self.session.generation() {
                    return Err(CaptureError::Invalid("message outside connection"));
                }
                self.messages += 1;
                let selection = self.probes.select();
                let delivery_selected = cfg!(feature = "delivery-traces")
                    && delivery.is_some_and(|d| d.delivered.is_some());
                let sampled = selection.any() || delivery_selected;
                let start = (selection.callback || selection.decode || delivery_selected)
                    .then(Instant::now);
                let decoded = protocol::decode(bytes);
                let decoded_at = (selection.decode || selection.preflight).then(Instant::now);
                let mut markers: [Option<Instant>; 3] = [None; 3];
                let mut population = "control";
                let mut market = None;
                let mut sequence = None;
                let result = decoded
                    .map_err(|error| error.to_string())
                    .and_then(|message| {
                        match &message {
                            Message::Snapshot(snapshot) => {
                                population = "snapshot";
                                market = Some(snapshot.market_id);
                                sequence = Some(snapshot.latest_seq_num);
                            }
                            Message::Event(event) => {
                                population = "event";
                                market = Some(event.order.market_id);
                                sequence = Some(event.data.seq_num);
                            }
                            _ => {}
                        }
                        let states = &mut self.markets;
                        let before = |book: &pax_book::state::Book| {
                            states
                                .get_mut(&book.market())
                                .unwrap()
                                .close_interval(book, record.offset_ns);
                        };
                        let outcome = if selection.book() {
                            self.session.apply_observed(
                                record.generation,
                                message,
                                before,
                                |phase| {
                                    let (index, selected) = match phase {
                                        Phase::Preflight => {
                                            (0, selection.preflight || selection.mutation)
                                        }
                                        Phase::Mutation => {
                                            (1, selection.mutation || selection.validation)
                                        }
                                        Phase::Validation => (2, selection.validation),
                                    };
                                    if selected {
                                        markers[index] = Some(Instant::now());
                                    }
                                },
                            )
                        } else {
                            self.session
                                .apply_observed(record.generation, message, before, |_| {})
                        }
                        .map_err(|error| error.to_string())?;
                        if population == "snapshot"
                            && let SessionOutcome::Book {
                                sequence: committed,
                                ..
                            } = outcome
                        {
                            sequence = committed;
                        }
                        Ok(outcome)
                    });
                let recover = result.is_err();
                // The benchmark observer below supplies its own clock, including
                // when all production probe groups are disabled.
                let finished = (selection.callback || selection.bookkeeping).then(Instant::now);
                let changed = result.as_ref().is_ok_and(|outcome| outcome.committed());
                if let Ok(SessionOutcome::Book { outcome, .. }) = &result {
                    population = match outcome {
                        Outcome::Buffered => "buffered",
                        Outcome::Duplicate => "duplicate",
                        _ => population,
                    };
                }
                let id = TraceId {
                    generation: record.generation,
                    message: self.messages,
                };
                observer(CommitPoint {
                    id,
                    received: queue_origin,
                    population: if recover { "rejected" } else { population },
                    sequence,
                });
                if changed {
                    self.applied += 1;
                    let state = self.markets.get_mut(&market.unwrap()).unwrap();
                    state.last_update_ns = Some(record.offset_ns);
                    state.last_trace_id = Some(id);
                }
                if let Err(reason) = result {
                    self.rejected += 1;
                    // This cold error path preserves the cause before transport
                    // recovery replaces last_error with its disconnect reason.
                    self.last_rejection = Some(super::view::RejectedInput {
                        id,
                        observed_ns: record.offset_ns,
                        market: market.map(|value| value.0.to_string()),
                        sequence: sequence.map(|value| value.0.to_string()),
                        reason: reason.chars().take(512).collect(),
                    });
                    self.invalidate(&reason);
                    population = "rejected";
                }
                if let Some(trace) = receive.and_then(|timing| {
                    ReceiveTrace::new(id, record.offset_ns, queue_origin, timing)
                }) {
                    while self.receive_traces.front().is_some_and(|old| {
                        trace.received_ns.saturating_sub(old.received_ns) > 10_000_000_000
                    }) {
                        self.receive_traces.pop_front();
                        self.receive_age_expirations =
                            self.receive_age_expirations.saturating_add(1);
                    }
                    if self.receive_traces.len() == 2048 {
                        self.receive_traces.pop_front();
                        self.receive_capacity_drops += 1;
                    }
                    self.receive_traces.push_back(trace);
                }
                if cfg!(any(
                    feature = "processing-traces",
                    feature = "delivery-traces"
                )) && (sampled || (recover && cfg!(feature = "processing-traces")))
                {
                    let queue_ns = start
                        .filter(|_| selection.callback || delivery_selected)
                        .map(|start| {
                            queue_ns
                                .saturating_add(ns(start.saturating_duration_since(queue_origin)))
                        });
                    let decode_ns = if selection.decode {
                        start
                            .zip(decoded_at)
                            .map(|(a, b)| ns(b.saturating_duration_since(a)))
                    } else {
                        None
                    };
                    let mut previous = decoded_at;
                    let mut stages = [None; 3];
                    for (index, at) in markers.iter().enumerate() {
                        if [
                            selection.preflight,
                            selection.mutation,
                            selection.validation,
                        ][index]
                        {
                            stages[index] = previous
                                .zip(*at)
                                .map(|(a, b)| ns(b.saturating_duration_since(a)));
                        }
                        previous = *at;
                    }
                    let processing_ns = if selection.callback {
                        start
                            .zip(finished)
                            .map(|(a, b)| ns(b.saturating_duration_since(a)))
                    } else {
                        None
                    };
                    let measured_book_ns = stages.iter().flatten().sum::<u64>();
                    let other_ns = processing_ns.zip(decode_ns).and_then(|(total, decode)| {
                        total.checked_sub(decode.checked_add(measured_book_ns)?)
                    });
                    self.traces.push(Trace {
                        policy_revision: self.probes.revision,
                        policy: self.probes.policy,
                        id,
                        generation: record.generation,
                        market,
                        sequence,
                        population,
                        received_ns: record.offset_ns,
                        queue_ns,
                        delivery: delivery.filter(|_| delivery_selected).and_then(|timing| {
                            let start = start?;
                            let delivered = timing.delivered?;
                            Some(DeliveryStages {
                                transport_ns: ns(timing
                                    .prepared
                                    .checked_duration_since(queue_origin)?),
                                handoff_ns: ns(delivered.checked_duration_since(timing.prepared)?),
                                preparation_ns: ns(start.checked_duration_since(delivered)?),
                                pending: timing.pending?,
                            })
                        }),
                        bookkeeping_ns: None,
                        pending_gap_ns: previous_callback
                            .filter(|previous| selection.bookkeeping && queue_origin <= *previous)
                            .and_then(|previous| start?.checked_duration_since(previous))
                            .map(ns),
                        decode_ns,
                        preflight_ns: stages[0],
                        mutation_ns: stages[1],
                        validation_ns: stages[2],
                        other_ns,
                        processing_ns,
                    });
                    if selection.bookkeeping
                        && let Some(finished) = finished
                    {
                        let end = Instant::now();
                        self.traces
                            .bookkeeping(id, ns(end.saturating_duration_since(finished)));
                        self.last_callback = Some(end);
                    }
                }
                Ok(ProcessResult { recover, changed })
            }
        }
    }
    fn invalidate(&mut self, reason: &str) {
        self.last_error = Some(reason.chars().take(512).collect());
        let states = &mut self.markets;
        self.session.invalidate_observed(
            BookError::Inconsistent("external continuity failure"),
            |book| {
                states
                    .get_mut(&book.market())
                    .unwrap()
                    .close_interval(book, self.offset_ns)
            },
        );
    }
}
