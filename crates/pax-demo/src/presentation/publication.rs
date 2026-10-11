//! First encoded-publication checkpoints, maintained outside ingestion.
use crate::{measurement::trace::TraceId, runner::Published};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(super) struct First {
    market: String,
    trace_id: TraceId,
    sequence: String,
    published_ns: u64,
    encoding_ns: u64,
    ready_ns: u64,
}

#[derive(Default)]
pub(super) struct Publications {
    session: String,
    first: BTreeMap<String, First>,
}
impl Publications {
    pub fn observe(
        &mut self,
        report: &Published,
        encoding_ns: u64,
        ready_ns: u64,
    ) -> &BTreeMap<String, First> {
        if self.session != report.session {
            self.first.clear();
            self.session.clone_from(&report.session);
        }
        if report.view.mode != "live" {
            self.first.clear();
            return &self.first;
        }
        self.first.retain(|id, _| {
            report
                .view
                .markets
                .iter()
                .any(|m| &m.id == id && m.status == "live")
        });
        for market in &report.view.markets {
            if market.status != "live" {
                continue;
            }
            let (Some(trace_id), Some(sequence)) = (market.trace_id, market.sequence.as_ref())
            else {
                self.first.remove(&market.id);
                continue;
            };
            if self
                .first
                .get(&market.id)
                .is_some_and(|first| first.trace_id == trace_id && &first.sequence == sequence)
            {
                continue;
            }
            self.first.insert(
                market.id.clone(),
                First {
                    market: market.id.clone(),
                    trace_id,
                    sequence: sequence.clone(),
                    published_ns: report.published_ns,
                    encoding_ns,
                    ready_ns,
                },
            );
        }
        &self.first
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn report() -> Published {
        let metadata = crate::runner::offline::fixture_metadata().unwrap();
        let (_, tx) =
            crate::runner::channel(&metadata, &[pax_types::units::MarketId(1)], "live").unwrap();
        let mut report = (**tx.borrow()).clone();
        report.published_ns = 100;
        let m = &mut report.view.markets[0];
        m.status = "live";
        m.sequence = Some("123".into());
        m.trace_id = Some(TraceId {
            generation: 1,
            message: u64::MAX,
        });
        report
    }
    #[test]
    fn quiet_publications_keep_first_checkpoint_without_a_retained_sample() {
        let mut r = report();
        let mut tracker = Publications::default();
        let first = tracker.observe(&r, 20, 150)["1"].clone();
        r.published_ns = 10_000;
        assert_eq!(tracker.observe(&r, 80, 10_500)["1"], first);
        let m = &mut r.view.markets[0];
        m.trace_id = Some(TraceId {
            generation: 2,
            message: 1,
        });
        m.sequence = Some("124".into());
        assert_eq!(tracker.observe(&r, 80, 10_500)["1"].published_ns, 10_000);
    }
    #[test]
    fn market_lifecycle_and_sessions_bound_checkpoint_storage() {
        let mut r = report();
        let mut tracker = Publications::default();
        tracker.observe(&r, 20, 150);
        let mut second = r.view.markets[0].clone();
        second.id = "2".into();
        r.view.markets.push(second);
        r.published_ns = 200;
        let state = tracker.observe(&r, 30, 260);
        assert_eq!(state.len(), 2);
        assert_eq!(state["1"].published_ns, 100);
        assert_eq!(state["2"].published_ns, 200);
        r.session = "another".into();
        r.published_ns = 300;
        assert_eq!(tracker.observe(&r, 40, 380)["1"].published_ns, 300);
        r.view.markets[0].status = "invalid";
        assert_eq!(tracker.observe(&r, 40, 380).len(), 1);
        r.view.markets.clear();
        assert!(tracker.observe(&r, 40, 380).is_empty());
        r = report();
        r.view.mode = "fixture replay".into();
        assert!(tracker.observe(&r, 1, 101).is_empty());
    }
}
