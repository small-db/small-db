use std::collections;

use super::{Event, Span};

pub struct Ob {
    pub(crate) events: Vec<Event>,
}

impl Ob {
    pub(crate) fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub(crate) fn add_event(&mut self, event: Event) {
        self.events.push(event);
    }

    pub fn analyze(&self) {
        log::info!("=== ayalyze start ===");

        let event_count = self.events.len();
        log::info!("event_count = {}", event_count);

        let mut v: collections::HashMap<String, Vec<Event>> = collections::HashMap::new();
        for event in &self.events {
            v.entry(event.serialize_span_tags())
                .or_insert(Vec::new())
                .push(event.clone());
        }

        let mut released_locks = Vec::new();

        for (span, events) in &v {
            match events.len() {
                1 => {
                    // ignore
                }
                2 => {
                    let event1 = &events[0];
                    let event2 = &events[1];
                    if event1.local_tags.get("action") == Some(&"acquired".to_string())
                        && event2.local_tags.get("action") == Some(&"released".to_string())
                    {
                        released_locks.push((event1, event2));
                    } else {
                        log::info!("weird span = {}, events: {:?}", span, events);
                    }
                }
                _ => {
                    // log::info!("weird span = {}, event_count = {}", span, events.len());
                }
            }
        }

        // log the 10 longest lock holdings
        released_locks.sort_by_key(|(event1, event2)| event2.timestamp - event1.timestamp);
        for (event1, event2) in released_locks.iter().take(10) {
            log::info!(
                "lock held for {:?}, span = {}",
                event2.timestamp.duration_since(event1.timestamp),
                event1.serialize_span_tags(),
            );
        }

        log::info!("=== analyze end ===");
    }
}
