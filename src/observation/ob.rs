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
        log::info!("=== analyze start ===");

        let event_count = self.events.len();
        log::info!("event_count = {}", event_count);

        let mut v: collections::HashMap<String, Vec<Event>> = collections::HashMap::new();
        for event in &self.events {
            v.entry(event.serialize_span_tags())
                .or_insert(Vec::new())
                .push(event.clone());
        }

        // normal events that are single
        let mut single_events = Vec::new();
        // normal events that consist of a pair of acquired and released
        let mut span_lock_released = Vec::new();

        // a pair of events that are not acquired and released
        let mut weird_pair_events = Vec::new();
        // multiple (> 2) events
        let mut weird_multiple_events = Vec::new();

        for (span, events) in &v {
            match events.len() {
                1 => {
                    single_events.push(events[0].clone());
                }
                2 => {
                    let event1 = &events[0];
                    let event2 = &events[1];
                    if event1.local_tags.get("action") == Some(&"acquired".to_string())
                        && event2.local_tags.get("action") == Some(&"released".to_string())
                    {
                        span_lock_released.push((event1, event2));
                    } else {
                        weird_pair_events.push((event1, event2));
                    }
                }
                _ => {
                    weird_multiple_events.push(events.clone());
                }
            }
        }

        log::info!("single_events = {}", single_events.len());
        for (i, event) in single_events.iter().take(10).enumerate() {
            log::info!("single_events[{}] = {:?}", i, event);
        }

        log::info!("weird_pair_events = {}", weird_pair_events.len());
        for (i, (event1, event2)) in weird_pair_events.iter().take(10).enumerate() {
            log::info!("weird_pair_events[{}] = {:?}, {:?}", i, event1, event2,);
        }

        log::info!("weird_multiple_events = {}", weird_multiple_events.len());
        for (i, events) in weird_multiple_events.iter().take(10).enumerate() {
            log::info!("weird_multiple_events[{}] = {:?}", i, events);
        }

        // log the 10 longest lock holdings
        span_lock_released.sort_by_key(|(event1, event2)| event2.timestamp - event1.timestamp);
        log::info!("span_lock_released = {}", span_lock_released.len());
        for (event1, event2) in span_lock_released.iter().rev().take(10) {
            log::info!(
                "lock held for {:?}, event1 = {:?}, event2 = {:?}, span_tags = {}",
                event2.timestamp.duration_since(event1.timestamp),
                event1,
                event2,
                event1.serialize_span_tags(),
            );
        }

        log::info!("=== analyze end ===");
    }
}
