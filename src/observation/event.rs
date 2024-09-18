use std::{collections, time};

#[derive(Clone, Debug)]
pub(crate) struct Event {
    span_tags: collections::HashMap<String, String>,
    pub(crate) local_tags: collections::HashMap<String, String>,
    pub(crate) timestamp: time::Instant,
}

impl Event {
    pub(crate) fn new(
        span_tags: collections::HashMap<String, String>,
        local_tags: collections::HashMap<String, String>,
    ) -> Self {
        Self {
            span_tags,
            local_tags,
            timestamp: time::Instant::now(),
        }
    }

    pub(crate) fn serialize_span_tags(&self) -> String {
        let mut s = String::new();
        for (k, v) in &self.span_tags {
            s.push_str(&format!("[{}: {}]", k, v));
        }
        s
    }
}
