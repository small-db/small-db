use std::{collections, time};

/// A `Span` represents a single unit of work that is timed and can be tagged with
/// metadata.
///
/// This struct is designed to help identify the root cause of "lock acquisition timeout"
/// errors.

pub(crate) struct Span {
    tags: collections::HashMap<String, String>,

    start: time::Instant,
    end: time::Instant,
}

impl Span {
    /// Create a new `Span` with the given tags.
    pub(crate) fn new(tags: collections::HashMap<String, String>) -> Self {
        Self {
            tags,
            start: time::Instant::now(),
            end: time::Instant::now(),
        }
    }

    /// Finish the `Span` and return the duration.
    pub(crate) fn finish(&mut self) -> time::Duration {
        self.end = time::Instant::now();
        self.end - self.start
    }
}

pub(crate) struct Spans {
    spans: Vec<Span>,
}

impl Spans {
    pub(crate) fn new() -> Self {
        Self { spans: Vec::new() }
    }

    pub(crate) fn push(&mut self, span: Span) {
        self.spans.push(span);
    }
}
