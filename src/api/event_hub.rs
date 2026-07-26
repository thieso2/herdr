#[derive(Clone, Default)]
pub struct EventHub {
    inner: std::sync::Arc<std::sync::Mutex<EventHubState>>,
    /// Connected sessions that negotiated the catalog capability. Server
    /// loops treat them like attached clients when deciding whether
    /// client-facing background work (git status refresh) should run.
    catalog_clients: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Default)]
struct EventHubState {
    next_sequence: u64,
    events: Vec<(u64, crate::api::schema::EventEnvelope)>,
}

impl EventHub {
    const MAX_EVENTS: usize = 512;

    pub fn push(&self, event: crate::api::schema::EventEnvelope) {
        let Ok(mut state) = self.inner.lock() else {
            return;
        };
        state.next_sequence += 1;
        let sequence = state.next_sequence;
        state.events.push((sequence, event));
        let overflow = state.events.len().saturating_sub(Self::MAX_EVENTS);
        if overflow > 0 {
            state.events.drain(0..overflow);
        }
    }

    pub fn events_after(&self, sequence: u64) -> Vec<(u64, crate::api::schema::EventEnvelope)> {
        self.events_after_with_loss(sequence).0
    }

    /// Events newer than `sequence`, plus whether the bounded buffer already
    /// dropped events the caller has not seen. Sequences are contiguous, so
    /// loss is exactly "the oldest retained event is more than one past the
    /// cursor": a consumer that sees `lost == true` cannot catch up from the
    /// buffer and must resync from a fresh snapshot.
    pub fn events_after_with_loss(
        &self,
        sequence: u64,
    ) -> (Vec<(u64, crate::api::schema::EventEnvelope)>, bool) {
        let Ok(state) = self.inner.lock() else {
            return (Vec::new(), false);
        };
        let lost = state
            .events
            .first()
            .is_some_and(|(oldest, _)| *oldest > sequence.saturating_add(1));
        let events = state
            .events
            .iter()
            .filter(|(event_sequence, _)| *event_sequence > sequence)
            .cloned()
            .collect();
        (events, lost)
    }

    /// Registers a catalog-capable session for the guard's lifetime. The
    /// count is panic-safe: dropping the guard on any exit path
    /// decrements it.
    pub fn register_catalog_client(&self) -> CatalogClientGuard {
        self.catalog_clients
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        CatalogClientGuard {
            catalog_clients: std::sync::Arc::clone(&self.catalog_clients),
        }
    }

    /// Number of currently connected catalog-capable sessions.
    pub fn catalog_client_count(&self) -> usize {
        self.catalog_clients
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn current_sequence(&self) -> u64 {
        let Ok(state) = self.inner.lock() else {
            return 0;
        };
        state.next_sequence
    }
}

/// RAII registration of one catalog-capable session on an [`EventHub`].
pub struct CatalogClientGuard {
    catalog_clients: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for CatalogClientGuard {
    fn drop(&mut self) {
        self.catalog_clients
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope() -> crate::api::schema::EventEnvelope {
        serde_json::from_value(serde_json::json!({
            "event": "pane_focused",
            "data": { "type": "pane_focused", "pane_id": "p_1_1", "workspace_id": "ws_1" }
        }))
        .expect("canned event deserializes")
    }

    #[test]
    fn events_after_reports_no_loss_within_the_buffer() {
        let hub = EventHub::default();
        for _ in 0..10 {
            hub.push(envelope());
        }
        let (events, lost) = hub.events_after_with_loss(4);
        assert_eq!(events.len(), 6);
        assert!(!lost, "cursor 4 is still inside the buffer");

        // A cursor exactly at the oldest retained event minus one is fine.
        let (events, lost) = hub.events_after_with_loss(0);
        assert_eq!(events.len(), 10);
        assert!(!lost);

        // An empty hub never reports loss.
        let (events, lost) = EventHub::default().events_after_with_loss(0);
        assert!(events.is_empty());
        assert!(!lost);
    }

    #[test]
    fn ring_overflow_past_the_cursor_reports_loss() {
        let hub = EventHub::default();
        for _ in 0..(EventHub::MAX_EVENTS + 8) {
            hub.push(envelope());
        }
        // Oldest retained sequence is 9; a cursor of 3 missed events 4..=8.
        let (events, lost) = hub.events_after_with_loss(3);
        assert_eq!(events.len(), EventHub::MAX_EVENTS);
        assert!(lost, "overflowed events must be reported as lost");

        // A cursor at or past the drop boundary sees no loss.
        let (_, lost) = hub.events_after_with_loss(8);
        assert!(!lost);
    }
}
