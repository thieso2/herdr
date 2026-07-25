//! Raw PTY output tap for pane streams.
//!
//! Every pane runtime owns a [`PaneOutputTap`]. The PTY read callback routes
//! terminal byte processing through [`PaneOutputTap::publish_with`], which
//! appends the raw bytes to every live subscriber buffer under the tap lock.
//! Subscribing with [`PaneOutputTap::subscribe_with_snapshot`] takes the same
//! lock while capturing a screen snapshot, so the returned byte sequence,
//! the snapshot, and the subsequent output tail are mutually consistent:
//! every published byte is either inside the snapshot or delivered by the
//! subscription, never both and never neither.
//!
//! Buffers are bounded: a subscriber that falls more than
//! [`PANE_OUTPUT_BUFFER_LIMIT_BYTES`] behind is marked overloaded and its
//! buffer is dropped. The framed session layer disconnects overloaded
//! clients, which reconnect and reseed from a fresh snapshot.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

/// Maximum bytes buffered per subscriber before it is marked overloaded.
pub(crate) const PANE_OUTPUT_BUFFER_LIMIT_BYTES: usize = 32 * 1024 * 1024;

/// Per-pane raw output fan-out point.
#[derive(Default)]
pub(crate) struct PaneOutputTap {
    inner: Mutex<TapState>,
}

#[derive(Default)]
struct TapState {
    /// Total bytes ever published through this tap.
    sequence: u64,
    subscribers: Vec<Weak<Mutex<SubscriberState>>>,
}

#[derive(Default)]
struct SubscriberState {
    buffered: Vec<u8>,
    overloaded: bool,
    closed: bool,
}

/// One subscriber's live view of a pane's raw output tail.
pub(crate) struct PaneOutputSubscription {
    /// Pane output byte sequence at subscribe time.
    sequence: u64,
    state: Arc<Mutex<SubscriberState>>,
}

/// Result of draining a subscription buffer.
pub(crate) struct PaneOutputDrain {
    pub(crate) bytes: Vec<u8>,
    pub(crate) overloaded: bool,
    pub(crate) closed: bool,
}

// Mutex poisoning only happens after a panic while holding the lock; the
// buffered bytes stay structurally valid, so continue with the inner state.
fn lock_ignoring_poison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

impl PaneOutputTap {
    /// Runs `process` (the terminal byte ingestion) and publishes `bytes` to
    /// all live subscribers under the tap lock, keeping terminal state and
    /// subscriber tails consistent for concurrent snapshot subscribers.
    pub(crate) fn publish_with<R>(&self, bytes: &[u8], process: impl FnOnce() -> R) -> R {
        let mut state = lock_ignoring_poison(&self.inner);
        let result = process();
        if !bytes.is_empty() {
            state.sequence = state.sequence.saturating_add(bytes.len() as u64);
            state.subscribers.retain(|subscriber| {
                let Some(subscriber) = subscriber.upgrade() else {
                    return false;
                };
                let mut subscriber = lock_ignoring_poison(&subscriber);
                if subscriber.closed || subscriber.overloaded {
                    return true;
                }
                if subscriber.buffered.len().saturating_add(bytes.len())
                    > PANE_OUTPUT_BUFFER_LIMIT_BYTES
                {
                    subscriber.overloaded = true;
                    subscriber.buffered = Vec::new();
                } else {
                    subscriber.buffered.extend_from_slice(bytes);
                }
                true
            });
        }
        result
    }

    /// Subscribes to the output tail, capturing `snapshot` under the tap lock
    /// so the subscription sequence and the snapshot describe the same
    /// instant of terminal state.
    pub(crate) fn subscribe_with_snapshot<R>(
        &self,
        snapshot: impl FnOnce() -> R,
    ) -> (PaneOutputSubscription, R) {
        let mut state = lock_ignoring_poison(&self.inner);
        let snapshot = snapshot();
        let subscriber = Arc::new(Mutex::new(SubscriberState::default()));
        state.subscribers.push(Arc::downgrade(&subscriber));
        (
            PaneOutputSubscription {
                sequence: state.sequence,
                state: subscriber,
            },
            snapshot,
        )
    }
}

impl Drop for PaneOutputTap {
    fn drop(&mut self) {
        let state = lock_ignoring_poison(&self.inner);
        for subscriber in &state.subscribers {
            if let Some(subscriber) = subscriber.upgrade() {
                lock_ignoring_poison(&subscriber).closed = true;
            }
        }
    }
}

impl PaneOutputSubscription {
    /// Pane output byte sequence at subscribe time. The first drained byte
    /// is byte `sequence() + 1` of the pane's output.
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Takes all buffered output bytes in publish (FIFO) order.
    pub(crate) fn drain(&self) -> PaneOutputDrain {
        let mut state = lock_ignoring_poison(&self.state);
        PaneOutputDrain {
            bytes: std::mem::take(&mut state.buffered),
            overloaded: state.overloaded,
            closed: state.closed,
        }
    }
}

// ---------------------------------------------------------------------------
// Pending stream handoff registry
// ---------------------------------------------------------------------------
//
// stream.open is dispatched from an API socket thread to the app thread as a
// JSON request, which cannot carry a subscription object back. The session
// thread registers the server-allocated stream id here before dispatching;
// the app-side handler fulfills the slot with the live subscription, and the
// session claims it after the response arrives. Cancelling the slot makes a
// late fulfillment drop the subscription instead of leaking it.

/// What the app thread hands back for one opened pane stream: the live
/// output subscription plus, for write-mode streams, the pane write grant the
/// session holds for as long as the stream is open.
pub(crate) struct PaneStreamAttachment {
    pub(crate) subscription: PaneOutputSubscription,
    pub(crate) write_grant: Option<crate::pane::write_grant::WriteGrant>,
}

impl PaneStreamAttachment {
    /// Read-mode attachment: output tail only.
    #[cfg(test)]
    pub(crate) fn read_only(subscription: PaneOutputSubscription) -> Self {
        Self {
            subscription,
            write_grant: None,
        }
    }
}

static PENDING_STREAM_SUBSCRIPTIONS: OnceLock<Mutex<HashMap<u32, Option<PaneStreamAttachment>>>> =
    OnceLock::new();

fn pending_streams() -> &'static Mutex<HashMap<u32, Option<PaneStreamAttachment>>> {
    PENDING_STREAM_SUBSCRIPTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Registers an empty slot for a stream id about to be dispatched.
pub(crate) fn register_pending_stream(stream_id: u32) {
    lock_ignoring_poison(pending_streams()).insert(stream_id, None);
}

/// Fulfills a registered slot with a live attachment. Returns false (and
/// drops the attachment, releasing any write grant it carries) when the slot
/// was cancelled or never registered.
pub(crate) fn fulfill_pending_stream(stream_id: u32, attachment: PaneStreamAttachment) -> bool {
    let mut pending = lock_ignoring_poison(pending_streams());
    match pending.get_mut(&stream_id) {
        Some(slot) => {
            *slot = Some(attachment);
            true
        }
        None => false,
    }
}

/// Claims a fulfilled attachment, removing the slot.
pub(crate) fn claim_pending_stream(stream_id: u32) -> Option<PaneStreamAttachment> {
    lock_ignoring_poison(pending_streams()).remove(&stream_id)?
}

/// Cancels a slot so any late fulfillment is dropped.
pub(crate) fn cancel_pending_stream(stream_id: u32) {
    lock_ignoring_poison(pending_streams()).remove(&stream_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_bytes_arrive_in_fifo_order_after_subscribe() {
        let tap = PaneOutputTap::default();
        tap.publish_with(b"before", || ());

        let (subscription, snapshot) = tap.subscribe_with_snapshot(|| "snap".to_owned());
        assert_eq!(snapshot, "snap");
        assert_eq!(subscription.sequence(), 6);

        tap.publish_with(b"hello ", || ());
        tap.publish_with(b"world", || ());

        let drain = subscription.drain();
        assert_eq!(drain.bytes, b"hello world");
        assert!(!drain.overloaded);
        assert!(!drain.closed);

        // Draining empties the buffer.
        assert!(subscription.drain().bytes.is_empty());
    }

    #[test]
    fn sequence_counts_total_published_bytes() {
        let tap = PaneOutputTap::default();
        tap.publish_with(b"12345", || ());
        let (first, ()) = tap.subscribe_with_snapshot(|| ());
        tap.publish_with(b"678", || ());
        let (second, ()) = tap.subscribe_with_snapshot(|| ());
        assert_eq!(first.sequence(), 5);
        assert_eq!(second.sequence(), 8);
        assert_eq!(first.drain().bytes, b"678");
        assert!(second.drain().bytes.is_empty());
    }

    #[test]
    fn publish_returns_process_result() {
        let tap = PaneOutputTap::default();
        assert_eq!(tap.publish_with(b"x", || 42), 42);
    }

    #[test]
    fn slow_subscriber_is_marked_overloaded_and_buffer_dropped() {
        let tap = PaneOutputTap::default();
        let (subscription, ()) = tap.subscribe_with_snapshot(|| ());

        let chunk = vec![0_u8; PANE_OUTPUT_BUFFER_LIMIT_BYTES];
        tap.publish_with(&chunk, || ());
        // At the bound: still fine.
        {
            let state = lock_ignoring_poison(&subscription.state);
            assert!(!state.overloaded);
            assert_eq!(state.buffered.len(), PANE_OUTPUT_BUFFER_LIMIT_BYTES);
        }

        tap.publish_with(b"one more byte", || ());
        let drain = subscription.drain();
        assert!(drain.overloaded);
        assert!(drain.bytes.is_empty(), "overloaded buffer must be dropped");

        // Later publishes never resurrect an overloaded subscriber.
        tap.publish_with(b"after", || ());
        assert!(subscription.drain().overloaded);
        assert!(subscription.drain().bytes.is_empty());
    }

    #[test]
    fn overload_of_one_subscriber_does_not_affect_others() {
        let tap = PaneOutputTap::default();
        let (slow, ()) = tap.subscribe_with_snapshot(|| ());
        tap.publish_with(&vec![0_u8; PANE_OUTPUT_BUFFER_LIMIT_BYTES], || ());
        tap.publish_with(b"x", || ());
        assert!(slow.drain().overloaded);

        let (fresh, ()) = tap.subscribe_with_snapshot(|| ());
        tap.publish_with(b"tail", || ());
        let drain = fresh.drain();
        assert!(!drain.overloaded);
        assert_eq!(drain.bytes, b"tail");
    }

    #[test]
    fn dropping_the_tap_marks_subscribers_closed() {
        let tap = PaneOutputTap::default();
        let (subscription, ()) = tap.subscribe_with_snapshot(|| ());
        tap.publish_with(b"bye", || ());
        drop(tap);

        let drain = subscription.drain();
        assert!(drain.closed);
        assert_eq!(drain.bytes, b"bye", "buffered bytes survive tap drop");
    }

    #[test]
    fn dropped_subscriptions_are_pruned_from_the_tap() {
        let tap = PaneOutputTap::default();
        let (subscription, ()) = tap.subscribe_with_snapshot(|| ());
        drop(subscription);
        tap.publish_with(b"x", || ());
        assert!(lock_ignoring_poison(&tap.inner).subscribers.is_empty());
    }

    #[test]
    fn pending_stream_registry_round_trips_and_cancels() {
        let tap = PaneOutputTap::default();

        // Fulfill without registration is rejected.
        let (orphan, ()) = tap.subscribe_with_snapshot(|| ());
        assert!(!fulfill_pending_stream(
            u32::MAX,
            PaneStreamAttachment::read_only(orphan)
        ));
        assert!(claim_pending_stream(u32::MAX).is_none());

        // Normal flow: register, fulfill, claim.
        register_pending_stream(u32::MAX - 1);
        let (subscription, ()) = tap.subscribe_with_snapshot(|| ());
        assert!(fulfill_pending_stream(
            u32::MAX - 1,
            PaneStreamAttachment::read_only(subscription)
        ));
        let claimed = claim_pending_stream(u32::MAX - 1).expect("fulfilled slot");
        tap.publish_with(b"z", || ());
        assert_eq!(claimed.subscription.drain().bytes, b"z");
        assert!(claim_pending_stream(u32::MAX - 1).is_none());

        // Cancelled slot rejects late fulfillment.
        register_pending_stream(u32::MAX - 2);
        cancel_pending_stream(u32::MAX - 2);
        let (late, ()) = tap.subscribe_with_snapshot(|| ());
        assert!(!fulfill_pending_stream(
            u32::MAX - 2,
            PaneStreamAttachment::read_only(late)
        ));

        // Registered but unfulfilled slot claims as empty.
        register_pending_stream(u32::MAX - 3);
        assert!(claim_pending_stream(u32::MAX - 3).is_none());
    }
}
