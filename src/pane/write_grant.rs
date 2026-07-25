//! Pane write grants: the single-writer authority behind framed pane streams.
//!
//! A write-mode `stream.open` acquires the pane's write grant, keyed on the
//! server-allocated stream id rather than on a client identity. Holding the
//! grant is what makes a stream allowed to resize and scroll the pane, and it
//! is what makes the pane's geometry follow that stream instead of the TUI
//! layout.
//!
//! Two rules define the table:
//!
//! - Without takeover, opening a write stream on a pane that already has a
//!   live grant fails with the current holder's stream id. The requesting
//!   connection stays up; only the request is refused.
//! - With takeover, the previous holder's grant is marked revoked and the new
//!   stream takes the entry. The old holder observes the flag on its own
//!   stream, reports `stream.revoked`, and closes that stream — its
//!   connection and its other streams are untouched.
//!
//! Grants release when the holder drops its [`WriteGrant`], which covers
//! stream close, connection close, and server shutdown alike. A stale handle
//! (one whose entry was already taken over) releases nothing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

/// A held pane write grant. Dropping it releases the pane.
#[derive(Debug)]
pub(crate) struct WriteGrant {
    pane_id: String,
    stream_id: u32,
    revoked: Arc<AtomicBool>,
}

/// Why a write grant could not be acquired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteGrantConflict {
    /// Stream id of the live grant holder.
    pub(crate) holder_stream_id: u32,
}

#[derive(Debug)]
struct GrantEntry {
    stream_id: u32,
    revoked: Arc<AtomicBool>,
}

static WRITE_GRANTS: OnceLock<Mutex<HashMap<String, GrantEntry>>> = OnceLock::new();

fn write_grants() -> &'static Mutex<HashMap<String, GrantEntry>> {
    WRITE_GRANTS.get_or_init(|| Mutex::new(HashMap::new()))
}

// Mutex poisoning only happens after a panic while holding the lock; the
// grant table stays structurally valid, so continue with the inner state.
fn lock_ignoring_poison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Acquires the write grant for `pane_id` on behalf of `stream_id`.
pub(crate) fn acquire_write_grant(
    pane_id: &str,
    stream_id: u32,
    takeover: bool,
) -> Result<WriteGrant, WriteGrantConflict> {
    let mut grants = lock_ignoring_poison(write_grants());
    if let Some(existing) = grants.get(pane_id) {
        if existing.stream_id == stream_id {
            return Err(WriteGrantConflict {
                holder_stream_id: existing.stream_id,
            });
        }
        if !takeover {
            return Err(WriteGrantConflict {
                holder_stream_id: existing.stream_id,
            });
        }
        existing.revoked.store(true, Ordering::Release);
    }
    let revoked = Arc::new(AtomicBool::new(false));
    grants.insert(
        pane_id.to_owned(),
        GrantEntry {
            stream_id,
            revoked: Arc::clone(&revoked),
        },
    );
    Ok(WriteGrant {
        pane_id: pane_id.to_owned(),
        stream_id,
        revoked,
    })
}

/// Stream id currently holding the pane's write grant, if any.
pub(crate) fn write_grant_holder(pane_id: &str) -> Option<u32> {
    lock_ignoring_poison(write_grants())
        .get(pane_id)
        .map(|entry| entry.stream_id)
}

impl WriteGrant {
    /// True once another `stream.open` took this grant over.
    pub(crate) fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::Acquire)
    }

    /// Public pane id this grant holds.
    pub(crate) fn pane_id(&self) -> &str {
        &self.pane_id
    }
}

impl Drop for WriteGrant {
    fn drop(&mut self) {
        let mut grants = lock_ignoring_poison(write_grants());
        let still_ours = grants
            .get(&self.pane_id)
            .is_some_and(|entry| entry.stream_id == self.stream_id);
        if still_ours {
            grants.remove(&self.pane_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pane(name: &str) -> String {
        format!("write-grant-test-{name}")
    }

    #[test]
    fn first_write_open_takes_the_grant_and_dropping_releases_it() {
        let pane_id = pane("first");
        let grant = acquire_write_grant(&pane_id, 1, false).expect("first grant");
        assert!(!grant.is_revoked());
        assert_eq!(write_grant_holder(&pane_id), Some(1));

        drop(grant);
        assert_eq!(write_grant_holder(&pane_id), None);
        // The pane is free again.
        let next = acquire_write_grant(&pane_id, 2, false).expect("grant after release");
        assert_eq!(write_grant_holder(&pane_id), Some(2));
        drop(next);
    }

    #[test]
    fn second_write_open_without_takeover_reports_the_holder() {
        let pane_id = pane("locked");
        let held = acquire_write_grant(&pane_id, 7, false).expect("grant");

        assert_eq!(
            acquire_write_grant(&pane_id, 8, false).err(),
            Some(WriteGrantConflict {
                holder_stream_id: 7
            })
        );
        // The holder keeps its grant; nothing was revoked.
        assert!(!held.is_revoked());
        assert_eq!(write_grant_holder(&pane_id), Some(7));
        drop(held);
    }

    #[test]
    fn takeover_revokes_the_previous_holder_without_touching_other_panes() {
        let pane_id = pane("takeover");
        let other_pane = pane("takeover-other");
        let first = acquire_write_grant(&pane_id, 10, false).expect("first grant");
        let elsewhere = acquire_write_grant(&other_pane, 11, false).expect("other pane grant");

        let second = acquire_write_grant(&pane_id, 12, true).expect("takeover grant");
        assert!(
            first.is_revoked(),
            "previous holder must observe revocation"
        );
        assert!(!second.is_revoked());
        assert!(!elsewhere.is_revoked(), "other panes are unaffected");
        assert_eq!(write_grant_holder(&pane_id), Some(12));

        // The revoked holder releases nothing when it finally drops.
        drop(first);
        assert_eq!(write_grant_holder(&pane_id), Some(12));
        drop(second);
        assert_eq!(write_grant_holder(&pane_id), None);
        drop(elsewhere);
    }

    #[test]
    fn reopening_with_the_same_stream_id_is_a_conflict() {
        let pane_id = pane("same-id");
        let grant = acquire_write_grant(&pane_id, 21, false).expect("grant");
        assert_eq!(
            acquire_write_grant(&pane_id, 21, true).err(),
            Some(WriteGrantConflict {
                holder_stream_id: 21
            })
        );
        drop(grant);
    }
}
