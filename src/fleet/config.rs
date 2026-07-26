//! Fleet remote configuration stored wholesale in `remotes.toml`.
//!
//! The file lives in the herdr config dir and is owned by the app: dialog
//! saves go through [`update`] (keyed read-modify-write under an advisory
//! lock, then wholesale re-serialization), while hand edits only take effect
//! through an explicit reload that diffs the freshly loaded entries against
//! the running fleet with [`diff_remotes`].
//!
//! The local runtime is the implicit remote `#0`; the name `local` is
//! reserved and never appears in the file.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::warn;

/// Reserved name of the implicit local runtime (remote `#0`).
pub const LOCAL_REMOTE_NAME: &str = "local";

const REMOTES_FILE: &str = "remotes.toml";
const REMOTES_LOCK_FILE: &str = ".remotes.lock";
const MAX_REMOTE_NAME_LEN: usize = 64;

fn default_session() -> String {
    crate::session::DEFAULT_SESSION_NAME.to_string()
}

fn default_enabled() -> bool {
    true
}

/// One configured remote runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteEntry {
    /// Unique fleet-local name; `local` is reserved.
    pub name: String,
    /// SSH destination (`[user@]host` or an ssh_config alias).
    pub target: String,
    /// Remote herdr session name; defaults to the default session.
    #[serde(default = "default_session")]
    pub session: String,
    /// Disabled remotes stay listed but get no connection.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl RemoteEntry {
    /// The connection identity of an entry. A change here is a different
    /// remote runtime and must be treated as remove-plus-add.
    pub fn connection_identity(&self) -> (&str, &str) {
        (&self.target, &self.session)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RemotesFile {
    #[serde(default, rename = "remote")]
    remotes: Vec<RemoteEntry>,
}

pub fn remotes_path() -> PathBuf {
    crate::config::config_dir().join(REMOTES_FILE)
}

fn remotes_lock_path() -> PathBuf {
    crate::config::config_dir().join(REMOTES_LOCK_FILE)
}

fn with_remotes_lock<T>(operation: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    let lock_path = remotes_lock_path();
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    lock.lock()?;
    operation()
}

/// Validates a remote name: session-name character rules plus the reserved
/// `local` name.
pub fn validate_remote_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("remote name cannot be empty".to_string());
    }
    if name.len() > MAX_REMOTE_NAME_LEN {
        return Err(format!(
            "remote name cannot be longer than {MAX_REMOTE_NAME_LEN} bytes"
        ));
    }
    if name == "." || name == ".." {
        return Err("remote name cannot be . or ..".to_string());
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(
            "remote name may only contain ASCII letters, numbers, '.', '_' and '-'".to_string(),
        );
    }
    if name == LOCAL_REMOTE_NAME {
        return Err(format!(
            "remote name '{LOCAL_REMOTE_NAME}' is reserved for the implicit local runtime"
        ));
    }
    Ok(())
}

/// Validates one entry.
pub fn validate_entry(entry: &RemoteEntry) -> Result<(), String> {
    validate_remote_name(&entry.name)?;
    if entry.target.is_empty() {
        return Err(format!("remote '{}' has an empty target", entry.name));
    }
    if entry.target.starts_with('-') {
        return Err(format!(
            "remote '{}' target must not start with '-'",
            entry.name
        ));
    }
    crate::session::validate_name(&entry.session)
        .map_err(|err| format!("remote '{}': {err}", entry.name))?;
    Ok(())
}

/// Validates a whole entry list, including name uniqueness.
pub fn validate_entries(entries: &[RemoteEntry]) -> Result<(), String> {
    for (index, entry) in entries.iter().enumerate() {
        validate_entry(entry)?;
        if entries[..index]
            .iter()
            .any(|other| other.name == entry.name)
        {
            return Err(format!("duplicate remote name '{}'", entry.name));
        }
    }
    Ok(())
}

/// Drops invalid and duplicate-name entries with a warning, keeping the
/// first occurrence of each name. Used by every lenient path (startup and
/// config-only listings) so one bad hand-edited entry cannot take the fleet
/// down and all listings agree on what the fleet contains; the strict reload
/// path surfaces the same problems as errors instead.
pub fn sanitize_entries(entries: Vec<RemoteEntry>) -> Vec<RemoteEntry> {
    let mut sanitized: Vec<RemoteEntry> = Vec::with_capacity(entries.len());
    for entry in entries {
        if let Err(err) = validate_entry(&entry) {
            warn!(remote = %entry.name, err = %err, "skipping invalid fleet remote entry");
            continue;
        }
        if sanitized.iter().any(|existing| existing.name == entry.name) {
            warn!(remote = %entry.name, "skipping duplicate fleet remote entry");
            continue;
        }
        sanitized.push(entry);
    }
    sanitized
}

/// Loads the fleet config leniently: a missing or corrupt file yields an
/// empty fleet so startup is never blocked, and invalid or duplicate entries
/// are dropped via [`sanitize_entries`]. Mutations and explicit reloads use
/// strict reads instead.
pub fn load() -> Vec<RemoteEntry> {
    sanitize_entries(load_from_path(&remotes_path()))
}

/// Strict load for callers that must surface hand-edit errors (explicit
/// reload).
pub fn try_load() -> io::Result<Vec<RemoteEntry>> {
    with_remotes_lock(|| load_from_path_strict(&remotes_path()))
}

pub fn load_from_path(path: &Path) -> Vec<RemoteEntry> {
    match load_from_path_strict(path) {
        Ok(entries) => entries,
        Err(err) => {
            warn!(path = %path.display(), err = %err, "failed to load fleet remotes config");
            Vec::new()
        }
    }
}

fn load_from_path_strict(path: &Path) -> io::Result<Vec<RemoteEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = std::fs::read_to_string(path)?;
    let file: RemotesFile = toml::from_str(&content)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    Ok(file.remotes)
}

/// Wholesale-serializes the fleet to `path` via a temp file and rename.
pub fn save_to_path(path: &Path, remotes: &[RemoteEntry]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = RemotesFile {
        remotes: remotes.to_vec(),
    };
    let content = toml::to_string_pretty(&file)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    let tmp_path = path.with_extension("toml.tmp");
    std::fs::write(&tmp_path, content)?;
    #[cfg(windows)]
    if path.exists() {
        if let Err(err) = std::fs::remove_file(path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
    }
    if let Err(err) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(err);
    }
    Ok(())
}

/// Keyed read-modify-write under the fleet lock: strict load, mutate, validate,
/// wholesale re-serialize. This is the save path dialog-driven edits use.
pub fn update<T>(
    mutation: impl FnOnce(&mut Vec<RemoteEntry>) -> T,
) -> io::Result<(T, Vec<RemoteEntry>)> {
    with_remotes_lock(|| {
        let path = remotes_path();
        let mut remotes = load_from_path_strict(&path)?;
        let result = mutation(&mut remotes);
        validate_entries(&remotes)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        save_to_path(&path, &remotes)?;
        Ok((result, remotes))
    })
}

/// Inserts or replaces the entry with the same name, keeping list order.
pub fn upsert_in(remotes: &mut Vec<RemoteEntry>, entry: RemoteEntry) {
    if let Some(existing) = remotes.iter_mut().find(|remote| remote.name == entry.name) {
        *existing = entry;
    } else {
        remotes.push(entry);
    }
}

/// Removes the entry with `name`; returns whether it existed.
pub fn remove_in(remotes: &mut Vec<RemoteEntry>, name: &str) -> bool {
    let before = remotes.len();
    remotes.retain(|remote| remote.name != name);
    remotes.len() != before
}

/// Saves one entry keyed by name (dialog save API).
// The remotes dialogs land with ticket #23; this is the save API they call.
#[allow(dead_code)]
pub fn upsert_remote(entry: RemoteEntry) -> io::Result<Vec<RemoteEntry>> {
    let ((), remotes) = update(|remotes| upsert_in(remotes, entry))?;
    Ok(remotes)
}

/// Removes one entry keyed by name (explicit removal API).
// The remotes dialogs land with ticket #23; this is the removal API they call.
#[allow(dead_code)]
pub fn remove_remote(name: &str) -> io::Result<(bool, Vec<RemoteEntry>)> {
    update(|remotes| remove_in(remotes, name))
}

/// One difference between a running fleet and a freshly loaded config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteChange {
    Added(RemoteEntry),
    Removed(RemoteEntry),
    /// Same name and connection identity; only flags such as `enabled`
    /// changed. Identity changes (target or session) never produce this —
    /// they are reported as `Removed` plus `Added`.
    SettingsChanged {
        old: RemoteEntry,
        new: RemoteEntry,
    },
}

/// Pure diff by name between the running entries and a reloaded config. A
/// changed target or session is reported as remove-plus-add because it is a
/// different remote runtime.
pub fn diff_remotes(old: &[RemoteEntry], new: &[RemoteEntry]) -> Vec<RemoteChange> {
    let mut changes = Vec::new();
    for old_entry in old {
        match new.iter().find(|entry| entry.name == old_entry.name) {
            None => changes.push(RemoteChange::Removed(old_entry.clone())),
            Some(new_entry) => {
                if old_entry.connection_identity() != new_entry.connection_identity() {
                    changes.push(RemoteChange::Removed(old_entry.clone()));
                    changes.push(RemoteChange::Added(new_entry.clone()));
                } else if old_entry != new_entry {
                    changes.push(RemoteChange::SettingsChanged {
                        old: old_entry.clone(),
                        new: new_entry.clone(),
                    });
                }
            }
        }
    }
    for new_entry in new {
        if !old.iter().any(|entry| entry.name == new_entry.name) {
            changes.push(RemoteChange::Added(new_entry.clone()));
        }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, target: &str) -> RemoteEntry {
        RemoteEntry {
            name: name.to_string(),
            target: target.to_string(),
            session: crate::session::DEFAULT_SESSION_NAME.to_string(),
            enabled: true,
        }
    }

    fn temp_remotes_path(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir()
            .join(format!("herdr-fleet-{name}-{}-{nanos}", std::process::id()))
            .join("remotes.toml")
    }

    #[test]
    fn save_and_load_roundtrip_preserves_order_and_fields() {
        let path = temp_remotes_path("roundtrip");
        let mut second = entry("gpu2", "can@gpu2.example");
        second.session = "work".to_string();
        second.enabled = false;
        let remotes = vec![entry("gpu1", "can@gpu1.example"), second];

        save_to_path(&path, &remotes).unwrap();
        let loaded = load_from_path(&path);
        assert_eq!(loaded, remotes);

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn missing_file_loads_empty() {
        let path = temp_remotes_path("missing");
        assert!(load_from_path(&path).is_empty());
        assert!(load_from_path_strict(&path).unwrap().is_empty());
    }

    #[test]
    fn corrupt_file_loads_empty_but_strict_load_errors() {
        let path = temp_remotes_path("corrupt");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let corrupt = b"[[remote]\nname = broken";
        std::fs::write(&path, corrupt).unwrap();

        assert!(load_from_path_strict(&path).is_err());
        assert!(load_from_path(&path).is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), corrupt);

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn session_and_enabled_default_when_absent() {
        let path = temp_remotes_path("defaults");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "[[remote]]\nname = \"a\"\ntarget = \"host-a\"\n").unwrap();

        let loaded = load_from_path(&path);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].session, crate::session::DEFAULT_SESSION_NAME);
        assert!(loaded[0].enabled);

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn upsert_in_is_keyed_by_name_and_keeps_order() {
        let mut remotes = vec![entry("a", "host-a"), entry("b", "host-b")];
        upsert_in(&mut remotes, entry("a", "host-a-new"));
        assert_eq!(remotes.len(), 2);
        assert_eq!(remotes[0].target, "host-a-new");
        assert_eq!(remotes[1].name, "b");

        upsert_in(&mut remotes, entry("c", "host-c"));
        assert_eq!(remotes.len(), 3);
        assert_eq!(remotes[2].name, "c");
    }

    #[test]
    fn remove_in_reports_whether_the_entry_existed() {
        let mut remotes = vec![entry("a", "host-a")];
        assert!(remove_in(&mut remotes, "a"));
        assert!(!remove_in(&mut remotes, "a"));
        assert!(remotes.is_empty());
    }

    #[test]
    fn local_name_is_reserved() {
        assert!(validate_remote_name("local").is_err());
        assert!(validate_entries(&[entry("local", "host")]).is_err());
    }

    #[test]
    fn remote_names_follow_session_name_rules() {
        assert!(validate_remote_name("gpu-1.example_a").is_ok());
        assert!(validate_remote_name("").is_err());
        assert!(validate_remote_name("has space").is_err());
        assert!(validate_remote_name("..").is_err());
        assert!(validate_remote_name(&"x".repeat(65)).is_err());
    }

    #[test]
    fn entry_validation_rejects_bad_targets_and_sessions() {
        let mut bad_target = entry("a", "");
        assert!(validate_entry(&bad_target).is_err());
        bad_target.target = "-oProxyCommand=evil".to_string();
        assert!(validate_entry(&bad_target).is_err());

        let mut bad_session = entry("a", "host");
        bad_session.session = "no spaces allowed".to_string();
        assert!(validate_entry(&bad_session).is_err());
    }

    #[test]
    fn validate_entries_rejects_duplicate_names() {
        let remotes = vec![entry("a", "host-1"), entry("a", "host-2")];
        assert!(validate_entries(&remotes)
            .unwrap_err()
            .contains("duplicate remote name"));
    }

    #[test]
    fn sanitize_drops_invalid_and_duplicate_entries() {
        let reserved = entry("local", "host");
        let bad_target = entry("bad", "-oProxyCommand=evil");
        let entries = vec![
            entry("a", "host-a"),
            bad_target,
            reserved,
            entry("a", "host-dup"),
        ];
        assert_eq!(sanitize_entries(entries), vec![entry("a", "host-a")]);
    }

    #[test]
    fn diff_is_empty_for_identical_fleets() {
        let remotes = vec![entry("a", "host-a"), entry("b", "host-b")];
        assert!(diff_remotes(&remotes, &remotes).is_empty());
    }

    #[test]
    fn diff_reports_added_and_removed_by_name() {
        let old = vec![entry("a", "host-a")];
        let new = vec![entry("b", "host-b")];
        assert_eq!(
            diff_remotes(&old, &new),
            vec![
                RemoteChange::Removed(entry("a", "host-a")),
                RemoteChange::Added(entry("b", "host-b")),
            ]
        );
    }

    #[test]
    fn diff_treats_target_change_as_remove_plus_add() {
        let old = vec![entry("a", "host-a")];
        let new = vec![entry("a", "host-a2")];
        assert_eq!(
            diff_remotes(&old, &new),
            vec![
                RemoteChange::Removed(entry("a", "host-a")),
                RemoteChange::Added(entry("a", "host-a2")),
            ]
        );
    }

    #[test]
    fn diff_treats_session_change_as_remove_plus_add() {
        let old = vec![entry("a", "host-a")];
        let mut changed = entry("a", "host-a");
        changed.session = "work".to_string();
        let new = vec![changed.clone()];
        assert_eq!(
            diff_remotes(&old, &new),
            vec![
                RemoteChange::Removed(entry("a", "host-a")),
                RemoteChange::Added(changed),
            ]
        );
    }

    #[test]
    fn diff_reports_enabled_toggle_as_settings_change() {
        let old = vec![entry("a", "host-a")];
        let mut disabled = entry("a", "host-a");
        disabled.enabled = false;
        let new = vec![disabled.clone()];
        assert_eq!(
            diff_remotes(&old, &new),
            vec![RemoteChange::SettingsChanged {
                old: entry("a", "host-a"),
                new: disabled,
            }]
        );
    }
}
