//! Fleet remote configuration stored wholesale in `remotes.toml`.
//!
//! The file lives in the herdr config dir and is owned by the app: dialog
//! saves go through [`update`] (keyed read-modify-write under an advisory
//! lock, then wholesale re-serialization), while hand edits only take effect
//! through an explicit reload that diffs the freshly loaded entries against
//! the running fleet with [`diff_remotes`].
//!
//! The file is the whole fleet: there is no implicit local runtime. An entry
//! with no `target` is a *local* runtime, reached over this machine's API
//! socket with no ssh; an entry with a `target` is reached over an ssh
//! bridge. That makes "my own box" configurable, removable, and repeatable
//! across sessions, exactly like any other remote.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::warn;

const REMOTES_FILE: &str = "remotes.toml";
const REMOTES_LOCK_FILE: &str = ".remotes.lock";
const MAX_REMOTE_NAME_LEN: usize = 64;

fn default_session() -> String {
    crate::session::DEFAULT_SESSION_NAME.to_string()
}

fn default_enabled() -> bool {
    true
}

/// One configured runtime in the fleet, local or remote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteEntry {
    /// Unique fleet-local name.
    pub name: String,
    /// SSH destination (`[user@]host` or an ssh_config alias). `None` is a
    /// *local* runtime: this machine's API socket, no ssh, no sshd.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Herdr session name; defaults to the default session. Two local
    /// entries differing only by session are two independent runtimes.
    #[serde(default = "default_session")]
    pub session: String,
    /// Disabled remotes stay listed but get no connection.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Identity hue index into the remote palette, persisted so that
    /// reordering or disabling one remote never recolours the rest of the
    /// fleet. `None` is a file written before hues were stored; [`load`]
    /// derives one and the next write persists it.
    ///
    /// This is presentation state in a config file, and a deliberate
    /// exception to the runtime/client guardrail: `remotes.toml` is the
    /// client's own fleet config, not server state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hue: Option<usize>,
}

impl RemoteEntry {
    /// Whether this entry is reached over this machine's API socket rather
    /// than an ssh bridge.
    pub fn is_local(&self) -> bool {
        self.target.is_none()
    }

    /// The connection identity of an entry. A change here is a different
    /// runtime and must be treated as remove-plus-add.
    pub fn connection_identity(&self) -> (Option<&str>, &str) {
        (self.target.as_deref(), &self.session)
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
    // `local` used to be reserved for an implicit remote #0. There is no
    // implicit runtime any more, so the name is ordinary and usable.
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
    Ok(())
}

/// Validates one entry. A missing target is a local runtime and always
/// valid; a present one must still be a usable ssh destination.
pub fn validate_entry(entry: &RemoteEntry) -> Result<(), String> {
    validate_remote_name(&entry.name)?;
    if let Some(target) = entry.target.as_deref() {
        validate_target(&entry.name, target)?;
    }
    crate::session::validate_name(&entry.session)
        .map_err(|err| format!("remote '{}': {err}", entry.name))?;
    Ok(())
}

fn validate_target(name: &str, target: &str) -> Result<(), String> {
    if target.is_empty() {
        // Distinct from an omitted target: an empty string in the file is
        // far more likely a mistake than a request for a local runtime.
        return Err(format!(
            "remote '{name}' has an empty target; omit `target` entirely for a local runtime"
        ));
    }
    if target.starts_with('-') {
        return Err(format!("remote '{name}' target must not start with '-'"));
    }
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
    let mut remotes = file.remotes;
    assign_missing_hues(&mut remotes);
    Ok(remotes)
}

/// The lowest hue index no entry in `remotes` is already using.
///
/// Deliberately a plain index rather than a resolved colour: consulting the
/// theme would make stored values theme-dependent, and a later theme switch
/// would reintroduce duplicates anyway. Past the palette's size hues repeat,
/// which persisting does not change - it buys stability, not uniqueness.
pub fn lowest_unused_hue(remotes: &[RemoteEntry]) -> usize {
    lowest_unused_in(&remotes.iter().filter_map(|remote| remote.hue).collect())
}

/// The lowest index not in `used`.
fn lowest_unused_in(used: &std::collections::BTreeSet<usize>) -> usize {
    let mut candidate = 0usize;
    while used.contains(&candidate) {
        candidate += 1;
    }
    candidate
}

/// Fills in hues for entries loaded from a file written before hues were
/// persisted.
///
/// Enabled entries take their position *among enabled entries*, which is
/// exactly the rule the chip strip used to derive at render time, so an
/// upgrade never recolours a fleet anyone is already used to. Disabled
/// entries had no colour to preserve - they were filtered out before the
/// index was computed - so they take whatever is left, lowest first.
fn assign_missing_hues(remotes: &mut [RemoteEntry]) {
    let mut used: std::collections::BTreeSet<usize> =
        remotes.iter().filter_map(|remote| remote.hue).collect();

    // Pass 1: every enabled entry keeps the colour it renders with today,
    // unless an explicitly stored hue in a partially migrated file has
    // already claimed that index.
    let mut enabled_position = 0usize;
    for entry in remotes.iter_mut() {
        if !entry.enabled {
            continue;
        }
        let position = enabled_position;
        enabled_position += 1;
        if entry.hue.is_some() || used.contains(&position) {
            continue;
        }
        used.insert(position);
        entry.hue = Some(position);
    }

    // Pass 2: disabled entries, and any enabled entry whose position was
    // taken, fall back to the lowest free index.
    for entry in remotes.iter_mut() {
        if entry.hue.is_some() {
            continue;
        }
        let candidate = lowest_unused_in(&used);
        used.insert(candidate);
        entry.hue = Some(candidate);
    }
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

/// Which way a reorder moves an entry in the fleet list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveDirection {
    /// Toward the start of the list, and so toward the left of the chip strip.
    Up,
    /// Toward the end of the list.
    Down,
}

/// Inserts or replaces the entry with the same name, keeping list order.
///
/// An entry arriving with no hue takes the one it already had when it is a
/// replacement, and the lowest unused index when it is new. That is what
/// lets the field dialog stay ignorant of hues: editing a remote's hostname
/// never changes the colour the user has learned to associate with it.
pub fn upsert_in(remotes: &mut Vec<RemoteEntry>, mut entry: RemoteEntry) {
    if let Some(existing) = remotes.iter_mut().find(|remote| remote.name == entry.name) {
        if entry.hue.is_none() {
            entry.hue = existing.hue;
        }
        *existing = entry;
    } else {
        if entry.hue.is_none() {
            entry.hue = Some(lowest_unused_hue(remotes));
        }
        remotes.push(entry);
    }
}

/// Sets `enabled` on the entry named `name`; returns whether it existed.
///
/// Keyed by name, like every other list mutation: each one runs against a
/// list loaded inside the fleet lock, so an index-based mutation applied to
/// a list something else reordered would corrupt it silently, while a
/// name-based one is either correct or a no-op.
// Driven from the remotes list, which lives in the unix-only pure client;
// Windows has no production consumer yet (#20), like `mod client_state`.
#[cfg_attr(windows, allow(dead_code))]
pub fn set_enabled_in(remotes: &mut [RemoteEntry], name: &str, enabled: bool) -> bool {
    match remotes.iter_mut().find(|remote| remote.name == name) {
        Some(entry) => {
            entry.enabled = enabled;
            true
        }
        None => false,
    }
}

/// Moves the entry named `name` one slot in `direction`; returns whether it
/// moved. A missing entry, or one already at that edge of the list, is a
/// no-op. Hues are stored per entry, so reordering never recolours anything.
#[cfg_attr(windows, allow(dead_code))] // see `set_enabled_in`
pub fn move_in(remotes: &mut [RemoteEntry], name: &str, direction: MoveDirection) -> bool {
    let Some(index) = remotes.iter().position(|remote| remote.name == name) else {
        return false;
    };
    let target = match direction {
        MoveDirection::Up => index.checked_sub(1),
        MoveDirection::Down => (index + 1 < remotes.len()).then_some(index + 1),
    };
    let Some(target) = target else {
        return false;
    };
    remotes.swap(index, target);
    true
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
            target: Some(target.to_string()),
            session: crate::session::DEFAULT_SESSION_NAME.to_string(),
            enabled: true,
            hue: None,
        }
    }

    fn hued(name: &str, target: &str, hue: usize) -> RemoteEntry {
        RemoteEntry {
            hue: Some(hue),
            ..entry(name, target)
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
        let mut second = hued("gpu2", "can@gpu2.example", 1);
        second.session = "work".to_string();
        second.enabled = false;
        let remotes = vec![hued("gpu1", "can@gpu1.example", 0), second];

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
        assert_eq!(remotes[0].target.as_deref(), Some("host-a-new"));
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
    fn local_is_an_ordinary_name_and_a_missing_target_is_a_local_runtime() {
        // Regression: `local` was reserved for an implicit remote #0 that no
        // config could remove. Both are gone - the fleet is exactly the file.
        assert!(validate_remote_name("local").is_ok());
        assert!(validate_entries(&[entry("local", "host")]).is_ok());

        let me = RemoteEntry {
            name: "me".into(),
            target: None,
            session: "default".into(),
            enabled: true,
            hue: None,
        };
        assert!(me.is_local());
        assert_eq!(validate_entry(&me), Ok(()), "a missing target is legal");
        assert_eq!(me.connection_identity(), (None, "default"));

        // An *empty* target is still a mistake, and says how to fix itself.
        let mut empty = me.clone();
        empty.target = Some(String::new());
        let err = validate_entry(&empty).expect_err("empty target refused");
        assert!(err.contains("omit `target`"), "{err}");

        // Two local entries are distinct runtimes when their sessions differ.
        let mut scratch = me.clone();
        scratch.session = "scratch".into();
        assert_ne!(me.connection_identity(), scratch.connection_identity());
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
        bad_target.target = Some("-oProxyCommand=evil".to_string());
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
        let bad_target = entry("bad", "-oProxyCommand=evil");
        let mut empty_target = entry("empty", "host");
        empty_target.target = Some(String::new());
        let entries = vec![
            entry("a", "host-a"),
            bad_target,
            empty_target,
            entry("a", "host-dup"),
        ];
        assert_eq!(sanitize_entries(entries), vec![entry("a", "host-a")]);
    }

    #[test]
    fn sanitize_keeps_local_entries() {
        // A target-less entry is a local runtime, not a malformed remote.
        let me = RemoteEntry {
            name: "me".into(),
            target: None,
            session: "default".into(),
            enabled: true,
            hue: None,
        };
        assert_eq!(
            sanitize_entries(vec![entry("a", "host-a"), me.clone()]),
            vec![entry("a", "host-a"), me]
        );
    }

    #[test]
    fn hue_is_allocated_lowest_unused_on_add_and_freed_on_remove() {
        let mut remotes = vec![hued("a", "host-a", 0), hued("b", "host-b", 1)];

        upsert_in(&mut remotes, entry("c", "host-c"));
        assert_eq!(remotes[2].hue, Some(2), "a new remote takes the next hue");

        // Removing the middle remote frees its index, so a fleet that churns
        // does not drift into all-one-colour.
        assert!(remove_in(&mut remotes, "b"));
        upsert_in(&mut remotes, entry("d", "host-d"));
        assert_eq!(remotes[2].hue, Some(1), "the freed hue is reused");
    }

    #[test]
    fn editing_a_remote_keeps_its_hue_and_reorder_and_disable_leave_every_hue_untouched() {
        let mut remotes = vec![
            hued("a", "host-a", 0),
            hued("b", "host-b", 1),
            hued("c", "host-c", 2),
        ];

        // The field dialog carries no hue of its own.
        upsert_in(&mut remotes, entry("b", "host-b-renamed"));
        assert_eq!(remotes[1].hue, Some(1), "editing fields keeps the colour");
        assert_eq!(remotes[1].target.as_deref(), Some("host-b-renamed"));

        assert!(move_in(&mut remotes, "c", MoveDirection::Up));
        assert!(set_enabled_in(&mut remotes, "a", false));

        let by_name: Vec<(&str, Option<usize>)> = remotes
            .iter()
            .map(|remote| (remote.name.as_str(), remote.hue))
            .collect();
        assert_eq!(
            by_name,
            vec![("a", Some(0)), ("c", Some(2)), ("b", Some(1))],
            "reorder and disable recolour nothing"
        );
    }

    #[test]
    fn migration_derives_hues_from_position_among_enabled_entries() {
        // The rule the chip strip used to derive at render time, so an
        // upgrade never recolours a fleet anyone is already used to.
        let path = temp_remotes_path("migrate-hues");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            "[[remote]]\nname = \"a\"\ntarget = \"host-a\"\n\n\
             [[remote]]\nname = \"off\"\ntarget = \"host-off\"\nenabled = false\n\n\
             [[remote]]\nname = \"c\"\ntarget = \"host-c\"\n",
        )
        .unwrap();

        let loaded = load_from_path(&path);
        assert_eq!(
            loaded
                .iter()
                .map(|remote| (remote.name.as_str(), remote.hue))
                .collect::<Vec<_>>(),
            vec![("a", Some(0)), ("off", Some(2)), ("c", Some(1))],
            "enabled entries keep today's colours; the disabled one takes what is left"
        );

        // The derived hues persist on the next write.
        save_to_path(&path, &loaded).unwrap();
        let reread = std::fs::read_to_string(&path).unwrap();
        assert!(reread.contains("hue = 0"), "{reread}");
        assert_eq!(load_from_path(&path), loaded);

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn name_keyed_mutations_survive_an_external_reorder_and_no_op_on_an_external_remove() {
        // Each list action runs against a list loaded inside the fleet lock,
        // so the list a mutation lands on may not be the one the user saw.
        let mut reordered = vec![
            hued("c", "host-c", 2),
            hued("a", "host-a", 0),
            hued("b", "host-b", 1),
        ];
        assert!(set_enabled_in(&mut reordered, "b", false));
        assert!(
            !reordered
                .iter()
                .find(|remote| remote.name == "b")
                .expect("b is still present")
                .enabled,
            "the entry the user selected is the one that changed"
        );

        let mut without_b = vec![hued("a", "host-a", 0)];
        assert!(!set_enabled_in(&mut without_b, "b", false));
        assert!(!move_in(&mut without_b, "b", MoveDirection::Up));
        assert!(!remove_in(&mut without_b, "b"));
        assert_eq!(without_b, vec![hued("a", "host-a", 0)], "no corruption");
    }

    #[test]
    fn move_in_is_a_no_op_at_the_list_edges() {
        let mut remotes = vec![hued("a", "host-a", 0), hued("b", "host-b", 1)];
        assert!(!move_in(&mut remotes, "a", MoveDirection::Up));
        assert!(!move_in(&mut remotes, "b", MoveDirection::Down));
        assert_eq!(remotes[0].name, "a");

        assert!(move_in(&mut remotes, "a", MoveDirection::Down));
        assert_eq!(remotes[0].name, "b");
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
