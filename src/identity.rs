//! Fork-owned brand identity.
//!
//! Upstream has never seen this file, so it cannot conflict on a merge. Every
//! fork-specific name derives from the two constants below; if an upstream
//! merge is ever resolved upstream's way, the guard tests here fail loudly in
//! `just check` instead of silently moving the user's config back.

/// Release build brand: on-disk directories, worktrees tree, completion bin name.
pub const BRAND: &str = "overherdr";

/// Debug build brand, so a `cargo run` session never shares config with a
/// release install.
pub const BRAND_DEV: &str = "overherdr-dev";

/// Git tag prefix for fork releases.
///
/// Deliberately not `v*`: upstream's `release.yml` fires on `v*`, needs three
/// secrets this fork does not have, and would build upstream-named assets. A
/// fork tag must never match it.
// Declarative fork facts: consumed by the guard tests below, by
// `.github/workflows/overherdr-release.yml` and by
// `scripts/overherdr_manifest.py`, which cannot read Rust constants. They have
// no runtime caller by design, so dead_code is expected rather than a smell.
#[allow(dead_code)]
pub const RELEASE_TAG_PREFIX: &str = "overherdr-v";

/// Lowest patch component a fork release may carry.
///
/// Fork versions are the upstream base with the patch offset by 100
/// (`0.7.5` -> `0.7.100`). This keeps the upstream base visible, stays
/// monotonic across upstream minor bumps, and never realistically collides
/// with an upstream patch. A suffix such as `0.7.5-0.1.0` is not an option:
/// `update::Version::parse` accepts exactly three integer components, and two
/// same-core suffixed builds compare as `Unordered`, which silently skips
/// fleet upgrades.
// See the note on RELEASE_TAG_PREFIX: guard-test and tooling constant, no
// runtime caller by design.
#[allow(dead_code)]
pub const FORK_PATCH_FLOOR: u32 = 100;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brand_constants_are_fork_owned() {
        assert_eq!(BRAND, "overherdr");
        assert_eq!(BRAND_DEV, "overherdr-dev");
    }

    #[test]
    fn app_dir_name_matches_build_profile() {
        let expected = if cfg!(debug_assertions) {
            BRAND_DEV
        } else {
            BRAND
        };
        assert_eq!(crate::config::app_dir_name(), expected);
    }

    #[test]
    fn completion_bin_name_is_brand() {
        assert_eq!(crate::cli::COMPLETION_BIN_NAME, BRAND);
    }

    #[test]
    fn release_tag_prefix_never_collides_with_upstream_release_workflow() {
        assert!(
            !RELEASE_TAG_PREFIX.starts_with('v'),
            "a `v*` tag fires upstream's release.yml, which this fork cannot run"
        );
    }

    #[test]
    fn fork_version_carries_the_patch_offset() {
        let version = crate::update::Version::current();
        assert!(
            version.patch >= FORK_PATCH_FLOOR,
            "fork Cargo version {version} must offset the patch by {FORK_PATCH_FLOOR} \
             (upstream 0.7.5 -> fork 0.7.100); see issue #37"
        );
    }
}
