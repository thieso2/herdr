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
}
