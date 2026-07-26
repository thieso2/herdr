# overherdr

**This is a vibecoded experiment in multi-remote herdr. A weekend project, not a product, and not intended to go upstream.**

`overherdr` is a fork of [ogulcancelik/herdr](https://github.com/ogulcancelik/herdr) that exists to try one idea: driving a *fleet* of remote herdr servers from a single client. Everything else here is scaffolding to make that idea installable and testable on real machines.

If you want the real thing — supported, documented, released on a schedule — use [herdr.dev](https://herdr.dev). Nothing in this fork is a criticism of upstream; it is just a place to try things without asking anyone's permission.

## Status

Experimental. Expect rough edges and breaking changes without notice. There is no support, no roadmap and no compatibility promise. Issues and discussions live on this fork, never upstream.

Known rough edges at the time of writing:

- macOS and Windows *test* compilation is broken (inherited from the fleet work; release builds are unaffected).
- The preview update channel is inert — the fork publishes a stable manifest only.
- Remote hosts are still bootstrapped with a binary named `herdr` at `~/.local/bin/herdr`; remote-side renaming is not done yet.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/thieso2/herdr/master/install.sh | sh
```

Installs an `overherdr` binary into `~/.local/bin` (override with `OVERHERDR_INSTALL_DIR`). Built targets are Linux x86_64, Linux aarch64 and macOS aarch64 — **Apple Silicon only on macOS**, and no Windows build. Intel Macs and Windows have to build from source. Binaries are unsigned; a `curl` install is not quarantined, but if you download one through a browser on macOS you will need `xattr -d com.apple.quarantine`.

Update in place with `overherdr update`, which reads this fork's own manifest.

Or build from source — Rust 1.96.1 and Zig 0.15.2 (for the vendored libghostty-vt):

```sh
cargo build --release   # target/release/herdr; install it as `overherdr`
```

## Coexisting with upstream herdr

Installing this does not disturb an existing `herdr`. The two keep separate on-disk state:

| | upstream `herdr` | `overherdr` |
| --- | --- | --- |
| config / sessions | `~/.config/herdr` | `~/.config/overherdr` |
| debug build | `~/.config/herdr-dev` | `~/.config/overherdr-dev` |
| worktrees | `~/.herdr/worktrees` | `~/.overherdr/worktrees` |
| state | `~/.local/state/herdr` | `~/.local/state/overherdr` |

Both the release and debug builds share one worktrees tree, deliberately: splitting it would strand work created by a `cargo run` session.

## What deliberately keeps the herdr name

The rename is kept as small as possible so upstream merges apply verbatim. Unchanged:

- the Cargo package and `[[bin]]` target (`herdr`) — renaming it would silently break `EnvFilter("herdr=info")`, which keys on the crate name, and 58 `CARGO_BIN_EXE_herdr` sites
- the entire `HERDR_*` environment namespace
- socket and log filenames (`herdr.sock`, `herdr-client.sock`, `herdr-server.log`)
- the bundled agent hook assets

The binary is renamed at install time; `argv[0]` carries `overherdr` at runtime.

Fork-specific behaviour lives in files upstream has never seen — `src/identity.rs`, `overherdr.just`, `install.sh`, `dist/`, `scripts/overherdr_manifest.py`, `.github/workflows/overherdr-release.yml` — so a merge cannot conflict on them, and guard tests fail loudly in `just check` if an upstream merge is ever resolved upstream's way.

## Versioning

Fork versions take the upstream base and offset the patch by 100: upstream `0.7.5` becomes `0.7.100`, the next fork release `0.7.101`, and a rebase onto upstream `0.8.0` becomes `0.8.100`.

This keeps the upstream base visible and stays monotonic. A suffix such as `0.7.5-0.1.0` is not an option: the updater's version parser accepts exactly three integer components, and two builds sharing a release core but differing in suffix compare as *unordered*, which silently skips fleet upgrades.

## Releasing

Tags use the `overherdr-v` prefix — never `v*`, which would fire upstream's release workflow, one that needs secrets this fork does not have and builds upstream-named assets.

```sh
just overherdr-release 0.7.100
```

Pushing the tag builds the four supported targets, publishes a GitHub release with `overherdr`-named assets, and commits the regenerated `dist/latest.json` back to `master`, which is what `overherdr update` and the SSH remote bootstrap read.

## Upstream

Fetch-only. This fork does not send changes back:

```sh
git remote -v
# origin    https://github.com/thieso2/herdr.git      (fetch/push)
# upstream  https://github.com/ogulcancelik/herdr.git (fetch)
```

herdr is Apache-2.0; so is this. All credit for the actual product goes upstream.
