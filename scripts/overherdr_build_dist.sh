#!/usr/bin/env bash
# Fork-owned local release-binary builder. Cross-compiles with zig instead of
# docker/cross, so every release asset can be produced from one Linux (or
# macOS) checkout with nothing but rustup, zig and cargo-zigbuild installed.
#
# Upstream has never seen this file, so it cannot conflict on a merge. Driven
# by `mise run build:dist` (see mise.toml).
#
# The libghostty-vt build in build.rs shells out to `zig build -Dtarget=...`
# and always writes vendor/libghostty-vt/zig-out/lib. That output directory is
# shared by every target, so a stale zig-out from the previous target would be
# linked into the next binary. Each target therefore clears zig-out and touches
# a rerun-if-changed input so cargo re-runs the build script. .zig-cache is
# keyed by target and is deliberately kept, so the rebuild stays cheap.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# Match the release workflow (.github/workflows/overherdr-release.yml).
export LIBGHOSTTY_VT_OPTIMIZE="${LIBGHOSTTY_VT_OPTIMIZE:-ReleaseFast}"
export LIBGHOSTTY_VT_SIMD="${LIBGHOSTTY_VT_SIMD:-true}"

required_zig="0.15.2"
out_dir="$repo_root/dist"

# asset name -> rust target triple
all_assets=(
    overherdr-linux-x86_64
    overherdr-linux-aarch64
    overherdr-macos-aarch64
)

target_for() {
    case "$1" in
        overherdr-linux-x86_64) echo "x86_64-unknown-linux-musl" ;;
        overherdr-linux-aarch64) echo "aarch64-unknown-linux-musl" ;;
        overherdr-macos-x86_64) echo "x86_64-apple-darwin" ;;
        overherdr-macos-aarch64) echo "aarch64-apple-darwin" ;;
        *)
            echo "error: unknown asset $1" >&2
            return 1
            ;;
    esac
}

# Accept either the asset name or the short suffix (linux-x86_64, macos-aarch64).
normalize_asset() {
    case "$1" in
        overherdr-*) echo "$1" ;;
        *) echo "overherdr-$1" ;;
    esac
}

require_tools() {
    local missing=0
    if ! command -v zig >/dev/null 2>&1; then
        echo "error: zig not found; herdr needs zig $required_zig on PATH" >&2
        missing=1
    else
        local zig_version
        zig_version="$(zig version)"
        if [ "$zig_version" != "$required_zig" ]; then
            echo "warning: zig $zig_version found, herdr is verified against $required_zig" >&2
        fi
    fi
    if ! command -v cargo-zigbuild >/dev/null 2>&1; then
        echo "error: cargo-zigbuild not found; install it with 'cargo install --locked cargo-zigbuild'" >&2
        missing=1
    fi
    [ "$missing" -eq 0 ] || exit 1
}

# Cross-linking a macOS binary needs the real SDK: herdr links the Carbon and
# CoreFoundation frameworks plus libobjc, none of which ship with zig. On
# macOS the toolchain finds its own; elsewhere an explicit SDKROOT wins, and
# otherwise a pinned SDK is fetched into a cache directory.
ensure_macos_sdk() {
    [ "$(uname -s)" != "Darwin" ] || return 0
    [ -z "${SDKROOT:-}" ] || return 0
    [ -z "${DEVELOPER_DIR:-}" ] || return 0

    if [ "${OVERHERDR_FETCH_MACOS_SDK:-1}" != "1" ]; then
        echo "error: no macOS SDK available and OVERHERDR_FETCH_MACOS_SDK is off; set SDKROOT=/path/to/MacOSX.sdk" >&2
        return 1
    fi

    local sdk
    sdk="$(scripts/overherdr_fetch_macos_sdk.sh)"
    export SDKROOT="$sdk"
}

build_asset() {
    local asset="$1"
    local target
    target="$(target_for "$asset")"

    echo "==> $asset ($target)"
    rustup target add "$target" >/dev/null

    rm -rf vendor/libghostty-vt/zig-out
    touch vendor/libghostty-vt/VERSION

    cargo zigbuild --release --locked --target "$target"

    mkdir -p "$out_dir"
    cp "target/$target/release/herdr" "$out_dir/$asset"
    chmod 755 "$out_dir/$asset"

    case "$asset" in
        overherdr-linux-*)
            # Same guards the release workflow applies to its Linux artifacts.
            if command -v readelf >/dev/null 2>&1; then
                if readelf -d "$out_dir/$asset" 2>/dev/null | grep -q NEEDED; then
                    echo "error: $asset is not statically linked" >&2
                    exit 1
                fi
            fi
            if command -v nm >/dev/null 2>&1; then
                if nm -u "$out_dir/$asset" 2>/dev/null | grep -qE '(__cxa|GLIBCXX|CXXABI|_ZSt)'; then
                    echo "error: $asset has unresolved C++ runtime symbols" >&2
                    exit 1
                fi
            fi
            ;;
    esac

    echo "    dist/$asset"
}

require_tools

requested=()
if [ "$#" -gt 0 ]; then
    for arg in "$@"; do
        requested+=("$(normalize_asset "$arg")")
    done
else
    requested=("${all_assets[@]}")
fi

skipped=()
for asset in "${requested[@]}"; do
    target_for "$asset" >/dev/null
    case "$asset" in
        overherdr-macos-*)
            if ! ensure_macos_sdk; then
                skipped+=("$asset")
                continue
            fi
            ;;
    esac
    build_asset "$asset"
done

if [ "${#skipped[@]}" -gt 0 ]; then
    echo
    echo "skipped (no macOS SDK): ${skipped[*]}"
    echo "Point SDKROOT at a MacOSX.sdk, or allow the pinned SDK download (OVERHERDR_FETCH_MACOS_SDK=1)."
fi
