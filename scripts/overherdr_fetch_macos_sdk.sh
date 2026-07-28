#!/usr/bin/env bash
# Fork-owned macOS SDK fetcher for local cross-builds.
#
# zig ships only libSystem.tbd, but herdr's macOS build links the Carbon and
# CoreFoundation frameworks and rustc pulls in libobjc, so cross-compiling
# aarch64-apple-darwin from Linux needs a real MacOSX.sdk. This downloads a
# pinned, checksummed SDK tarball into a cache directory and prints its path.
#
# Apple's SDK is licensed for use on Apple-branded hardware. Fetching it here
# is a local developer convenience; the release workflow builds the macOS asset
# on a real macOS runner and does not use this script.
#
# Usage:
#   scripts/overherdr_fetch_macos_sdk.sh          # ensure + print SDK path
#   OVERHERDR_MACOS_SDK_DIR=/somewhere ...        # override cache location
#
# Called automatically by scripts/overherdr_build_dist.sh when a macOS target
# is requested and no SDKROOT/DEVELOPER_DIR is set.

set -euo pipefail

sdk_version="15.5"
sdk_sha256="c15cf0f3f17d714d1aa5a642da8e118db53d79429eb015771ba816aa7c6c1cbd"
sdk_url="https://github.com/joseluisq/macosx-sdks/releases/download/${sdk_version}/MacOSX${sdk_version}.sdk.tar.xz"

cache_dir="${OVERHERDR_MACOS_SDK_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/overherdr/macos-sdk}"
sdk_root="$cache_dir/MacOSX${sdk_version}.sdk"
tarball="$cache_dir/MacOSX${sdk_version}.sdk.tar.xz"

# Already unpacked: nothing to do.
if [ -f "$sdk_root/SDKSettings.plist" ] || [ -f "$sdk_root/SDKSettings.json" ]; then
    echo "$sdk_root"
    exit 0
fi

mkdir -p "$cache_dir"

verify_tarball() {
    [ -f "$tarball" ] || return 1
    local actual
    actual="$(sha256sum "$tarball" | cut -d' ' -f1)"
    [ "$actual" = "$sdk_sha256" ]
}

if ! verify_tarball; then
    echo "fetching MacOSX${sdk_version}.sdk (~100 MB) into $cache_dir" >&2
    curl -fsSL --retry 3 -o "$tarball.part" "$sdk_url"
    mv "$tarball.part" "$tarball"
    if ! verify_tarball; then
        echo "error: checksum mismatch for $tarball; refusing to use it" >&2
        rm -f "$tarball"
        exit 1
    fi
fi

echo "unpacking MacOSX${sdk_version}.sdk" >&2
rm -rf "$sdk_root.part"
mkdir -p "$sdk_root.part"
tar -xJf "$tarball" -C "$sdk_root.part" --strip-components=1
rm -rf "$sdk_root"
mv "$sdk_root.part" "$sdk_root"

echo "$sdk_root"
