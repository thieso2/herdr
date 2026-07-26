#!/bin/sh
# overherdr installer.
#
#   curl -fsSL https://raw.githubusercontent.com/thieso2/herdr/master/install.sh | sh
#
# Reads the fork's own update manifest and installs a binary named `overherdr`.
# No secrets, no package manager, no sudo unless you point INSTALL_DIR at a
# system path.
set -eu

BRAND="overherdr"
MANIFEST_URL="${OVERHERDR_MANIFEST_URL:-https://raw.githubusercontent.com/thieso2/herdr/master/dist/latest.json}"
INSTALL_DIR="${OVERHERDR_INSTALL_DIR:-$HOME/.local/bin}"

die() {
    echo "error: $*" >&2
    exit 1
}

need() {
    command -v "$1" >/dev/null 2>&1 || die "$1 is required but not installed"
}

need uname
need mktemp

if command -v curl >/dev/null 2>&1; then
    fetch() { curl -fsSL "$1"; }
    fetch_to() { curl -fsSL "$1" -o "$2"; }
elif command -v wget >/dev/null 2>&1; then
    fetch() { wget -qO- "$1"; }
    fetch_to() { wget -qO "$2" "$1"; }
else
    die "curl or wget is required"
fi

case "$(uname -s)" in
    Linux) os="linux" ;;
    Darwin) os="macos" ;;
    *) die "unsupported operating system: $(uname -s) (overherdr ships linux and macos)" ;;
esac

case "$(uname -m)" in
    x86_64 | amd64) arch="x86_64" ;;
    arm64 | aarch64) arch="aarch64" ;;
    *) die "unsupported architecture: $(uname -m)" ;;
esac

target="${os}-${arch}"

manifest="$(fetch "$MANIFEST_URL")" || die "could not reach $MANIFEST_URL"
[ -n "$manifest" ] || die "empty manifest at $MANIFEST_URL"

# Pull "version" and the asset URL for this platform without requiring jq.
version="$(printf '%s' "$manifest" | sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"
url="$(printf '%s' "$manifest" \
    | tr ',' '\n' \
    | sed -n "s|.*\"$target\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*|\\1|p" \
    | head -n 1)"

[ -n "$version" ] || die "could not read version from manifest"
if [ -z "$url" ]; then
    die "no $target binary in the manifest (version $version). If this is the seeded manifest, the fork has not published a release yet."
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

echo "downloading $BRAND $version for $target..."
fetch_to "$url" "$tmp/$BRAND" || die "download failed: $url"
[ -s "$tmp/$BRAND" ] || die "downloaded an empty file from $url"
chmod 755 "$tmp/$BRAND"

mkdir -p "$INSTALL_DIR"
mv "$tmp/$BRAND" "$INSTALL_DIR/$BRAND"

echo "installed $BRAND $version to $INSTALL_DIR/$BRAND"

case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *) echo "note: $INSTALL_DIR is not on your PATH; add it to your shell profile" ;;
esac

if [ "$os" = "macos" ]; then
    # curl/wget downloads are not quarantined, but a browser download would be.
    # Clearing the attribute is harmless when it is absent.
    xattr -d com.apple.quarantine "$INSTALL_DIR/$BRAND" 2>/dev/null || true
fi

echo "run: $BRAND"
