#!/usr/bin/env python3
"""Regenerate the fork's update manifest (`dist/latest.json`) after a release.

Fork-owned: upstream has never seen this file, so it cannot conflict on a
merge. It deliberately does not reuse `scripts/changelog.py`, which hardcodes
upstream's repo, its `v*` tag prefix, `website/latest.json` and `herdr-*` asset
names -- editing those in place would both conflict with upstream churn and
break upstream's own tests.

The output schema is upstream's, unchanged: the updater deserializes it with
the same code either way. Only the host and the asset names differ.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

BRAND = "overherdr"
TAG_PREFIX = "overherdr-v"
REPO = "thieso2/herdr"
FORK_PATCH_FLOOR = 100

# macOS x86_64 is deliberately not built: this fork ships Apple Silicon only.
ASSET_TARGETS = (
    "linux-x86_64",
    "linux-aarch64",
    "macos-aarch64",
)

VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


class ManifestError(ValueError):
    pass


def parse_version(value: str) -> tuple[int, int, int]:
    """Parse a plain three-component version, matching `update::Version`."""
    match = VERSION_RE.match(value.strip().removeprefix("v"))
    if not match:
        raise ManifestError(
            f"version {value!r} is not three integer components; the updater's "
            "parser rejects suffixes such as '0.7.5-0.1.0'"
        )
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def asset_urls(version: str) -> dict[str, str]:
    tag = f"{TAG_PREFIX}{version}"
    return {
        target: f"https://github.com/{REPO}/releases/download/{tag}/{BRAND}-{target}"
        for target in ASSET_TARGETS
    }


def build_manifest(
    previous: dict, version: str, protocol: int, notes: str
) -> dict:
    """Return the new manifest, archiving the previous release under `releases`."""
    major_minor_patch = parse_version(version)
    if major_minor_patch[2] < FORK_PATCH_FLOOR:
        raise ManifestError(
            f"fork version {version} must offset the patch by {FORK_PATCH_FLOOR} "
            "(upstream 0.7.5 -> fork 0.7.100)"
        )

    previous_version = str(previous.get("version", "0.0.0"))
    if parse_version(previous_version) >= major_minor_patch:
        raise ManifestError(
            f"release {version} is not newer than the manifest's {previous_version}"
        )

    if not notes.strip():
        raise ManifestError("release notes must not be empty")

    releases = dict(previous.get("releases") or {})
    # An empty-notes previous entry is the seed; archiving it would create a
    # phantom release with dead asset URLs.
    if previous_version != "0.0.0" and str(previous.get("notes", "")).strip():
        releases[previous_version] = {
            "notes": previous["notes"],
            "assets": previous.get("assets", {}),
            "protocol": previous.get("protocol"),
        }

    assets = asset_urls(version)
    releases[version] = {"notes": notes, "assets": assets, "protocol": protocol}

    return {
        "version": version,
        "protocol": protocol,
        "notes": notes,
        "assets": assets,
        "releases": dict(sorted(releases.items())),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="release version, e.g. 0.7.100")
    parser.add_argument("--protocol", required=True, type=int)
    parser.add_argument("--notes", required=True, help="release notes body")
    parser.add_argument("--manifest", default="dist/latest.json", type=Path)
    args = parser.parse_args(argv)

    if not args.manifest.exists():
        raise ManifestError(
            f"{args.manifest} is missing; seed it before the first tag"
        )

    previous = json.loads(args.manifest.read_text())
    manifest = build_manifest(previous, args.version, args.protocol, args.notes)
    args.manifest.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"wrote {args.manifest} at {args.version}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ManifestError as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
