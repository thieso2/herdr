# Fork-owned release recipes.
#
# Upstream has never seen this file, so it cannot conflict on a merge. Recipes
# are prefixed `overherdr-` so they can never collide with upstream's.
#
# Remote topology: `origin` is this fork (thieso2/herdr). Upstream, if present
# at all, is a fetch-only `upstream` remote. Set `overherdr_remote` if yours
# differs.
#
# Two landmines these exist to avoid:
#   1. Upstream's `release-publish` recipe pushes the release commit and tag to
#      whatever `origin` is. In a clone that still has origin=upstream that
#      aims straight at ogulcancelik/herdr, so these recipes always name the
#      remote explicitly rather than inheriting it.
#   2. Upstream's release.yml fires on `v*`. The fork tag prefix is
#      `overherdr-v` so the two pipelines can never overlap.
#
# The crate is still named `herdr`, so the crate-name-keyed lock update stays
# verbatim. The release-docs check is skipped: the fork does not publish docs.

overherdr_tag_prefix := "overherdr-v"
overherdr_remote := env_var_or_default("OVERHERDR_REMOTE", "origin")

# Prepare the fork release commit without tagging or pushing (usage: just overherdr-release-prepare 0.7.100)
overherdr-release-prepare version:
    @printf '%s\n' '{{version}}' | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || { \
        echo "error: version must look like 0.7.100 without a v prefix"; \
        exit 1; \
    }
    @patch="$(printf '%s' '{{version}}' | sed 's/.*\.//')"; \
    if [ "$patch" -lt 100 ]; then \
        echo "error: fork versions offset the patch by 100 (upstream 0.7.5 -> fork 0.7.100)"; \
        exit 1; \
    fi
    @if [ -n "$(git status --porcelain)" ]; then \
        echo "error: commit your changes first"; \
        exit 1; \
    fi
    @git fetch {{overherdr_remote}} master --tags
    @if git rev-parse "{{overherdr_tag_prefix}}{{version}}" >/dev/null 2>&1; then \
        echo "error: tag {{overherdr_tag_prefix}}{{version}} already exists"; \
        exit 1; \
    fi
    sed -i.bak 's/^version = ".*"/version = "{{version}}"/' Cargo.toml && rm -f Cargo.toml.bak
    cargo update -p herdr --offline
    just check
    git add Cargo.toml Cargo.lock
    git diff --cached --quiet || git commit -m "release: overherdr {{version}}"
    @echo "overherdr {{version}} prepared. Review, then: just overherdr-release-publish {{version}}"

# Tag and push an already-prepared fork release commit (usage: just overherdr-release-publish 0.7.100)
overherdr-release-publish version:
    @printf '%s\n' '{{version}}' | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || { \
        echo "error: version must look like 0.7.100 without a v prefix"; \
        exit 1; \
    }
    @cargo_version="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1)"; \
    if [ "$cargo_version" != "{{version}}" ]; then \
        echo "error: Cargo.toml says $cargo_version, not {{version}}; run overherdr-release-prepare first"; \
        exit 1; \
    fi
    @if [ -n "$(git status --porcelain)" ]; then \
        echo "error: commit your changes first"; \
        exit 1; \
    fi
    @git fetch {{overherdr_remote}} master --tags
    @remote_head="$(git rev-parse {{overherdr_remote}}/master)"; \
    if ! git merge-base --is-ancestor "$remote_head" HEAD; then \
        echo "error: {{overherdr_remote}}/master is not an ancestor of HEAD; pull or rebase first"; \
        exit 1; \
    fi
    git push {{overherdr_remote}} HEAD:master
    git tag -a {{overherdr_tag_prefix}}{{version}} -m "overherdr {{version}}"
    git push {{overherdr_remote}} {{overherdr_tag_prefix}}{{version}}
    @echo "pushed {{overherdr_tag_prefix}}{{version}}; the overherdr release workflow takes it from here"

# Prepare and publish in one step (usage: just overherdr-release 0.7.100)
overherdr-release version:
    just overherdr-release-prepare {{version}}
    just overherdr-release-publish {{version}}
