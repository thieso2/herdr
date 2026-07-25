# Issue tracker: GitHub (fork `thieso2/herdr`)

Issues and PRDs for this repo live as GitHub issues **on the fork
`thieso2/herdr`**. Use the `gh` CLI for all operations.

> **Warning — never target upstream.** In this clone, `origin` points at the
> upstream repo `ogulcancelik/herdr`, whose project guardrail forbids agents
> from opening issues. `gh` infers the repo from `origin` by default, so **do
> not rely on remote inference**. Every `gh issue`, `gh pr`, and issue-related
> `gh api` call must explicitly target the fork with `--repo thieso2/herdr`
> (or by exporting `GH_REPO=thieso2/herdr` for the command).

## Conventions

- **Create an issue**: `gh issue create --repo thieso2/herdr --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view <number> --repo thieso2/herdr --comments`, filtering comments by `jq` and also fetching labels.
- **List issues**: `gh issue list --repo thieso2/herdr --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --repo thieso2/herdr --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --repo thieso2/herdr --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --repo thieso2/herdr --comment "..."`

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)_

When set to `yes`, PRs run through the same labels and states as issues, using the `gh pr` equivalents (always with `--repo thieso2/herdr`):

- **Read a PR**: `gh pr view <number> --repo thieso2/herdr --comments` and `gh pr diff <number> --repo thieso2/herdr` for the diff.
- **List external PRs for triage**: `gh pr list --repo thieso2/herdr --state open --json number,title,body,labels,author,authorAssociation,comments` then keep only `authorAssociation` of `CONTRIBUTOR`, `FIRST_TIME_CONTRIBUTOR`, or `NONE` (drop `OWNER`/`MEMBER`/`COLLABORATOR`).
- **Comment / label / close**: `gh pr comment`, `gh pr edit --add-label`/`--remove-label`, `gh pr close`.

GitHub shares one number space across issues and PRs, so a bare `#42` may be either — resolve with `gh pr view 42 --repo thieso2/herdr` and fall back to `gh issue view 42 --repo thieso2/herdr`.

## When a skill says "publish to the issue tracker"

Create a GitHub issue on `thieso2/herdr`.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --repo thieso2/herdr --comments`.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a single issue with **child** issues as tickets. All commands target `thieso2/herdr`.

- **Map**: a single issue labelled `wayfinder:map`, holding the Notes / Decisions-so-far / Fog body. `gh issue create --repo thieso2/herdr --label wayfinder:map`.
- **Child ticket**: an issue linked to the map as a GitHub sub-issue (`gh api` on the sub-issues endpoint). Where sub-issues aren't enabled, add the child to a task list in the map body and put `Part of #<map>` at the top of the child body. Labels: `wayfinder:<type>` (`research`/`prototype`/`grilling`/`task`). Once claimed, the ticket is assigned to the driving dev.
- **Blocking**: GitHub's **native issue dependencies** — the canonical, UI-visible representation. Add an edge with `gh api --method POST repos/thieso2/herdr/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`, where `<blocker-db-id>` is the blocker's numeric **database id** (`gh api repos/thieso2/herdr/issues/<n> --jq .id`, _not_ the `#number` or `node_id`). GitHub reports `issue_dependencies_summary.blocked_by` (open blockers only — the live gate). Where dependencies aren't available, fall back to a `Blocked by: #<n>, #<n>` line at the top of the child body. A ticket is unblocked when every blocker is closed.
- **Frontier query**: list the map's open children (`gh issue list --repo thieso2/herdr --state open`, scoped to the map's sub-issues / task list), drop any with an open blocker (`issue_dependencies_summary.blocked_by > 0`, or an open issue in the `Blocked by` line) or an assignee; first in map order wins.
- **Claim**: `gh issue edit <n> --repo thieso2/herdr --add-assignee @me` — the session's first write.
- **Resolve**: `gh issue comment <n> --repo thieso2/herdr --body "<answer>"`, then `gh issue close <n> --repo thieso2/herdr`, then append a context pointer (gist + link) to the map's Decisions-so-far.
