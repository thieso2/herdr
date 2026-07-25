# Issue tracker: Local Markdown

Issues and specs (you may know a spec as a PRD) for this repo live as markdown
files under `.local/issues/`. `.local/` is gitignored and locally controlled, so
these files never enter git history — matching the existing `.local/prd/`
convention for PRDs and planning notes.

## Conventions

- One feature per directory: `.local/issues/<feature-slug>/`
- The spec is `.local/issues/<feature-slug>/spec.md`
- Implementation issues are one file per ticket at
  `.local/issues/<feature-slug>/issues/<NN>-<slug>.md`, numbered from `01` —
  never a single combined tickets file
- Triage state is recorded as a `Status:` line near the top of each issue file
  (see `triage-labels.md` for the role strings)
- Comments and conversation history append to the bottom of the file under a
  `## Comments` heading

## When a skill says "publish to the issue tracker"

Create a new file under `.local/issues/<feature-slug>/`, creating the directory
if needed. Never `gh issue create`.

## When a skill says "fetch the relevant ticket"

Read the file at the referenced path. The user will normally pass the path or
the issue number directly.

## Relationship to upstream GitHub issues

`github.com/ogulcancelik/herdr` has GitHub Issues, but the external contributor
guardrail in `AGENTS.md` forbids agents from opening issues or PRs there.

Agents may *read* upstream issues for context:

- `gh issue view <number> --comments`
- `gh issue list --state open --json number,title,body,labels`

Agents may also draft a report following `CONTRIBUTING.md` for a human to file.
Agents must never submit an issue or PR via the GitHub CLI, API, or browser
automation.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a file with one **child** file per ticket.

- **Map**: `.local/issues/<effort>/map.md` — the Notes / Decisions-so-far / Fog
  body.
- **Child ticket**: `.local/issues/<effort>/issues/NN-<slug>.md`, numbered from
  `01`, with the question in the body. A `Type:` line records the ticket type
  (`research`/`prototype`/`grilling`/`task`); a `Status:` line records
  `claimed`/`resolved`.
- **Blocking**: a `Blocked by: NN, NN` line near the top. A ticket is unblocked
  when every file it lists is `resolved`.
- **Frontier**: scan `.local/issues/<effort>/issues/` for files that are open,
  unblocked, and unclaimed; first by number wins.
- **Claim**: set `Status: claimed` and save before any work.
- **Resolve**: append the answer under an `## Answer` heading, set
  `Status: resolved`, then append a context pointer (gist + link) to the map's
  Decisions-so-far in `map.md`.
