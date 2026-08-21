# Issue tracker: Local Markdown (docs/issues/)

Issues and specs for this repo live as markdown files in `docs/issues/`. This is a flat, single-directory convention — not per-feature subdirectories — because the repo currently tracks one product (Adidos Poll App) rather than several independent features.

## Conventions

- One issue per file: `docs/issues/I-0XX-<slug>.md`, numbered sequentially from `001` across the whole repo (not reset per feature)
- The master index is `docs/issues/INDEX.md` — it lists every issue grouped by phase and by epic, with a checkbox per issue
- Each issue file follows a fixed section order: header block (`**Status**`, `**Epic**`, `**Priority**`, `**Estimated Effort**`, `**Depends on**` where relevant), Problem Statement, Solution, User Stories, Implementation Decisions, Acceptance Criteria, Testing Strategy, Out of Scope (when relevant), Related Issues, Implementation Checklist, closing `**Acceptance**:` line
- The header's `**Status**` line (e.g. "Ready for Implementation") tracks spec completeness, not triage state — it is not the same field as the triage role
- Triage state (see `triage-labels.md`) is recorded as a separate `**Triage**:` line directly under the header block, e.g. `**Triage**: ready-for-agent`. Omit this line until an issue has actually been triaged.
- Cross-references between issues are plain text (`- I-005: Vote Acceptance & Queueing (...)`) in the Related Issues section, not markdown links — match this style
- Comments and conversation history append to the bottom of the file under a `## Comments` heading

## When a skill says "publish to the issue tracker"

1. Find the next unused `I-0XX` number by checking the highest number already present in `docs/issues/`
2. Create `docs/issues/I-0XX-<slug>.md` following the section order above
3. Add a checkbox entry to `docs/issues/INDEX.md` under the relevant phase, and to the relevant epic's issue list if one applies (create a new phase/epic section if the work doesn't fit an existing one)

## When a skill says "fetch the relevant ticket"

Read the file at `docs/issues/I-0XX-<slug>.md`. The user will normally pass the number or the filename directly; if only a number is given, resolve the slug via `INDEX.md`.

## Wayfinding operations

Used by `/wayfinder`. This repo hasn't used wayfinder yet — when it does, maps and child tickets live under `docs/issues/wayfinder/<effort>/` so they stay out of the flat `I-0XX` numbering used for implementation tickets.

- **Map**: `docs/issues/wayfinder/<effort>/map.md` — the Notes / Decisions-so-far / Fog body.
- **Child ticket**: `docs/issues/wayfinder/<effort>/issues/NN-<slug>.md`, numbered from `01`, with the question in the body. A `Type:` line records the ticket type (`research`/`prototype`/`grilling`/`task`); a `Status:` line records `claimed`/`resolved`.
- **Blocking**: a `Blocked by: NN, NN` line near the top. A ticket is unblocked when every file it lists is `resolved`.
- **Frontier**: scan `docs/issues/wayfinder/<effort>/issues/` for files that are open, unblocked, and unclaimed; first by number wins.
- **Claim**: set `Status: claimed` and save before any work.
- **Resolve**: append the answer under an `## Answer` heading, set `Status: resolved`, then append a context pointer (gist + link) to the map's Decisions-so-far in `map.md`.
- When a map resolves into a buildable plan, it hands off to `/to-spec` — the resulting spec's tickets go into the normal flat `I-0XX` numbering, not the wayfinder subdirectory.
