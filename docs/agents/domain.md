# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root, or
- **`CONTEXT-MAP.md`** at the repo root if it exists — it points at one `CONTEXT.md` per context. Read each one relevant to the topic.
- **`docs/adr/`** — read ADRs that touch the area you're about to work in. In multi-context repos, also check `src/<context>/docs/adr/` for context-scoped decisions.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

This repo also has `DOMAIN_MODEL.md` at the root, written before this skill setup existed — it holds the same kind of content `CONTEXT.md` would (entities, aggregates, bounded contexts, ubiquitous language glossary). Treat it as the current source of truth until it's superseded by a `CONTEXT.md` created through `/domain-modeling`; read it alongside (or instead of) `CONTEXT.md` until that happens.

## File structure

Single-context repo (most repos, including this one):

```
/
├── CONTEXT.md              ← not yet created; DOMAIN_MODEL.md fills this role for now
├── docs/adr/                ← not yet created
└── src/                      ← not yet created; repo is spec-only so far
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `DOMAIN_MODEL.md` (or `CONTEXT.md`, once it exists). Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (event-sourced orders) — but worth reopening because…_

No ADRs exist yet in this repo (`docs/adr/` is not yet created) — the closest equivalent today is the numbered "Implementation Decisions" in `SPECIFICATION.md`. Treat contradicting one of those the same way: surface it explicitly rather than silently overriding.
