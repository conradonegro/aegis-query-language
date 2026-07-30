# Initiatives

One file per initiative. Everything about a piece of work — status, next
action, decisions taken, gotchas, artifacts — lives in its own file here.

## Why this exists

Context should be loaded by *what you're working on*, not by what happened to
be important last month. Read the one file for your initiative; ignore the
rest. Nothing here is loaded automatically.

Two failure modes this replaces:

- **State in memory goes stale silently.** Two initiatives below were recorded
  as "READY TO IMPLEMENT" and "IN-PROGRESS (on hold)" when both had in fact
  shipped. A file in the repo sits next to the code it describes and shows up
  in diffs.
- **One loud initiative crowds out the rest.** BIRD benchmark state used to be
  flagged "START HERE" in always-loaded memory, so every unrelated session
  paid for 518 lines about it.

## Index

| Initiative | File | Status |
|---|---|---|
| BIRD benchmark — execution accuracy | [bird-benchmark.md](bird-benchmark.md) | **active** |
| Database selection / `source_database` scoping | [database-selection-design.md](database-selection-design.md) | shipped — design reference |
| Tenant-scoped registry | [tenant-scoped-registry.md](tenant-scoped-registry.md) | shipped |
| Schema hints hardening | [schema-hints-hardening.md](schema-hints-hardening.md) | shipped |
| Bug log | [bug-log.md](bug-log.md) | rolling record |

## Conventions

- **Status is the first thing in the file**, and it must be true. If you
  finish something, change it in the same commit.
- Record *decisions and their rationale*, not a narrative of what happened.
  The next reader needs to know why the choice was made, so they can tell
  whether it still applies.
- Record **hidden coupling** explicitly: what looks like a local change but
  isn't. This is the most valuable thing in any of these files.
- Keep figures with their provenance — measured, when, and by what. An
  unattributed number gets treated as fact and repeated.
- When an initiative is finished, leave the file. Set its status and let it
  serve as design reference.

## Adding one

Create `docs/initiatives/<name>.md`, add a row to the table above, and add a
line to the router in the project's memory index so a cold session can find
it. Keep the router line to one line — it is loaded in every session.
