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
| BIRD benchmark — execution accuracy | [bird-benchmark.md](bird-benchmark.md) | **active** — the only one |
| Database selection / `source_database` scoping | [database-selection-design.md](database-selection-design.md) | shipped |
| Tenant-scoped registry | [tenant-scoped-registry.md](tenant-scoped-registry.md) | shipped |
| Schema hints hardening | [schema-hints-hardening.md](schema-hints-hardening.md) | shipped |
| Bug log | [bug-log.md](bug-log.md) | *not an initiative* — a rolling record, never "finished" |

## Statuses

| Status | Meaning |
|---|---|
| `active` | Being worked now. Should be rare — ideally one at a time. |
| `shipped` | Done and verified in code. The file survives as design reference. |
| `superseded` | Replaced by another document. Do not work from it. |
| `parked` | Deliberately paused. Must say **why**, and what would restart it. |

## When an initiative ends

**Distil — don't archive.** A finished initiative holds two kinds of content
with very different lifespans:

- **Durable design knowledge** — why MAX not SUM, why the composite FK, the
  verified sqlglot API facts. Valuable indefinitely.
- **Process residue** — task lists, checkboxes, what shipped when. Dead weight,
  and *actively harmful* once stale.

So on completion: set the status, **strip the task tracking**, and keep the
decisions, rationale and gotchas. The file stops being a plan and becomes the
subsystem's design reference.

This is not theoretical. Ten historical plan documents in `docs/` and
`docs/superpowers/plans/` carried **205 unchecked `- [ ]` boxes for work that
had entirely shipped**. Anyone grepping for open tasks would have found a
backlog that did not exist — and nearly did. They are now marked
`SUPERSEDED` at the top.

**Promote what is repo-wide.** Test: *"will this bite someone who isn't working
on this initiative?"* If yes, it belongs in `CLAUDE.md` (as the FastAPI
exception-handler gotchas do). If it is subsystem-specific, it stays here.

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
