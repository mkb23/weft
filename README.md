# weft

A tiny shared work graph for agents and the people they work with.

A weft project is **an append-only log of events about tasks**. Everything else is computed from the log: what's ready, what's blocked, who holds what. It's one Python file with no dependencies beyond the standard library, and one SQLite table.

weft was made for **science**, which branches, gets reframed and goes quiet for months, and where "done" is rarely crisp. So it has no priorities, assignees, estimates or "done" state. A task is *closed* with a reason in your own words.

## The model

**Task:** `id` · `title` · `refs` (what it's about: docs, entities, URLs) · `parent` (grouping, never blocking) · `data` (free JSON, e.g. budgets) · `await` (optional, see gates) · `values` (the latest of whatever has been recorded against it, see below)

**Edges:** `A needs B` means A can't be ready until B is **closed, for any reason**. Any other relation (`spawned_from`, `supersedes`, `informs`) is a label.

**Gates:** a task with `await` waits on something outside weft: a decision, a date, a long job (`ask:DECISIONS.md#D4`, `time:2026-11-03`, `cmd:test -f out/done`). **weft never checks it.** Whoever can tell it has happened closes the gate, and everything that needs it unblocks.

**States** (computed, checked in this order):

| State | Means |
|---|---|
| `closed` | closed, with a reason: produced, abandoned, superseded, approved, no… |
| `claimed` | someone holds a live claim: a lease that lapses after its TTL unless renewed. The holder reports a status: **working** · **waiting** (paused on something outside, keeping the claim) · **stuck** (needs help) |
| `blocked` | something it needs isn't closed |
| `awaiting` | a gate, waiting on the outside world |
| `ready` | none of the above: claim it and do it |

**Rules** (enforced on every write; a refused batch writes nothing):
1. No cycles in `needs`.
2. One live claim per task.
3. While a claim is live, only its holder can release or close the task.

## The commands

| Command | Does |
|---|---|
| `weft ready` | tasks anyone could start now |
| `weft show [T]` | one task in detail (state, needs, needed by, recent comments), or all |
| `weft create "title" [--needs T] [--refs R] [--parent T] [--await W] [--data JSON] [--id ID]` | a new task (prints its id) |
| `weft update T [--title …] [--refs …] [--parent …] [--await …] [--data …]` | change fields (`data` merges) |
| `weft link T needs U` · `weft unlink T needs U` | add or remove an edge (any other rel is a label) |
| `weft claim T [--ttl 2h] [--status working\|waiting\|stuck] [-m "…"]` | take a task. Claim again to renew and say how it's going. The status persists until changed |
| `weft record T NAME=VALUE …` | record values against a task: progress, spend, tokens… (see below) |
| `weft define NAME TYPE [--min] [--max] [--one-of] [--unit] [-d]` · `weft define --strict` · `weft names` | type what the project records · refuse undefined names · list them |
| `weft release T [-m "resume: …"]` | let go without finishing, saying how to resume |
| `weft close T --reason WORD [-m "…"]` · `weft reopen T` | finish (or give up) · undo |
| `weft comment "…" [--on T] [--to WHO] [--re SEQ] [--data JSON]` | say something. It stays on the task for whoever comes next |
| `weft watch [--down T] [--up T] [--task T] [--refs R] [--parent T] [--to WHO] [--since SEQ]` | follow events live, filtered by position in the graph |
| `weft export` · `weft import FILE` | the log as JSON lines, and back |
| `weft init [--url URL]` · `weft serve DIR` | set up a project · share stores with other boxes |

Add `--json` anywhere for machine-readable output. `weft COMMAND --help` gives each command's flags and an example.

## The agent loop

```sh
weft ready                                   # what can I do?
weft claim t-3c9e01 --ttl 2h                 # it's mine
weft record t-3c9e01 progress=0.5 spend_usd=1.80   # how far, how much
weft claim t-3c9e01 --status stuck -m "results disagree with the simulation"
weft close t-3c9e01 --reason produced -m "results in runs/arm3/summary.md"
# or, if you must stop:
weft release t-3c9e01 -m "resume: start from the observer"
# and if you hit a decision you can't make:
weft create "Approve the larger model" --await "ask:DECISIONS.md#D21"   # → t-9a0b1c
weft create "Run with the larger model" --needs t-9a0b1c
```

**Use it well:**
- A task is **an action one agent session could do**. Hypotheses, findings and experiments are not tasks: point at them with `--refs`.
- **Proceed** when there's a clear recommendation. Make a gate only when you need someone's say.
- **Branch** with `create` plus `link NEW spawned_from OLD`. **Reframe** with `supersedes`, then close the old task as `superseded`. Abandoning is an honest outcome.
- Put comments **on tasks**, not to people: a comment waits on its task, even for months.

## Recording

`weft record T progress=0.6 spend_usd=3.75 tokens=182000` stores timestamped values against a task. weft keeps the latest value of each name, which `weft show` displays, and the full history stays in the log. **weft never adds anything up:** sums, rates, roll-ups and budgets are for whoever reads the log (shirka draws them).

Conventions, so that readers can chart without configuration:
- **Record running totals**, not increments (`spend_usd=3.75`, not "+1.25"). Each record then stands alone, and rates come from the history.
- **Put the unit in the name:** `spend_usd`, `tokens`, `gpu_hours`, `games`, `progress` (0–1, or a string like `60/100`).
- **Anyone can record**, attributed: an agent its progress, an orchestrator its spend.
- **Budgets** go in task data: `weft update T --data '{"budget": {"spend_usd": 20}}'`.

**Types are enforced on write, for every agent on every box.** A name nobody has defined takes the type of its first use, so `spend_usd=1.25` then `spend_usd="3.75$"` is refused. A project can be precise with `define`:

```sh
weft define spend_usd number --min 0 --unit USD -d "cumulative spend, recorded by the orchestrator"
weft define progress number --min 0 --max 1
weft define phase string --one-of design,build,run,analyse
weft define --strict           # optional: refuse names nobody has defined
weft names                     # the project's vocabulary; agents should read this first
```

Types: `number` · `integer` · `string` · `boolean`. A definition applies to records from then on, and never rewrites history. Task `data` stays free JSON.

A project that wants cost reporting just agrees on names, and defines them. For example: `spend_usd` and `tokens` by the orchestrator, `progress` by the agent, `budget.spend_usd` on the parent task.

## Where the data lives

`weft init` makes `.weft/weft.db` (gitignored). The CLI finds the store via `$WEFT` (a db path or a server URL), else the nearest `.weft/` above you. `$WEFT_ACTOR` names you (default `user@host`).

**Many boxes:** one box holds a project's store, and agents there use the file directly, with no server needed. Other boxes go through `weft serve DIR` (it serves `DIR/<project>.db`, and needs `DIR/tokens`, one `<token> <actor>` per line, beyond localhost):

```sh
export WEFT=http://master:7411/myproject WEFT_TOKEN=…
weft watch --since 0          # replay everything, then follow live
```

`weft export > .weft/events.jsonl` keeps the whole history next to the work, and `weft import` rebuilds a store from it.

Event format, HTTP API and conventions: [SPEC.md](SPEC.md). Tests: `python3 -m unittest`.
