# weft

A tiny shared work graph for agents and the people they work with.

A weft project is **an append-only log of events about tasks**. Everything else is computed from the log: what's ready, what's blocked, who holds what. It's one Python file with no dependencies beyond the standard library, and one SQLite table.

weft was made for **science**, which branches, gets reframed and goes quiet for months, and where "done" is rarely crisp. So it has no priorities, assignees, estimates or "done" state. A task is *closed* with a reason in your own words.

**Most work needs no task.** Make one when work outlives your session, your box or someone's attention: a job that runs overnight, a hand-off to another agent, a decision you're waiting for, spend that needs approval, or anything that has to survive a long pause.

## The model

**Task:** `id` · `title` · `refs` (what it's about: docs, entities, URLs) · `parent` (grouping) · `data` (a JSON object, e.g. budgets) · `await` (makes it a gate) · `values` (the latest of whatever has been recorded against it)

**Edges:** `A needs B` means A can't be ready until B is **closed, for any reason**. `approved` and `no` both unblock A, so read B's reason before acting on A. Any other relation (`spawned_from`, `supersedes`, `informs`) is a label.

**Gates:** a task with `await` waits on something outside weft: a decision (`ask:DECISIONS.md#D4`), a long job (`cmd:test -f out/done`), a date (`time:2026-11-03`). **weft never checks it, and nobody claims it.** Whoever can tell it has happened closes it, and everything that needs it unblocks.

**Groups:** a task with children (other tasks whose `parent` it is) is blocked until they're all closed, so a group is never mistaken for work.

**States** (computed, checked in this order):

| State | Means |
|---|---|
| `closed` | closed, with a reason: produced, abandoned, superseded, approved, no… |
| `claimed` | someone holds a live claim: a lease that lapses after its TTL unless renewed. The holder reports a status: **working** · **waiting** (a short pause, back soon) · **stuck** (needs help) |
| `blocked` | something it needs, or one of its children, isn't closed |
| `awaiting` | a gate, waiting on the outside world |
| `ready` | none of the above: claim it and do it |

A lapsed claim stays visible (`lapsed: ann · waiting, 2d ago`), so a job that died is never mistaken for work nobody started.

**Rules** (enforced on every write; a refused batch writes nothing):
1. No cycles in `needs`.
2. You can only claim a **ready** task, or renew a claim you hold.
3. While a claim is live, only its holder can release or close the task. The exception is `release --force`, which is recorded, for a holder who has vanished or got stuck.

## The commands

| Command | Does |
|---|---|
| `weft ready` | tasks you could claim now (with the reasons their gates closed) |
| `weft show [T]` | one task in detail (state, needs, children, gates, what points at it, recent comments), or all |
| `weft create "title" [--needs T] [--refs R] [--parent T] [--await W] [--data JSON] [--id ID]` | a new task (prints its id) |
| `weft update T [--title …] [--refs …] [--parent …] [--await …] [--data …]` | change fields (`data` merges) |
| `weft link T needs U` · `weft unlink T needs U` | add or remove an edge (any other rel is a label) |
| `weft claim T [--ttl 2h] [--status working\|waiting\|stuck] [-m "…"]` | take a ready task, or renew yours and say how it's going |
| `weft record T NAME=VALUE …` | record values: progress, spend, tokens… (see below) |
| `weft comment "…" [--on T] [--to WHO] [--re SEQ] [--data JSON]` | say something. It stays on the task for whoever comes next |
| `weft release T [-m "resume: …"] [--force]` | let go without finishing, saying how to resume |
| `weft close T --reason WORD [-m "…"]` · `weft close T --superseded-by U` | finish, or give up · hand T's dependents to U in one step |
| `weft reopen T [-m "why"]` | undo a close |
| `weft define NAME TYPE [--min] [--max] [--unit] [-d]` · `weft names` | type what the project records · list it (read this first) |
| `weft watch [--down T] [--up T] [--task T] [--refs R] [--parent T] [--to WHO] [--since SEQ] [--no-follow]` | follow events, filtered by position in the graph |
| `weft export` · `weft import FILE` | the log as JSON lines, and back |
| `weft init [--url URL]` · `weft serve DIR` | set up a project · share stores with other boxes |

Add `--json` anywhere: results and errors come back as JSON. `weft COMMAND --help` shows each command's flags and an example.

## The agent loop

```sh
export WEFT_ACTOR=arm3-agent                 # your own name: agents sharing a name share claims
weft ready                                   # what can I do?
weft claim t-3c9e01 --ttl 2h                 # it's mine
weft record t-3c9e01 progress=0.5 spend_usd=1.80   # how far (0-1), how much (running totals)
weft claim t-3c9e01 --status stuck -m "results disagree with the simulation"
weft close t-3c9e01 --reason produced -m "results in runs/arm3/summary.md"
# or, if you must stop:
weft release t-3c9e01 -m "resume: start from the observer"
# a decision you can't make, and the work that waits on it:
weft create "Approve the larger model" --await "ask:DECISIONS.md#D21"   # → t-9a0b1c
weft create "Run with the larger model" --needs t-9a0b1c
# a long job: a gate, not a claim held for hours
weft create "Sweep finishes" --await "cmd:test -f runs/sweep/done" --id sweep-done
```

**Use it well:**
- A task is **an action one agent session could do**. Hypotheses, findings and experiments are not tasks: point at them with `--refs`. Don't create tasks in advance for candidate experiments; they stay prose until someone picks them up.
- **Proceed** when there's a clear recommendation. Make a gate only when you need someone's say.
- **Defer** by adding an `await` (`weft update T --await ask:ROADMAP.md#later`), not by closing.
- **Branch** with `create` plus `link NEW spawned_from OLD`. **Reframe** with `close OLD --superseded-by NEW`. Abandoning is an honest outcome.
- Put comments **on tasks**, not to people: a comment waits on its task, even for months.
- Models under test never get weft credentials. weft coordinates the people and agents running experiments, not the subjects of them.

## Recording

`weft record T progress=0.6 spend_usd=3.75 tokens=182000` stores timestamped values against a task. weft keeps the latest value of each name, which `weft show` displays, and the full history stays in the log. **weft never adds anything up:** sums, rates, roll-ups and budgets are for whoever reads the log (shirka draws them).

- **Record running totals**, not increments (`spend_usd=3.75`, not "+1.25"). Each record then stands alone, and rates come from the history.
- **Put the unit in the name:** `spend_usd`, `tokens`, `gpu_hours`. **`progress` is a number from 0 to 1.**
- **Record cost and progress, never results.** Results belong in documents.
- **Anyone can record**, attributed: an agent its progress, an orchestrator its spend.
- **Budgets** go in task data: `weft update T --data '{"budget": {"spend_usd": 20}}'`.

**Types are enforced on write, for every agent on every box.** A name nobody has defined takes the type of its first use, so `spend_usd=1.25` then `spend_usd="3.75$"` is refused. A project can be precise:

```sh
weft define spend_usd number --min 0 --unit USD -d "cumulative spend, recorded by the orchestrator"
weft define progress number --min 0 --max 1
weft names                     # the project's vocabulary: agents should read this first
```

Types: `number` · `integer` · `string` · `boolean`. A definition applies to records from then on, and never rewrites history.

## Where the data lives

`weft init` makes `.weft/weft.db` (gitignored) at the repo's main checkout, so worktrees and subdirectories all share it. It refuses to make a second store. The CLI finds the store via `$WEFT` (a db path or a server URL), else the nearest `.weft/` above you, else the repo's main checkout.

**Many boxes:** one box holds a project's store, and agents there use the file directly, with no server needed. Other boxes go through `weft serve DIR`. It serves `DIR/<project>.db`, and beyond localhost it needs `DIR/tokens`, one `<token> <actor>` per line.

```sh
export WEFT=http://master:7411/myproject WEFT_TOKEN=…
weft watch --since 0          # replay everything, then follow live
```

`weft export > .weft/events.jsonl` keeps the whole history next to the work, and `weft import` rebuilds a store from it.

Event format, HTTP API and conventions: [SPEC.md](SPEC.md). Tests: `python3 -m unittest`.
