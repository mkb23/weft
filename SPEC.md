# weft spec, v1

weft is **an append-only log of events about tasks, with three rules enforced on write, that anyone can read from any point and follow.** Everything else is derived on read, or done by clients.

## Events

Every event has the same shape:

```json
{"seq": 42, "ts": "2026-10-27T21:40:00Z", "actor": "ann", "verb": "claim", "task": "t-7f3a91", "body": {"ttl": 3600}}
```

| Field | Set by | Meaning |
|---|---|---|
| `seq` | the store | Position in this project's log, strictly increasing. Kept by export and import, so `re` references survive |
| `ts` | the store | UTC time, to the second, when the store accepted the event |
| `actor` | the store | Who wrote it. Through a server it comes from the bearer token, never from the request |
| `verb` | the writer | One of the eleven verbs below |
| `task` | the writer | A non-empty string id, or null for `define` and for a `comment` on no task |
| `body` | the writer | A JSON object of the verb's fields. Unknown fields are stored, and the fold ignores them |

## The eleven verbs

| Verb | Body | Effect |
|---|---|---|
| `create` | `title`, and optionally `refs`, `parent`, `await`, `data` | A new task. The writer chooses the id (the CLI uses `t-` plus 6 hex digits) |
| `update` | any of `title`, `refs`, `parent`, `await`, `data` | Fields are replaced, except `data`, which is merged key by key |
| `link` | `rel`, `to` | Adds an edge. **Only `rel: "needs"` and `rel: "uses"` have meaning**: `needs` means this task can't be ready until `to` is closed; `uses` means it holds a unit of resource `to` while claimed. Any other `rel` (a lowercase word) is a label |
| `unlink` | `rel`, `to` | Removes that edge |
| `claim` | `ttl` (seconds, 1 s to 7 days, default 3600), optional `status` | A lease until `ts + ttl`. Claiming again renews it, so a claim is the holder's heartbeat. `status` is `working` (a new claim's default), `waiting` (a short pause) or `stuck` (needs help). It persists across renewals until changed. With `task: null` and `next: {refs?, parent?}`, the store claims the oldest ready task that fits, inside the write, so workers fanning out never collide; the log records an ordinary claim |
| `release` | optional `force: true` | Ends the lease. `force` releases someone else's live claim, and the event records who did it |
| `close` | `reason` (a non-empty string) | The task is over: *produced*, *abandoned*, *superseded*, *approved*, *no*… Ends any lease |
| `reopen` | — | The task is open again |
| `comment` | `text` and/or `data`, optional `to`, `re` | Says something. `to` addresses a task, an actor or a role; `re` is the `seq` it replies to; `data` is a JSON object |
| `record` | `values`: `{name: value}` | Each name (`[a-z][a-z0-9_.]*`) takes its latest value on the task. Values are finite numbers, strings or booleans. The history stays in the log. **weft never aggregates.** Anyone can record |
| `define` | `name`, `type` (`number`, `integer`, `string`, `boolean`), optional `min`, `max`, `unit`, `description` | Declares a recorded name's type, replacing any earlier declaration. It applies to later records only |

### Task fields

| Field | Meaning |
|---|---|
| `title` | A short description |
| `refs` | Strings naming what the task is about: documents, entities, URLs. weft never interprets them |
| `parent` | Another task's id. **A parent is blocked until its children are closed**, so a group is never claimable work |
| `capacity` | A whole number ≥ 1 that makes the task a **resource**: a box, a GPU, a licence. Tasks that `uses` it hold one unit each while claimed; claiming the resource itself is an explicit hold, for long jobs with no task claim. A resource is never work: `ready` and `claim --next` never offer it |
| `for` | Who should do the task: an actor or a role. `claim --next` skips it for anyone else, and `ready` labels it. A plain `claim` still works, so a person can pair with an agent |
| `await` | An opaque string that makes the task a **gate**, a wait on the world (`ask:docs/DECISIONS.md#D4`, `cmd:test -f out/done`, `time:2026-11-03`). **weft never evaluates it.** Whoever can tell, closes it. A gate can't be claimed |
| `data` | A JSON object, e.g. `{"budget": {"spend_usd": 20}}` |

## The three rules

A store applies each batch of events in one transaction. Every event is checked against the graph folded from everything before it, including earlier events in the same batch. If any event fails, nothing is written.

1. **No cycles in `needs`.** `link A needs B` is refused if `A` is already upstream of `B`, or if `A = B`.
2. **Claim only what's ready.** A `claim` succeeds only on a **ready** task, or as a renewal by the actor who holds the live claim. That excludes tasks that are claimed by someone else, blocked, awaiting (gates) or closed. A task is blocked, among other things, while a resource it `uses` is full; holds by the claimer don't count, so your own hold never blocks you. Claiming a resource itself needs it **free** for you, and no live claim on it by someone else.
3. **Only the holder lets go.** While a claim is live, only its holder can `release` or `close` the task. The exception is `release` with `force`, which is recorded.

**Validation.** Shapes are checked too, because one malformed event would otherwise sit in the log for ever:
- Ids are non-empty strings, and bodies are objects.
- `title`, `parent`, `await` and `for` are strings; `capacity` is a whole number ≥ 1, and a task can't have both `capacity` and `await`; `refs` is a list of strings; `data` and `comment.data` are objects.
- A `parent` must exist, and can't be the task itself.
- `link` needs an existing target, and `unlink` an existing edge. `uses` must point at a resource.
- Relations that look like a misspelt `needs` are refused (`need`, `depends_on`, `blocks`…).
- You can't `close` a closed task (reopen it first), `reopen` an open one, or `release` an unclaimed one.
- Comment metadata (`seq`, `ts`, `actor`) can't be overridden from the body.

**Types.** Every recorded value is checked against its name's `define`, or else against the type of the name's first recorded use (`number`, `string` or `boolean`). Integers count as numbers. Non-finite numbers (NaN, ±∞) are always refused. Errors name the expected type, so a writer can correct itself: `progress is number ≥ 0 ≤ 1, got "60%"`.

## Derived on read

A fold is a pure function from events to a graph of tasks. From it:

| State | When |
|---|---|
| **closed** | its latest close/reopen was a close |
| **free** / **full** | for a resource (a task with `capacity`) only: whether its live holds are below its capacity. A resource is never ready, claimed or blocked |
| **claimed** | it has a live lease (`now < until`). The lease carries `actor`, `until`, `status` and `since` (when the status last changed) |
| **blocked** | a task it `needs`, or one of its children, isn't closed; or a resource it `uses` is full or closed |
| **awaiting** | it has an `await`, and nothing else stops it |
| **ready** | none of the above: anyone can claim it |

The states are checked in that order.

**Any close reason satisfies `needs`.** That means `approved` and `no` both unblock a dependent task. Readers, and especially dispatchers, must read the reasons, which `show` reports as `gates: {id: reason}` and the CLI summary prints inline (`d21: no`). Use `no`, `abandoned` or `moot` for outcomes that should stop dependent work, and close or re-gate that work.

Also derived on read:
- **upstream / downstream**: everything a task needs, or that needs it, transitively. Downstream is what closing a gate releases.
- **children, needed_by, linked_from**: what points at a task. This is how a superseded task shows what replaced it.
- **values**: the latest recorded value of each name. **names**: each name's type, defined or from first use.
- **holds**: a resource's live holds are its own lease (an explicit hold, for a long job) and the leases on open tasks that `use` it. `show` reports `load` and `holders: [{actor, via, until, status}]`, lapsed ones included, so "held by X, lapsed 3h ago" is visible. A dead job frees the resource when its lease lapses. An open gate never holds a resource, so a forgotten gate can't wedge one.
- **lapsed claims**: a lease past `until` no longer counts, and it stays visible as "lapsed: who · status, how long ago". A dead job is never mistaken for work nobody started.

## Storage

One SQLite table per project:

```sql
create table events (seq integer primary key autoincrement, ts text not null, actor text not null,
                     verb text not null, task text, body text not null);
```

`weft export` writes the log as JSON lines in the event shape above. `weft import` restores an export into an **empty** store in one transaction, keeping `seq`, `ts` and `actor` and re-checking every event. An export is the portable record of a project: commit it next to the work, and any store can be rebuilt from it.

**Cost.** Every write folds the whole log, while holding the write lock. That takes about 0.02 s at 10k events and 0.2 s at 100k, which is fine for research projects. Treat it as a known limit, not an accident.

## Access

There's **one store per project**, on one box (the master).

- **Box-local** clients open the file directly; no server needs to be running. SQLite's `begin immediate` serialises writers, so the rules hold across processes. Don't put the file on a network filesystem.
- **Remote** clients use HTTP to `weft serve DIR`, which serves `DIR/<project>.db`:

| Request | Response |
|---|---|
| `POST /<project>/events`, a JSON array of `{verb, task, body}` (≤ 1 MB) | `200` with the stored events · `409 {"error"}` when a rule or check refused it (nothing written) · `400` for a malformed request · `413` if it's too large |
| `GET /<project>/events?since=N` | `200` with the events after `seq` N |
| `GET /<project>/events?since=N&follow=1` | `text/event-stream`: one `data: <event>` per event (the backlog, then live), and a `: ping` comment every 15 s while idle |
| any, with a bad token or an unknown project | `401` / `404` |

**Auth:** `DIR/tokens` holds lines of `<token> <actor>`, and requests send `Authorization: Bearer <token>`. Without a tokens file the server binds only to loopback and takes the actor from `X-Weft-Actor`. Tokens travel in plain HTTP, so use a private network or a TLS proxy.

**Clocks:** the rules judge every event at its own `ts`: now for a live write, and the original time for an import, so an export always re-imports. When *reading*, a lease's state is computed with the reader's clock. The master's clock is the one that enforces the rules, so a remote reader with a skewed clock may *show* a different state but can't *break* a rule.

**Finding the store** (CLI): `$WEFT` (a db path or a server URL); otherwise the nearest `.weft/` (holding `weft.db` or `url`) walking up from the current directory; otherwise the one at the git repo's main checkout. Worktrees and clones' subdirectories therefore share one store, and `weft init` refuses to make a second one. The bearer token comes from `$WEFT_TOKEN`, else `~/.config/weft/tokens` (`$WEFT_TOKENS` overrides the path): lines of `<url-prefix> <token>`, longest matching prefix wins. **`$WEFT_ACTOR` names you, and every agent should set its own**: agents sharing an actor share claims. The default is `user@host`.

## Conventions (not rules)

Shapes that clients agree on. weft stores them but doesn't check them.

| Where | Convention |
|---|---|
| `await` prefixes | `ask:<doc>#<id>` (a decision held elsewhere, e.g. a shirka ask) · `cmd:<shell>` (long jobs and fan-in: true when done) · `time:<ISO date>` |
| close reasons | `produced` · `abandoned` · `superseded` · `approved` · `no` · `moot` · `deferred`, or any word |
| `comment.data` | `{"resume": "…"}` with a `release` (what to do when it's picked up again) · `{"artifact": "path/or/url"}` · `{"ephemeral": true}` (clients may skip it) |
| `record` names | **Running totals, not increments** (`spend_usd=3.75`), so each record stands alone and rates come from the history. **Units in names:** `spend_usd`, `tokens`, `gpu_hours`. **`progress` is a number from 0 to 1.** Record cost and progress, **never results**: results belong in documents |
| `task.data` | `{"budget": {"spend_usd": 20}}` sets a budget per recorded name, and `{"billing": "…"}` names a spend ledger's code |
| `link` rels | `needs` (blocking) · `uses` (holds a resource while claimed) · `informs` (a soft input: "proceeding unless this gate steers us"; it never blocks) · `spawned_from` · `supersedes` |

## Versioning

This is v1. New verbs and body fields are additive, and a reader ignores what it doesn't know. A change to the meaning of an existing verb or rule would be v2.
