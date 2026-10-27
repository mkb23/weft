# weft spec, v1

weft is **an append-only log of events about tasks, with three rules enforced on write, that anyone can read from any point and follow.** Everything else is derived on read, or done by clients.

## Events

Every event has the same shape:

```json
{"seq": 42, "ts": "2026-10-27T21:40:00Z", "actor": "ann@box", "verb": "claim", "task": "t-7f3a91", "body": {"ttl": 3600}}
```

| Field | Set by | Meaning |
|---|---|---|
| `seq` | the store | Position in this project's log. Strictly increasing, with no gaps that clients rely on |
| `ts` | the store | UTC time the store accepted the event |
| `actor` | the store | Who wrote it. With a server, it comes from the bearer token and never from the request body |
| `verb` | the writer | One of the nine verbs below |
| `task` | the writer | The task id. Can be null only for a `comment` addressed to everyone |
| `body` | the writer | Verb-specific fields. Unknown fields are kept and ignored |

## The nine verbs

| Verb | Body | Effect on the task |
|---|---|---|
| `create` | `title`, and optionally `refs`, `parent`, `await`, `data` | A new task. The writer chooses the id (the CLI uses `t-` plus 6 hex digits) |
| `update` | any of `title`, `refs`, `parent`, `await`, `data` | Fields are replaced, except `data`, which is merged key by key |
| `link` | `rel`, `to` | Adds an edge. **Only `rel: "needs"` has meaning**: this task can't be ready until `to` is closed. Any other `rel` is a label |
| `unlink` | `rel`, `to` | Removes that edge |
| `claim` | `ttl` (seconds, default 3600), and optionally `status`, `progress` | A lease on the task until `ts + ttl`. Claiming again renews it, so a claim is also the holder's heartbeat. `status` is `working` (default), `waiting` (paused on something outside, keeping the claim) or `stuck` (needs help). `progress` is a free string. Both persist across renewals until changed. A new holder starts at `working` |
| `release` | — | Ends the lease |
| `close` | `reason` | The task is over. `reason` is a free word: *produced*, *abandoned*, *superseded*, *approved*, *no*… Ends any lease |
| `reopen` | — | The task is open again |
| `comment` | any of `text`, `to`, `re`, `data` | Says something. `to` addresses a task, an actor or a role, `re` is the `seq` being replied to, and `data` is free JSON |

### Task fields

| Field | Meaning |
|---|---|
| `title` | A short description |
| `refs` | Strings naming what the task is about, such as documents, entities or URLs. weft never interprets them |
| `parent` | Another task's id, for grouping and roll-ups. It doesn't block anything |
| `await` | An opaque string that makes the task a **gate**, something that waits on the world (`ask:docs/DECISIONS.md#D4`, `time:2026-11-03`, `cmd:test -f out/done`). **weft never evaluates it.** Whoever can tell that it's satisfied closes the task |
| `data` | Free JSON, e.g. `{"cap_usd": 12, "est_usd": 10}` |

## The three rules

A store applies each batch of events in one transaction. It checks every event against the graph folded from everything before it, including earlier events in the same batch. If any event fails, the whole batch is refused.

1. **No cycles in `needs`.** `link A needs B` is refused if `A` is already upstream of `B`, or if `A = B`.
2. **One live claim.** A `claim` is refused while a lease held by someone else is live (`now < until`). A closed task can't be claimed.
3. **Only the holder lets go.** While a lease is live, only its holder can `release` or `close` the task.

There is also basic validation: a claim `status` must be `working`, `waiting` or `stuck`; `create` needs a title and a fresh id; every other verb needs an existing task (except a `comment` with no task); `link` needs an existing target; and `unlink` needs an existing edge.

## Derived on read

A fold is a pure function from events to a graph of tasks. From it:

| State | When |
|---|---|
| **closed** | its latest close/reopen was a close |
| **claimed** | it has a live lease. The lease carries `actor`, `until`, `status`, `progress` and `since` (when the status last changed) |
| **blocked** | some task it `needs` isn't closed |
| **awaiting** | it has an `await` and nothing else stops it |
| **ready** | none of the above: anyone can claim it |

The states are checked in that order. **Any close reason satisfies `needs`.** A dependent task sees *why* its blocker closed by reading the reason and its comments, so "approved" and "no" both unblock it and it reacts accordingly.

Also derived on read:
- **upstream**: everything a task needs, transitively.
- **downstream**: everything that needs a task, transitively. This is what closing a gate would unblock.
- **lapsed leases**: a lease is ignored once `until` has passed. There's no "lapsed" event.

## Storage

One SQLite table per project:

```sql
create table events (seq integer primary key autoincrement, ts text, actor text, verb text, task text, body text);
```

`weft export` writes the log as JSON lines in the event shape above. `weft import` restores an export into an empty store. An export is the portable record of a project: commit it next to the work, and any store can be rebuilt from it.

## Access

There's **one master store per project**: a box that holds the SQLite file.

- **Box-local** clients open the file directly. No server needs to be running. SQLite's `begin immediate` serialises writers, so the rules hold across processes.
- **Remote** clients use HTTP to `weft serve DIR`, which serves `DIR/<project>.db`:

| Request | Response |
|---|---|
| `POST /<project>/events` with a JSON array of `{verb, task, body}` | `200` and the stored events, or `409 {"error": …}` (nothing written) |
| `GET /<project>/events?since=N` | `200` and the events with `seq > N` |
| `GET /<project>/events?since=N&follow=1` | `text/event-stream`, one `data: <event>` per event: first the backlog, then live |

**Auth:** `DIR/tokens` holds lines of `<token> <actor>`, and requests send `Authorization: Bearer <token>`. Without a tokens file the server only binds to loopback, and it takes the actor from `X-Weft-Actor`.

**Finding the store** (CLI): `$WEFT` (a db path or a server URL), otherwise the nearest `.weft/weft.db` or `.weft/url` walking up from the current directory. `$WEFT_TOKEN` holds the bearer token, and `$WEFT_ACTOR` names a box-local writer (default `user@host`).

## Conventions (not rules)

These are the shapes clients agree on. weft stores them but doesn't check them.

| Where | Convention |
|---|---|
| `await` prefixes | `ask:<doc>#<id>` (a decision held elsewhere), `time:<ISO date>`, `cmd:<shell>`, `human:<what>` |
| `comment.data` | `{"resume": "…"}` with a `release` (what to do when picked up again) · `{"spent_usd": 1.2}` · `{"progress": "60/100"}` · `{"artifact": "path/or/url"}` · `{"ephemeral": true}` (clients may skip it in summaries and exports) |
| `task.data` | `{"cap_usd": …, "est_usd": …}` for budgets, and `{"billing": "…"}` for a spend ledger's code |
| `link` rels | `needs` (blocking) · `spawned_from` · `supersedes` · `informs` |

## Versioning

This is v1. New verbs or body fields are additive, and a reader ignores what it doesn't know. A change to the meaning of an existing verb or rule would be v2.
