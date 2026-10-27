# weft

A tiny shared work graph for agents and the people they work with.

weft is an append-only log of events about tasks, with three rules enforced on write: no dependency cycles, one live claim per task, and only the holder lets go. Anyone can read the log from any point and follow it. What's ready, what's blocked and what closing something would unblock are all worked out on read. It's one Python file with no dependencies beyond the standard library, plus one SQLite table.

It was made for **science projects**, which branch, get reframed and go quiet for months, and where "done" is rarely crisp. So it deliberately has no priorities, assignees, estimates, sprints or "done" state. A task is *closed* with a reason in your own words, and closing it for any reason unblocks whatever needed it.

```sh
pip install git+https://github.com/mkb23/weft    # or just copy weft.py
weft init

weft create "Decide the testbed" --id q1 --await "ask:DECISIONS.md#Q1"
weft create "Build the toy environment" --needs q1 --refs E6
weft ready                     # nothing yet: blocked on q1
weft close q1 --reason decided -m "chose the synthetic world"
weft ready                     # → the build task
weft claim t-3c9e01 --ttl 2h   # a lease that lapses unless renewed
weft release t-3c9e01 -m "resume: oracle done, start on the observer"
weft watch --down q1           # follow everything downstream of q1
```

## Many boxes

One box holds a project's store. Agents on that box use the file directly. Other boxes go through a small server:

```sh
weft serve /srv/weft --host 0.0.0.0     # serves /srv/weft/<project>.db; needs /srv/weft/tokens
# elsewhere:
export WEFT=http://master:7411/myproject WEFT_TOKEN=…
weft watch --since 0                     # replay, then follow live
```

`weft export > .weft/events.jsonl` puts the whole history next to the work, and `weft import` rebuilds a store from it.

## Using it well

These are guidelines for agents, and for the people steering them:

- **A task is an action** that one agent session could do: build, run, analyse, read, write up. Hypotheses, findings and experiments aren't tasks. Tasks point at them with `--refs`.
- **Gates are tasks with `--await`.** A decision, a date or a long job: whoever can tell it has happened closes it. weft never checks.
- **Proceed rather than wait** when there's a clear recommendation. Use a gate only when you really need someone's say.
- **Park by releasing with a resume note**, not by holding a claim.
- **Branch by creating**, and link it `spawned_from`. **Reframe with `supersedes`**, and close the old task with reason `superseded`. Abandoning work is an honest outcome, not a failure.
- **Comment on tasks rather than to people.** A comment on a task waits there for whoever picks it up next, even months later.

See [SPEC.md](SPEC.md) for the event format, the rules, the HTTP API and the conventions clients share.

Tests: `python3 -m unittest`
