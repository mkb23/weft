#!/usr/bin/env python3
"""weft: a tiny shared work graph for agents.

An append-only log of events about tasks, three rules enforced on write,
and a fold that derives everything else. Standard library only. See SPEC.md.
"""
import argparse, contextlib, getpass, json, os, re, secrets, socket, sqlite3, sys, time
import urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

VERBS = ("create", "update", "link", "unlink", "claim", "release", "close", "reopen", "comment")
FIELDS = ("title", "refs", "parent", "await", "data")
DEFAULT_TTL = 3600
CLAIM_STATUSES = ("working", "waiting", "stuck")


class WeftError(Exception):
    pass


# ── time ─────────────────────────────────────────────────────────────────────

def now():
    return datetime.now(timezone.utc)

def stamp(t=None):
    return (t or now()).isoformat(timespec="seconds").replace("+00:00", "Z")

def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def seconds(s):
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    s = str(s)
    return int(float(s[:-1]) * units[s[-1]]) if s[-1] in units else int(s)


# ── the fold: events → graph ─────────────────────────────────────────────────

def blank(id):
    return {"id": id, "title": None, "refs": [], "parent": None, "await": None, "data": {},
            "needs": [], "links": [], "closed": False, "reason": None, "lease": None, "comments": []}

def apply(g, e):
    """Fold one (already checked) event into graph g, a dict of tasks by id."""
    verb, body, t = e["verb"], e["body"], g.get(e["task"])
    if verb == "create":
        t = g[e["task"]] = blank(e["task"])
    if verb in ("create", "update"):
        t.update({k: body[k] for k in FIELDS if k in body and k != "data"})
        t["data"].update(body.get("data", {}))
    elif verb in ("link", "unlink"):
        rel, to = body["rel"], body["to"]
        edges, edge = (t["needs"], to) if rel == "needs" else (t["links"], [rel, to])
        if verb == "link" and edge not in edges:
            edges.append(edge)
        elif verb == "unlink":
            edges.remove(edge)
    elif verb == "claim":   # a claim is also a heartbeat: status and progress persist until changed
        until = parse_ts(e["ts"]) + timedelta(seconds=body.get("ttl", DEFAULT_TTL))
        old = t["lease"] if t["lease"] and t["lease"]["actor"] == e["actor"] else {}
        status = body.get("status") or old.get("status", "working")
        t["lease"] = {"actor": e["actor"], "until": stamp(until), "status": status,
                      "progress": body.get("progress", old.get("progress")),
                      "since": old["since"] if old.get("status") == status else e["ts"]}
    elif verb == "release":
        t["lease"] = None
    elif verb == "close":
        t.update(closed=True, reason=body.get("reason"), lease=None)
    elif verb == "reopen":
        t.update(closed=False, reason=None)
    elif verb == "comment" and t:
        t["comments"].append({"seq": e.get("seq"), "ts": e["ts"], "actor": e["actor"], **body})
    return g

def fold(events, g=None):
    g = {} if g is None else g
    for e in events:
        apply(g, e)
    return g

def holder(t, at=None):
    lease = t["lease"]
    return lease["actor"] if lease and parse_ts(lease["until"]) > (at or now()) else None

def blockers(g, t):
    return [x for x in t["needs"] if not g[x]["closed"]]

def state(g, t):
    if t["closed"]:
        return "closed"
    if holder(t):
        return "claimed"
    if blockers(g, t):
        return "blocked"
    return "awaiting" if t["await"] else "ready"

def walk(g, start, step):
    seen, todo = set(), [start]
    while todo:
        x = todo.pop()
        if x not in seen:
            seen.add(x)
            todo += step(x)
    return seen

def upstream(g, id):
    return walk(g, id, lambda x: g[x]["needs"])

def downstream(g, id):
    return walk(g, id, lambda x: [y for y in g if x in g[y]["needs"]])


# ── the three rules ──────────────────────────────────────────────────────────

def check(g, e):
    verb, body, id = e["verb"], e["body"], e["task"]
    t = g.get(id)
    if verb not in VERBS:
        raise WeftError(f"unknown verb: {verb}")
    if verb == "create":
        if t:
            raise WeftError(f"{id} already exists")
        if not body.get("title"):
            raise WeftError("a new task needs a title")
        return
    if verb == "comment" and id is None:
        return
    if not t:
        raise WeftError(f"no such task: {id}")
    if verb in ("link", "unlink"):
        rel, to = body.get("rel"), body.get("to")
        if not rel or to not in g:
            raise WeftError(f"{verb} needs a rel and an existing target")
        if verb == "unlink" and (to not in t["needs"] if rel == "needs" else [rel, to] not in t["links"]):
            raise WeftError(f"{id} has no {rel} link to {to}")
        if verb == "link" and rel == "needs" and id in upstream(g, to):      # rule 1
            raise WeftError(f"{id} needs {to} would make a cycle")
    if verb in ("claim", "release", "close"):
        who = holder(t)
        if who and who != e["actor"]:                                         # rules 2 and 3
            raise WeftError(f"{id} is claimed by {who} until {t['lease']['until']}")
        if verb == "claim" and body.get("status", "working") not in CLAIM_STATUSES:
            raise WeftError(f"claim status must be one of {', '.join(CLAIM_STATUSES)}")
        if verb == "claim" and t["closed"]:
            raise WeftError(f"{id} is closed ({t['reason']})")


# ── stores: a local SQLite file, or a remote server ──────────────────────────

SCHEMA = """create table if not exists events (
  seq integer primary key autoincrement, ts text not null, actor text not null,
  verb text not null, task text, body text not null)"""

class Local:
    def __init__(self, path):
        self.path = str(path)
        with contextlib.closing(self.connect()) as db:
            db.execute("pragma journal_mode=wal")
            db.execute(SCHEMA)

    def connect(self):
        return sqlite3.connect(self.path, timeout=30, isolation_level=None)

    def read(self, since=0, db=None):
        with contextlib.closing(self.connect()) if db is None else contextlib.nullcontext(db) as db:
            rows = db.execute("select seq, ts, actor, verb, task, body from events where seq > ? order by seq",
                              (since,)).fetchall()
        return [dict(seq=s, ts=ts, actor=a, verb=v, task=t, body=json.loads(b)) for s, ts, a, v, t, b in rows]

    def append(self, events, actor):
        db = self.connect()
        db.execute("begin immediate")
        try:
            g, out = fold(self.read(db=db)), []
            for raw in events:
                e = {"ts": stamp(), "actor": actor, "verb": raw.get("verb"),
                     "task": raw.get("task"), "body": raw.get("body", {})}
                check(g, e)
                e["seq"] = db.execute("insert into events (ts, actor, verb, task, body) values (?,?,?,?,?)",
                                      (e["ts"], actor, e["verb"], e["task"], json.dumps(e["body"]))).lastrowid
                apply(g, e)
                out.append(e)
            db.execute("commit")
            return out
        except BaseException:
            db.execute("rollback")
            raise
        finally:
            db.close()

    def load(self, events):
        """Restore exported events verbatim into an empty store."""
        with contextlib.closing(self.connect()) as db:
            if db.execute("select count(*) from events").fetchone()[0]:
                raise WeftError("import needs an empty store")
            db.executemany("insert into events (ts, actor, verb, task, body) values (?,?,?,?,?)",
                           [(e["ts"], e["actor"], e["verb"], e["task"], json.dumps(e["body"])) for e in events])

    def follow(self, since=0, every=0.5):
        while True:
            for e in self.read(since):
                since = e["seq"]
                yield e
            time.sleep(every)

class Remote:
    def __init__(self, url, token=None):
        self.url, self.token = url.rstrip("/"), token

    def request(self, path, data=None, actor=None):
        headers = {"Content-Type": "application/json", "X-Weft-Actor": actor or ""}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        body = json.dumps(data).encode() if data is not None else None
        try:
            return urllib.request.urlopen(urllib.request.Request(self.url + path, body, headers))
        except urllib.error.HTTPError as err:
            raise WeftError(json.load(err).get("error", str(err)))

    def read(self, since=0):
        return json.load(self.request(f"/events?since={since}"))

    def append(self, events, actor):
        return json.load(self.request("/events", events, actor))

    def follow(self, since=0):
        for line in self.request(f"/events?since={since}&follow=1"):
            if line.startswith(b"data: "):
                yield json.loads(line[6:])

def actor():
    return os.environ.get("WEFT_ACTOR") or f"{getpass.getuser()}@{socket.gethostname().split('.')[0]}"

def open_store():
    target = os.environ.get("WEFT")
    for d in [] if target else [Path.cwd(), *Path.cwd().parents]:
        if (d / ".weft" / "weft.db").exists():
            target = str(d / ".weft" / "weft.db")
        elif (d / ".weft" / "url").exists():
            target = (d / ".weft" / "url").read_text().strip()
        if target:
            break
    if not target:
        raise WeftError("no weft here: run `weft init`, or set WEFT to a db path or a server URL")
    if target.startswith(("http://", "https://")):
        return Remote(target, os.environ.get("WEFT_TOKEN"))
    return Local(target)


# ── the server: POST /<project>/events, GET /<project>/events?since=N[&follow=1]

PROJECT = re.compile(r"[A-Za-z0-9_.-]+")
LOOPBACK = ("127.0.0.1", "localhost", "::1")

def server(root, host="127.0.0.1", port=7411):
    root = Path(root)
    tokens = None
    if (root / "tokens").exists():   # lines of "<token> <actor>"
        tokens = dict(l.split()[:2] for l in (root / "tokens").read_text().splitlines()
                      if l.strip() and not l.startswith("#"))
    elif host not in LOOPBACK:
        raise WeftError(f"refusing to serve beyond loopback without {root / 'tokens'}")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, code, obj):
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def route(self):
            who = (tokens.get(self.headers.get("Authorization", "").removeprefix("Bearer "))
                   if tokens is not None else self.headers.get("X-Weft-Actor") or "anonymous")
            url = urllib.parse.urlparse(self.path)
            name, _, rest = url.path.strip("/").partition("/")
            if not who:
                return self.reply(401, {"error": "unknown token"})
            if rest != "events" or not PROJECT.fullmatch(name) or not (root / f"{name}.db").exists():
                return self.reply(404, {"error": f"no such project: {name}"})
            return who, Local(root / f"{name}.db"), urllib.parse.parse_qs(url.query)

        def do_GET(self):
            if not (r := self.route()):
                return
            _, store, q = r
            since = int(q.get("since", ["0"])[0])
            if "follow" not in q:
                return self.reply(200, store.read(since))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            try:
                for e in store.follow(since):
                    self.wfile.write(f"data: {json.dumps(e)}\n\n".encode())
                    self.wfile.flush()
            except (OSError, sqlite3.Error):   # the client went away, or the store did
                pass

        def do_POST(self):
            if not (r := self.route()):
                return
            who, store, _ = r
            try:
                events = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                self.reply(200, store.append(events, who))
            except (WeftError, ValueError, KeyError, TypeError) as err:
                self.reply(409, {"error": str(err)})

    s = ThreadingHTTPServer((host, port), Handler)
    s.daemon_threads = True
    return s


# ── the CLI ──────────────────────────────────────────────────────────────────

def summary(g, t):
    s = state(g, t)
    lease = t["lease"] or {}
    claim = " · ".join(str(x) for x in (lease.get("actor"), lease.get("status"), lease.get("progress")) if x)
    extra = {"closed": t["reason"], "claimed": claim,
             "blocked": " ".join(blockers(g, t)), "awaiting": t["await"]}.get(s)
    return f"{t['id']:<10} {s:<9} {t['title']}" + (f"  ({extra})" if extra else "")

def matcher(a):
    """A predicate on (graph, event) for `weft watch` filters, by position in the graph."""
    def match(g, e):
        if a.to and e["body"].get("to") == a.to:
            return True
        if not (a.task or a.up or a.down or a.refs or a.parent):
            return not a.to
        t = g.get(e["task"])
        return bool(t) and (e["task"] == a.task
                            or bool(a.up and e["task"] in upstream(g, a.up))
                            or bool(a.down and e["task"] in downstream(g, a.down))
                            or a.refs in t["refs"] or bool(a.parent and t["parent"] == a.parent))
    return match

HELP = """\
weft: a tiny shared work graph for agents and the people they work with.

A project is an append-only log of events about TASKS. Everything you see
(ready, blocked, who holds what) is computed from that log.

TASK      id · title · refs (what it's about) · parent (grouping) · data (JSON)
          await (makes it a GATE: waits on something outside weft; whoever
          can tell it has happened closes it, weft never checks)
EDGES     A needs B   A can't be ready until B is closed (any close reason)
          A <rel> B   any other relation is a label (spawned_from, supersedes…)
STATES    closed    closed, with a reason in your own words
          claimed   someone holds a live lease (claims lapse after --ttl), with a
                    status: working · waiting (paused on something outside) · stuck
          blocked   something it needs isn't closed
          awaiting  a gate, waiting on the outside world
          ready     none of the above: claim it and do it
RULES     no cycles in needs · one live claim per task ·
          only the holder may release or close while the claim is live

AGENT LOOP
  weft ready                          what can I do?
  weft claim T                        it's mine (renew by claiming again)
  weft claim T --status stuck -m "…"  report how it's going: working·waiting·stuck
  weft comment "…" --on T             progress, findings, questions
  weft close T --reason produced      done for now (or abandoned, superseded…)
  weft release T -m "resume: …"       stop without finishing; say how to resume
  weft create "…" --needs G           new work that waits on G
  weft create "Approve X" --await "ask:DECISIONS.md#D4"   a gate on a decision

STORE     $WEFT (db path or server URL), else the nearest .weft/ upward.
          $WEFT_TOKEN for a server, $WEFT_ACTOR names you (default user@host).
OUTPUT    add --json anywhere for machine-readable output.
"""

def main(argv=None):
    fmt = argparse.RawDescriptionHelpFormatter
    p = argparse.ArgumentParser(prog="weft", description=HELP, formatter_class=fmt,
                                epilog="Run `weft COMMAND --help` for details. Full spec: SPEC.md")
    p.add_argument("--json", action="store_true", help="machine-readable output (JSON)")
    sub = p.add_subparsers(dest="cmd", required=True, metavar="COMMAND")
    shared = argparse.ArgumentParser(add_help=False)   # so --json also works after the command
    shared.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="machine-readable output")
    cmd = lambda name, help, example="": sub.add_parser(
        name, help=help, description=help, formatter_class=fmt, parents=[shared],
        epilog=f"example:\n  {example}" if example else None)

    c = cmd("init", "create .weft/ here: a local store, or --url to point at a server",
            "weft init   |   weft init --url http://master:7411/myproject")
    c.add_argument("--url", help="use this server instead of a local store")

    create = cmd("create", "create a task (prints its id)",
            'weft create "Run arm 3" --needs t-1a2b3c --refs H2 --data \'{"cap_usd": 12}\'')
    create.add_argument("title")
    create.add_argument("--id", help="choose the id (default: t-<6 hex>)")
    update = cmd("update", "change a task's fields (data is merged, the rest replaced)",
            'weft update t-1a2b3c --title "Run arm 3 (GPT-4.1-mini)"')
    update.add_argument("task")
    update.add_argument("--title")
    for c in create, update:
        c.add_argument("--refs", action="append", metavar="REF",
                       help="what the task is about: a doc, entity or URL (repeatable)")
        c.add_argument("--parent", metavar="TASK", help="group under another task (doesn't block)")
        c.add_argument("--await", dest="await_", metavar="WHAT",
                       help="make it a gate on something outside weft, e.g. ask:DOC#ID, time:DATE")
        c.add_argument("--data", type=json.loads, metavar="JSON", help="a JSON object merged into data")
    create.add_argument("--needs", action="append", default=[], metavar="TASK",
                                       help="a task this one needs (repeatable)")

    for name, verb in (("link", "add"), ("unlink", "remove")):
        c = cmd(name, f"{verb} an edge: TASK needs OTHER blocks; any other REL is a label",
                f"weft {name} t-2b3c4d needs t-1a2b3c   |   weft {name} t-3c4d5e spawned_from t-2b3c4d")
        c.add_argument("task"); c.add_argument("rel"); c.add_argument("to", metavar="other")

    c = cmd("claim", "claim a task, or (as its holder) renew the claim and report how it's going",
            'weft claim t-1a2b3c --ttl 2h\n'
            '  weft claim t-1a2b3c --status waiting -m "cluster job running, ~3h"\n'
            '  weft claim t-1a2b3c --status stuck -m "results disagree with the simulation"')
    c.add_argument("task"); c.add_argument("--ttl", default="1h", help="lease length: 90s, 30m, 2h, 1d (default 1h)")
    c.add_argument("--status", choices=CLAIM_STATUSES,
                   help="working (default) · waiting: paused on something outside, keeping the claim · "
                        "stuck: needs help. Kept until you change it")
    c.add_argument("--progress", help="free-form, e.g. 60/100 or 'analysis half done'. Kept until changed")
    c.add_argument("-m", "--message", help="comment first (what's happening)")
    c = cmd("release", "let go of a claim without closing; leave a resume note with -m",
            'weft release t-1a2b3c -m "resume: oracle done, start the observer"')
    c.add_argument("task"); c.add_argument("-m", "--message", help="comment first (how to resume)")
    c = cmd("close", "close a task with a reason; anything that needs it unblocks",
            'weft close t-1a2b3c --reason produced -m "results in runs/arm3/summary.md"')
    c.add_argument("task")
    c.add_argument("--reason", required=True, help="a word: produced, abandoned, superseded, approved, no, …")
    c.add_argument("-m", "--message", help="comment first (what came of it)")
    c = cmd("reopen", "reopen a closed task", "weft reopen t-1a2b3c -m \"the result didn't hold\"")
    c.add_argument("task"); c.add_argument("-m", "--message", help="comment first (why)")

    c = cmd("comment", "say something on a task (or to everyone); it waits there for whoever comes next",
            'weft comment "60/100 games done" --on t-1a2b3c --data \'{"spent_usd": 2.1}\'')
    c.add_argument("text"); c.add_argument("--on", dest="task", metavar="TASK", help="the task it's about")
    c.add_argument("--to", help="who it's for: a task, an actor or a role")
    c.add_argument("--re", type=int, metavar="SEQ", help="the event it replies to")
    c.add_argument("--data", type=json.loads, metavar="JSON", help="structured payload")

    c = cmd("ready", "list tasks nobody holds whose needs are all closed", "weft ready --refs H2")
    c.add_argument("--refs", metavar="REF", help="only tasks that refer to REF")
    c.add_argument("--parent", metavar="TASK", help="only tasks grouped under TASK")
    c = cmd("show", "show one task in detail (state, needs, needed by, recent comments), or list all",
            "weft show t-1a2b3c   |   weft --json show")
    c.add_argument("task", nargs="?")
    c = cmd("watch", "print events as they happen, filtered by position in the graph",
            "weft watch --down t-1a2b3c          # everything waiting on this gate\n"
            "  weft watch --since 0 --no-follow     # the whole history")
    c.add_argument("--since", type=int, metavar="SEQ", help="replay after this event (default: only new events)")
    c.add_argument("--task", help="this task only"); c.add_argument("--up", metavar="TASK", help="everything TASK needs")
    c.add_argument("--down", metavar="TASK", help="everything that needs TASK")
    c.add_argument("--refs", metavar="REF", help="tasks that refer to REF")
    c.add_argument("--parent", metavar="TASK", help="tasks grouped under TASK")
    c.add_argument("--to", help="comments addressed to this")
    c.add_argument("--no-follow", action="store_true", help="stop at the end instead of waiting")

    cmd("export", "print the whole log as JSON lines (commit it next to the work)",
        "weft export > .weft/events.jsonl")
    c = cmd("import", "restore exported JSON lines into an empty local store", "weft import .weft/events.jsonl")
    c.add_argument("file")
    c = cmd("serve", "serve DIR/<project>.db to other boxes over HTTP (needs DIR/tokens beyond localhost)",
            "weft serve /srv/weft --host 0.0.0.0   # tokens file: one '<token> <actor>' per line")
    c.add_argument("dir"); c.add_argument("--host", default="127.0.0.1"); c.add_argument("--port", type=int, default=7411)
    a = p.parse_args(argv)
    try:
        run(a)
    except WeftError as err:
        sys.exit(f"weft: {err}")
    except KeyboardInterrupt:
        pass

def run(a):
    out = lambda obj, text: print(json.dumps(obj) if a.json else text)
    if a.cmd == "init":
        d = Path(".weft")
        d.mkdir(exist_ok=True)
        (d / ".gitignore").write_text("weft.db*\n")
        if a.url:
            (d / "url").write_text(a.url + "\n")
        else:
            Local(d / "weft.db")
        return out({"dir": str(d.resolve())}, f"weft initialised in {d.resolve()}")
    if a.cmd == "serve":
        s = server(a.dir, a.host, a.port)
        print(f"weft serving {Path(a.dir).resolve()} on http://{a.host}:{a.port}/<project>/events", file=sys.stderr)
        return s.serve_forever()
    store, me = open_store(), actor()
    if a.cmd == "import":
        if not isinstance(store, Local):
            raise WeftError("import works on a local store")
        return store.load([json.loads(l) for l in Path(a.file).read_text().splitlines() if l.strip()])
    if a.cmd == "export":
        return print("\n".join(json.dumps(e) for e in store.read()))
    if a.cmd == "watch":
        events = store.read()
        since = events[-1]["seq"] if events and a.since is None else (a.since or 0)
        g, match = fold(e for e in events if e["seq"] <= since), matcher(a)
        for e in (store.read(since) if a.no_follow else store.follow(since)):
            if match(apply(g, e), e):
                body = e["body"].get("text") or json.dumps(e["body"])
                out(e, f"{e['seq']:>5} {e['ts']} {e['actor']:<16} {e['verb']:<8} {e['task'] or '-':<10} {body}")
                sys.stdout.flush()
        return
    if a.cmd in ("ready", "show"):
        g = fold(store.read())
        if a.cmd == "show" and a.task:
            if a.task not in g:
                raise WeftError(f"no such task: {a.task}")
            t = dict(g[a.task], state=state(g, g[a.task]),
                     needed_by=[y for y in g if a.task in g[y]["needs"]])
            lines = [summary(g, t)] + [f"  {k}: {v}" for k, v in t.items()
                                       if k not in ("id", "title", "comments", "state") and v not in (None, False, [], {})]
            lines += [f"  #{c['seq']} {c['actor']}: {c.get('text', json.dumps(c.get('data')))}" for c in t["comments"][-5:]]
            return out(t, "\n".join(lines))
        ts = [t for t in g.values() if a.cmd == "show" or (state(g, t) == "ready"
              and (not a.refs or a.refs in t["refs"]) and (not a.parent or t["parent"] == a.parent))]
        return out([dict(t, state=state(g, t)) for t in ts], "\n".join(summary(g, t) for t in ts))

    # everything else writes events
    id = a.task if a.cmd != "create" else (a.id or f"t-{secrets.token_hex(3)}")
    if a.cmd in ("create", "update"):
        body = {k: v for k, v in (("title", a.title), ("refs", a.refs), ("parent", a.parent),
                                  ("await", a.await_), ("data", a.data)) if v is not None}
        events = [{"verb": a.cmd, "task": id, "body": body}]
        events += [{"verb": "link", "task": id, "body": {"rel": "needs", "to": n}} for n in getattr(a, "needs", [])]
    elif a.cmd in ("link", "unlink"):
        events = [{"verb": a.cmd, "task": id, "body": {"rel": a.rel, "to": a.to}}]
    elif a.cmd == "claim":
        body = {"ttl": seconds(a.ttl), "status": a.status, "progress": a.progress}
        events = [{"verb": "claim", "task": id, "body": {k: v for k, v in body.items() if v is not None}}]
    elif a.cmd == "comment":
        body = {k: v for k, v in (("text", a.text), ("to", a.to), ("re", a.re), ("data", a.data)) if v is not None}
        events = [{"verb": "comment", "task": id, "body": body}]
    else:  # release, close, reopen
        events = [{"verb": a.cmd, "task": id, "body": {"reason": a.reason} if a.cmd == "close" else {}}]
    if getattr(a, "message", None):
        events.insert(0, {"verb": "comment", "task": id, "body": {"text": a.message}})
    written = store.append(events, me)
    out(written, id)


if __name__ == "__main__":
    main()
