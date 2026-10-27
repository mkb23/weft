#!/usr/bin/env python3
"""weft: a tiny shared work graph for agents.

An append-only log of events about tasks, three rules enforced on write,
and a fold that derives everything else. Standard library only. See SPEC.md.
"""
import argparse, contextlib, getpass, json, math, os, re, secrets, socket, sqlite3, subprocess, sys, time
import urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

VERBS = ("create", "update", "link", "unlink", "claim", "release", "close", "reopen", "comment", "record", "define")
TYPES = {"number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
         "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
         "string": lambda v: isinstance(v, str), "boolean": lambda v: isinstance(v, bool)}
CLAIM_STATUSES = ("working", "waiting", "stuck")
DEFAULT_TTL, MAX_TTL = 3600, 7 * 86400
NAME = re.compile(r"[a-z][a-z0-9_.]*")
LOOKS_LIKE_NEEDS = {"need", "needed", "require", "requires", "depends", "depends_on", "blocked_by", "blocks", "after"}


class WeftError(Exception):
    pass


# ── time ─────────────────────────────────────────────────────────────────────

def now():
    return datetime.now(timezone.utc)

def stamp(t=None):
    return (t or now()).isoformat(timespec="seconds").replace("+00:00", "Z")

def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def ago(ts):
    s = int((now() - parse_ts(ts)).total_seconds())
    return f"{s // 86400}d" if s >= 86400 else f"{s // 3600}h" if s >= 3600 else f"{s // 60}m"


# ── the fold: events → graph ─────────────────────────────────────────────────

class Graph(dict):
    """Tasks by id, plus the types of the names recorded in this project."""
    def __init__(self):
        super().__init__()
        self.names = {}

def blank(id):
    return {"id": id, "title": None, "refs": [], "parent": None, "await": None, "data": {},
            "needs": [], "links": [], "closed": False, "reason": None, "lease": None, "comments": [], "values": {}}

def type_of(v):
    return "boolean" if isinstance(v, bool) else "number" if TYPES["number"](v) else "string"

def apply(g, e):
    """Fold one (already checked) event into graph g."""
    verb, body, t = e["verb"], e["body"], g.get(e["task"])
    if verb == "create":
        t = g[e["task"]] = blank(e["task"])
    if verb in ("create", "update"):
        t.update({k: body[k] for k in ("title", "refs", "parent", "await") if k in body})
        t["data"].update(body.get("data", {}))
    elif verb in ("link", "unlink"):
        edges, edge = (t["needs"], body["to"]) if body["rel"] == "needs" else (t["links"], [body["rel"], body["to"]])
        if verb == "link" and edge not in edges:
            edges.append(edge)
        elif verb == "unlink":
            edges.remove(edge)
    elif verb == "claim":   # a claim is also a heartbeat: its status persists until changed
        until = parse_ts(e["ts"]) + timedelta(seconds=body.get("ttl", DEFAULT_TTL))
        old = t["lease"] if t["lease"] and t["lease"]["actor"] == e["actor"] else {}
        status = body.get("status") or old.get("status", "working")
        t["lease"] = {"actor": e["actor"], "until": stamp(until), "status": status,
                      "since": old["since"] if old.get("status") == status else e["ts"]}
    elif verb == "release":
        t["lease"] = None
    elif verb == "close":
        t.update(closed=True, reason=body["reason"], lease=None)
    elif verb == "reopen":
        t.update(closed=False, reason=None)
    elif verb == "record":   # the latest value per name; the history stays in the log
        t["values"].update(body["values"])
        for k, v in body["values"].items():   # a name nobody defined takes the type of its first use
            g.names.setdefault(k, {"type": type_of(v), "by": "first use"})
    elif verb == "define":
        g.names[body["name"]] = {k: v for k, v in body.items() if k != "name"} | {"by": "define"}
    elif verb == "comment" and t:
        t["comments"].append({**body, "seq": e.get("seq"), "ts": e["ts"], "actor": e["actor"]})
    return g

def fold(events, g=None):
    g = Graph() if g is None else g
    for e in events:
        apply(g, e)
    return g

def holder(t, at=None):
    """Who holds t's live claim at time `at` (default: now)."""
    lease = t["lease"]
    return lease["actor"] if lease and parse_ts(lease["until"]) > (at or now()) else None

def children(g, id):
    return [y for y in g if g[y]["parent"] == id]

def blockers(g, t):
    """What stops t: needs that aren't closed, and (for a group) children that aren't."""
    return [x for x in t["needs"] + children(g, t["id"]) if not g[x]["closed"]]

def state(g, t, at=None):
    if t["closed"]:
        return "closed"
    if holder(t, at):
        return "claimed"
    if blockers(g, t):
        return "blocked"
    return "awaiting" if t["await"] else "ready"

def walk(start, step):
    seen, todo = set(), [start]
    while todo:
        x = todo.pop()
        if x not in seen:
            seen.add(x)
            todo += step(x)
    return seen

def upstream(g, id):
    return walk(id, lambda x: g[x]["needs"])

def downstream(g, id):
    return walk(id, lambda x: [y for y in g if x in g[y]["needs"]])


# ── the three rules, and the shape of every event ────────────────────────────

def check(g, e):
    verb, body, id, who = e.get("verb"), e.get("body"), e.get("task"), e.get("actor")
    try:   # every event is judged at its own time: now for a live write, then for an import
        at = parse_ts(e["ts"])
    except (KeyError, TypeError, ValueError):
        raise WeftError("an event needs a ts like 2026-10-27T21:40:00Z")
    if verb not in VERBS:
        raise WeftError(f"unknown verb: {verb!r}; verbs are {', '.join(VERBS)}")
    if not isinstance(body, dict):
        raise WeftError("an event body must be a JSON object")
    if verb == "define":
        if id is not None:
            raise WeftError("define takes no task")
        return check_define(body)
    if verb == "comment" and id is None:
        return check_comment(body)
    if not isinstance(id, str) or not id.strip():
        raise WeftError("a task id must be a non-empty string")
    t = g.get(id)
    if verb == "create":
        if t:
            raise WeftError(f"{id} already exists")
        if not body.get("title"):
            raise WeftError("a new task needs a title")
    elif not t:
        raise WeftError(f"no such task: {id}")
    if verb in ("create", "update"):
        check_fields(g, id, body)
    elif verb in ("link", "unlink"):
        rel, to = body.get("rel"), body.get("to")
        if not isinstance(rel, str) or not NAME.fullmatch(rel):
            raise WeftError("a relation is a lowercase word: needs, spawned_from, supersedes, informs…")
        if rel in LOOKS_LIKE_NEEDS:
            raise WeftError(f"did you mean `needs`? (`{id} needs X` means X must close before {id} is ready)")
        if to not in g:
            raise WeftError(f"no such task: {to}")
        if verb == "unlink" and (to not in t["needs"] if rel == "needs" else [rel, to] not in t["links"]):
            raise WeftError(f"{id} has no {rel} link to {to}")
        if verb == "link" and rel == "needs" and id in upstream(g, to):            # rule 1
            raise WeftError(f"{id} needs {to} would make a cycle")
    elif verb == "claim":
        ttl = body.get("ttl", DEFAULT_TTL)
        if not TYPES["number"](ttl) or not 0 < ttl <= MAX_TTL:
            raise WeftError("ttl must be between 1 second and 7 days")
        if body.get("status", "working") not in CLAIM_STATUSES:
            raise WeftError(f"claim status must be one of {', '.join(CLAIM_STATUSES)}")
        if holder(t, at) != who:                                                       # rule 2: renew, or claim a ready task
            if holder(t, at):
                raise WeftError(f"{id} is claimed by {holder(t, at)} until {t['lease']['until']}")
            if state(g, t, at) != "ready":
                why = {"closed": f"it's closed ({t['reason']})", "awaiting": "it's a gate: whoever can tell, closes it",
                       "blocked": f"it's blocked by {' '.join(blockers(g, t))}"}[state(g, t, at)]
                raise WeftError(f"can't claim {id}: {why}")
    elif verb in ("release", "close"):
        if holder(t, at) and holder(t, at) != who and not (verb == "release" and body.get("force")):   # rule 3
            raise WeftError(f"{id} is claimed by {holder(t, at)} until {t['lease']['until']}"
                            + (" (a stuck or vanished holder can be released with --force)" if verb == "release" else ""))
        if verb == "release" and not holder(t, at):
            raise WeftError(f"{id} isn't claimed")
        if verb == "close" and t["closed"]:
            raise WeftError(f"{id} is already closed ({t['reason']}); reopen it first")
        if verb == "close" and not (isinstance(body.get("reason"), str) and body["reason"].strip()):
            raise WeftError("close needs a reason: produced, abandoned, superseded, approved, no, …")
    elif verb == "reopen" and not t["closed"]:
        raise WeftError(f"{id} is already open")
    elif verb == "comment":
        check_comment(body)
    elif verb == "record":
        values = body.get("values")
        if not isinstance(values, dict) or not values or not all(NAME.fullmatch(k) for k in values):
            raise WeftError("record needs NAME=VALUE pairs, names lowercase like spend_usd or progress")
        for k, v in values.items():
            check_value(g, k, v)

def check_fields(g, id, body):
    for k in ("title", "await", "parent"):
        if k in body and not isinstance(body[k], str):
            raise WeftError(f"{k} must be a string")
    if "refs" in body and not (isinstance(body["refs"], list) and all(isinstance(r, str) for r in body["refs"])):
        raise WeftError("refs must be a list of strings")
    if "data" in body and not isinstance(body["data"], dict):
        raise WeftError("data must be a JSON object")
    if "parent" in body and (body["parent"] not in g or body["parent"] == id):
        raise WeftError(f"no such parent task: {body['parent']}")

def check_comment(body):
    if not any(k in body for k in ("text", "data")):
        raise WeftError("a comment needs text or data")
    for k, kind in (("text", str), ("to", str), ("re", int), ("data", dict)):
        if k in body and not isinstance(body[k], kind):
            raise WeftError(f"comment {k} must be a {kind.__name__}")

def describe(d):
    bits = [d["type"]]
    bits += [f"≥ {d['min']:g}"] if "min" in d else []
    bits += [f"≤ {d['max']:g}"] if "max" in d else []
    bits += [f"({d['unit']})"] if "unit" in d else []
    return " ".join(bits)

def check_value(g, k, v):
    if not (TYPES["number"](v) or isinstance(v, (str, bool))) or (TYPES["number"](v) and not math.isfinite(v)):
        raise WeftError(f"{k}: a recorded value must be a finite number, a string or a boolean")
    d = g.names.get(k)
    if d and not (TYPES[d["type"]](v) and not (TYPES["number"](v) and not d.get("min", v) <= v <= d.get("max", v))):
        raise WeftError(f"{k} is {describe(d)}, got {json.dumps(v)} (see `weft names`)")

def check_define(body):
    if not NAME.fullmatch(str(body.get("name"))) or body.get("type") not in TYPES:
        raise WeftError(f"define needs a lowercase name like spend_usd and a type: {', '.join(TYPES)}")
    if any(k in body and not (TYPES["number"](body[k]) and math.isfinite(body[k])) for k in ("min", "max")):
        raise WeftError("min and max must be numbers")
    if body.get("min", -math.inf) > body.get("max", math.inf):
        raise WeftError("min is above max")


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

    def write(self, events, stamped):
        """Check and insert events in one transaction; all or nothing."""
        db = self.connect()
        db.execute("begin immediate")
        try:
            g, out = fold(self.read(db=db)), []
            if stamped and g:
                raise WeftError("import needs an empty store")
            for e in events:
                check(g, e)
                e["seq"] = db.execute("insert into events (seq, ts, actor, verb, task, body) values (?,?,?,?,?,?)",
                                      (e.get("seq"), e["ts"], e["actor"], e["verb"], e["task"],
                                       json.dumps(e["body"]))).lastrowid
                apply(g, e)
                out.append(e)
            db.execute("commit")
            return out
        except BaseException:
            db.execute("rollback")
            raise
        finally:
            db.close()

    def append(self, events, actor):
        return self.write([{"ts": stamp(), "actor": actor, "verb": e.get("verb"), "task": e.get("task"),
                            "body": e.get("body", {})} for e in events], stamped=False)

    def load(self, events):
        """Restore an export (seq, ts and actor kept) into an empty store, checking every event."""
        return self.write(events, stamped=True)

    def follow(self, since=0, every=0.5, idle=15):
        """Yield events after `since` as they arrive; yield None after `idle` quiet seconds (a keepalive)."""
        quiet = 0.0
        while True:
            events = self.read(since)
            for e in events:
                since = e["seq"]
                yield e
            quiet = 0.0 if events else quiet + every
            if quiet >= idle:
                quiet = 0.0
                yield None
            time.sleep(every)

class Remote:
    def __init__(self, url, token=None):
        self.url, self.token = url.rstrip("/"), token

    def request(self, path, data=None, actor=None, timeout=30):
        headers = {"Content-Type": "application/json", "X-Weft-Actor": actor or ""}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        body = json.dumps(data).encode() if data is not None else None
        try:
            return urllib.request.urlopen(urllib.request.Request(self.url + path, body, headers), timeout=timeout)
        except urllib.error.HTTPError as err:
            raise WeftError(json.load(err).get("error", str(err)))
        except (urllib.error.URLError, OSError) as err:
            raise WeftError(f"can't reach {self.url}: {err}")

    def read(self, since=0):
        return json.load(self.request(f"/events?since={since}"))

    def append(self, events, actor):
        return json.load(self.request("/events", events, actor))

    def follow(self, since=0):
        for line in self.request(f"/events?since={since}&follow=1", timeout=60):   # the server pings every 15s
            if line.startswith(b"data: "):
                yield json.loads(line[6:])

def actor():
    return os.environ.get("WEFT_ACTOR") or f"{getpass.getuser()}@{socket.gethostname().split('.')[0]}"

def repo_root():
    """The main checkout of the current git repo, so worktrees and clones' subdirectories share one store."""
    try:
        common = subprocess.run(["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
                                capture_output=True, text=True, timeout=5).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None
    return Path(common).parent if common.endswith(".git") else None

def find_store():
    """$WEFT, else the nearest .weft/ above here, else the one at the repo's main checkout."""
    if os.environ.get("WEFT"):
        return os.environ["WEFT"]
    root = repo_root()
    for d in [Path.cwd(), *Path.cwd().parents] + ([root] if root else []):
        if (d / ".weft" / "weft.db").exists():
            return str(d / ".weft" / "weft.db")
        if (d / ".weft" / "url").exists():
            return (d / ".weft" / "url").read_text().strip()
    return None

def open_store():
    target = find_store()
    if not target:
        raise WeftError("no weft here: run `weft init`, or set WEFT to a db path or a server URL")
    if target.startswith(("http://", "https://")):
        return Remote(target, os.environ.get("WEFT_TOKEN"))
    return Local(target)


# ── the server: POST /<project>/events, GET /<project>/events?since=N[&follow=1]

PROJECT = re.compile(r"[A-Za-z0-9_.-]+")
LOOPBACK = ("127.0.0.1", "localhost", "::1")
MAX_BODY = 1 << 20

def server(root, host="127.0.0.1", port=7411):
    root, stores = Path(root), {}
    tokens = None
    if (root / "tokens").exists():   # lines of "<token> <actor>"
        tokens = {}
        for line in (root / "tokens").read_text().splitlines():
            parts = line.split()
            if len(parts) >= 2 and not line.startswith("#"):
                tokens[parts[0]] = parts[1]
    elif host not in LOOPBACK:
        raise WeftError(f"refusing to serve beyond loopback without {root / 'tokens'}")

    class Handler(BaseHTTPRequestHandler):
        timeout = 60   # seconds a client may stall a request

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
            if name not in stores:
                stores[name] = Local(root / f"{name}.db")
            return who, stores[name], urllib.parse.parse_qs(url.query)

        def do_GET(self):
            if not (r := self.route()):
                return
            _, store, q = r
            try:
                since = int(q.get("since", ["0"])[0])
            except ValueError:
                return self.reply(400, {"error": "since must be an integer"})
            if "follow" not in q:
                return self.reply(200, store.read(since))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            try:
                for e in store.follow(since):
                    self.wfile.write(b": ping\n\n" if e is None else f"data: {json.dumps(e)}\n\n".encode())
                    self.wfile.flush()
            except (OSError, sqlite3.Error):   # the client went away, or the store did
                pass

        def do_POST(self):
            if not (r := self.route()):
                return
            who, store, _ = r
            try:
                size = int(self.headers.get("Content-Length", 0))
                if size > MAX_BODY:
                    return self.reply(413, {"error": f"a batch is at most {MAX_BODY} bytes"})
                events = json.loads(self.rfile.read(size))
                if not isinstance(events, list) or not all(isinstance(e, dict) for e in events):
                    return self.reply(400, {"error": "POST a JSON array of {verb, task, body} objects"})
                self.reply(200, store.append(events, who))
            except WeftError as err:
                self.reply(409, {"error": str(err)})
            except ValueError as err:
                self.reply(400, {"error": f"bad request: {err}"})

    s = ThreadingHTTPServer((host, port), Handler)
    s.daemon_threads = True
    return s


# ── the CLI ──────────────────────────────────────────────────────────────────

def json_object(text):
    try:
        value = json.loads(text)
    except ValueError:
        value = None
    if not isinstance(value, dict):
        raise argparse.ArgumentTypeError(f"must be a JSON object, like '{{\"budget\": 20}}', got {text!r}")
    return value

def duration(text):
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd]?)", text.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"must be a duration like 90s, 30m, 2h or 1d, got {text!r}")
    return int(float(m[1]) * {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[m[2]])

def parse_value(pair):
    name, eq, raw = pair.partition("=")
    if not eq:
        raise WeftError(f"expected NAME=VALUE, got {pair!r} (the task comes first: weft record TASK NAME=VALUE …)")
    try:
        value = json.loads(raw)
    except ValueError:
        value = raw
    return name, value if isinstance(value, (int, float, str, bool)) else raw

def summary(g, t):
    """One line: id, state, title, and whatever explains the state."""
    s, lease = state(g, t), t["lease"]
    extra = {"closed": t["reason"], "blocked": " ".join(blockers(g, t)), "awaiting": t["await"],
             "claimed": lease and f"{lease['actor']} · {lease['status']}"}.get(s)
    if s != "claimed" and lease and not t["closed"]:
        extra = " · ".join(filter(None, [extra, f"lapsed: {lease['actor']} · {lease['status']}, {ago(lease['until'])} ago"]))
    gates = [f"{x}: {g[x]['reason']}" for x in t["needs"] if g[x]["closed"] and g[x]["await"]]
    extra = " · ".join(filter(None, [extra] + gates))
    return f"{t['id']:<10} {s:<9} {t['title']}" + (f"  ({extra})" if extra else "")

def detail(g, t):
    """Everything about one task, for `show`, including what points at it."""
    return dict(t, state=state(g, t), children=children(g, t["id"]),
                needed_by=[y for y in g if t["id"] in g[y]["needs"]],
                linked_from=[[rel, y] for y in g for rel, to in g[y]["links"] if to == t["id"]],
                gates={x: g[x]["reason"] for x in t["needs"] if g[x]["closed"]})

def matcher(a):
    """A predicate on (graph, event) for `weft watch` filters, by position in the graph."""
    def match(g, e):
        if a.to and e["verb"] == "comment" and e["body"].get("to") == a.to:
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
(ready, blocked, who holds what) is computed from that log. Most work needs no
task: make one when work outlives your session, your box, or someone's attention.

TASK      id · title · refs (what it's about) · parent (grouping) · data (JSON)
          values (latest of what's been recorded: progress, spend_usd, tokens…)
          await (makes it a GATE: waits on something outside weft; whoever
          can tell it has happened closes it, weft never checks)
EDGES     A needs B   A can't be ready until B is closed, for ANY reason: approved
                      and no both unblock A, so read B's reason before acting
          A <rel> B   any other relation is a label (spawned_from, supersedes…)
STATES    closed    closed, with a reason in your own words
          claimed   someone holds a live claim (it lapses after --ttl), with a
                    status: working · waiting (short pause) · stuck (needs help)
          blocked   something it needs, or one of its children, isn't closed
          awaiting  a gate, waiting on the outside world
          ready     none of the above: claim it and do it
RULES     no cycles in needs · you can only claim a ready task (or renew your
          own claim) · only the holder can release or close a claimed task

AGENT LOOP
  weft ready                          what can I do?
  weft claim T                        it's mine (renew by claiming again)
  weft claim T --status stuck -m "…"  how it's going: working · waiting · stuck
  weft record T progress=0.6 spend_usd=3.75   how far (0-1), how much (totals)
  weft comment "…" --on T             findings, questions
  weft close T --reason produced      done (or abandoned, superseded, no…)
  weft release T -m "resume: …"       stop without finishing; say how to resume
  weft create "…" --needs G           new work that waits on G
  weft create "Approve X" --await "ask:DECISIONS.md#D4"   a gate on a decision
  weft create "Run sweep" --await "cmd:test -f out/done"  a long job: a gate

YOU       set WEFT_ACTOR to your own name; agents sharing a name share claims.
STORE     $WEFT (db path or server URL), else the nearest .weft/ upward, else
          the repo's main checkout. $WEFT_TOKEN authenticates to a server.
OUTPUT    add --json anywhere: results and errors become JSON.
"""

def main(argv=None):
    fmt = argparse.RawDescriptionHelpFormatter
    p = argparse.ArgumentParser(prog="weft", description=HELP, formatter_class=fmt,
                                epilog="Run `weft COMMAND --help` for details. Full spec: SPEC.md")
    p.add_argument("--json", action="store_true", help="machine-readable output (JSON)")
    sub = p.add_subparsers(dest="cmd", required=True, metavar="COMMAND")
    shared = argparse.ArgumentParser(add_help=False)   # so --json also works after the command
    shared.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help=argparse.SUPPRESS)

    def cmd(name, help, example="", message=None):
        c = sub.add_parser(name, help=help, description=help, formatter_class=fmt, parents=[shared],
                           epilog=f"example:\n  {example}" if example else None)
        if message:
            c.add_argument("-m", "--message", help=f"comment first ({message})")
        return c

    c = cmd("init", "create the project's .weft/ (at the repo's main checkout), or --url to use a server",
            "weft init   |   weft init --url http://master:7411/myproject")
    c.add_argument("--url", help="use this server instead of a local store")

    create = cmd("create", "create a task (prints its id)",
                 'weft create "Run arm 3" --needs t-1a2b3c --refs H2 --data \'{"budget": {"spend_usd": 12}}\'')
    create.add_argument("title")
    create.add_argument("--id", help="choose the id (default: t-<6 hex>)")
    update = cmd("update", "change a task's fields (data is merged, the rest replaced)",
                 'weft update t-1a2b3c --title "Run arm 3 (GPT-4.1-mini)"')
    update.add_argument("task")
    update.add_argument("--title")
    for c in create, update:
        c.add_argument("--refs", action="append", metavar="REF",
                       help="what the task is about: a doc, entity or URL (repeatable)")
        c.add_argument("--parent", metavar="TASK", help="group under another task, which stays blocked until its children close")
        c.add_argument("--await", dest="await_", metavar="WHAT",
                       help="make it a gate on something outside weft: ask:DOC#ID, cmd:SHELL, time:DATE")
        c.add_argument("--data", type=json_object, metavar="JSON", help="a JSON object merged into data")
    create.add_argument("--needs", action="append", default=[], metavar="TASK", help="a task this one needs (repeatable)")

    for name, verb in (("link", "add"), ("unlink", "remove")):
        c = cmd(name, f"{verb} an edge. `T needs OTHER` blocks T until OTHER closes; any other REL is a label",
                f"weft {name} t-2b3c4d needs t-1a2b3c   |   weft {name} t-3c4d5e spawned_from t-2b3c4d")
        c.add_argument("task"); c.add_argument("rel"); c.add_argument("to", metavar="other")

    c = cmd("claim", "claim a ready task, or renew your own claim and say how it's going",
            'weft claim t-1a2b3c --ttl 2h\n'
            '  weft claim t-1a2b3c --status stuck -m "results disagree with the simulation"',
            message="what's happening")
    c.add_argument("task")
    c.add_argument("--ttl", type=duration, default=DEFAULT_TTL, help="how long until it lapses: 90s, 30m, 2h, 1d (default 1h; at most 7d)")
    c.add_argument("--status", choices=CLAIM_STATUSES,
                   help="working (a new claim's default) · waiting: a short pause, you'll be back · "
                        "stuck: needs help. Kept across renewals until you change it")
    c = cmd("release", "let go of a claim without closing it; say how to resume",
            'weft release t-1a2b3c -m "resume: oracle done, start the observer"\n'
            '  weft release t-1a2b3c --force -m "holder vanished; taking over"', message="how to resume")
    c.add_argument("task"); c.add_argument("--force", action="store_true", help="release someone else's claim (recorded)")
    c = cmd("close", "close a task with a reason; anything that needs it unblocks",
            'weft close t-1a2b3c --reason produced -m "results in runs/arm3/summary.md"\n'
            "  weft close t-1a2b3c --superseded-by t-4d5e6f   # move its dependents to the new task", message="what came of it")
    c.add_argument("task")
    c.add_argument("--reason", help="a word: produced, abandoned, superseded, approved, no, …")
    c.add_argument("--superseded-by", metavar="TASK", help="close as superseded and move its dependents onto TASK")
    c = cmd("reopen", "reopen a closed task", "weft reopen t-1a2b3c -m \"the result didn't hold\"", message="why")
    c.add_argument("task")

    c = cmd("comment", "say something on a task (or to everyone); it waits there for whoever comes next",
            'weft comment "halfway; the oracle works" --on t-1a2b3c')
    c.add_argument("text"); c.add_argument("--on", dest="task", metavar="TASK", help="the task it's about")
    c.add_argument("--to", help="who it's for: a task, an actor or a role")
    c.add_argument("--re", type=int, metavar="SEQ", help="the event it replies to")
    c.add_argument("--data", type=json_object, metavar="JSON", help="structured payload")

    c = cmd("record", "record values against a task: progress, spend, tokens… (weft stores them, never adds them up)",
            "weft record t-1a2b3c progress=0.6 spend_usd=3.75 tokens=182000\n"
            "  # running totals, units in the name, progress 0-1; results belong in docs, not here", message="")
    c.add_argument("task"); c.add_argument("values", nargs="+", metavar="NAME=VALUE")
    c = cmd("define", "declare a recorded name's type, range and unit (otherwise its first use decides)",
            'weft define spend_usd number --min 0 --unit USD -d "cumulative spend"')
    c.add_argument("name"); c.add_argument("type", choices=TYPES)
    c.add_argument("--min", type=float); c.add_argument("--max", type=float)
    c.add_argument("--unit"); c.add_argument("-d", "--description")
    cmd("names", "list what this project records: each name's type, range, unit and description", "weft names")

    c = cmd("ready", "list tasks you could claim now", "weft ready --refs H2")
    c.add_argument("--refs", metavar="REF", help="only tasks that refer to REF")
    c.add_argument("--parent", metavar="TASK", help="only tasks grouped under TASK")
    c = cmd("show", "one task in detail (state, needs, children, what points at it, comments), or list all",
            "weft show t-1a2b3c   |   weft --json show")
    c.add_argument("task", nargs="?")
    c = cmd("watch", "print events as they happen, filtered by position in the graph",
            "weft watch --down t-1a2b3c          # everything waiting on this gate\n"
            "  weft watch --since 0 --no-follow     # the whole history, then stop")
    c.add_argument("--since", type=int, metavar="SEQ", help="replay after this event (default: only new events)")
    c.add_argument("--task", help="this task only"); c.add_argument("--up", metavar="TASK", help="everything TASK needs")
    c.add_argument("--down", metavar="TASK", help="everything that needs TASK")
    c.add_argument("--refs", metavar="REF", help="tasks that refer to REF")
    c.add_argument("--parent", metavar="TASK", help="tasks grouped under TASK")
    c.add_argument("--to", help="comments addressed to this")
    c.add_argument("--no-follow", action="store_true", help="stop at the end instead of waiting (follows by default)")

    cmd("export", "print the whole log as JSON lines (commit it next to the work)", "weft export > .weft/events.jsonl")
    c = cmd("import", "restore an export into an empty local store (every event is re-checked)",
            "weft import .weft/events.jsonl")
    c.add_argument("file")
    c = cmd("serve", "serve DIR/<project>.db to other boxes over HTTP (needs DIR/tokens beyond localhost)",
            "weft serve /srv/weft --host 0.0.0.0   # tokens file: one '<token> <actor>' per line")
    c.add_argument("dir"); c.add_argument("--host", default="127.0.0.1"); c.add_argument("--port", type=int, default=7411)
    a = p.parse_args(argv)
    try:
        run(a)
    except WeftError as err:
        if a.json:
            print(json.dumps({"error": str(err)}))
            sys.exit(1)
        sys.exit(f"weft: {err}")
    except KeyboardInterrupt:
        pass

def run(a):
    out = lambda obj, text: print(json.dumps(obj) if a.json else text)
    if a.cmd == "init":
        if existing := find_store():
            raise WeftError(f"this project already has a store: {existing}")
        d = (repo_root() or Path.cwd()) / ".weft"
        d.mkdir(exist_ok=True)
        (d / ".gitignore").write_text("weft.db*\n")
        if a.url:
            (d / "url").write_text(a.url + "\n")
        else:
            Local(d / "weft.db")
        return out({"dir": str(d)}, f"weft initialised in {d}")
    if a.cmd == "serve":
        s = server(a.dir, a.host, a.port)
        print(f"weft serving {Path(a.dir).resolve()} on http://{a.host}:{a.port}/<project>/events", file=sys.stderr)
        return s.serve_forever()
    store, me = open_store(), actor()
    if a.cmd == "import":
        if not isinstance(store, Local):
            raise WeftError("import works on a local store")
        try:
            events = [json.loads(l) for l in Path(a.file).read_text().splitlines() if l.strip()]
        except ValueError as err:
            raise WeftError(f"{a.file} isn't JSON lines: {err}")
        if not all(isinstance(e, dict) and {"ts", "actor", "verb", "task", "body"} <= e.keys() for e in events):
            raise WeftError(f"{a.file} isn't a weft export")
        return out({"imported": len(store.load(events))}, f"imported {len(events)} events")
    if a.cmd == "export":
        return print("\n".join(json.dumps(e) for e in store.read()))
    if a.cmd == "watch":
        events = store.read()
        since = events[-1]["seq"] if events and a.since is None else (a.since or 0)
        g, match = fold(e for e in events if e["seq"] <= since), matcher(a)
        for e in (store.read(since) if a.no_follow else store.follow(since)):
            if e and match(apply(g, e), e):
                body = e["body"].get("text") or json.dumps(e["body"])
                out(e, f"{e['seq']:>5} {e['ts']} {e['actor']:<16} {e['verb']:<8} {e['task'] or '-':<10} {body}")
                sys.stdout.flush()
        return
    g = fold(store.read())
    if a.cmd == "names":
        lines = [f"{k:<16} {describe(d)}  [{d['by']}]" + (f"  {d['description']}" if "description" in d else "")
                 for k, d in sorted(g.names.items())]
        return out(g.names, "\n".join(lines))
    if a.cmd == "show" and a.task:
        if a.task not in g:
            raise WeftError(f"no such task: {a.task}")
        t = detail(g, g[a.task])
        lines = [summary(g, t)]
        for k, v in t.items():
            if k not in ("id", "title", "comments", "state", "closed", "reason") and v not in (None, [], {}):
                lines.append(f"  {k}: {json.dumps(v) if isinstance(v, (dict, list)) else v}")
        lines += [f"  #{c['seq']} {c['actor']}: {c.get('text') or json.dumps(c.get('data'))}" for c in t["comments"][-5:]]
        return out(t, "\n".join(lines))
    if a.cmd in ("ready", "show"):
        ts = [t for t in g.values() if a.cmd == "show" or (state(g, t) == "ready"
              and (not a.refs or a.refs in t["refs"]) and (not a.parent or t["parent"] == a.parent))]
        return out([detail(g, t) for t in ts], "\n".join(summary(g, t) for t in ts))

    # everything else writes events
    id = getattr(a, "task", None) if a.cmd != "create" else (a.id or f"t-{secrets.token_hex(3)}")
    if a.cmd in ("create", "update"):
        body = {k: v for k, v in (("title", a.title), ("refs", a.refs), ("parent", a.parent),
                                  ("await", a.await_), ("data", a.data)) if v is not None}
        events = [{"verb": a.cmd, "task": id, "body": body}]
        events += [{"verb": "link", "task": id, "body": {"rel": "needs", "to": n}} for n in getattr(a, "needs", [])]
    elif a.cmd in ("link", "unlink"):
        events = [{"verb": a.cmd, "task": id, "body": {"rel": a.rel, "to": a.to}}]
    elif a.cmd == "claim":
        body = {"ttl": a.ttl} | ({"status": a.status} if a.status else {})
        events = [{"verb": "claim", "task": id, "body": body}]
    elif a.cmd == "release":
        events = [{"verb": "release", "task": id, "body": {"force": True} if a.force else {}}]
    elif a.cmd == "close" and a.superseded_by:   # one batch: the new task takes over the old one's dependents
        new = a.superseded_by
        dependents = [y for y in g if id in g[y]["needs"]] if id in g else []
        events = [{"verb": "link", "task": new, "body": {"rel": "supersedes", "to": id}}]
        for y in dependents:
            events.append({"verb": "unlink", "task": y, "body": {"rel": "needs", "to": id}})
            if new not in g[y]["needs"]:
                events.append({"verb": "link", "task": y, "body": {"rel": "needs", "to": new}})
        events.append({"verb": "close", "task": id, "body": {"reason": a.reason or "superseded"}})
    elif a.cmd == "close":
        if not a.reason:
            raise WeftError("close needs --reason (produced, abandoned, superseded, approved, no, …) or --superseded-by")
        events = [{"verb": "close", "task": id, "body": {"reason": a.reason}}]
    elif a.cmd == "define":
        body = {k: v for k, v in (("name", a.name), ("type", a.type), ("min", a.min), ("max", a.max),
                                  ("unit", a.unit), ("description", a.description)) if v is not None}
        events = [{"verb": "define", "task": None, "body": body}]
        id = a.name
    elif a.cmd == "record":
        events = [{"verb": "record", "task": id, "body": {"values": dict(parse_value(v) for v in a.values)}}]
    elif a.cmd == "comment":
        body = {k: v for k, v in (("text", a.text), ("to", a.to), ("re", a.re), ("data", a.data)) if v is not None}
        events = [{"verb": "comment", "task": id, "body": body}]
    else:  # reopen
        events = [{"verb": a.cmd, "task": id, "body": {}}]
    if getattr(a, "message", None):
        events.insert(0, {"verb": "comment", "task": id, "body": {"text": a.message}})
    if a.cmd == "claim" and "WEFT_ACTOR" not in os.environ and not a.json:
        print(f"weft: note: claiming as {me}; set WEFT_ACTOR so agents on this box don't share claims", file=sys.stderr)
    out({"id": id, "events": store.append(events, me)}, id or "ok")


if __name__ == "__main__":
    main()
