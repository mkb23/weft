"""Tests for weft. Standard library only: python3 -m unittest"""
import io, json, os, subprocess, tempfile, threading, unittest, urllib.request
from contextlib import redirect_stdout, redirect_stderr
from datetime import timedelta
from pathlib import Path
from unittest import mock

import weft
from weft import Local, Remote, WeftError, fold, state


def ev(verb, task=None, **body):
    return {"verb": verb, "task": task, "body": body}

def later(**kw):
    """Pretend it's later, so leases lapse."""
    return mock.patch("weft.now", return_value=weft.now() + timedelta(**kw))


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.store = Local(Path(self.dir.name) / "p.db")

    def tearDown(self):
        self.dir.cleanup()

    def put(self, *events, actor="ann"):
        return self.store.append(list(events), actor)

    def graph(self):
        return fold(self.store.read())

    def state(self, id):
        g = self.graph()
        return state(g, g[id])

    def tasks(self, *ids):
        self.put(*(ev("create", i, title=i.upper()) for i in ids))


class Rules(Base):
    def test_create_needs_title_and_fresh_id(self):
        self.assertRaises(WeftError, self.put, ev("create", "a"))
        self.tasks("a")
        self.assertRaises(WeftError, self.put, ev("create", "a", title="again"))

    def test_update_merges_data_and_replaces_the_rest(self):
        self.put(ev("create", "a", title="A", data={"budget": 10}))
        self.put(ev("update", "a", refs=["E7"], data={"est": 8}))
        a = self.graph()["a"]
        self.assertEqual((a["title"], a["refs"], a["data"]), ("A", ["E7"], {"budget": 10, "est": 8}))
        self.assertRaises(WeftError, self.put, ev("update", "nope", title="x"))

    def test_rule1_no_needs_cycles(self):
        self.tasks("a", "b", "c")
        self.put(ev("link", "b", rel="needs", to="a"), ev("link", "c", rel="needs", to="b"))
        self.assertRaises(WeftError, self.put, ev("link", "a", rel="needs", to="c"))
        self.assertRaises(WeftError, self.put, ev("link", "a", rel="needs", to="a"))
        self.put(ev("link", "a", rel="informs", to="c"))   # other relations are labels

    def test_rule2_one_live_claim(self):
        self.tasks("a")
        self.put(ev("claim", "a"), actor="ann")
        self.assertRaisesRegex(WeftError, "claimed by ann", self.put, ev("claim", "a"), actor="bob")
        self.put(ev("claim", "a", ttl=60), actor="ann")   # renewing is fine
        with later(hours=2):
            self.put(ev("claim", "a"), actor="bob")       # a lapsed claim is up for grabs

    def test_rule2_only_ready_tasks_can_be_claimed(self):
        self.tasks("a", "b", "kid")
        self.put(ev("create", "gate", title="G", **{"await": "ask:D1"}),
                 ev("link", "b", rel="needs", to="a"), ev("update", "kid", parent="a"))
        self.assertRaisesRegex(WeftError, "blocked by a", self.put, ev("claim", "b"))
        self.assertRaisesRegex(WeftError, "it's a gate", self.put, ev("claim", "gate"))
        self.assertRaisesRegex(WeftError, "blocked by kid", self.put, ev("claim", "a"))   # a group
        self.put(ev("close", "kid", reason="produced"))
        self.put(ev("claim", "a"), ev("close", "a", reason="produced"))
        self.assertRaisesRegex(WeftError, "closed", self.put, ev("claim", "a"))

    def test_rule2_holder_may_renew_after_adding_a_blocker(self):
        self.tasks("a", "fix")
        self.put(ev("claim", "a"), ev("link", "a", rel="needs", to="fix"))
        self.put(ev("claim", "a", status="waiting"))
        self.assertEqual(self.graph()["a"]["lease"]["status"], "waiting")

    def test_rule3_only_holder_lets_go_unless_forced(self):
        self.tasks("a")
        self.put(ev("claim", "a"))
        self.assertRaises(WeftError, self.put, ev("close", "a", reason="produced"), actor="bob")
        self.assertRaisesRegex(WeftError, "--force", self.put, ev("release", "a"), actor="bob")
        self.put(ev("release", "a", force=True), actor="bob")
        self.assertEqual(self.state("a"), "ready")
        self.assertRaisesRegex(WeftError, "isn't claimed", self.put, ev("release", "a"))

    def test_no_silent_no_ops(self):
        self.tasks("a")
        self.assertRaisesRegex(WeftError, "needs a reason", self.put, ev("close", "a"))
        self.put(ev("close", "a", reason="approved"))
        self.assertRaisesRegex(WeftError, "already closed", self.put, ev("close", "a", reason="no"))
        self.put(ev("reopen", "a"))
        self.assertRaisesRegex(WeftError, "already open", self.put, ev("reopen", "a"))

    def test_needs_typos_refused(self):
        self.tasks("a", "b")
        for rel in ("need", "depends_on", "blocks", "Needs"):
            self.assertRaises(WeftError, self.put, ev("link", "a", rel=rel, to="b"))

    def test_claim_status_is_a_heartbeat(self):
        self.tasks("a")
        self.put(ev("claim", "a"))
        lease = lambda: self.graph()["a"]["lease"]
        self.assertEqual(lease()["status"], "working")
        self.put(ev("claim", "a", status="stuck"))
        since = lease()["since"]
        self.put(ev("claim", "a", ttl=600))   # a plain renewal keeps the status and its since
        self.assertEqual((lease()["status"], lease()["since"]), ("stuck", since))
        self.assertRaises(WeftError, self.put, ev("claim", "a", status="done"))
        for ttl in (0, -5, 8 * 86400, float("nan"), "1h"):
            self.assertRaises(WeftError, self.put, ev("claim", "a", ttl=ttl))

    def test_batch_is_atomic(self):
        self.assertRaises(WeftError, self.put, ev("create", "a", title="A"), ev("link", "a", rel="needs", to="ghost"))
        self.assertEqual(self.store.read(), [])

    def test_unlink(self):
        self.tasks("a", "b")
        self.put(ev("link", "b", rel="needs", to="a"))
        self.assertEqual(self.state("b"), "blocked")
        self.put(ev("unlink", "b", rel="needs", to="a"))
        self.assertEqual(self.state("b"), "ready")
        self.assertRaises(WeftError, self.put, ev("unlink", "b", rel="needs", to="a"))


class Shapes(Base):
    """One malformed event would poison the log forever, so every shape is checked."""
    def test_bad_events_refused(self):
        self.tasks("a")
        bad = [{"verb": "create", "task": None, "body": {"title": "x"}},
               {"verb": "create", "task": 7, "body": {"title": "x"}},
               {"verb": "create", "task": "", "body": {"title": "x"}},
               {"verb": "create", "task": "b", "body": "title"},
               ev("create", "b", title={"x": 1}), ev("create", "b", title="B", refs="E7"),
               ev("create", "b", title="B", data=[["k", 1]]), ev("create", "b", title="B", parent="ghost"),
               ev("update", "a", parent="a"), ev("comment", "a"), ev("comment", "a", text=5),
               ev("comment", None, text="hi", re="3"), {"verb": "explode", "task": "a", "body": {}}]
        for e in bad:
            with self.subTest(e=e):
                self.assertRaises(WeftError, self.put, e)
        self.assertEqual(len(self.store.read()), 1)

    def test_comments_cannot_spoof_who_or_when(self):
        self.tasks("a")
        self.put(ev("comment", "a", text="hi", actor="boss", seq=999, ts="1999-01-01T00:00:00Z"))
        c = self.graph()["a"]["comments"][0]
        self.assertEqual((c["actor"], c["seq"]), ("ann", 2))

    def test_non_finite_numbers_refused(self):
        self.tasks("a")
        for v in (float("nan"), float("inf")):
            self.assertRaises(WeftError, self.put, ev("record", "a", values={"x": v}))


class Types(Base):
    def setUp(self):
        super().setUp()
        self.tasks("a")

    def rec(self, actor="ann", **values):
        return self.put(ev("record", "a", values=values), actor=actor)

    def test_record_keeps_latest_values_and_history(self):
        self.rec(spend_usd=1.25, progress=0.3, actor="overcode")
        self.rec(spend_usd=3.75)
        self.assertEqual(self.graph()["a"]["values"], {"spend_usd": 3.75, "progress": 0.3})
        self.assertEqual(len([e for e in self.store.read() if e["verb"] == "record"]), 2)
        for values in ({}, {"Bad Name": 1}, {"x": [1, 2]}):
            self.assertRaises(WeftError, self.put, ev("record", "a", values=values))

    def test_first_use_fixes_the_type(self):
        self.rec(spend_usd=1.25)
        self.rec(spend_usd=3)   # integers are numbers too
        self.assertRaisesRegex(WeftError, r'spend_usd is number, got "3.75\$"', self.rec, spend_usd="3.75$")
        self.assertEqual(self.graph().names["spend_usd"], {"type": "number", "by": "first use"})

    def test_define_enforces_type_and_range(self):
        self.put(ev("define", None, name="progress", type="number", min=0, max=1),
                 ev("define", None, name="games", type="integer", unit="games"))
        self.rec(progress=0.6, games=60)
        for bad in ({"progress": 1.5}, {"progress": "60%"}, {"games": 6.5}, {"games": True}):
            self.assertRaises(WeftError, self.rec, **bad)

    def test_define_overrides_first_use_going_forward(self):
        self.rec(progress="3/10")
        self.put(ev("define", None, name="progress", type="number", min=0, max=1))
        self.rec(progress=0.3)
        self.assertRaises(WeftError, self.rec, progress="4/10")

    def test_define_first_returns_the_event(self):
        fresh = Local(Path(self.dir.name) / "fresh.db")
        written = fresh.append([ev("define", None, name="x", type="number")], "ann")
        self.assertEqual(written[0]["verb"], "define")

    def test_bad_definitions_refused(self):
        for body in ({}, {"name": "x"}, {"name": "x", "type": "date"}, {"name": "Bad", "type": "number"},
                     {"name": "x", "type": "number", "min": "0"}, {"name": "x", "type": "number", "min": 2, "max": 1}):
            self.assertRaises(WeftError, self.put, ev("define", None, **body))
        self.assertRaises(WeftError, self.put, ev("define", "a", name="x", type="number"))


class Derived(Base):
    def test_gates_unblock_and_show_their_reason(self):
        self.put(ev("create", "d21", title="Approve", **{"await": "ask:DECISIONS.md#D21"}),
                 ev("create", "arm3", title="Run arm 3"), ev("link", "arm3", rel="needs", to="d21"))
        self.assertEqual((self.state("d21"), self.state("arm3")), ("awaiting", "blocked"))
        self.assertEqual(weft.downstream(self.graph(), "d21"), {"d21", "arm3"})
        self.put(ev("close", "d21", reason="no"), actor="bridge")
        g = self.graph()
        self.assertEqual(state(g, g["arm3"]), "ready")   # any reason unblocks…
        self.assertIn("d21: no", weft.summary(g, g["arm3"]))   # …and the reason is in plain sight
        self.assertEqual(weft.detail(g, g["arm3"])["gates"], {"d21": "no"})

    def test_reopen_reblocks(self):
        self.tasks("a", "b")
        self.put(ev("link", "b", rel="needs", to="a"), ev("close", "a", reason="abandoned"))
        self.assertEqual(self.state("b"), "ready")
        self.put(ev("reopen", "a"))
        self.assertEqual(self.state("b"), "blocked")

    def test_a_group_is_blocked_by_its_children(self):
        self.tasks("sweep", "r1", "r2")
        self.put(ev("update", "r1", parent="sweep"), ev("update", "r2", parent="sweep"))
        g = self.graph()
        self.assertEqual((state(g, g["sweep"]), weft.detail(g, g["sweep"])["children"]), ("blocked", ["r1", "r2"]))
        self.put(ev("close", "r1", reason="produced"), ev("close", "r2", reason="abandoned"))
        self.assertEqual(self.state("sweep"), "ready")

    def test_lapsed_claims_are_visible(self):
        self.tasks("job")
        self.put(ev("claim", "job", ttl=3600, status="waiting"))
        with later(days=3):
            g = self.graph()
            self.assertEqual(state(g, g["job"]), "ready")
            self.assertIn("lapsed: ann · waiting, 2d ago", weft.summary(g, g["job"]))

    def test_show_lists_what_points_at_a_task(self):
        self.tasks("old", "new", "kid")
        self.put(ev("link", "new", rel="supersedes", to="old"), ev("link", "kid", rel="needs", to="old"))
        d = weft.detail(self.graph(), self.graph()["old"])
        self.assertEqual((d["linked_from"], d["needed_by"]), ([["supersedes", "new"]], ["kid"]))


class Store(Base):
    def test_export_import_roundtrip_keeps_seq(self):
        self.tasks("a")
        self.put(ev("comment", None, text="hello all"), ev("comment", "a", text="hi", re=1))
        other = Local(Path(self.dir.name) / "q.db")
        other.load(self.store.read())
        self.assertEqual(other.read(), self.store.read())
        self.assertRaisesRegex(WeftError, "empty store", other.load, self.store.read())

    def test_import_rechecks_and_is_atomic(self):
        self.tasks("a")
        events = self.store.read() + [{"seq": 9, "ts": "2026-01-01T00:00:00Z", "actor": "x",
                                        "verb": "create", "task": None, "body": {"title": "poison"}}]
        other = Local(Path(self.dir.name) / "q.db")
        self.assertRaises(WeftError, other.load, events)
        self.assertEqual(other.read(), [])

    def test_follow_sends_keepalives_when_idle(self):
        self.tasks("a")
        stream = self.store.follow(0, every=0.01, idle=0.02)
        self.assertEqual(next(stream)["task"], "a")
        self.assertIsNone(next(stream))


class Server(unittest.TestCase):
    def setUp(self):
        # a follow stream may still be polling (and touching -wal files) as we clean up
        self.dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        root = Path(self.dir.name)
        Local(root / "proj.db")
        (root / "tokens").write_text("# token actor\nsecret-a ann\nsecret-b bob\nmalformed-line\n")
        self.http = weft.server(root, port=0)
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.http.server_address[1]}/proj"

    def tearDown(self):
        self.http.shutdown()
        self.http.server_close()
        self.dir.cleanup()

    def test_remote_rules_actor_and_follow(self):
        ann, bob = Remote(self.url, "secret-a"), Remote(self.url, "secret-b")
        ann.append([ev("create", "a", title="A"), ev("claim", "a")], actor="spoofed")
        self.assertEqual(bob.read()[1]["actor"], "ann")   # the token decides who you are
        self.assertRaisesRegex(WeftError, "claimed by ann", bob.append, [ev("claim", "a")], None)
        self.assertRaises(WeftError, Remote(self.url, "wrong").read)
        stream = bob.follow(since=2)
        ann.append([ev("comment", "a", text="halfway", to="bob")], actor=None)
        self.assertEqual(next(stream)["body"]["text"], "halfway")
        stream.close()

    def test_bad_requests_get_answers(self):
        def status(path, body):
            req = urllib.request.Request(self.url + path, body, {"Authorization": "Bearer secret-a"})
            try:
                return urllib.request.urlopen(req, timeout=5).status
            except urllib.error.HTTPError as err:
                return err.code
        self.assertEqual(status("/events", b'{"verb": "create"}'), 400)
        self.assertEqual(status("/events", b"not json"), 400)
        self.assertEqual(status("/events?since=abc", None), 400)

    def test_refuses_open_network_without_tokens(self):
        with tempfile.TemporaryDirectory() as empty:
            self.assertRaises(WeftError, weft.server, empty, "0.0.0.0", 0)


class Cli(unittest.TestCase):
    def setUp(self):
        self.cwd, self.dir = os.getcwd(), tempfile.TemporaryDirectory()
        os.chdir(self.dir.name)
        os.environ.pop("WEFT", None)
        os.environ["WEFT_ACTOR"] = "ann"
        self.run_("init")

    def tearDown(self):
        os.chdir(self.cwd)
        self.dir.cleanup()

    def run_(self, *args):
        buf = io.StringIO()
        with redirect_stdout(buf):
            weft.main(list(args))
        return buf.getvalue()

    def fails(self, *args):
        with self.assertRaises(SystemExit) as caught, redirect_stderr(io.StringIO()):
            self.run_(*args)
        return str(caught.exception.code)

    def test_a_small_session(self):
        self.run_("create", "Decide the testbed", "--id", "q", "--await", "ask:D4")
        self.run_("create", "Build game 1", "--id", "g1", "--needs", "q", "--refs", "E6")
        self.assertIn("blocked", self.run_("show", "g1"))
        self.assertEqual(self.run_("ready"), "\n")
        self.run_("close", "q", "--reason", "decided", "-m", "Mike chose (d)")
        self.assertIn("q: decided", self.run_("ready", "--refs", "E6"))
        self.run_("claim", "g1", "--ttl", "2h")
        self.run_("record", "g1", "spend_usd=0.5", "progress=0.25", "done=false")
        self.assertIn('values: {"spend_usd": 0.5, "progress": 0.25, "done": false}', self.run_("show", "g1"))
        self.run_("release", "g1", "-m", "resume: start from the oracle")
        tasks = json.loads(self.run_("--json", "show"))
        self.assertEqual({t["id"]: t["state"] for t in tasks}, {"q": "closed", "g1": "ready"})
        lines = self.run_("watch", "--since", "0", "--down", "q", "--no-follow").splitlines()
        self.assertTrue(any("Mike chose (d)" in l for l in lines))
        self.assertEqual(len(self.run_("export").splitlines()), 9)

    def test_json_results_and_errors(self):
        made = json.loads(self.run_("create", "x", "--id", "x", "--json"))
        self.assertEqual((made["id"], made["events"][0]["verb"]), ("x", "create"))
        with self.assertRaises(SystemExit), redirect_stdout(io.StringIO()) as buf:
            weft.main(["claim", "ghost", "--json"])
        self.assertIn("no such task", json.loads(buf.getvalue())["error"])

    def test_bad_input_is_an_error_not_a_traceback(self):
        self.run_("create", "x", "--id", "x")
        self.assertIn("2", self.fails("claim", "x", "--ttl", "2hours"))           # argparse usage error
        self.assertIn("2", self.fails("update", "x", "--data", "[1,2]"))
        self.assertIn("task comes first", self.fails("record", "progress=0.5", "x"))
        self.assertIn("did you mean `needs`", self.fails("link", "x", "need", "x"))

    def test_superseded_by_moves_dependents(self):
        for t in ("old", "new", "write"):
            self.run_("create", t, "--id", t)
        self.run_("link", "write", "needs", "old")
        self.run_("close", "old", "--superseded-by", "new")
        show = json.loads(self.run_("--json", "show", "write"))
        self.assertEqual((show["needs"], show["state"]), (["new"], "blocked"))
        self.assertEqual(json.loads(self.run_("--json", "show", "old"))["linked_from"], [["supersedes", "new"]])

    def test_define_and_names(self):
        self.run_("define", "spend_usd", "number", "--min", "0", "--unit", "USD", "-d", "cumulative spend")
        self.run_("create", "x", "--id", "x")
        self.run_("record", "x", "spend_usd=2", "tokens=5")
        names = self.run_("names")
        self.assertIn("number ≥ 0 (USD)  [define]  cumulative spend", names)
        self.assertIn("tokens           number  [first use]", names)
        self.assertIn("spend_usd is number ≥ 0 (USD)", self.fails("record", "x", "spend_usd=-1"))

    def test_init_refuses_a_second_store(self):
        self.assertIn("already has a store", self.fails("init"))


class Worktrees(unittest.TestCase):
    """A repo's worktrees and subdirectories share the store at its main checkout."""
    def test_worktree_finds_the_main_store(self):
        git = lambda *a, cwd: subprocess.run(["git", *a], cwd=cwd, check=True, capture_output=True)
        with tempfile.TemporaryDirectory() as d:
            main, tree = Path(d, "main"), Path(d, "tree")
            main.mkdir()
            git("init", "-q", cwd=main)
            git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "x", cwd=main)
            git("worktree", "add", "-q", str(tree), cwd=main)
            cwd = os.getcwd()
            try:
                os.environ.pop("WEFT", None)
                (main / "sub").mkdir()
                os.chdir(main / "sub")
                with redirect_stdout(io.StringIO()):
                    weft.main(["init"])   # lands at the main checkout, not in sub/
                self.assertTrue((main / ".weft" / "weft.db").exists())
                os.chdir(tree)
                self.assertEqual(Path(weft.find_store()).resolve(), (main / ".weft" / "weft.db").resolve())
            finally:
                os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
