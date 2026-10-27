"""Tests for weft. Standard library only: python3 -m unittest"""
import io, json, os, tempfile, threading, unittest
from contextlib import redirect_stdout
from pathlib import Path

import weft
from weft import Local, Remote, WeftError, fold, state


def ev(verb, task=None, **body):
    return {"verb": verb, "task": task, "body": body}


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


class Rules(Base):
    def test_create_needs_title_and_fresh_id(self):
        self.assertRaises(WeftError, self.put, ev("create", "a"))
        self.put(ev("create", "a", title="A"))
        self.assertRaises(WeftError, self.put, ev("create", "a", title="again"))

    def test_update_merges_fields_and_data(self):
        self.put(ev("create", "a", title="A", data={"cap_usd": 10}))
        self.put(ev("update", "a", refs=["E7"], data={"est_usd": 8}))
        a = self.graph()["a"]
        self.assertEqual((a["title"], a["refs"], a["data"]), ("A", ["E7"], {"cap_usd": 10, "est_usd": 8}))
        self.assertRaises(WeftError, self.put, ev("update", "nope", title="x"))

    def test_rule1_needs_cycles_refused(self):
        self.put(ev("create", "a", title="A"), ev("create", "b", title="B"), ev("create", "c", title="C"),
                 ev("link", "b", rel="needs", to="a"), ev("link", "c", rel="needs", to="b"))
        self.assertRaises(WeftError, self.put, ev("link", "a", rel="needs", to="c"))
        self.assertRaises(WeftError, self.put, ev("link", "a", rel="needs", to="a"))
        self.put(ev("link", "a", rel="informs", to="c"))  # other relations are just labels

    def test_rule2_one_live_claim(self):
        self.put(ev("create", "a", title="A"))
        self.put(ev("claim", "a", ttl=60), actor="ann")
        self.assertRaises(WeftError, self.put, ev("claim", "a"), actor="bob")
        self.put(ev("claim", "a", ttl=60), actor="ann")  # renewing is fine

    def test_claim_status_is_a_heartbeat(self):
        self.put(ev("create", "a", title="A"), ev("claim", "a"))
        lease = lambda: self.graph()["a"]["lease"]
        self.assertEqual(lease()["status"], "working")
        self.put(ev("claim", "a", status="stuck"))
        since = lease()["since"]
        self.put(ev("claim", "a", ttl=600))                  # a plain renewal keeps the status and its since
        self.assertEqual((lease()["status"], lease()["since"]), ("stuck", since))
        self.assertRaises(WeftError, self.put, ev("claim", "a", status="done"))
        self.assertRaises(WeftError, self.put, ev("claim", "a", status="working"), actor="bob")
        self.assertIn("ann · stuck", weft.summary(self.graph(), self.graph()["a"]))

    def test_record_keeps_latest_values_and_history(self):
        self.put(ev("create", "a", title="A"))
        self.put(ev("record", "a", values={"spend_usd": 1.25, "progress": "3/10"}), actor="overcode")
        self.put(ev("record", "a", values={"spend_usd": 3.75}))
        self.assertEqual(self.graph()["a"]["values"], {"spend_usd": 3.75, "progress": "3/10"})
        self.assertEqual(len([e for e in self.store.read() if e["verb"] == "record"]), 2)
        self.assertRaises(WeftError, self.put, ev("record", "a", values={}))
        self.assertRaises(WeftError, self.put, ev("record", "a", values={"Bad Name": 1}))
        self.assertRaises(WeftError, self.put, ev("record", "a", values={"x": [1, 2]}))

    def test_rule3_only_holder_releases_or_closes(self):
        self.put(ev("create", "a", title="A"), ev("claim", "a"))
        self.assertRaises(WeftError, self.put, ev("close", "a", reason="produced"), actor="bob")
        self.assertRaises(WeftError, self.put, ev("release", "a"), actor="bob")
        self.put(ev("close", "a", reason="produced"))
        self.assertEqual(state(self.graph(), self.graph()["a"]), "closed")

    def test_expired_claim_lapses_on_read(self):
        self.put(ev("create", "a", title="A"), ev("claim", "a", ttl=-1))
        self.assertEqual(state(self.graph(), self.graph()["a"]), "ready")
        self.put(ev("claim", "a"), actor="bob")

    def test_batch_is_atomic(self):
        self.assertRaises(WeftError, self.put, ev("create", "a", title="A"), ev("link", "a", rel="needs", to="ghost"))
        self.assertEqual(self.store.read(), [])

    def test_unlink(self):
        self.put(ev("create", "a", title="A"), ev("create", "b", title="B"), ev("link", "b", rel="needs", to="a"))
        self.assertEqual(state(self.graph(), self.graph()["b"]), "blocked")
        self.put(ev("unlink", "b", rel="needs", to="a"))
        self.assertEqual(state(self.graph(), self.graph()["b"]), "ready")
        self.assertRaises(WeftError, self.put, ev("unlink", "b", rel="needs", to="a"))


class Types(Base):
    def setUp(self):
        super().setUp()
        self.put(ev("create", "a", title="A"))

    def rec(self, actor="ann", **values):
        return self.put(ev("record", "a", values=values), actor=actor)

    def test_first_use_fixes_the_type(self):
        self.rec(spend_usd=1.25)
        self.rec(spend_usd=3)                       # integers are numbers too
        with self.assertRaisesRegex(WeftError, "spend_usd is number, got \"3.75\\$\""):
            self.rec(spend_usd="3.75$", actor="sloppy")
        self.assertEqual(self.graph().names["spend_usd"], {"type": "number", "by": "first use"})

    def test_define_enforces_type_range_and_choices(self):
        self.put(ev("define", None, name="progress", type="number", min=0, max=1),
                 ev("define", None, name="phase", type="string", one_of=["build", "run"]),
                 ev("define", None, name="games", type="integer", unit="games"))
        self.rec(progress=0.6, phase="run", games=60)
        for bad in ({"progress": 1.5}, {"progress": "60%"}, {"phase": "dance"}, {"games": 6.5}, {"games": True}):
            self.assertRaises(WeftError, self.rec, **bad)

    def test_define_overrides_first_use_going_forward(self):
        self.rec(progress="3/10")                   # an early string…
        self.put(ev("define", None, name="progress", type="number", min=0, max=1))
        self.rec(progress=0.3)                      # …then the project decides
        self.assertRaises(WeftError, self.rec, progress="4/10")
        self.assertEqual(self.graph()["a"]["values"]["progress"], 0.3)

    def test_strict_refuses_undefined_names(self):
        self.put(ev("define", None, strict=True))
        self.assertRaisesRegex(WeftError, "isn't defined", self.rec, tokens=10)
        self.put(ev("define", None, name="tokens", type="integer"))
        self.rec(tokens=10)
        self.put(ev("define", None, strict=False))
        self.rec(anything="goes")

    def test_bad_definitions_refused(self):
        for body in ({}, {"name": "x"}, {"name": "x", "type": "date"}, {"name": "Bad", "type": "number"},
                     {"name": "x", "type": "number", "min": "0"}, {"name": "x", "type": "string", "one_of": []},
                     {"strict": "yes"}):
            self.assertRaises(WeftError, self.put, ev("define", None, **body))


class Derived(Base):
    def test_states_and_unblocking(self):
        self.put(ev("create", "d21", title="Approve the proxy arm", **{"await": "ask:DECISIONS.md#D21"}),
                 ev("create", "arm3", title="Run arm 3"),
                 ev("link", "arm3", rel="needs", to="d21"))
        g = self.graph()
        self.assertEqual((state(g, g["d21"]), state(g, g["arm3"])), ("awaiting", "blocked"))
        self.assertEqual(weft.downstream(g, "d21"), {"d21", "arm3"})
        self.put(ev("comment", "d21", text="approved", data={"choice": "a"}),
                 ev("close", "d21", reason="approved"), actor="bridge")
        g = self.graph()
        self.assertEqual(state(g, g["arm3"]), "ready")
        self.assertEqual(g["d21"]["comments"][0]["data"], {"choice": "a"})

    def test_any_close_reason_unblocks_and_reopen_reblocks(self):
        self.put(ev("create", "a", title="A"), ev("create", "b", title="B"), ev("link", "b", rel="needs", to="a"),
                 ev("close", "a", reason="abandoned"))
        self.assertEqual(state(self.graph(), self.graph()["b"]), "ready")
        self.put(ev("reopen", "a"))
        self.assertEqual(state(self.graph(), self.graph()["b"]), "blocked")

    def test_export_import_roundtrip(self):
        self.put(ev("create", "a", title="A"), ev("comment", None, text="hello all"))
        other = Local(Path(self.dir.name) / "q.db")
        other.load(self.store.read())
        strip = lambda es: [{k: v for k, v in e.items() if k != "seq"} for e in es]
        self.assertEqual(strip(other.read()), strip(self.store.read()))
        self.assertRaises(WeftError, other.load, self.store.read())


class Server(unittest.TestCase):
    def setUp(self):
        # a follow stream may still be polling (and touching -wal files) as we clean up
        self.dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        root = Path(self.dir.name)
        Local(root / "proj.db")
        (root / "tokens").write_text("# token actor\nsecret-a ann\nsecret-b bob\n")
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
        self.assertEqual(bob.read()[1]["actor"], "ann")  # the token decides who you are
        with self.assertRaisesRegex(WeftError, "claimed by ann"):
            bob.append([ev("claim", "a")], actor=None)
        self.assertRaises(WeftError, Remote(self.url, "wrong").read)
        stream = bob.follow(since=2)
        ann.append([ev("comment", "a", text="halfway", to="bob")], actor=None)
        self.assertEqual(next(stream)["body"]["text"], "halfway")
        stream.close()

    def test_refuses_open_network_without_tokens(self):
        empty = tempfile.mkdtemp()
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

    def test_a_small_session(self):
        self.run_("create", "Decide the testbed", "--id", "q", "--await", "ask:D4")
        self.run_("create", "Build game 1", "--id", "g1", "--needs", "q", "--refs", "E6")
        self.assertIn("blocked", self.run_("show", "g1"))
        self.assertEqual(self.run_("ready"), "\n")
        self.run_("close", "q", "--reason", "decided", "-m", "Mike chose (d)")
        self.assertIn("g1", self.run_("ready", "--refs", "E6"))
        self.run_("claim", "g1", "--ttl", "2h")
        self.run_("record", "g1", "spend_usd=0.5", "progress=1/4", "done=false")
        self.assertIn("'spend_usd': 0.5, 'progress': '1/4', 'done': False", self.run_("show", "g1"))
        self.run_("release", "g1", "-m", "resume: start from the oracle")
        tasks = json.loads(self.run_("--json", "show"))
        self.assertEqual({t["id"]: t["state"] for t in tasks}, {"q": "closed", "g1": "ready"})
        lines = self.run_("watch", "--since", "0", "--down", "q", "--no-follow").splitlines()
        self.assertTrue(any("Mike chose (d)" in l for l in lines))
        self.assertEqual(len(self.run_("export").splitlines()), 9)
        self.assertEqual(Path(".weft/.gitignore").read_text(), "weft.db*\n")

    def test_define_and_names(self):
        self.run_("define", "spend_usd", "number", "--min", "0", "--unit", "USD", "-d", "cumulative spend")
        self.run_("define", "phase", "string", "--one-of", "build,run")
        self.run_("create", "x", "--id", "x")
        self.run_("record", "x", "phase=run", "tokens=5")
        names = self.run_("names")
        self.assertIn("number ≥ 0 (USD)  [define]  cumulative spend", names)
        self.assertIn("tokens", names)
        with self.assertRaises(SystemExit) as caught:
            self.run_("record", "x", "phase=dance")
        self.assertIn("phase is string one of build, run", str(caught.exception))

    def test_errors_exit_cleanly(self):
        with self.assertRaises(SystemExit) as caught:
            self.run_("claim", "ghost")
        self.assertIn("no such task", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
