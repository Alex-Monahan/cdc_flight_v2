"""Tests for tools/check_secrets.py. Run: python3 -m unittest tests.test_check_secrets -v

Every fake credential below is assembled at run time, so this file itself contains nothing that looks like one.
"""
import base64
import importlib.util
import os
import random
import shutil
import string
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCANNER = os.path.join(ROOT, "tools", "check_secrets.py")
spec = importlib.util.spec_from_file_location("check_secrets", SCANNER)
cs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cs)

rng = random.Random(7)


def rand(n, alphabet=string.ascii_letters + string.digits):
    return "".join(rng.choice(alphabet) for _ in range(n))


def b64url(n):
    return base64.urlsafe_b64encode(rand(n).encode()).decode().rstrip("=")


FAKES = {
    "private-key-block": "-----BEGIN " + "OPENSSH PRIVATE" + " KEY-----",
    "github-token": "gh" + "p_" + rand(36),
    "github-token (fine-grained)": "github" + "_pat_" + rand(60),
    "aws-access-key-id": "AK" + "IA" + rand(16, string.ascii_uppercase + string.digits),
    "slack-token": "xo" + "xb-" + rand(12, string.digits) + "-" + rand(24),
    "slack-webhook": "https://hooks.slack" + ".com/services/T" + rand(8) + "/B" + rand(8) + "/" + rand(24),
    "jwt": "ey" + "J" + b64url(24) + "." + "ey" + "J" + b64url(60) + "." + b64url(32),
    "planetscale-credential": "pscale" + "_pw_" + rand(43),
    "anthropic-key": "sk-" + "ant-" + rand(40),
    "google-api-key": "AI" + "za" + rand(35),
    "url-with-credentials": "postgres" + "://app_user:" + rand(20) + "@db.example.com:5432/app",
    "secret-assignment": "PG_" + "PASSWORD=" + rand(20),
    "secret-assignment (yaml)": "api_" + "key: '" + rand(24) + "'",
    "high-entropy-string": rand(48),
}

CLEAN = b"""
password: ${PG_PASSWORD}
token = os.environ["MOTHERDUCK_TOKEN"]
PG_PASSWORD=<your password>
salt_env: CDC_PII_SALT
url = "postgres://user:password@localhost:55432/postgres"
sha256 = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
uuid = "0b0e7a52-7c1e-4e5b-9d0a-2f3f6f0f1a11"
path = src/main/java/dev/example/engine/PostgresLogicalReplicationSlotVerifierTest.java
Claude-Session: https://claude.ai/code/session_012frBvfckCxHP2mwrxFKN7z
"""


def rules(findings):
    return {f[2] for f in findings}


class ScanRules(unittest.TestCase):
    def test_each_fake_credential_is_found(self):
        for name, text in FAKES.items():
            with self.subTest(name):
                found = cs.scan("notes.md", ("x = 1\n" + text + "\n").encode(), [], [])
                self.assertIn(name.split(" (")[0], rules(found))
                self.assertTrue(all(text not in f[3] for f in found), "finding must not print the value")

    def test_ordinary_content_is_clean(self):
        self.assertEqual(cs.scan("config.yaml", CLEAN, [], []), [])

    def test_pragma_and_allow_file_suppress_pattern_rules_only(self):
        line = FAKES["github-token"]
        self.assertEqual(cs.scan("a.txt", (line + "  # secret-scan: allow\n").encode(), [], []), [])
        import re
        self.assertEqual(cs.scan("a.txt", line.encode(), [], [re.compile("^gh")]), [])

    def test_forbidden_file_names(self):
        for name in [".env", "prod.env", "deploy/id_ed25519", "server.pem", "certs/tls.key", "env", ".pgpass"]:
            with self.subTest(name):
                self.assertIn("forbidden-file-name", rules(cs.scan(name, b"x", [], [])))
        self.assertEqual(cs.scan(".env.example", b"PG_HOST=\n", [], []), [])

    def test_exact_values_from_the_local_secrets_file(self):
        value = "plain looking words"          # no pattern or entropy rule would catch this
        with tempfile.TemporaryDirectory() as d:
            env = os.path.join(d, "env")
            with open(env, "w") as f:
                f.write("# comment\nPG_HOST=%s\nPG_PORT=5432\nexport SHORT=abc\nPG_USER='postgres'\n" % value)
            os.environ["CDC_SECRETS_ENV_FILE"] = env
            try:
                exact = cs.load_exact_values()
            finally:
                del os.environ["CDC_SECRETS_ENV_FILE"]
        self.assertEqual([k for k, _ in exact], ["PG_HOST"])
        for text in [value, base64.b64encode(value.encode()).decode(), "plain%20looking%20words"]:
            with self.subTest(text[:5]):
                found = cs.scan("a.txt", ("see " + text + " # secret-scan: allow\n").encode(), exact, [])
                self.assertEqual(rules(found), {"exact-secret-value"})
                self.assertIn("PG_HOST", found[0][3])
                self.assertNotIn(value, found[0][3])


class GitHooks(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, True)
        shutil.copytree(os.path.join(ROOT, ".githooks"), os.path.join(self.dir, ".githooks"))
        os.makedirs(os.path.join(self.dir, "tools"))
        for name in ("check_secrets.py", "install_hooks.sh"):
            shutil.copy(os.path.join(ROOT, "tools", name), os.path.join(self.dir, "tools", name))
        # only this repository's own git configuration applies: none from the user, the system or the environment
        clean = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        self.env = dict(clean, CDC_SECRETS_ENV_FILE=os.path.join(self.dir, "no-such-file"),
                        GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "test@example.com")
        self.git("config", "user.name", "test")
        subprocess.run(["sh", "tools/install_hooks.sh"], cwd=self.dir, env=self.env, check=True,
                       stdout=subprocess.DEVNULL)
        self.git("add", "-A")
        self.assertEqual(self.git("commit", "-q", "-m", "init", check=False).returncode, 0)

    def git(self, *args, check=True, stdin=None):
        return subprocess.run(("git",) + args, cwd=self.dir, env=self.env, input=stdin, check=check,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def write(self, name, text):
        with open(os.path.join(self.dir, name), "w") as f:
            f.write(text)
        self.git("add", name)

    def test_commit_with_a_secret_is_blocked_and_clean_commit_passes(self):
        self.write("notes.md", "token " + FAKES["github-token"] + "\n")
        blocked = self.git("commit", "-q", "-m", "add notes", check=False)
        self.assertNotEqual(blocked.returncode, 0)
        self.assertIn(b"github-token", blocked.stderr)
        self.assertNotIn(FAKES["github-token"].encode(), blocked.stderr + blocked.stdout)
        self.write("notes.md", "nothing to see\n")
        self.assertEqual(self.git("commit", "-q", "-m", "add notes", check=False).returncode, 0)

    def test_forbidden_file_and_commit_message_are_blocked(self):
        self.write("prod.env", "A=1\n")
        self.assertNotEqual(self.git("commit", "-q", "-m", "env", check=False).returncode, 0)
        self.git("rm", "-q", "--cached", "prod.env")
        self.write("ok.txt", "fine\n")
        bad = self.git("commit", "-q", "-m", "use " + FAKES["anthropic-key"], check=False)
        self.assertNotEqual(bad.returncode, 0)

    def test_pre_push_and_history_scan_catch_commits_made_without_hooks(self):
        self.write("leak.txt", FAKES["planetscale-credential"] + "\n")
        self.git("commit", "-q", "--no-verify", "-m", "skipped hooks")
        head = self.git("rev-parse", "HEAD").stdout.decode().strip()
        line = "refs/heads/main %s refs/heads/main %s\n" % (head, "0" * 40)
        run = lambda *a, **k: subprocess.run([sys.executable, "tools/check_secrets.py"] + list(a), cwd=self.dir,
                                             env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **k)
        self.assertEqual(run("--pre-push", input=line.encode()).returncode, 1)
        self.assertEqual(run("--all-history").returncode, 1)
        # removing the file later does not clean the history
        self.git("rm", "-q", "leak.txt")
        self.git("commit", "-q", "-m", "remove")
        self.assertEqual(run("--all-history").returncode, 1)


if __name__ == "__main__":
    unittest.main()
