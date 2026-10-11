"""Exercise Cargo freshness and embedded provenance in disposable Git worktrees."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[2]


class BuildInfoTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="pax-build-identity-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.repo = self.root / "repo"
        self.package = Path("crates/pax-demo")
        self.repo.mkdir()
        self.env = {
            key: value for key, value in os.environ.items()
            if not key.startswith(("GIT_", "PAX_SOURCE_", "CARGO_FEATURE_"))
        }
        self.env.update(PAX_SCCACHE="0", CARGO_TARGET_DIR=str(self.root / "target"))
        self.git("init", "--quiet")
        self.git("config", "user.name", "Build Identity Test")
        self.git("config", "user.email", "identity@example.invalid")
        self.write("Cargo.toml", '[workspace]\nmembers=["crates/pax-demo"]\nresolver="3"\n')
        self.write(self.package / "Cargo.toml", '[package]\nname="identity-fixture"\nversion="0.0.0"\nedition="2024"\n')
        self.write(self.package / "build.rs", '#[path="../../build-support/build_info.rs"]\nmod build_info;\nfn main() { build_info::embed(); }\n')
        self.write(self.package / "src/main.rs", 'fn main() { println!("{} {}", env!("PAX_REVISION"), env!("PAX_DIRTY")); }\n')
        for name in ("pax-types", "pax-book", "pax-client"):
            self.write(f"crates/{name}/src/lib.rs", "// Identity input.\n")
        self.write("build-support/build_info.rs", (REPO / "build-support/build_info.rs").read_text())
        self.cargo("generate-lockfile")
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "Initial identity fixture")

    def write(self, relative, contents):
        path = self.repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)

    def git(self, *args, repo=None):
        return subprocess.check_output(
            ["git", "-C", str(repo or self.repo), *args], env=self.env,
            text=True, stderr=subprocess.PIPE,
        ).strip()

    def cargo(self, command, repo=None, env=None):
        return subprocess.check_output(
            [str(REPO / "scripts/cargo.sh"), command, "--quiet", "--offline",
             "--manifest-path", str((repo or self.repo) / "Cargo.toml")],
            cwd=REPO, env=env or self.env, text=True, stderr=subprocess.PIPE,
            timeout=60,
        ).strip()

    def identity(self, repo=None, dirty=False):
        repo = repo or self.repo
        self.assertEqual(self.cargo("run", repo), f"{self.git('rev-parse', 'HEAD', repo=repo)} {str(dirty).lower()}")

    def fresh(self, repo=None):
        outputs = list((self.root / "target/debug/build").glob("identity-fixture-*/output"))
        self.assertTrue(outputs)
        before = {str(path): path.stat().st_mtime_ns for path in outputs}
        self.cargo("run", repo)
        after = {str(path): path.stat().st_mtime_ns for path in outputs}
        self.assertEqual(before, after, "unchanged invocation reran the identity build script")

    def test_missing_optional_directories_stay_fresh_and_new_source_invalidates(self):
        self.identity()
        self.fresh()
        self.write(self.package / "examples/added.rs", "fn main() {}\n")
        self.identity(dirty=True)
        self.fresh()
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "Add optional source")
        self.identity()
        self.fresh()

    def test_linked_worktree_and_packed_refs_keep_correct_fresh_identity(self):
        worktree = self.root / "worktree"
        self.git("worktree", "add", "--quiet", "-b", "study", str(worktree))
        self.identity(worktree)
        self.fresh(worktree)
        self.git("pack-refs", "--all", "--prune")
        self.identity(worktree)
        self.fresh(worktree)
        # The new loose ref must invalidate a build made from a packed branch.
        self.git("commit", "--allow-empty", "--quiet", "-m", "Advance packed branch", repo=worktree)
        self.identity(worktree)
        self.fresh(worktree)
        self.git("checkout", "--detach", "--quiet", "HEAD~1", repo=worktree)
        self.identity(worktree)
        self.fresh(worktree)

    def test_sealed_source_overrides_remain_available_without_git(self):
        shutil.rmtree(self.repo / ".git")
        env = dict(self.env, PAX_SOURCE_REVISION="a" * 40, PAX_SOURCE_DIRTY="false")
        self.assertEqual(self.cargo("run", env=env), "a" * 40 + " false")


if __name__ == "__main__":
    unittest.main()
