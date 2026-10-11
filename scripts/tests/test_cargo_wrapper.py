"""Verify cache selection, fallback, and argument handling without a cache server."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


WRAPPER = Path(__file__).resolve().parents[1] / "cargo.sh"


class CargoWrapperTests(unittest.TestCase):
    def invoke(self, mode=None, installed=True):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cargo = root / "cargo"
            cargo.write_text(
                f"#!{sys.executable}\n"
                "import json, os, sys\n"
                "keys = ['RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', "
                "'CARGO_INCREMENTAL', 'SCCACHE_DIR', 'SCCACHE_CACHE_SIZE']\n"
                "print(json.dumps({'env': {k: os.environ.get(k) for k in keys}, "
                "'args': sys.argv[1:]}))\n"
            )
            cargo.chmod(0o755)
            if installed:
                (root / "sccache").write_text("#!/bin/sh\nexit 99\n")
                (root / "sccache").chmod(0o755)
            environment = dict(os.environ)
            environment.update(
                PATH=directory,
                RUSTC_WRAPPER="inherited-wrapper",
                RUSTC_WORKSPACE_WRAPPER="workspace-wrapper",
                CARGO_INCREMENTAL="1",
                SCCACHE_DIR="/explicit/cache",
                SCCACHE_CACHE_SIZE="3G",
            )
            environment.pop("PAX_SCCACHE", None)
            if mode is not None:
                environment["PAX_SCCACHE"] = mode
            result = subprocess.run(
                ["/bin/bash", str(WRAPPER), "build", "-p", "argument with spaces"],
                env=environment, text=True, capture_output=True, check=False,
            )
            return result, str(root / "sccache")

    def test_auto_cache_disables_incremental_and_preserves_arguments(self):
        result, cache = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output["env"], {
            "RUSTC_WRAPPER": cache,
            "RUSTC_WORKSPACE_WRAPPER": "workspace-wrapper",
            "CARGO_INCREMENTAL": "0",
            "SCCACHE_DIR": "/explicit/cache",
            "SCCACHE_CACHE_SIZE": "3G",
        })
        self.assertEqual(output["args"], ["build", "-p", "argument with spaces"])

    def test_disabled_cache_clears_inherited_wrappers(self):
        result, _ = self.invoke("0")
        output = json.loads(result.stdout)
        self.assertEqual(output["env"]["RUSTC_WRAPPER"], "")
        self.assertEqual(output["env"]["RUSTC_WORKSPACE_WRAPPER"], "")
        self.assertEqual(output["env"]["CARGO_INCREMENTAL"], "1")

    def test_auto_without_cache_falls_back_to_cargo(self):
        result, _ = self.invoke(installed=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output["env"]["RUSTC_WRAPPER"], "")
        self.assertEqual(output["env"]["CARGO_INCREMENTAL"], "1")

    def test_required_cache_without_binary_fails(self):
        result, _ = self.invoke("1", installed=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires sccache", result.stderr)

    def test_invalid_mode_fails(self):
        result, _ = self.invoke("yes")
        self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
