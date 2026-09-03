#!/usr/bin/env python3
"""Run mock tests without a real Hermes profile, credentials or network access."""

import os
from pathlib import Path
import sys
import tempfile

import pytest


def deny_network(event, args):
    if event in {"socket.connect", "socket.getaddrinfo", "socket.sendto"}:
        raise RuntimeError("Network disabled in the offline test suite")


if __name__ == "__main__":
    repo = Path(__file__).resolve().parent.parent
    os.chdir(repo)
    sys.path.insert(0, str(repo))
    sys.dont_write_bytecode = True
    for key in tuple(os.environ):
        if key.startswith(("FEISHU_", "LARK_")):
            os.environ.pop(key, None)
    with tempfile.TemporaryDirectory(prefix="hls-test-profile-") as profile:
        os.environ["HERMES_HOME"] = profile
        sys.addaudithook(deny_network)
        if os.environ.get("HERMES_SRC_DIR"):
            sys.path.append(str(Path(os.environ["HERMES_SRC_DIR"]).resolve()))
            # Compatibility jobs must really import Hermes. Never report a
            # green build after silently skipping its native delivery tests.
            from plugins.platforms.feishu.adapter import FeishuAdapter  # noqa: F401
        arguments = sys.argv[1:] or ["tests"]
        raise SystemExit(pytest.main([
            *arguments, "--ignore=tests/e2e", "-p", "no:cacheprovider",
        ]))
