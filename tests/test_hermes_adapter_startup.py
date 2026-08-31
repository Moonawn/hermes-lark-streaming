"""Regression tests for Hermes background plugin discovery startup."""

from __future__ import annotations

import builtins
import sys
from types import ModuleType

from hermes_lark_streaming.patching.hermes_adapter import HermesCompat


def test_gateway_runner_resolution_never_imports_unloaded_module(monkeypatch) -> None:
    """Plugin discovery must not wait on gateway.run's import lock."""
    monkeypatch.delitem(sys.modules, "gateway.run", raising=False)
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "gateway.run":
            raise AssertionError("gateway.run must not be imported during plugin discovery")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    compat = object.__new__(HermesCompat)

    assert compat._resolve_gateway_runner() is None


def test_gateway_runner_resolution_uses_completed_loaded_module(monkeypatch) -> None:
    """Once gateway.run is complete, the normal immediate patch path remains."""
    class FakeGatewayRunner:
        pass

    module = ModuleType("gateway.run")
    module.GatewayRunner = FakeGatewayRunner
    monkeypatch.setitem(sys.modules, "gateway.run", module)
    compat = object.__new__(HermesCompat)

    assert compat._resolve_gateway_runner() is FakeGatewayRunner
