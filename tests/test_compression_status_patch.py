from __future__ import annotations

from types import SimpleNamespace

from hermes_lark_streaming.patching.compression import (
    apply_compression_status_patch,
)


def _module_with_native_lifecycle(events: list[str]) -> SimpleNamespace:
    module = SimpleNamespace()

    def emit_done(_agent):
        events.append("complete")

    def compress_context(agent, *, commit_fence=None):
        module._emit_compaction_done(agent)
        return "compressed"

    module.compress_context = compress_context
    module._emit_compaction_done = emit_done
    return module


def test_cancelled_compression_does_not_emit_false_complete_status() -> None:
    events: list[str] = []
    module = _module_with_native_lifecycle(events)
    assert apply_compression_status_patch(module) is True

    result = module.compress_context(
        SimpleNamespace(session_id="session-timeout"),
        commit_fence=SimpleNamespace(is_cancelled=True),
    )

    assert result == "compressed"
    assert events == []


def test_successful_compression_keeps_native_complete_status() -> None:
    events: list[str] = []
    module = _module_with_native_lifecycle(events)
    assert apply_compression_status_patch(module) is True

    module.compress_context(
        SimpleNamespace(session_id="session-success"),
        commit_fence=SimpleNamespace(is_cancelled=False),
    )

    assert events == ["complete"]


def test_fence_context_is_worker_local_and_cleared_after_call() -> None:
    events: list[str] = []
    module = _module_with_native_lifecycle(events)
    assert apply_compression_status_patch(module) is True

    module.compress_context(
        SimpleNamespace(session_id="session-timeout"),
        commit_fence=SimpleNamespace(is_cancelled=lambda: True),
    )
    module._emit_compaction_done(SimpleNamespace(session_id="unrelated"))

    assert events == ["complete"]


def test_patch_is_idempotent_and_feature_detected() -> None:
    events: list[str] = []
    module = _module_with_native_lifecycle(events)

    assert apply_compression_status_patch(module) is True
    first_compress = module.compress_context
    first_emit = module._emit_compaction_done
    assert apply_compression_status_patch(module) is True
    assert module.compress_context is first_compress
    assert module._emit_compaction_done is first_emit
    assert apply_compression_status_patch(SimpleNamespace()) is False
