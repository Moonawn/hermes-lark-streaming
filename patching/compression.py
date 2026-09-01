"""Compatibility guards for Hermes context-compression status messages."""

from __future__ import annotations

import functools
import logging
import threading
from collections.abc import Callable
from types import ModuleType
from typing import Any

_logger = logging.getLogger("hermes_lark_streaming")
_compression_call = threading.local()
_MISSING = object()


def _fence_is_cancelled(fence: Any) -> bool:
    """Read modern and older commit-fence cancellation shapes safely."""
    if fence is None:
        return False
    try:
        value = getattr(fence, "is_cancelled", False)
        return bool(value() if callable(value) else value)
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return False


def _wrap_compress_context_for_status(orig_compress: Callable) -> Callable:
    """Expose the invocation's commit fence to its worker-thread status edge."""

    @functools.wraps(orig_compress)
    def wrapper(*args, **kwargs):
        previous = getattr(_compression_call, "commit_fence", _MISSING)
        _compression_call.commit_fence = kwargs.get("commit_fence")
        try:
            return orig_compress(*args, **kwargs)
        finally:
            if previous is _MISSING:
                try:
                    del _compression_call.commit_fence
                except AttributeError:
                    pass
            else:
                _compression_call.commit_fence = previous

    wrapper._hls_compaction_status_context = True  # type: ignore[attr-defined]
    return wrapper


def _wrap_emit_compaction_done(orig_emit: Callable) -> Callable:
    """Suppress Hermes' success wording after the host cancelled this attempt."""

    @functools.wraps(orig_emit)
    def wrapper(agent, *args, **kwargs):
        fence = getattr(_compression_call, "commit_fence", None)
        if _fence_is_cancelled(fence):
            _logger.info(
                "HLS: suppressed stale compaction-complete status after "
                "commit-fence cancellation (session=%s)",
                str(getattr(agent, "session_id", "") or "none")[:24],
            )
            return None
        return orig_emit(agent, *args, **kwargs)

    wrapper._hls_cancelled_compaction_guard = True  # type: ignore[attr-defined]
    return wrapper


def apply_compression_status_patch(module: ModuleType | Any | None = None) -> bool:
    """Install a narrow guard for Hermes' cancelled-compaction terminal edge.

    Hermes currently calls its private ``_emit_compaction_done`` lifecycle edge
    from lock cleanup even when a ``CompressionCommitFence`` has already
    cancelled the commit.  The native text says compaction completed, which is
    false in that path and can arrive minutes after the turn's final answer.

    This patch is deliberately feature-detected and fail-open.  It tracks the
    fence only on the compression worker thread, suppresses only the cancelled
    attempt's terminal success text, and leaves genuine completion untouched.
    """
    if module is None:
        try:
            from agent import conversation_compression as module
        except (ImportError, AttributeError):
            return False

    compress = getattr(module, "compress_context", None)
    emit_done = getattr(module, "_emit_compaction_done", None)
    if not callable(compress) or not callable(emit_done):
        return False

    if not getattr(compress, "_hls_compaction_status_context", False):
        module.compress_context = _wrap_compress_context_for_status(compress)
    if not getattr(emit_done, "_hls_cancelled_compaction_guard", False):
        module._emit_compaction_done = _wrap_emit_compaction_done(emit_done)
    _logger.info(
        "hermes-lark-streaming: cancelled compaction status guard patched ✓"
    )
    return True


__all__ = [
    "apply_compression_status_patch",
]
