"""Regression coverage for one-card ownership of Hermes status callbacks."""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from hermes_lark_streaming.patching import (
    _HLS_STATUS_DELIVERY_KEY,
    _HLS_STATUS_KEY,
    _HLS_STATUS_TURN_KEY,
    _msg_ctx,
)
from hermes_lark_streaming.patching.adapter import (
    _dispatch_feishu_outbound,
    _is_compression_lifecycle_message,
)
from hermes_lark_streaming.patching.gateway import _wrap_send_or_update_status


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        independent_final_delivery=False,
        gateway_cards=True,
        serialized_chat_ids=frozenset(),
    )


def _session(*, state: str = "idle", terminal: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        message_id="event",
        chat_id="oc_chat",
        state=state,
        is_terminal_phase=terminal,
        card_msg_id=None,
        final_answer="",
    )


@pytest.mark.asyncio
async def test_status_wrapper_captures_feishu_turn_before_scheduling() -> None:
    captured: dict = {}

    async def native(adapter, chat_id, status_key, content, metadata=None):
        captured.update(
            chat_id=chat_id,
            status_key=status_key,
            content=content,
            metadata=metadata,
        )
        return "sent"

    wrapped = _wrap_send_or_update_status(native)
    token = _msg_ctx.set(
        {"message_id": "event", "event_message_id": "event"}
    )
    try:
        pending = wrapped(
            object(), "oc_chat", "compacting", "Compacting context", None
        )
        assert inspect.isawaitable(pending)
        assert await pending == "sent"
    finally:
        _msg_ctx.reset(token)

    assert captured["metadata"] == {
        _HLS_STATUS_DELIVERY_KEY: True,
        _HLS_STATUS_KEY: "compacting",
        _HLS_STATUS_TURN_KEY: "event",
    }


@pytest.mark.asyncio
async def test_status_wrapper_does_not_touch_other_platform_metadata() -> None:
    original_metadata = {"thread_id": "thread"}
    captured: dict = {}

    async def native(adapter, chat_id, status_key, content, metadata=None):
        captured["metadata"] = metadata
        return "sent"

    wrapped = _wrap_send_or_update_status(native)
    assert await wrapped(
        object(), "slack-channel", "thinking", "Working", original_metadata
    ) == "sent"
    assert captured["metadata"] is original_metadata


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state,terminal",
    [("idle", False), ("creating", False), ("completed", True)],
)
async def test_tagged_turn_status_is_suppressed_even_around_card_races(
    state: str,
    terminal: bool,
) -> None:
    session = _session(state=state, terminal=terminal)
    ctrl = SimpleNamespace(
        enabled=True,
        _sess_get=lambda message_id: session if message_id == "event" else None,
        _sess_values_snapshot=lambda: [session],
        _do_gateway_deliver=AsyncMock(),
    )
    native = AsyncMock(return_value=SimpleNamespace(success=True))
    metadata = {
        _HLS_STATUS_DELIVERY_KEY: True,
        _HLS_STATUS_KEY: "lifecycle",
        _HLS_STATUS_TURN_KEY: "event",
    }

    with (
        patch("hermes_lark_streaming.controller.get_controller", return_value=ctrl),
        patch("hermes_lark_streaming.patching.adapter._get_config", return_value=_config()),
    ):
        result = await _dispatch_feishu_outbound(
            object(), "oc_chat", "temporary provider status", native,
            metadata=metadata,
        )

    native.assert_not_awaited()
    ctrl._do_gateway_deliver.assert_not_awaited()
    assert getattr(result, "success", True)


@pytest.mark.asyncio
async def test_unmarked_compression_status_is_suppressed_for_older_hermes() -> None:
    session = _session()
    ctrl = SimpleNamespace(
        enabled=True,
        _sess_get=lambda message_id: session if message_id == "event" else None,
        _sess_values_snapshot=lambda: [session],
        _do_gateway_deliver=AsyncMock(),
    )
    native = AsyncMock()
    token = _msg_ctx.set(
        {"message_id": "event", "event_message_id": "event", "card_sent": False}
    )
    try:
        with (
            patch("hermes_lark_streaming.controller.get_controller", return_value=ctrl),
            patch("hermes_lark_streaming.patching.adapter._get_config", return_value=_config()),
        ):
            await _dispatch_feishu_outbound(
                object(),
                "oc_chat",
                "📦 Preflight compression: ~120,000 tokens >= 100,000 threshold.",
                native,
            )
    finally:
        _msg_ctx.reset(token)

    native.assert_not_awaited()
    ctrl._do_gateway_deliver.assert_not_awaited()


@pytest.mark.parametrize(
    "content",
    [
        "📦 Preflight compression: ~120,000 tokens >= 100,000 threshold.",
        "🗜️ Compacting context — summarizing earlier conversation.",
        "✓ Context compaction complete — continuing turn...",
        "⚠️ Context compression timed out after 45.0s with no output.",
        "⚠ Context is over the compression threshold (~120,000 tokens).",
    ],
)
def test_legacy_compression_classifier_accepts_known_status_lines(
    content: str,
) -> None:
    assert _is_compression_lifecycle_message(content)


@pytest.mark.parametrize(
    "content",
    [
        "下面解释 Context compression timed out after 45s 的原因与修复方式。",
        "如果日志出现 Compacting context，不代表 CardKit 已经卡死。",
        "The answer discusses preflight compression without emitting a status.",
        "Compressed with fallback: 100 → 20 messages",
    ],
)
def test_legacy_compression_classifier_rejects_answer_and_manual_feedback(
    content: str,
) -> None:
    assert not _is_compression_lifecycle_message(content)


@pytest.mark.asyncio
async def test_answer_discussing_compression_is_never_swallowed() -> None:
    session = _session()
    ctrl = SimpleNamespace(
        enabled=True,
        _sess_get=lambda message_id: session if message_id == "event" else None,
        _sess_values_snapshot=lambda: [session],
    )
    native_result = SimpleNamespace(success=True, message_id="native")
    native = AsyncMock(return_value=native_result)
    token = _msg_ctx.set(
        {"message_id": "event", "event_message_id": "event", "card_sent": False}
    )
    try:
        with (
            patch("hermes_lark_streaming.controller.get_controller", return_value=ctrl),
            patch("hermes_lark_streaming.patching.adapter._get_config", return_value=_config()),
        ):
            result = await _dispatch_feishu_outbound(
                object(),
                "oc_chat",
                "下面解释 Context compression timed out after 45s 的原因。",
                native,
            )
    finally:
        _msg_ctx.reset(token)

    native.assert_awaited_once()
    assert result is native_result


@pytest.mark.asyncio
async def test_status_without_matching_turn_remains_visible() -> None:
    ctrl = SimpleNamespace(
        enabled=True,
        _sess_get=lambda _message_id: None,
        _sess_values_snapshot=lambda: [],
        _do_gateway_deliver=AsyncMock(return_value=("message", "card")),
    )
    native = AsyncMock()
    metadata = {
        _HLS_STATUS_DELIVERY_KEY: True,
        _HLS_STATUS_KEY: "lifecycle",
        _HLS_STATUS_TURN_KEY: "manual-command",
    }

    with (
        patch("hermes_lark_streaming.controller.get_controller", return_value=ctrl),
        patch("hermes_lark_streaming.patching.adapter._get_config", return_value=_config()),
    ):
        await _dispatch_feishu_outbound(
            object(), "oc_chat", "manual lifecycle feedback", native,
            metadata=metadata,
        )

    ctrl._do_gateway_deliver.assert_awaited_once()
    native.assert_not_awaited()


@pytest.mark.asyncio
async def test_ordinary_agent_text_before_card_ready_still_passes_through() -> None:
    session = _session()
    ctrl = SimpleNamespace(
        enabled=True,
        _sess_get=lambda message_id: session if message_id == "event" else None,
        _sess_values_snapshot=lambda: [session],
    )
    native_result = SimpleNamespace(success=True, message_id="native")
    native = AsyncMock(return_value=native_result)
    token = _msg_ctx.set(
        {"message_id": "event", "event_message_id": "event", "card_sent": False}
    )
    try:
        with (
            patch("hermes_lark_streaming.controller.get_controller", return_value=ctrl),
            patch("hermes_lark_streaming.patching.adapter._get_config", return_value=_config()),
        ):
            result = await _dispatch_feishu_outbound(
                object(), "oc_chat", "ordinary final candidate", native,
            )
    finally:
        _msg_ctx.reset(token)

    native.assert_awaited_once()
    assert result is native_result
