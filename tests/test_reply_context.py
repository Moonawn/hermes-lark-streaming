"""CardKit reply-context regression tests."""

from __future__ import annotations

from collections import OrderedDict

import pytest


@pytest.fixture(autouse=True)
def _clear_reply_context_registry() -> None:
    from hermes_lark_streaming.patching import adapter as adapter_mod

    with adapter_mod._card_reply_contexts_lock:
        adapter_mod._card_reply_contexts.clear()
    yield
    with adapter_mod._card_reply_contexts_lock:
        adapter_mod._card_reply_contexts.clear()


@pytest.mark.asyncio
async def test_interactive_fallback_uses_registered_card_answer(monkeypatch) -> None:
    """A CardKit parent fallback resolves to its completed answer excerpt."""
    from hermes_lark_streaming import patching as P
    from hermes_lark_streaming.patching import adapter as adapter_mod

    class FakeFeishuAdapter:
        async def send(self, *args, **kwargs):
            return None

        async def _fetch_message_text(self, message_id: str) -> str:
            return "[Interactive message]"

    monkeypatch.setattr(
        adapter_mod,
        "_card_reply_contexts",
        OrderedDict({"om_card": "Useful completed answer"}),
        raising=False,
    )
    P._patched_feishu_classes.discard(id(FakeFeishuAdapter))
    try:
        assert P._apply_feishu_adapter_patches(FakeFeishuAdapter) is True
        result = await FakeFeishuAdapter()._fetch_message_text("om_card")
        assert result == "Useful completed answer"
    finally:
        P._patched_feishu_classes.discard(id(FakeFeishuAdapter))


@pytest.mark.asyncio
async def test_valid_parent_text_is_never_replaced() -> None:
    from hermes_lark_streaming.patching.adapter import (
        _register_card_reply_context,
        _wrap_feishu_adapter_fetch_message_text,
    )

    async def fetch(_self, _message_id: str) -> str:
        return "Valid plain or post parent"

    _register_card_reply_context("om_card", "Stored card answer")
    result = await _wrap_feishu_adapter_fetch_message_text(fetch)(object(), "om_card")
    assert result == "Valid plain or post parent"


@pytest.mark.asyncio
async def test_missing_registry_preserves_interactive_fallback() -> None:
    from hermes_lark_streaming.patching.adapter import (
        _wrap_feishu_adapter_fetch_message_text,
    )

    async def fetch(_self, _message_id: str) -> str:
        return "[Interactive message]"

    result = await _wrap_feishu_adapter_fetch_message_text(fetch)(object(), "unknown")
    assert result == "[Interactive message]"


def test_registry_normalizes_truncates_and_prunes_lru(monkeypatch) -> None:
    from hermes_lark_streaming.patching import adapter as adapter_mod

    monkeypatch.setattr(adapter_mod, "_CARD_REPLY_CONTEXT_MAX", 2)
    monkeypatch.setattr(adapter_mod, "_CARD_REPLY_CONTEXT_CHARS", 12)
    adapter_mod._register_card_reply_context("one", "  first\n\nanswer extra  ")
    adapter_mod._register_card_reply_context("two", "second answer")

    # Touch one so two becomes least recently used, then exceed capacity.
    assert adapter_mod._lookup_card_reply_context("one") == "first answe…"
    adapter_mod._register_card_reply_context("three", "third answer")

    assert adapter_mod._lookup_card_reply_context("two") is None
    assert adapter_mod._lookup_card_reply_context("one") == "first answe…"
    assert len(adapter_mod._card_reply_contexts) == 2


@pytest.mark.asyncio
async def test_successful_streaming_completion_registers_answer() -> None:
    from hermes_lark_streaming.controller.mixin import STREAMING
    from hermes_lark_streaming.patching.adapter import _lookup_card_reply_context

    from tests.test_controller import _make_session, _setup_ctrl

    ctrl = _setup_ctrl()
    session = _make_session("inbound", linear=True)
    session.state = STREAMING
    session.card_id = "card_id"
    session.card_msg_id = "outbound_card_message"
    session.unified_state.on_answer_delta("  Completed\nanswer  ")
    ctrl._sessions["inbound"] = session

    assert await ctrl._do_linear_complete(session) is True
    assert _lookup_card_reply_context("outbound_card_message") == "Completed answer"
