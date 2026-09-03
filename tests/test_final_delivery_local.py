"""Personal endpoint: a progress card must never own final-message delivery."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from hermes_lark_streaming.config import Config
from hermes_lark_streaming.controller import CardSession, StreamCardController
from hermes_lark_streaming.patching import _msg_ctx, _patched_feishu_classes
from hermes_lark_streaming.patching.adapter import _wrap_feishu_adapter_send, _wrap_feishu_native_send_retry
from hermes_lark_streaming.patching.gateway import (
    _wrap_handle_message_with_agent, _wrap_run_agent,
)
from hermes_lark_streaming.state.linear import UnifiedLinearState
from hermes_lark_streaming.state.phase import CardPhase


def setup_controller(*, separate=False):
    cfg = Config()
    cfg._raw = {
        "feishu": {"app_id": "test", "app_secret": "test"},
        "hermes_lark_streaming": {
            "enabled": True,
            "final_delivery": "separate_message" if separate else "card",
        },
    }
    ctrl = StreamCardController()
    session = CardSession("event", "chat", asyncio.get_event_loop())
    session.state = CardPhase.STREAMING
    session.linear = True
    session.card_msg_id = "card-message"
    session.unified_state = UnifiedLinearState()
    ctrl._sess_put("event", session)
    return ctrl, session


def test_shorter_authoritative_answer_replaces_long_streamed_preamble():
    ctrl, session = setup_controller()
    session.unified_state.answer_text = "old preamble " * 50 + "partial answer"
    final = "完整答案，含最后一段。"
    with patch.object(ctrl, "_complete_session"):
        assert ctrl.on_completed(message_id="event", answer=final)
    assert session.unified_state.answer_text == final
    assert session.final_answer == final


def test_distinct_second_completion_yields_to_gateway_without_mutating_first():
    ctrl, session = setup_controller()
    with patch.object(ctrl, "_complete_session") as complete:
        assert ctrl.on_completed(message_id="event", answer="first final")
        assert ctrl.on_completed(message_id="event", answer="first final")
        assert not ctrl.on_completed(message_id="event", answer="second final")
    assert session.unified_state.answer_text == "first final"
    assert session.final_answer == "first final"
    complete.assert_called_once()


def test_late_answer_and_review_do_not_change_final():
    ctrl, session = setup_controller()
    with patch.object(ctrl, "_complete_session"):
        ctrl.on_completed(message_id="event", answer="final")
    ctrl.on_answer(message_id="event", text="late tokens")
    assert not ctrl.defer_background_review(message_id="event", text="review", sender=lambda _: None)
    assert session.final_answer == session.unified_state.answer_text == "final"


@pytest.mark.asyncio
async def test_long_fallback_is_lossless_and_idempotent():
    ctrl, session = setup_controller()
    ctrl._client = SimpleNamespace(reply_text=AsyncMock(return_value="delivered"))
    final = "这是长正文，保留换行。\n" * 1000 + "最终校验尾标"
    await ctrl._send_text_fallback(session, fallback_text=final)
    calls = list(ctrl._client.reply_text.await_args_list)
    assert len(calls) > 1
    chunks = [call.args[1] for call in calls]
    assert "".join(chunks) == final
    assert all(len(chunk.encode("utf-8")) <= 16000 for chunk in chunks)
    assert len({call.kwargs["uuid"] for call in calls}) == len(chunks)
    await ctrl._send_text_fallback(session, fallback_text=final)
    assert ctrl._client.reply_text.await_count == len(calls)


@pytest.mark.asyncio
async def test_fallback_failure_is_not_silent_and_retries_reuse_uuid():
    ctrl, session = setup_controller()
    ctrl._client = SimpleNamespace(reply_text=AsyncMock(side_effect=RuntimeError("offline")))
    with patch("hermes_lark_streaming.controller.core.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="offline"):
            await ctrl._send_text_fallback(session, fallback_text="final")
    calls = ctrl._client.reply_text.await_args_list
    assert len(calls) == 3
    assert len({call.kwargs["uuid"] for call in calls}) == 1


@pytest.mark.asyncio
async def test_separate_mode_leaves_fallback_to_native_gateway():
    ctrl, session = setup_controller(separate=True)
    with patch.object(ctrl, "_do_linear_complete", new_callable=AsyncMock, return_value=False), \
         patch.object(ctrl, "_send_text_fallback", new_callable=AsyncMock) as fallback:
        await ctrl._do_linear_complete_with_fallback(session)
    fallback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("sent_flag", [False, True])
async def test_existing_progress_card_never_suppresses_native_final(sent_flag):
    setup_controller(separate=True)
    adapter = SimpleNamespace()
    _patched_feishu_classes.add(id(type(adapter)))
    token = _msg_ctx.set({"message_id": "event", "event_message_id": "event", "card_sent": sent_flag})
    result = SimpleNamespace(success=True, message_id="ack")
    native_send = AsyncMock(return_value=result)
    try:
        actual = await _wrap_feishu_adapter_send(native_send)(adapter, "chat", "完整最终答复")
    finally:
        _msg_ctx.reset(token)
    native_send.assert_awaited_once()
    assert actual is result


@pytest.mark.asyncio
async def test_native_failure_is_propagated_not_fake_success():
    setup_controller(separate=True)
    adapter = SimpleNamespace()
    _patched_feishu_classes.add(id(type(adapter)))
    token = _msg_ctx.set({"message_id": "event", "event_message_id": "event", "card_sent": True})
    native_send = AsyncMock(side_effect=RuntimeError("network unavailable"))
    try:
        with pytest.raises(RuntimeError, match="network unavailable"):
            await _wrap_feishu_adapter_send(native_send)(adapter, "chat", "final")
    finally:
        _msg_ctx.reset(token)


@pytest.mark.asyncio
async def test_final_after_context_cleanup_still_uses_native_sender():
    setup_controller(separate=True)
    adapter = SimpleNamespace()
    _patched_feishu_classes.add(id(type(adapter)))
    token = _msg_ctx.set(None)
    native_send = AsyncMock(return_value=SimpleNamespace(success=True))
    try:
        await _wrap_feishu_adapter_send(native_send)(adapter, "chat", "final after cleanup")
    finally:
        _msg_ctx.reset(token)
    native_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_gateway_completion_does_not_claim_async_card_was_delivered():
    setup_controller(separate=True)
    ctx = {"message_id": "event", "event_message_id": "event", "card_sent": False}
    token = _msg_ctx.set(ctx)
    result = {"final_response": "final", "model": "test"}
    try:
        with patch("hermes_lark_streaming.patching.hooks.on_message_completed", return_value=True):
            actual = await _wrap_run_agent(AsyncMock(return_value=result))(
                SimpleNamespace(), "question", "", [], SimpleNamespace(), "session",
                event_message_id="event",
            )
    finally:
        _msg_ctx.reset(token)
    assert actual is result
    assert not actual.get("already_sent")
    assert not ctx["card_sent"]


@pytest.mark.asyncio
async def test_handle_message_preserves_native_response_even_if_card_exists():
    ctrl, _ = setup_controller(separate=True)
    runner = SimpleNamespace(_reply_anchor_for_event=lambda event: event.message_id)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), chat_id="chat")
    event = SimpleNamespace(message_id="event")

    async def orig(*args, **kwargs):
        _msg_ctx.get()["event_message_id"] = "event"
        return "native final"

    with patch("hermes_lark_streaming.patching.hooks.on_message_started"), \
         patch("hermes_lark_streaming.controller.get_controller", return_value=ctrl):
        actual = await _wrap_handle_message_with_agent(orig)(runner, event, source)
    assert actual == "native final"
    assert _msg_ctx.get() is None


@pytest.mark.asyncio
async def test_real_native_chunking_fails_if_first_chunk_has_no_ack():
    # Real installed Hermes send()/formatter/chunker; only network is mocked.
    native = pytest.importorskip("plugins.platforms.feishu.adapter")
    setup_controller(separate=True)
    adapter = object.__new__(native.FeishuAdapter)
    adapter._client = object()
    bad = SimpleNamespace(success=lambda: False, code=99991400, msg="rate limited", data=None)
    good = SimpleNamespace(success=lambda: True, code=0, msg="ok", data=SimpleNamespace(message_id="ack"))
    wire = AsyncMock(side_effect=[bad, good])
    guarded = _wrap_feishu_native_send_retry(wire)
    adapter._feishu_send_with_retry = lambda **kwargs: guarded(adapter, **kwargs)
    result = await native.FeishuAdapter.send(adapter, "chat", "正文" * 5000 + "TAIL")
    assert not result.success
    assert wire.await_count == 1


@pytest.mark.asyncio
async def test_real_native_chunker_keeps_long_final_tail():
    native = pytest.importorskip("plugins.platforms.feishu.adapter")
    setup_controller(separate=True)
    adapter = object.__new__(native.FeishuAdapter)
    adapter._client = object()
    good = SimpleNamespace(success=lambda: True, code=0, msg="ok", data=SimpleNamespace(message_id="ack"))
    wire = AsyncMock(return_value=good)
    guarded = _wrap_feishu_native_send_retry(wire)
    adapter._feishu_send_with_retry = lambda **kwargs: guarded(adapter, **kwargs)
    content = "完整中文正文。" * 2500 + "最终独立答复尾标"
    result = await native.FeishuAdapter.send(adapter, "chat", content)
    assert result.success
    assert wire.await_count >= 2
    import json
    pieces = [json.loads(call.kwargs["payload"])["text"] for call in wire.await_args_list]
    import re
    # Native sender adds " (i/n)" after each chunk; remove presentation markers.
    delivered = "".join(re.sub(r" \(\d+/\d+\)$", "", piece) for piece in pieces)
    assert delivered == content
