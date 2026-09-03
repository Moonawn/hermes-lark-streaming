"""Contract regressions for incomplete finals, task cancellation and stale ACKs.

All transports are fake. These tests assert the actual outgoing content and
ordering, with no Feishu credentials or model calls.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from hermes_lark_streaming.config import Config
from hermes_lark_streaming.controller import CardSession, StreamCardController
from hermes_lark_streaming.feishu.client import FeishuClient
from hermes_lark_streaming.patching import _msg_ctx
from hermes_lark_streaming.patching.adapter import _dispatch_feishu_outbound
from hermes_lark_streaming.patching.gateway import (
    _gateway_terminal_payload,
    _wrap_handle_message_with_agent,
    _wrap_run_agent,
)
from hermes_lark_streaming.state.linear import UnifiedLinearState
from hermes_lark_streaming.state.phase import CardPhase


def setup(*, separate=False, message_id="om_turn"):
    Config()._raw = {"hermes_lark_streaming": {
        "final_delivery": "separate_message" if separate else "card",
    }}
    ctrl = StreamCardController()
    session = CardSession(message_id, "oc_test", asyncio.get_running_loop())
    session.state = CardPhase.STREAMING
    session.linear = True
    session.card_id = "card-test"
    session.card_msg_id = "om_card"
    session._card_ready.set()
    session._creation_stages.add("answer")
    session.unified_state = UnifiedLinearState()
    ctrl._sess_put(session.message_id, session)
    ctrl._client = SimpleNamespace(
        cardkit_stream_element=AsyncMock(),
        cardkit_batch_update=AsyncMock(),
        cardkit_close_streaming=AsyncMock(),
        reply_text=AsyncMock(return_value="om_fallback"),
    )
    ctrl._schedule_linear_flush = Mock()
    return ctrl, session


def test_gateway_terminal_payload_classifies_safe_gateway_error():
    error = (
        "Sorry, I encountered an unexpected error.\n"
        "Try again or use /reset to start a fresh session."
    )
    assert _gateway_terminal_payload(error) == ("", error)
    assert _gateway_terminal_payload("完整最终答复") == ("完整最终答复", "")


@pytest.mark.asyncio
async def test_preflight_gateway_error_seals_existing_streaming_card(monkeypatch):
    """A pre-_run_agent failure must terminate the one existing card."""
    ctrl, session = setup()
    monkeypatch.setitem(
        ctrl._cfg._raw,
        "feishu",
        {"app_id": "test", "app_secret": "test"},
    )
    ctrl._complete_session = Mock()

    import hermes_lark_streaming.controller as controller_module
    from hermes_lark_streaming.patching import hooks

    monkeypatch.setattr(controller_module, "get_controller", lambda: ctrl)
    monkeypatch.setattr(hooks, "get_controller", lambda: ctrl)
    monkeypatch.setattr(hooks, "on_message_started", lambda **_: None)

    runner = SimpleNamespace(_reply_anchor_for_event=lambda event: event.message_id)
    source = SimpleNamespace(
        platform=SimpleNamespace(value="feishu"),
        chat_id=session.chat_id,
    )
    event = SimpleNamespace(message_id=session.message_id)
    safe_error = (
        "Sorry, I encountered an unexpected error.\n"
        "Try again or use /reset to start a fresh session."
    )

    async def fail_before_run_agent(*_args, **_kwargs):
        _msg_ctx.get()["event_message_id"] = session.message_id
        return safe_error

    result = await _wrap_handle_message_with_agent(fail_before_run_agent)(
        runner, event, source,
    )

    assert result is None
    assert session.state == CardPhase.COMPLETING
    assert session.error_message == safe_error
    ctrl._complete_session.assert_called_once_with(session)
    assert _msg_ctx.get() is None


@pytest.mark.asyncio
async def test_card_publication_ack_loss_retries_with_one_uuid(monkeypatch):
    """Accepted card sends must stay idempotent when their ACK is lost."""
    accepted: dict[str, str] = {}
    seen_uuids: list[str] = []

    class MessageAPI:
        async def _accept(self, request):
            import httpx

            request_uuid = request.request_body.uuid
            seen_uuids.append(request_uuid)
            if request_uuid not in accepted:
                accepted[request_uuid] = f"om_card_{len(accepted) + 1}"
                # Simulate the server accepting the reply while its ACK is
                # lost on the wire. The retry must address the same operation.
                raise httpx.ReadTimeout("ACK lost after accept")
            return SimpleNamespace(
                success=lambda: True,
                code=0,
                msg="ok",
                data=SimpleNamespace(message_id=accepted[request_uuid]),
            )

        async def areply(self, request):
            return await self._accept(request)

        async def acreate(self, request):
            return await self._accept(request)

    client = object.__new__(FeishuClient)
    client._client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=MessageAPI()))
    )
    monkeypatch.setattr(
        "hermes_lark_streaming.feishu.client.asyncio.sleep", AsyncMock()
    )

    message_ids = [
        await client.reply_card_by_id("om_origin", "card_entity"),
        await client.reply_card("om_origin", {"schema": "2.0"}),
        await client.send_card_to_chat("oc_chat", {"schema": "2.0"}),
    ]

    assert message_ids == ["om_card_1", "om_card_2", "om_card_3"]
    assert len(seen_uuids) == 6
    assert seen_uuids[0] == seen_uuids[1]
    assert seen_uuids[2] == seen_uuids[3]
    assert seen_uuids[4] == seen_uuids[5]
    assert len(set(seen_uuids)) == len(accepted) == 3


@pytest.mark.asyncio
async def test_completion_timeout_also_applies_outside_serialized_chats():
    ctrl, session = setup()
    ctrl._cfg = SimpleNamespace(card_completion_timeout_sec=0.01, independent_final_delivery=False)
    session.state = CardPhase.COMPLETING
    session.final_answer = "Full final, including its tail END"

    async def stalled(_):
        await asyncio.Event().wait()

    ctrl._do_linear_complete = stalled
    await asyncio.wait_for(ctrl._do_linear_complete_with_fallback(session), 0.5)

    assert session.state == CardPhase.CREATION_FAILED
    assert session.terminal_source == "completion_timeout"
    assert session._delivery_done.is_set() and session._delivery_success
    assert ctrl._client.reply_text.await_args.args[1] == session.final_answer
    assert session.writer.closed


@pytest.mark.asyncio
async def test_cancellation_releases_waiters_preserves_final_and_allows_ttl_cleanup():
    ctrl, session = setup()
    session.state = CardPhase.COMPLETING
    session.final_answer = "Do not lose this immutable final END"
    entered = asyncio.Event()

    async def stalled(_):
        entered.set()
        await asyncio.Event().wait()

    ctrl._do_linear_complete = stalled
    session.flush._flush_in_progress = True
    waiter = asyncio.create_task(session.flush.wait_for_flush())
    task = asyncio.create_task(ctrl._do_linear_complete_with_fallback(session))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(waiter, 0.1)

    assert session.is_terminal_phase and session.writer.closed
    assert session._delivery_done.is_set() and not session._delivery_success
    assert session.final_answer.endswith("END")
    ctrl._client.reply_text.assert_not_awaited()
    session.created_at = 0
    ctrl._prune_stale_sessions()
    assert ctrl._sess_get(session.message_id) is None


@pytest.mark.asyncio
async def test_cancel_before_first_task_instruction_is_terminal_too():
    ctrl, session = setup()
    session.state = CardPhase.COMPLETING
    ctrl._complete_session(session)
    task = session.completion_task
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)  # run the completion done callback
    assert session.is_terminal_phase
    assert session._delivery_done.is_set() and not session._delivery_success
    assert session.completion_task is None
    ctrl._client.reply_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_prune_protects_live_generation_but_reaps_orphaned_completion():
    ctrl, live = setup(message_id="om_live")
    live.created_at = 0
    orphan = CardSession("om_orphan", "oc_test", asyncio.get_running_loop())
    orphan.state = CardPhase.COMPLETING
    orphan.created_at = 0
    ctrl._sess_put(orphan.message_id, orphan)

    ctrl._prune_stale_sessions()

    assert ctrl._sess_get(live.message_id) is live
    assert ctrl._sess_get(orphan.message_id) is None
    assert orphan.is_terminal_phase and orphan._delivery_done.is_set()


@pytest.mark.asyncio
async def test_recent_session_with_expired_completion_is_terminated_without_waiting_for_ttl():
    ctrl, session = setup()
    session.state = CardPhase.COMPLETING
    session.completion_started_at = time.monotonic() - 60
    ctrl._prune_stale_sessions()
    assert session.is_terminal_phase
    assert ctrl._sess_get(session.message_id) is session  # normal terminal TTL


@pytest.mark.asyncio
@pytest.mark.parametrize("answer_exists", [False, True])
async def test_delta_arriving_during_old_ack_is_pending_or_already_visible(answer_exists):
    ctrl, session = setup()
    if not answer_exists:
        session._creation_stages.clear()
    state = session.unified_state
    state.on_answer_delta("prefix")
    sent = []

    async def stream(_card, _element, content, **_):
        sent.append(content)
        if len(sent) == 1:
            # A worker callback arrives while the older snapshot is in flight.
            await asyncio.to_thread(state.on_answer_delta, " tail END")

    ctrl._client.cardkit_stream_element = stream
    await ctrl._do_unified_flush(session)

    assert state.answer_text == "prefix tail END"
    assert state.answer_dirty or sent[-1] == "prefix tail END"
    if answer_exists:
        assert state.answer_dirty and state.answer_acked_revision < state.answer_revision
        await ctrl._do_unified_flush(session)
        assert not state.answer_dirty
        assert sent[-1] == "prefix tail END"


@pytest.mark.asyncio
async def test_card_writes_are_ordered_and_final_seal_keeps_authoritative_text():
    ctrl, session = setup()
    state = session.unified_state
    state.on_answer_delta("old progress")
    entered, release = asyncio.Event(), asyncio.Event()
    active = 0
    peak = 0
    writes = []

    async def write(kind, sequence, content):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        writes.append((kind, sequence, content))
        try:
            if len(writes) == 1:
                entered.set()
                await release.wait()
            await asyncio.sleep(0)
        finally:
            active -= 1

    async def stream(_card, _element, content, *, sequence):
        await write("stream", sequence, content)

    async def batch(_card, actions, *, sequence):
        await write("batch", sequence, actions)

    async def close(_card, *, sequence, **_):
        await write("close", sequence, None)

    ctrl._client.cardkit_stream_element = stream
    ctrl._client.cardkit_batch_update = batch
    ctrl._client.cardkit_close_streaming = close
    flush = asyncio.create_task(ctrl._do_unified_flush(session))
    await entered.wait()
    state.replace_answer("authoritative final END", final=True)
    session.final_answer = state.answer_text
    session.state = CardPhase.COMPLETING
    completion = asyncio.create_task(ctrl._do_linear_complete_with_fallback(session))
    await asyncio.sleep(0)
    assert len(writes) == 1
    release.set()
    await asyncio.wait_for(asyncio.gather(flush, completion), 1)

    assert peak == 1
    sequences = [seq for _, seq, _ in writes]
    assert sequences == sorted(set(sequences))
    assert writes[-1][0] == "close"
    final_actions = next(body for kind, _, body in reversed(writes) if kind == "batch")
    assert any(action.get("params", {}).get("partial_element", {}).get("content") ==
               "authoritative final END" for action in final_actions)
    assert session._delivery_success and session.state == CardPhase.COMPLETED
    with pytest.raises(asyncio.CancelledError):
        await ctrl._do_unified_flush(session)
    assert writes[-1][0] == "close"


@pytest.mark.asyncio
async def test_final_arriving_during_card_creation_does_not_deadlock_writer():
    ctrl, session = setup()
    session.state = CardPhase.IDLE
    session.card_id = session.card_msg_id = None
    session._card_ready.clear()
    session._creation_stages.clear()
    session.unified_state.replace_answer("quick final END", final=True)
    session.final_answer = "quick final END"
    entered, release = asyncio.Event(), asyncio.Event()

    async def create(_):
        entered.set()
        await release.wait()
        return "new-card"

    ctrl._ensure_init = AsyncMock()
    ctrl._client.cardkit_create = create
    ctrl._client.reply_card_by_id = AsyncMock(return_value="om_new_card")
    creation = asyncio.create_task(ctrl._do_create_linear_card(session))
    await entered.wait()
    session.state = CardPhase.COMPLETING
    completion = asyncio.create_task(ctrl._do_linear_complete_with_fallback(session))
    await asyncio.sleep(0)
    release.set()
    await asyncio.wait_for(asyncio.gather(creation, completion), 1)
    assert session._delivery_success
    assert session.state == CardPhase.COMPLETED


@pytest.mark.asyncio
async def test_fallback_uuids_distinguish_turns_with_same_reply_anchor():
    ctrl, first = setup(message_id="om_first")
    second = CardSession("om_second", "oc_test", asyncio.get_running_loop())
    first.anchor_id = second.anchor_id = "om_shared_root"
    await ctrl._send_text_fallback(first, fallback_text="same answer")
    await ctrl._send_text_fallback(second, fallback_text="same answer")
    calls = ctrl._client.reply_text.await_args_list
    assert calls[0].args == calls[1].args
    assert calls[0].kwargs["uuid"] != calls[1].kwargs["uuid"]


@pytest.mark.asyncio
async def test_loading_only_card_in_separate_mode_does_not_duplicate_final():
    ctrl, session = setup(separate=True)
    session._creation_stages.clear()
    session.unified_state.replace_answer("final END", final=True)
    session.state = CardPhase.COMPLETING
    await ctrl._do_linear_complete_with_fallback(session)
    ctrl._client.reply_text.assert_not_awaited()
    assert session.state == CardPhase.COMPLETED


@pytest.mark.asyncio
@pytest.mark.parametrize("send_fails", [False, True])
async def test_distinct_second_final_bypasses_the_first_card_sent_flag(monkeypatch, send_fails):
    ctrl, session = setup()
    session.final_answer = "first final"
    session.state = CardPhase.COMPLETED
    import hermes_lark_streaming.controller as controller_module
    monkeypatch.setattr(controller_module, "get_controller", lambda: ctrl)
    assert not ctrl.on_completed(message_id=session.message_id, answer="second final END")
    token = _msg_ctx.set({"event_message_id": session.message_id, "card_sent": True})
    native = AsyncMock(return_value=SimpleNamespace(success=True, message_id="om_second"))
    try:
        if send_fails:
            native.side_effect = ConnectionError("wire unavailable")
            with pytest.raises(ConnectionError):
                await _dispatch_feishu_outbound(object(), session.chat_id, "second final END", native)
        else:
            result = await _dispatch_feishu_outbound(object(), session.chat_id, "second final END", native)
            assert result.message_id == "om_second"
    finally:
        _msg_ctx.reset(token)
    native.assert_awaited_once()


@pytest.mark.asyncio
async def test_distinct_final_survives_both_gateway_wrappers(monkeypatch):
    ctrl, session = setup()
    session.final_answer = "first final"
    session.state = CardPhase.COMPLETED
    import hermes_lark_streaming.controller as controller_module
    import hermes_lark_streaming.patching.hooks as hooks
    monkeypatch.setattr(controller_module, "get_controller", lambda: ctrl)
    monkeypatch.setattr(hooks, "get_controller", lambda: ctrl)
    monkeypatch.setattr(hooks, "on_message_started", lambda **_: None)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), chat_id=session.chat_id)
    runner = SimpleNamespace(_reply_anchor_for_event=lambda _: None)
    final = {"final_response": "second final END"}

    async def handle(*_):
        ctx = _msg_ctx.get()
        ctx["card_sent"] = True  # a previous queued phase already has its card
        result = await _wrap_run_agent(AsyncMock(return_value=final))(
            runner, "question", "", [], source, "session", event_message_id=session.message_id,
        )
        assert not result.get("already_sent")
        return result["final_response"]

    result = await _wrap_handle_message_with_agent(handle)(
        runner, SimpleNamespace(message_id=session.message_id), source,
    )
    assert result == "second final END"
    assert _msg_ctx.get() is None


@pytest.mark.asyncio
async def test_lost_answer_fallback_failure_never_records_success(monkeypatch):
    ctrl, session = setup()
    session._creation_stages.clear()
    session.state = CardPhase.COMPLETING
    session.final_answer = "Keep this answer END"
    session.unified_state.replace_answer(session.final_answer, final=True)
    ctrl._client.reply_text = AsyncMock(side_effect=ConnectionError("wire down"))
    monkeypatch.setattr("hermes_lark_streaming.controller.core.asyncio.sleep", AsyncMock())
    with pytest.raises(ConnectionError):
        await ctrl._do_linear_complete_with_fallback(session)
    assert session.state == CardPhase.CREATION_FAILED
    assert session._delivery_done.is_set() and not session._delivery_success
    assert session.final_answer == "Keep this answer END"


def test_worker_delta_cannot_append_after_authoritative_final_is_frozen():
    state = UnifiedLinearState()
    state.on_answer_delta("a much longer streamed preamble")
    state.answer_snapshot()  # populate upstream incremental escape cache
    state.replace_answer("short END", final=True)
    state.on_answer_delta("late callback")
    assert state.answer_text == state.answer_snapshot()[1] == "short END"


def test_global_completion_timeout_config_keeps_legacy_queue_alias():
    cfg = Config()
    cfg._raw = {"hermes_lark_streaming": {"queue": {"card_completion_timeout_sec": 7}}}
    assert cfg.card_completion_timeout_sec == 7
    cfg._raw["hermes_lark_streaming"]["card_completion_timeout_sec"] = 9
    assert cfg.card_completion_timeout_sec == 9


@pytest.mark.asyncio
async def test_compact_card_renders_status_without_losing_the_separate_final():
    ctrl, session = setup(separate=True)
    Config()._raw["hermes_lark_streaming"]["progress_card"] = "compact"
    final = "A long detailed final\n" * 500 + "FINAL TAIL END"
    state = session.unified_state
    state.on_answer_delta(final)
    await ctrl._do_unified_flush(session)
    streamed = ctrl._client.cardkit_stream_element.await_args.args[2]
    assert "Writing" in streamed and len(streamed) < 80
    session.final_answer = final
    state.freeze_answer()
    session.state = CardPhase.COMPLETING
    await ctrl._do_linear_complete_with_fallback(session)
    actions = ctrl._client.cardkit_batch_update.await_args.args[1]
    rendered = [a.get("params", {}).get("partial_element", {}).get("content") for a in actions]
    assert "生成完成 · Final answer follows" in rendered
    assert session.final_answer == final
    ctrl._client.reply_text.assert_not_awaited()


def test_compact_option_cannot_hide_the_only_answer_in_legacy_card_mode():
    cfg = Config()
    cfg._raw = {"hermes_lark_streaming": {"progress_card": "compact"}}
    assert not cfg.compact_progress_card
