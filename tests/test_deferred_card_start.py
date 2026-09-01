"""First-answer CardKit activation and compression-safe lifecycle tests."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from hermes_lark_streaming.config import Config
from hermes_lark_streaming.controller import CardSession, StreamCardController
from hermes_lark_streaming.controller.mixin import (
    ABORTED,
    COMPLETED,
    COMPLETING,
    CREATING,
    CREATION_FAILED,
    IDLE,
)
from hermes_lark_streaming.state.linear import UnifiedLinearState
from tests.test_controller import _mock_client


def _controller(*, first_answer: bool = True) -> StreamCardController:
    cfg = Config()
    cfg._raw = {
        "feishu": {"app_id": "app", "app_secret": "secret"},
        "hermes_lark_streaming": {
            "enabled": True,
            "streaming_card_start": (
                "first_answer" if first_answer else "message_start"
            ),
        },
    }
    ctrl = StreamCardController()
    ctrl._initialized = True
    ctrl._client = _mock_client()
    return ctrl


async def _finish_pending(ctrl: StreamCardController) -> None:
    for _ in range(10):
        tasks = [
            task
            for task in ctrl._pending_tasks
            if isinstance(task, asyncio.Task) and not task.done()
        ]
        if not tasks:
            return
        await asyncio.gather(*tasks)


class TestFirstAnswerConfig:
    @pytest.mark.parametrize("value", [None, "message_start", "unknown"])
    def test_message_start_is_compatible_default(self, value: str | None) -> None:
        section = {} if value is None else {"streaming_card_start": value}
        cfg = Config()
        cfg._raw = {"hermes_lark_streaming": section}
        assert not cfg.defer_streaming_card_until_answer

    def test_first_answer_is_explicit_opt_in(self) -> None:
        cfg = Config()
        cfg._raw = {
            "hermes_lark_streaming": {
                "streaming_card_start": " first_answer ",
            },
        }
        assert cfg.defer_streaming_card_until_answer


@pytest.mark.asyncio
async def test_preflight_buffers_tools_then_first_answer_publishes_without_loading_hint() -> None:
    ctrl = _controller()
    ctrl.on_message_started(message_id="event", chat_id="chat")
    session = ctrl._sess_get("event")
    assert session is not None
    assert session.state == IDLE
    assert session.linear
    assert session.unified_state is not None
    ctrl._client.cardkit_create.assert_not_awaited()

    # Pre-answer activity is buffered and does not create a long-lived CardKit
    # card during context preparation.
    ctrl.on_tool_update(
        message_id="event",
        tool_name="preflight",
        status="started",
        detail="compression",
    )
    assert session.state == IDLE
    ctrl._client.cardkit_create.assert_not_awaited()

    ctrl.on_answer(message_id="event", text="首个回答 token")
    assert session.state == CREATING
    # Repeated deltas before the event loop runs must not schedule another card.
    ctrl.on_answer(message_id="event", text="，继续")
    await asyncio.wait_for(session._card_ready.wait(), timeout=1)
    await _finish_pending(ctrl)

    ctrl._client.cardkit_create.assert_awaited_once()
    card = ctrl._client.cardkit_create.await_args.args[0]
    element_ids = {
        element.get("element_id")
        for element in card["body"]["elements"]
    }
    assert "answer_content" in element_ids
    assert "context_loading_hint" not in element_ids
    assert session.card_id == "card_id_abc"

    assert ctrl.on_completed(
        message_id="event",
        answer="首个回答 token，继续",
    )
    await asyncio.wait_for(asyncio.shield(session.completion_task), timeout=1)
    assert session.state == COMPLETED


@pytest.mark.asyncio
async def test_answer_embedded_in_thinking_callback_also_activates_card() -> None:
    ctrl = _controller()
    ctrl.on_message_started(message_id="event-thinking", chat_id="chat")
    session = ctrl._sess_get("event-thinking")
    assert session is not None and session.state == IDLE

    ctrl.on_thinking(message_id="event-thinking", text="可见答案")
    assert session.state == CREATING
    await asyncio.wait_for(session._card_ready.wait(), timeout=1)
    assert session.card_id == "card_id_abc"
    await _finish_pending(ctrl)


def test_final_without_delta_skips_late_card_and_yields_gateway() -> None:
    ctrl = _controller()
    with patch.object(ctrl, "_complete_session") as complete:
        ctrl.on_message_started(message_id="event-final-only", chat_id="chat")
        handled = ctrl.on_completed(
            message_id="event-final-only",
            answer="provider returned only a final",
        )

    session = ctrl._sess_get("event-final-only")
    assert session is not None
    assert handled is False
    assert session.state == COMPLETED
    assert session.final_answer == "provider returned only a final"
    assert session._card_ready.is_set()
    assert not session.card_id
    complete.assert_not_called()
    ctrl._client.cardkit_create.assert_not_awaited()


def test_stop_during_preflight_has_no_card_wait_or_orphan() -> None:
    ctrl = _controller()
    with patch.object(ctrl, "_complete_session") as complete:
        ctrl.on_message_started(message_id="event-stop", chat_id="chat")
        ctrl.on_aborted(message_id="event-stop")

    session = ctrl._sess_get("event-stop")
    assert session is not None
    assert session.state == ABORTED
    assert session._card_ready.is_set()
    assert session._delivery_done.is_set()
    assert not session.card_id
    complete.assert_not_called()
    ctrl._client.cardkit_create.assert_not_awaited()


def test_new_message_during_preflight_replaces_session_without_loading_cards() -> None:
    ctrl = _controller()
    with patch.object(ctrl, "_complete_session") as complete:
        ctrl.on_message_started(message_id="old-preflight", chat_id="chat")
        ctrl.on_message_started(message_id="new-preflight", chat_id="chat")

    old = ctrl._sess_get("old-preflight")
    new = ctrl._sess_get("new-preflight")
    assert old is not None and old.state == ABORTED
    assert old._card_ready.is_set()
    assert new is not None and new.state == IDLE
    assert new.defer_card_until_answer
    assert ctrl._interrupt_map["old-preflight"] == "new-preflight"
    complete.assert_not_called()
    ctrl._client.cardkit_create.assert_not_awaited()


@pytest.mark.asyncio
async def test_completion_cannot_overtake_claimed_card_creation() -> None:
    ctrl = _controller()
    session = CardSession("race", "chat", asyncio.get_running_loop())
    ctrl._prepare_linear_session(session)
    session.unified_state = UnifiedLinearState()
    session.unified_state.on_answer_delta("answer")
    session._card_activation_requested = True
    session._create_epoch_snap = session.create_epoch
    session.state = CREATING
    ctrl._sess_put("race", session)

    with patch.object(ctrl, "_complete_session"):
        assert ctrl.on_completed(message_id="race", answer="answer")
    assert session.state == COMPLETING

    # The queued create still runs in COMPLETING, publishes the card and
    # releases _card_ready; it cannot become a skipped orphan.
    await ctrl._do_create_linear_card(session)
    assert session.card_id == "card_id_abc"
    assert session._card_ready.is_set()


def test_scheduler_failure_is_terminal_and_releases_waiters() -> None:
    ctrl = _controller(first_answer=False)
    with patch.object(
        ctrl,
        "_fire_and_forget",
        side_effect=lambda coro, loop: coro.close(),
    ):
        ctrl.on_message_started(message_id="schedule-fail", chat_id="chat")

    session = ctrl._sess_get("schedule-fail")
    assert session is not None
    assert session.state == CREATION_FAILED
    assert session._card_ready.is_set()
    assert session._delivery_done.is_set()
