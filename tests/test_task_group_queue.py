from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from hermes_lark_streaming.config import Config
from hermes_lark_streaming.controller import CardSession, StreamCardController
from hermes_lark_streaming.patching.gateway import (
    _wrap_deliver_queued_first_response,
    _waiting_message_is_available,
    _wrap_handle_message,
)
from hermes_lark_streaming.patching.adapter import (
    _wrap_feishu_adapter_dispatch_inbound,
)


def _source(chat_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        platform=SimpleNamespace(value="feishu"),
        chat_id=chat_id,
        thread_id=None,
    )


@pytest.mark.asyncio
async def test_serialized_chat_runs_one_message_at_a_time() -> None:
    chat_id = "oc_serial_queue_test"
    Config()._raw = {
        "hermes_lark_streaming": {
            "queue": {
                "serialized_chat_ids": [chat_id],
                "verify_waiting_message_available": False,
            }
        }
    }
    active = 0
    peak = 0
    order: list[str] = []

    async def original(_runner, event):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        order.append(f"start:{event.message_id}")
        await asyncio.sleep(0.02)
        order.append(f"end:{event.message_id}")
        active -= 1

    runner = SimpleNamespace(
        _reply_anchor_for_event=lambda _event: None,
    )
    wrapped = _wrap_handle_message(original)
    first = SimpleNamespace(
        message_id="om_first",
        source=_source(chat_id),
        text="first",
        raw_message=None,
        reply_to_message_id=None,
    )
    second = SimpleNamespace(
        message_id="om_second",
        source=_source(chat_id),
        text="second",
        raw_message=None,
        reply_to_message_id=None,
    )

    await asyncio.gather(wrapped(runner, first), wrapped(runner, second))

    assert peak == 1
    assert order == [
        "start:om_first",
        "end:om_first",
        "start:om_second",
        "end:om_second",
    ]


@pytest.mark.asyncio
async def test_recalled_waiting_message_is_dropped() -> None:
    response = SimpleNamespace(code=230011, success=lambda: False)

    class Adapter:
        _client = SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(message=SimpleNamespace(get=object()))
            )
        )

        @staticmethod
        def _build_get_message_request(message_id):
            return message_id

        @staticmethod
        async def _run_blocking(_method, _request):
            return response

    runner = SimpleNamespace(_adapter_for_source=lambda _source: Adapter())
    event = SimpleNamespace(message_id="om_recalled", source=_source("oc_chat"))

    assert await _waiting_message_is_available(runner, event) is False


@pytest.mark.asyncio
async def test_delivery_wait_releases_only_after_final_delivery() -> None:
    controller = StreamCardController()
    session = CardSession("om_delivery", "oc_chat", asyncio.get_running_loop())
    controller._sess_put(session.message_id, session)

    waiter = asyncio.create_task(
        controller.wait_for_delivery(session.message_id, timeout=1)
    )
    await asyncio.sleep(0)
    assert waiter.done() is False

    session.mark_delivery_done(True)

    assert await waiter is True


@pytest.mark.asyncio
async def test_serialized_chat_text_bypasses_feishu_aggregation() -> None:
    chat_id = "oc_serial_queue_test"
    Config()._raw = {
        "hermes_lark_streaming": {
            "queue": {
                "serialized_chat_ids": [chat_id],
            }
        }
    }
    calls: list[str] = []

    async def original(_adapter, event):
        calls.append(f"batched:{event.message_id}")

    class Adapter:
        async def _handle_message_with_guards(self, event):
            calls.append(f"direct:{event.message_id}")

    wrapped = _wrap_feishu_adapter_dispatch_inbound(original)
    text_event = SimpleNamespace(
        source=_source(chat_id),
        message_type=SimpleNamespace(value="text"),
        message_id="om_text",
        is_command=lambda: False,
    )
    other_event = SimpleNamespace(
        source=_source("oc_other"),
        message_type=SimpleNamespace(value="text"),
        message_id="om_other",
        is_command=lambda: False,
    )

    adapter = Adapter()
    await wrapped(adapter, text_event)
    await wrapped(adapter, other_event)

    assert calls == ["direct:om_text", "batched:om_other"]


@pytest.mark.asyncio
async def test_serialized_card_completion_timeout_uses_text_fallback() -> None:
    controller = StreamCardController()
    controller._cfg = SimpleNamespace(
        serialized_chat_ids={"oc_task"},
        card_completion_timeout_sec=0.01,
    )
    session = CardSession("om_timeout", "oc_task", asyncio.get_running_loop())
    session.state = "completing"
    fallback_calls: list[str] = []

    async def stalled_completion(_session):
        await asyncio.Event().wait()

    async def fallback(_session, *, fallback_text=""):
        fallback_calls.append(fallback_text)
        return True

    controller._do_linear_complete = stalled_completion
    controller._send_text_fallback = fallback

    await controller._do_linear_complete_with_fallback(session)

    assert fallback_calls == [""]
    assert session.state == "creation_failed"
    assert session._delivery_success is True
    assert session._delivery_done.is_set()


@pytest.mark.asyncio
async def test_queued_followup_waits_for_current_card_delivery(monkeypatch) -> None:
    chat_id = "oc_serial_queue_test"
    Config()._raw = {
        "hermes_lark_streaming": {
            "queue": {
                "serialized_chat_ids": [chat_id],
                "delivery_wait_timeout_sec": 5,
            }
        }
    }
    session = SimpleNamespace(state="streaming")
    calls: list[tuple] = []

    class Controller:
        enabled = True

        @staticmethod
        def _sess_get(message_id):
            assert message_id == "om_first"
            return session

        @staticmethod
        def on_completed(**kwargs):
            calls.append(("complete", kwargs["message_id"], kwargs["answer"]))
            session.state = "completed"
            return True

        @staticmethod
        async def wait_for_delivery(message_id, timeout):
            calls.append(("wait", message_id, timeout))
            return True

    import hermes_lark_streaming.controller as controller_module

    monkeypatch.setattr(controller_module, "get_controller", lambda: Controller())

    async def original(
        _runner,
        response,
        source,
        adapter,
        metadata=None,
        event_message_id=None,
        text_already_delivered=False,
        deliver_media=True,
    ):
        calls.append(
            (
                "deliver",
                response,
                event_message_id,
                text_already_delivered,
                deliver_media,
            )
        )

    wrapped = _wrap_deliver_queued_first_response(original)
    await wrapped(
        SimpleNamespace(),
        "first answer",
        _source(chat_id),
        SimpleNamespace(),
        event_message_id="om_first",
    )

    assert calls == [
        ("complete", "om_first", "first answer"),
        ("wait", "om_first", 5),
        ("deliver", "first answer", "om_first", True, True),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("separate,owned", [(True, True), (False, False)])
async def test_queued_followup_cannot_treat_progress_or_another_final_as_delivered(monkeypatch, separate, owned):
    from unittest.mock import AsyncMock, Mock
    chat_id = "oc_queue_contract"
    Config()._raw = {"hermes_lark_streaming": {
        "final_delivery": "separate_message" if separate else "card",
        "queue": {"serialized_chat_ids": [chat_id]},
    }}
    controller = SimpleNamespace(
        enabled=True,
        _sess_get=Mock(return_value=SimpleNamespace(state="completed")),
        on_completed=Mock(return_value=owned),
        wait_for_delivery=AsyncMock(return_value=True),
    )
    import hermes_lark_streaming.controller as controller_module
    monkeypatch.setattr(controller_module, "get_controller", lambda: controller)
    native = AsyncMock()
    await _wrap_deliver_queued_first_response(native)(
        SimpleNamespace(), "new full final END", _source(chat_id), SimpleNamespace(),
        event_message_id="om_new_turn",
    )
    controller.wait_for_delivery.assert_not_awaited()
    assert native.await_args.kwargs["text_already_delivered"] is False
    assert native.await_args.args[1] == "new full final END"
