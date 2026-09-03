"""Delivery receipts use real Hermes formatters/builders and a fake wire.

Every scenario asserts remote content or send counts, not just local state.
No credentials, real Feishu network, model requests, or production DB writes.
"""

import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, patch

import pytest

from hermes_lark_streaming.config import Config
from hermes_lark_streaming.feishu.delivery import (
    VerifiedFeishuDelivery, canonical_body, delivery_context, lossless_chunks,
)
from hermes_lark_streaming.state.delivery import DeliveryLedger
from hermes_lark_streaming.patching.adapter import (
    _wrap_feishu_delivery_retry, _wrap_feishu_delivery_process,
    _wrap_feishu_delivery_connect, _wrap_feishu_delivery_disconnect,
    _wrap_feishu_adapter_send,
)


class Clock:
    def __init__(self):
        self.now = time.time()

    def __call__(self):
        return self.now

    def tick(self, seconds=301):
        self.now += seconds


class Wire:
    def __init__(self):
        self.messages = {}
        self.uuids = {}
        self.send_calls = []
        self.read_calls = []
        self.anchor_calls = []
        self.anchors = {
            key: NS(message_id=key, chat_id="oc_test", deleted=False, thread_id="omt_test")
            for key in ("om_origin", "om_root")
        }
        self.fail_before = 0
        self.fail_after_accept = 0
        self.fail_read = 0
        self.reject_post = False
        self.mutate_read = None

    @staticmethod
    def ok(**data):
        return NS(success=lambda: True, code=0, msg="ok", data=NS(**data))

    def reply(self, request):
        return self.send(request.request_body, getattr(request, "message_id", None))

    def create(self, request):
        return self.send(request.request_body, None)

    def send(self, body, parent):
        self.send_calls.append((body.uuid, body.msg_type, body.content, parent))
        if self.fail_before:
            self.fail_before -= 1
            raise ConnectionError("wire unavailable")
        if body.uuid in self.uuids:
            return self.ok(message_id=self.uuids[body.uuid])
        if body.msg_type == "post" and self.reject_post:
            return NS(success=lambda: False, code=230001,
                      msg="content format of the post type is incorrect", data=None)
        mid = "om_saved_" + str(len(self.messages) + 1)
        payload = json.loads(body.content)
        if body.msg_type == "post":
            payload = {"title": "", "content_v2": payload["zh_cn"]["content"],
                       "content": [[{"tag": "code_block", "text": "legacy rendered representation"}]]}
        self.messages[mid] = NS(
            message_id=mid, chat_id="oc_test", parent_id=parent,
            thread_id="omt_test" if getattr(body, "reply_in_thread", False) else None,
            msg_type=body.msg_type, deleted=False,
            sender=NS(id="cli_test", sender_type="app", id_type="app_id"),
            body=NS(content=json.dumps(payload, ensure_ascii=False)),
        )
        self.uuids[body.uuid] = mid
        if self.fail_after_accept:
            self.fail_after_accept -= 1
            raise TimeoutError("ACK lost after accepting the message")
        return self.ok(message_id=mid)

    def get(self, request):
        if request.message_id in self.anchors:
            self.anchor_calls.append(request.message_id)
            return self.ok(items=[self.anchors[request.message_id]])
        self.read_calls.append(request.message_id)
        if self.fail_read:
            self.fail_read -= 1
            raise ConnectionError("read-back unavailable")
        message = self.messages[request.message_id]
        if self.mutate_read:
            self.mutate_read(message)
        return self.ok(items=[message])


def fixture(tmp_path, monkeypatch, *, wire=None, clock=None):
    native = pytest.importorskip("plugins.platforms.feishu.adapter")
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    wire = wire or Wire()
    clock = clock or Clock()
    adapter = object.__new__(native.FeishuAdapter)
    adapter._app_id = "cli_test"
    adapter._bot_open_id = "ou_bot"
    adapter._client = NS(im=NS(v1=NS(message=wire)))
    async def run(func, request):
        return func(request)
    adapter._run_blocking = run
    ledger = DeliveryLedger(tmp_path / "receipts" / "outbox.sqlite3")
    sender = VerifiedFeishuDelivery(adapter, ledger, clock=clock)
    return sender, wire, clock


def enable():
    Config()._raw = {"gateway": {"delivery_ledger": False}, "hermes_lark_streaming": {
        "final_delivery": "separate_message", "verified_delivery": True,
    }}


@pytest.mark.asyncio
async def test_full_short_answer_and_real_reply_identity(tmp_path, monkeypatch):
    sender, wire, _ = fixture(tmp_path, monkeypatch)
    text = "**结论**\n\n```text\n证据\n```\n\n最后一段必须保留 END"
    result = await sender.deliver("oc_test", text, "om_origin")
    assert result.success
    key = result.raw_response["delivery_id"]
    job = sender.ledger.get(key, sender.scope)
    assert job["content"] == text
    assert job["state"] == "verified"
    assert job["parts"][0]["observed_hash"]
    msg = wire.messages[result.message_id]
    assert msg.parent_id == "om_origin"
    assert "最后一段必须保留 END" in msg.body.content
    assert sender.ledger.path.stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_long_multibyte_final_is_fully_delivered(tmp_path, monkeypatch):
    sender, wire, _ = fixture(tmp_path, monkeypatch)
    text = "这是完整中文正文，不允许丢失。\n" * 900 + "最终校验尾标 END"
    result = await sender.deliver("oc_test", text, "om_origin")
    assert result.success
    job = sender.ledger.get(result.raw_response["delivery_id"], sender.scope)
    assert len(job["parts"]) > 2
    import re
    actual = "".join(re.sub(r" \(\d+/\d+\)$", "", json.loads(m.body.content)["text"])
                     for m in wire.messages.values())
    assert actual == text
    assert len(wire.read_calls) == len(job["parts"])
    assert len({p["request_uuid"] for p in job["parts"]}) == len(job["parts"])


@pytest.mark.asyncio
async def test_read_failure_never_resends_an_acked_message(tmp_path, monkeypatch):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    wire.fail_read = 1
    result = await sender.deliver("oc_test", "全文 END", "om_origin")
    assert not result.success
    assert len(wire.send_calls) == 1
    assert result.raw_response["verified_parts"] == 0
    clock.tick()
    assert await sender.recover_due() == 1
    assert len(wire.send_calls) == 1
    assert len(wire.read_calls) == 2


@pytest.mark.asyncio
async def test_ack_loss_then_new_process_reuses_uuid_without_duplicate(tmp_path, monkeypatch):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    wire.fail_after_accept = 1
    result = await sender.deliver("oc_test", "正文已到达，但模拟回执丢失 END", "om_origin")
    assert not result.success
    assert len(wire.messages) == 1
    # New ledger connection and sender owner represent a new process.
    replacement, _, _ = fixture(tmp_path, monkeypatch, wire=wire, clock=clock)
    clock.tick()
    assert await replacement.recover_due() == 1
    assert len(wire.messages) == 1
    assert wire.send_calls[0][0] == wire.send_calls[1][0]


@pytest.mark.asyncio
async def test_partial_send_recovery_skips_verified_parts(tmp_path, monkeypatch):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    original = wire.get
    def get(request):
        answer = original(request)
        if request.message_id == "om_saved_1":
            wire.fail_before = 1
        return answer
    wire.get = get
    text = "完整正文" * 2500 + "尾标 END"
    result = await sender.deliver("oc_test", text, "om_origin")
    assert not result.success
    assert result.raw_response["verified_parts"] == 1
    first_uuid = wire.send_calls[0][0]
    replacement, _, _ = fixture(tmp_path, monkeypatch, wire=wire, clock=clock)
    clock.tick()
    assert await replacement.recover_due() == 1
    assert sum(c[0] == first_uuid for c in wire.send_calls) == 1


@pytest.mark.asyncio
async def test_same_turn_is_idempotent_but_new_turn_with_same_anchor_is_not(tmp_path, monkeypatch):
    sender, wire, _ = fixture(tmp_path, monkeypatch)
    token = delivery_context.set({"event_ref": "event-1", "session_key": "session"})
    try:
        first = await sender.deliver("oc_test", "相同回答", "om_root", {"thread_id": "omt_test"})
        repeat = await sender.deliver("oc_test", "相同回答", "om_root", {"thread_id": "omt_test"})
    finally:
        delivery_context.reset(token)
    assert first.success and repeat.success
    assert len(wire.send_calls) == 1
    token = delivery_context.set({"event_ref": "event-2", "session_key": "session"})
    try:
        second = await sender.deliver("oc_test", "相同回答", "om_root", {"thread_id": "omt_test"})
    finally:
        delivery_context.reset(token)
    assert second.success


@pytest.mark.asyncio
async def test_shared_anchor_without_turn_identity_does_not_collapse_distinct_sends(tmp_path, monkeypatch):
    sender, wire, _ = fixture(tmp_path, monkeypatch)
    first = await sender.deliver("oc_test", "same answer", "om_root")
    second = await sender.deliver("oc_test", "same answer", "om_root")
    assert first.success and second.success
    assert first.raw_response["delivery_id"] != second.raw_response["delivery_id"]
    assert len(wire.messages) == 2
    assert len(wire.send_calls) == 2
    assert first.raw_response["delivery_id"] != second.raw_response["delivery_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["body", "chat", "sender", "parent", "withdrawn", "thread"])
async def test_mismatch_never_claims_delivery_or_resends_original(tmp_path, monkeypatch, mutation):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    def mutate(msg):
        if mutation == "body":
            msg.body.content = json.dumps({"text": "少了尾段"})
        elif mutation == "chat":
            msg.chat_id = "oc_wrong"
        elif mutation == "sender":
            msg.sender.id = "cli_wrong"
        elif mutation == "parent":
            msg.parent_id = "om_wrong"
        elif mutation == "thread":
            msg.thread_id = "omt_wrong"
        else:
            msg.deleted = True
    wire.mutate_read = mutate
    result = await sender.deliver("oc_test", "完整正文 END", "om_origin", {"thread_id": "omt_test"})
    assert not result.success
    assert result.raw_response["delivery_state"] == "needs_attention"
    clock.tick()
    assert await sender.recover_due() == 0
    original_sends = [c for c in wire.send_calls if "交付编号" not in c[2]]
    assert len(original_sends) == 1


@pytest.mark.asyncio
async def test_ambiguous_send_outside_uuid_window_stops(tmp_path, monkeypatch):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    wire.fail_after_accept = 1
    result = await sender.deliver("oc_test", "不要盲目重发 END", "om_origin")
    clock.tick(3100)
    result = await sender.advance(result.raw_response["delivery_id"])
    assert not result.success
    assert result.raw_response["delivery_state"] == "needs_attention"
    assert len([c for c in wire.send_calls if "交付编号" not in c[2]]) == 1


@pytest.mark.asyncio
async def test_retry_budget_is_bounded(tmp_path, monkeypatch):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    wire.fail_before = 100
    result = await sender.deliver("oc_test", "全文 END", "om_origin")
    for _ in range(7):
        clock.tick()
        await sender.recover_due()
    job = sender.ledger.get(result.raw_response["delivery_id"], sender.scope)
    assert job["state"] == "needs_attention"
    assert job["parts"][0]["send_attempts"] == 5
    assert job["content"] == "全文 END"


@pytest.mark.asyncio
async def test_post_rejection_falls_back_to_full_text_not_3500_chars(tmp_path, monkeypatch):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    wire.reject_post = True
    text = "**标题**\n" + "x" * 6000 + "最后一段 END"
    result = await sender.deliver("oc_test", text, "om_origin")
    assert not result.success
    clock.tick()
    assert await sender.recover_due() == 1
    assert len(wire.messages) == 1
    body = json.loads(next(iter(wire.messages.values())).body.content)
    assert body["text"] == text


@pytest.mark.asyncio
async def test_scope_never_recovers_another_profile_or_app(tmp_path, monkeypatch):
    sender, wire, _ = fixture(tmp_path, monkeypatch)
    wire.fail_before = 1
    result = await sender.deliver("oc_test", "profile-private END", "om_origin")
    sender.adapter._app_id = "cli_other"
    other = VerifiedFeishuDelivery(sender.adapter, sender.ledger)
    assert await other.recover_due() == 0
    with pytest.raises(ValueError, match="Profile/app"):
        await other.advance(result.raw_response["delivery_id"])
    assert len(wire.send_calls) == 1


@pytest.mark.asyncio
async def test_live_lease_blocks_second_sender_and_expired_lease_recovers(tmp_path, monkeypatch):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    key = sender.stage("oc_test", "重启恢复 END", "om_origin")
    assert sender.ledger.claim(key, sender.scope, "dead-process", now=clock())
    result = await sender.advance(key)
    assert not result.success
    assert not wire.send_calls
    clock.tick(91)
    result = await sender.advance(key)
    assert result.success
    assert len(wire.send_calls) == 1


@pytest.mark.asyncio
async def test_cancel_during_read_preserves_receipt_for_restart(tmp_path, monkeypatch):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    entered = asyncio.Event()
    original_call = sender._call
    async def block_get(func, request):
        if getattr(func, "__name__", "") == "get":
            entered.set()
            await asyncio.Future()
        return await original_call(func, request)
    sender._call = block_get
    task = asyncio.create_task(sender.deliver("oc_test", "取消后不重复 END", "om_origin"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    replacement, _, _ = fixture(tmp_path, monkeypatch, wire=wire, clock=clock)
    clock.tick()
    assert await replacement.recover_due() == 1
    assert len(wire.send_calls) == 1


def test_verified_gate_requires_native_redeliverer_disabled():
    cfg = Config()
    cfg._raw = {"hermes_lark_streaming": {"final_delivery": "separate_message", "verified_delivery": True}}
    assert not cfg.verified_final_delivery
    cfg._raw["gateway"] = {"delivery_ledger": False}
    assert cfg.verified_final_delivery


@pytest.mark.asyncio
async def test_retry_wrapper_never_invokes_native_truncating_fallback():
    enable()
    failed = NS(success=False, error="needs attention")
    adapter = NS(send=AsyncMock(return_value=failed))
    native = AsyncMock()
    result = await _wrap_feishu_delivery_retry(native)(adapter, "oc_test", "全文" * 5000)
    assert result is failed
    native.assert_not_awaited()
    assert adapter.send.await_args.args[1] == "全文" * 5000


@pytest.mark.asyncio
async def test_process_context_preserves_event_id_and_restores_after_error():
    async def orig(adapter, event, session_key):
        assert delivery_context.get()["event_ref"] == "om_turn"
        assert delivery_context.get()["session_key"] == "session"
        raise RuntimeError("test")
    with pytest.raises(RuntimeError, match="test"):
        await _wrap_feishu_delivery_process(orig)(NS(), NS(message_id="om_turn", text="问题"), "session")
    assert delivery_context.get() is None


@pytest.mark.asyncio
async def test_connect_starts_recovery_disconnect_stops_it(tmp_path, monkeypatch):
    enable()
    sender, _, _ = fixture(tmp_path, monkeypatch)
    sender.adapter._hls_verified_delivery = sender
    connect = AsyncMock(return_value=True)
    disconnect = AsyncMock()
    assert await _wrap_feishu_delivery_connect(connect)(sender.adapter)
    assert sender._task is not None
    await _wrap_feishu_delivery_disconnect(disconnect)(sender.adapter)
    assert sender._task is None
    disconnect.assert_awaited_once()


def test_v2_raw_body_is_authoritative_over_legacy_rendered_rows():
    raw = [[{"tag": "md", "text": "**完整**\n```python\nprint(1)\n```\n末段 END"}]]
    expected = canonical_body("post", {"zh_cn": {"content": raw}})
    remote = {"title": "", "content": [[{"tag": "code_block", "text": "different legacy layout"}]],
              "content_v2": raw}
    assert canonical_body("post", remote) == expected
    remote["content_v2"] = [[{"tag": "md", "text": "**完整**"}]]
    assert canonical_body("post", remote) != expected


@pytest.mark.parametrize("text", [
    "```python\n" + "print('保留换行')\n" * 1000 + "```\n\n最后一段 END",
    "\n\n保留空白  \t\n" * 1300 + "END",
    "前文\n```text\n" + "未闭合代码行\n" * 1000,
    "😀混合UTF8" * 2200 + "尾部 END",
    "\x00\t\r\n" * 2000 + "END",
])
def test_chunk_plan_is_lossless_and_bounded(text):
    pieces = lossless_chunks(text)
    assert "".join(raw for raw, _ in pieces) == text
    assert all(len(json.dumps(rendered, ensure_ascii=False).encode("utf-8")) <= 12000
               for _, rendered in pieces)
    if "```" in text:
        assert all(sum(line.strip().startswith("```") for line in rendered.splitlines()) % 2 == 0
                   for _, rendered in pieces)


@pytest.mark.asyncio
async def test_no_untracked_send_if_local_receipt_cannot_be_saved(monkeypatch):
    pytest.importorskip("gateway.platforms.base")
    enable()
    adapter = NS()
    from hermes_lark_streaming.patching import _patched_feishu_classes
    _patched_feishu_classes.add(id(type(adapter)))
    native = AsyncMock()
    with patch("hermes_lark_streaming.feishu.delivery.get_delivery", side_effect=OSError("disk full")):
        result = await _wrap_feishu_adapter_send(native)(adapter, "oc_test", "必须完整保留" * 2000)
    assert not result.success
    native.assert_not_awaited()


@pytest.mark.asyncio
async def test_ephemeral_command_replies_do_not_enter_recovery_ledger():
    enable()
    adapter = NS(send=AsyncMock())
    native = AsyncMock(return_value=NS(success=True))
    token = delivery_context.set({"ephemeral": True})
    try:
        result = await _wrap_feishu_delivery_retry(native)(adapter, "oc_test", "session reset notice")
    finally:
        delivery_context.reset(token)
    assert result.success
    native.assert_awaited_once()
    adapter.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_wrong_reply_route_is_blocked_before_any_private_content_leaves(tmp_path, monkeypatch):
    sender, wire, _ = fixture(tmp_path, monkeypatch)
    wire.anchors["om_origin"].chat_id = "oc_other"
    result = await sender.deliver("oc_test", "这份私人正文不应离开目标群", "om_origin")
    assert not result.success
    assert result.raw_response["delivery_state"] == "needs_attention"
    assert not wire.send_calls


@pytest.mark.asyncio
async def test_disconnected_sender_does_not_spend_send_attempts(tmp_path, monkeypatch):
    sender, wire, _ = fixture(tmp_path, monkeypatch)
    client = sender.adapter._client
    sender.adapter._client = None
    result = await sender.deliver("oc_test", "连接恢复后送达 END", "om_origin")
    assert not result.success
    job = sender.ledger.get(result.raw_response["delivery_id"], sender.scope)
    assert job["parts"][0]["send_attempts"] == 0
    assert job["parts"][0]["first_attempt_at"] is None
    sender.adapter._client = client
    assert await sender.recover_due() == 1
    assert len(wire.messages) == 1


@pytest.mark.asyncio
async def test_concurrent_senders_share_one_lease_and_one_remote_message(tmp_path, monkeypatch):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    other, _, _ = fixture(tmp_path, monkeypatch, wire=wire, clock=clock)
    key = sender.stage("oc_test", "并发不重复 END", "om_origin")
    first, second = await asyncio.gather(sender.advance(key), other.advance(key))
    assert first.success or second.success
    assert len(wire.messages) == 1
    assert sender.ledger.get(key, sender.scope)["state"] == "verified"


def test_real_sdk_service_has_socket_timeout_without_mutating_connected_client(tmp_path, monkeypatch):
    lark = pytest.importorskip("lark_oapi")
    sender, _, _ = fixture(tmp_path, monkeypatch)
    client = lark.Client.builder().app_id("cli_test").app_secret("fake-unit-test-secret").build()
    sender.adapter._client = client
    before = client.config.timeout
    api = sender._messages_api()
    bounded = getattr(api, "config", getattr(api, "_config", None))
    assert bounded is not client.config
    assert bounded.timeout == 10.0
    assert client.config.timeout == before


@pytest.mark.asyncio
async def test_cached_sdk_identity_change_is_blocked_before_send(tmp_path, monkeypatch):
    sender, wire, clock = fixture(tmp_path, monkeypatch)
    key = sender.stage("oc_test", "不能换身份发送 END", "om_origin")
    sender.adapter._app_id = "cli_other"
    result = await sender.advance(key)
    assert not result.success
    assert result.raw_response["delivery_state"] == "needs_attention"
    assert not wire.send_calls


def test_stale_claim_cannot_mutate_a_newer_claim(tmp_path, monkeypatch):
    sender, _, clock = fixture(tmp_path, monkeypatch)
    key = sender.stage("oc_test", "休眠后防止旧任务覆盖新回执", "om_origin")
    assert sender.ledger.claim(key, sender.scope, "old-claim", now=clock())
    clock.tick(91)
    assert sender.ledger.claim(key, sender.scope, "new-claim", now=clock())
    with pytest.raises(RuntimeError, match="no longer owned"):
        sender.ledger.change_part(key, "old-claim", 0, state="verified")
    sender.ledger.release(key, "old-claim", state="pending", now=clock())
    job = sender.ledger.get(key, sender.scope)
    assert job["lease_owner"] == "new-claim"
    assert job["parts"][0]["state"] == "pending"
