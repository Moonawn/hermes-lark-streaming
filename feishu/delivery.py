"""Durable, read-back-verified Feishu text transport (explicit Profile opt-in).

The progress card is not involved. A saved message ID is read again, never
resent. An ambiguous send reuses the SAME uuid only inside a conservative
50-minute window; outside it human review is safer than an unlabeled duplicate.
No model calls, history rewrites, or fallback to a different chat/thread.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import re
import time
import uuid
from contextvars import ContextVar
from pathlib import Path

from ..state.delivery import DeliveryLedger, digest

logger = logging.getLogger("hermes_lark_streaming")
delivery_context = ContextVar("hls_delivery_context", default=None)
MAX_SEND_ATTEMPTS = 5
MAX_READ_ATTEMPTS = 10
SAFE_UUID_WINDOW = 50 * 60
MAX_JOB_AGE = 24 * 60 * 60
CALL_TIMEOUT = 12
BACKOFF = (2, 10, 30, 120, 300)


class NeedsAttention(Exception):
    pass


def canonical_body(msg_type, payload):
    """Compare content, not transport JSON field order or added empty titles."""
    obj = json.loads(payload) if isinstance(payload, str) else payload
    if not isinstance(obj, dict):
        raise NeedsAttention("message body is not an object")
    if msg_type == "text":
        if not isinstance(obj.get("text"), str):
            raise NeedsAttention("message text missing")
        return json.dumps(["text", obj["text"]], ensure_ascii=False)
    if msg_type != "post":
        raise NeedsAttention("unsupported final message type")
    post = obj.get("zh_cn", obj)
    if not isinstance(post, dict):
        raise NeedsAttention("post content missing")
    # Feishu GET returns legacy rendered rows in `content` (bold text becomes
    # several nodes, fences become code_block) AND the submitted md rows in
    # `content_v2`. Validate v2 when present; never compare flattened previews.
    content_rows = post.get("content_v2", post.get("content"))
    if not isinstance(content_rows, list):
        raise NeedsAttention("post content missing")
    rows = []
    for row in content_rows:
        if not isinstance(row, list):
            raise NeedsAttention("invalid post row")
        nodes = []
        for node in row:
            if not isinstance(node, dict) or node.get("tag") not in {"md", "text"}:
                # This sender only constructs md/text nodes. Never call a
                # missing/unsupported body 'verified' by comparing empty text.
                raise NeedsAttention("unexpected post element")
            if not isinstance(node.get("text"), str):
                raise NeedsAttention("post text missing")
            nodes.append(node["text"])
        rows.append(nodes)
    return json.dumps(["post", post.get("title", ""), rows], ensure_ascii=False)


def _long_fence_header(content):
    return any(len(m.group(0).encode("utf-8")) > 512 for m in
               re.finditer(r"(?m)^ {0,3}(?:`{3,}|~{3,})[^\r\n]*", content))


def lossless_chunks(content, max_bytes=12000):
    """Keep every original separator; balance code fences only for display.

    Hermes's generic splitter lstrips separators at natural split points.
    That is unsuitable for a full-content receipt. Return both the exact raw
    slices and their presentation wrappers so completeness is independently
    checkable before any network side effect.
    """
    wire_len = lambda value: len(json.dumps(value, ensure_ascii=False).encode("utf-8")) - 2
    events = []
    state = None
    pos = 0
    force_plain = False
    for line in content.splitlines(keepends=True):
        fence = re.match(r"^ {0,3}(`{3,}|~{3,})([^\r\n]*)[\r\n]*$", line)
        if fence:
            delimiter, info = fence.groups()
            if state is None:
                state = (delimiter, info)
                # Extremely long fence info strings must not consume a whole
                # transport part. Literal text remains an exact safe fallback.
                force_plain |= len(line.encode("utf-8")) > 512
                events.append((pos, pos + len(line), state))
            elif delimiter[0] == state[0][0] and len(delimiter) >= len(state[0]) and not info.strip():
                state = None
                events.append((pos, pos + len(line), None))
        pos += len(line)
    if wire_len(content) <= max_bytes - 600:
        suffix = (("" if content.endswith("\n") else "\n") + state[0]) if state and not force_plain else ""
        return [(content, content + suffix)]
    pieces = []
    start = 0
    active = None
    event_i = 0
    while start < len(content):
        prefix = (active[0] + active[1] + "\n") if active and not force_plain else ""
        budget = max_bytes - wire_len(prefix) - 600
        low, high = start, min(len(content), start + budget)
        while low < high:
            mid = (low + high + 1) // 2
            if wire_len(content[start:mid]) <= budget:
                low = mid
            else:
                high = mid - 1
        end = low
        if end < len(content):
            cut = max(content.rfind("\n", start, end), content.rfind(" ", start, end))
            if cut >= start + (end - start) // 2:
                end = cut + 1  # KEEP the separator instead of lstrip().
        if not force_plain:
            for line_start, line_end, _ in events[event_i:]:
                if line_start >= end:
                    break
                if line_start < end < line_end:
                    end = line_start if line_start > start else line_end
                    break
        if end <= start:
            raise ValueError("invalid delivery split boundary")
        raw = content[start:end]
        while event_i < len(events) and events[event_i][1] <= end:
            active = events[event_i][2]
            event_i += 1
        suffix = (("" if raw.endswith("\n") else "\n") + active[0]) if active and not force_plain else ""
        pieces.append((raw, prefix + raw + suffix))
        start = end
    if "".join(raw for raw, _ in pieces) != content:
        raise ValueError("delivery split is not lossless")
    return pieces


class VerifiedFeishuDelivery:
    def __init__(self, adapter, ledger=None, *, clock=time.time):
        self.adapter = adapter
        self.app_id = str(getattr(adapter, "_app_id", "") or "")
        if not self.app_id:
            raise ValueError("delivery requires an explicit app identity")
        home = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))).resolve()
        self.scope = digest(str(home) + "\0" + self.app_id)
        self.ledger = ledger or DeliveryLedger(home / "delivery" / "feishu-outbox.sqlite3")
        self.clock = clock
        self.owner = str(uuid.uuid4())
        self._task = None
        self._wake = asyncio.Event()
        self._closing = False
        self._anchor_cache = {}
        self._api_source = None
        self._api = None

    def start(self):
        if self._task is None or self._task.done():
            self._closing = False
            self._task = asyncio.create_task(self._run(), name="hls-verified-delivery")
            self._task.add_done_callback(self._task_done)
            logger.info("HLS verified delivery ready: scope=%s counts=%s",
                        self.scope[:12], self.ledger.counts(self.scope))

    @staticmethod
    def _task_done(task):
        if not task.cancelled() and task.exception() is not None:
            logger.error("HLS delivery recovery stopped: %s", type(task.exception()).__name__)

    async def stop(self):
        self._closing = True
        self._wake.set()
        if self._task and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None

    async def _run(self):
        while not self._closing:
            from ..config import Config
            if not Config().verified_final_delivery:
                break
            try:
                await self.recover_due()
            except Exception as exc:
                logger.error("HLS delivery recovery tick failed: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=15)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def recover_due(self):
        if not getattr(self.adapter, "_client", None):
            return 0
        completed = 0
        for key in self.ledger.due(self.scope, now=self.clock()):
            result = await self.advance(key)
            completed += bool(result.success)
        return completed

    def stage(self, chat_id, content, reply_to=None, metadata=None):
        metadata = metadata or {}
        context = delivery_context.get() or {}
        thread_id = metadata.get("thread_id") or None
        reply_to = reply_to or (metadata.get("reply_to_message_id") if thread_id else None)
        event_ref = metadata.get("hls_delivery_ref") or context.get("event_ref")
        # Preserve separators that the native display-only splitter strips.
        formatted = self.adapter.format_message(content)
        chunks = lossless_chunks(formatted)
        whole_type, _ = self.adapter._build_outbound_payload(formatted)
        literal_fallback = _long_fence_header(formatted)
        if literal_fallback:
            whole_type = "text"
        parts = []
        for index, (raw, chunk) in enumerate(chunks):
            if len(chunks) > 1:
                chunk += ("\n\n" if whole_type == "post" else " ") + f"({index + 1}/{len(chunks)})"
            if literal_fallback:
                msg_type, payload = "text", json.dumps({"text": chunk}, ensure_ascii=False)
            else:
                msg_type, payload = self.adapter._build_outbound_payload(
                    chunk, prefer_post=whole_type == "post",
                )
            if len(payload.encode("utf-8")) > 18000:
                raise ValueError("outbound part exceeds safe payload budget")
            parts.append({"chunk": chunk, "msg_type": msg_type, "payload": payload,
                          "expected": canonical_body(msg_type, payload)})
        return self.ledger.stage(
            scope=self.scope, app_id=self.app_id, chat_id=str(chat_id),
            reply_to=str(reply_to) if reply_to else None,
            thread_id=str(thread_id) if thread_id else None,
            event_ref=str(event_ref) if event_ref else None,
            session_key=context.get("session_key"), content=content, parts=parts,
            now=self.clock(),
        )

    async def deliver(self, chat_id, content, reply_to=None, metadata=None):
        key = self.stage(chat_id, content, reply_to, metadata)
        try:
            # A stalled transport must not occupy the foreground indefinitely.
            # advance() saves/relinquishes its lease on cancellation; the
            # worker resumes only the remaining, persisted parts.
            result = await asyncio.wait_for(self.advance(key), timeout=30)
        except asyncio.TimeoutError:
            result = self._result(self.ledger.get(key, self.scope))
        self._wake.set()
        return result

    @staticmethod
    def _result(job):
        from gateway.platforms.base import SendResult
        verified = job["state"] == "verified"
        last_id = next((p["message_id"] for p in reversed(job["parts"]) if p["message_id"]), None)
        return SendResult(
            success=verified, message_id=last_id,
            error=None if verified else f"Delivery {job['state']}; ref={job['id'][:12]}; "
                                      f"{job.get('last_error') or 'awaiting read-back'}",
            retryable=False,
            raw_response={"delivery_id": job["id"], "delivery_state": job["state"],
                          "part_count": len(job["parts"]),
                          "verified_parts": sum(p["state"] == "verified" for p in job["parts"])},
        )

    async def _call(self, func, request):
        return await asyncio.wait_for(self.adapter._run_blocking(func, request), CALL_TIMEOUT)

    def _messages_api(self):
        client = self.adapter._client
        if client is not self._api_source:
            config = getattr(client, "config", None)
            if config is not None:
                if config.app_id != self.app_id:
                    raise NeedsAttention("SDK client belongs to a different app")
                # wait_for alone cannot stop a blocked executor thread. Give
                # this sender its own SDK service config with a real socket
                # timeout, without changing the connected adapter's settings.
                from lark_oapi.api.im.service import ImService
                bounded = copy.copy(config)
                bounded.timeout = 10.0
                self._api = ImService(bounded).v1.message
            else:
                if type(client).__module__.startswith("lark_oapi"):
                    raise NeedsAttention("SDK client config unavailable")
                self._api = client.im.v1.message  # explicit fake wire in tests
            self._api_source = client
        return self._api

    async def _send(self, job, part):
        adapter = self.adapter
        if not adapter._client:
            raise ConnectionError("adapter not connected")
        if (job["app_id"] != self.app_id or getattr(adapter, "_app_id", None) != self.app_id
                or not job["chat_id"].startswith("oc_")):
            raise NeedsAttention("delivery identity or chat route mismatch")
        await self._validate_anchor(job)
        if job["reply_to"]:
            body = adapter._build_reply_message_body(
                content=part["payload"], msg_type=part["msg_type"],
                reply_in_thread=bool(job["thread_id"]), uuid_value=part["request_uuid"],
            )
            request = adapter._build_reply_message_request(job["reply_to"], body)
            return await self._call(self._messages_api().reply, request)
        destination = job["thread_id"] or job["chat_id"]
        body = adapter._build_create_message_body(
            receive_id=destination, msg_type=part["msg_type"], content=part["payload"],
            uuid_value=part["request_uuid"],
        )
        request = adapter._build_create_message_request(
            "thread_id" if job["thread_id"] else "chat_id", body,
        )
        return await self._call(self._messages_api().create, request)

    async def _validate_anchor(self, job):
        """A reply ID determines the destination. Check it BEFORE sending."""
        if not job["reply_to"]:
            return
        key = (job["chat_id"], job["reply_to"], job["thread_id"])
        if self._anchor_cache.get(key, 0) > self.clock():
            return
        request = self.adapter._build_get_message_request(job["reply_to"])
        response = await self._call(self._messages_api().get, request)
        if not self.adapter._response_succeeded(response):
            raise ConnectionError(f"anchor read API code={getattr(response, 'code', 'unknown')}")
        items = getattr(getattr(response, "data", None), "items", None) or []
        anchor = next((m for m in items if getattr(m, "message_id", None) == job["reply_to"]), None)
        if anchor is None:
            raise NeedsAttention("reply anchor not found; will not redirect")
        if getattr(anchor, "deleted", False):
            raise NeedsAttention("reply anchor withdrawn; will not redirect")
        if getattr(anchor, "chat_id", None) != job["chat_id"]:
            raise NeedsAttention("reply anchor belongs to a different chat; send blocked")
        if job["thread_id"] and job["thread_id"] not in {
            getattr(anchor, "thread_id", None), getattr(anchor, "message_id", None),
        }:
            raise NeedsAttention("reply anchor belongs to a different thread; send blocked")
        # Short-lived, bounded cache; network errors do not populate it.
        if len(self._anchor_cache) >= 500:
            self._anchor_cache.clear()
        self._anchor_cache[key] = self.clock() + 60

    async def _read(self, job, part):
        adapter = self.adapter
        request = adapter._build_get_message_request(part["message_id"])
        response = await self._call(self._messages_api().get, request)
        if not adapter._response_succeeded(response):
            raise ConnectionError(f"read API code={getattr(response, 'code', 'unknown')}")
        items = getattr(getattr(response, "data", None), "items", None) or []
        message = next((m for m in items if getattr(m, "message_id", None) == part["message_id"]), None)
        if message is None:
            raise ConnectionError("message not readable yet")
        if getattr(message, "deleted", False):
            raise NeedsAttention("message was withdrawn; will not recreate it")
        if getattr(message, "chat_id", None) != job["chat_id"]:
            raise NeedsAttention("received message belongs to a different chat")
        if job["thread_id"] and getattr(message, "thread_id", None) != job["thread_id"]:
            raise NeedsAttention("received message belongs to a different thread")
        if job["reply_to"] and getattr(message, "parent_id", None) != job["reply_to"]:
            raise NeedsAttention("received message has a different reply parent")
        sender = getattr(message, "sender", None)
        sender_id = getattr(sender, "id", None)
        permitted = {self.app_id, getattr(adapter, "_bot_open_id", None)} - {None, ""}
        if getattr(sender, "sender_type", None) != "app" or sender_id not in permitted:
            raise NeedsAttention("received message has a different sender")
        if getattr(message, "msg_type", None) != part["msg_type"]:
            raise NeedsAttention("received message has a different type")
        actual = canonical_body(part["msg_type"], getattr(getattr(message, "body", None), "content", None))
        if actual != part["expected"]:
            raise NeedsAttention("received body differs from the saved full part")
        return digest(actual)

    async def advance(self, key):
        job = self.ledger.get(key, self.scope)
        if job is None:
            raise ValueError("delivery does not belong to this Profile/app")
        if job["state"] == "pending" and not getattr(self.adapter, "_client", None):
            job["last_error"] = "adapter disconnected; waiting for reconnect"
            return self._result(job)
        if job["state"] != "pending":
            return self._result(job)
        # Each claim has its own nonce. A task waking after a long suspend
        # cannot mutate a newer claim made by the SAME adapter instance.
        owner = self.owner + ":" + uuid.uuid4().hex
        if not self.ledger.claim(key, self.scope, owner, now=self.clock()):
            return self._result(self.ledger.get(key, self.scope))
        error = ""
        state = "pending"
        attempts = 0
        try:
            if self.clock() - job["created_at"] > MAX_JOB_AGE:
                raise NeedsAttention("delivery older than 24h requires confirmation")
            for part in job["parts"]:
                if part["state"] == "verified":
                    continue
                if not part["message_id"]:
                    # Failed read-only route checks must not spend a send
                    # attempt or start the ambiguity/deduplication clock.
                    await self._validate_anchor(job)
                    if part["send_attempts"] >= MAX_SEND_ATTEMPTS:
                        raise NeedsAttention("send retry budget exhausted")
                    first = part["first_attempt_at"]
                    if first is not None and self.clock() - first >= SAFE_UUID_WINDOW:
                        raise NeedsAttention("ambiguous send exceeded safe deduplication window")
                    part["send_attempts"] += 1
                    attempts = part["send_attempts"]
                    part["first_attempt_at"] = first if first is not None else self.clock()
                    self.ledger.change_part(key, owner, part["part_no"],
                                            state="sending", send_attempts=attempts,
                                            first_attempt_at=part["first_attempt_at"])
                    response = await self._send(job, part)
                    if not self.adapter._response_succeeded(response):
                        detail = str(getattr(response, "msg", "") or "").lower()
                        if part["msg_type"] == "post" and "content format of the post type is incorrect" in detail:
                            # Explicit rejection only: preserve ALL original
                            # text, not the native 3500-character fallback.
                            payload = json.dumps({"text": part["original_chunk"]}, ensure_ascii=False)
                            self.ledger.change_part(key, owner, part["part_no"],
                                msg_type="text", payload=payload, expected=canonical_body("text", payload),
                                request_uuid=digest(f"{key}:{part['part_no']}:text")[:32], state="pending")
                            raise ConnectionError("post rejected; full text fallback queued")
                        raise ConnectionError(f"send API code={getattr(response, 'code', 'unknown')}")
                    message_id = getattr(getattr(response, "data", None), "message_id", None)
                    if not message_id:
                        raise ConnectionError("send returned no message ID")
                    part["message_id"] = message_id
                    self.ledger.change_part(key, owner, part["part_no"],
                                            message_id=message_id, state="sent")
                # A known message ID is never resent, even when GET fails.
                if part["read_attempts"] >= MAX_READ_ATTEMPTS:
                    raise NeedsAttention("read-back retry budget exhausted; not resending")
                part["read_attempts"] += 1
                attempts = part["read_attempts"]
                self.ledger.change_part(key, owner, part["part_no"], read_attempts=attempts)
                observed = await self._read(job, part)
                self.ledger.change_part(key, owner, part["part_no"], state="verified",
                                        observed_hash=observed, verified_at=self.clock())
            state = "verified"
        except NeedsAttention as exc:
            state, error = "needs_attention", str(exc)
        except asyncio.CancelledError:
            error = "sender interrupted; saved receipts retained"
            raise
        except Exception as exc:
            # Never log SDK exception payloads: they can contain URLs/tokens.
            error = f"{type(exc).__name__}: transport/read-back pending"
        finally:
            delay = BACKOFF[min(max(attempts - 1, 0), len(BACKOFF) - 1)] if state == "pending" else 0
            self.ledger.release(key, owner, state=state, error=error, delay=delay, now=self.clock())
            logger.log(logging.INFO if state == "verified" else logging.WARNING,
                       "HLS delivery receipt: ref=%s state=%s detail=%s", key[:12], state, error)
        job = self.ledger.get(key, self.scope)
        if state == "needs_attention":
            await self._notify_attention(job)
        return self._result(job)

    async def _notify_attention(self, job):
        if not self.ledger.claim_notice(job["id"], self.scope):
            return
        text = ("⚠️ 完整答复已保存在本地，但尚未确认全部送达。自动补发已暂停，"
                "以免重复或错发；无需重新分析。交付编号：" + job["id"][:12])
        notice = {"msg_type": "text", "payload": json.dumps({"text": text}, ensure_ascii=False),
                  "request_uuid": digest(job["id"] + ":attention")[:32]}
        try:
            response = await self._send(job, notice)
            if self.adapter._response_succeeded(response):
                self.ledger.notice_ack(job["id"], self.scope,
                    getattr(getattr(response, "data", None), "message_id", None))
        except Exception as exc:
            logger.warning("HLS delivery attention notice unavailable: %s", type(exc).__name__)


def get_delivery(adapter):
    current = getattr(adapter, "_hls_verified_delivery", None)
    if current is None:
        current = VerifiedFeishuDelivery(adapter)
        adapter._hls_verified_delivery = current
    return current
