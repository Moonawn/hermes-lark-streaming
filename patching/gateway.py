"""GatewayRunner method wrappers and cron delivery interception."""

from __future__ import annotations

import asyncio
import functools
import threading
import time
from typing import Any, Callable

from .. import __version__
from ..state.phase import TERMINAL_PHASES
from . import (
    _HLS_STATUS_DELIVERY_KEY,
    _HLS_STATUS_KEY,
    _HLS_STATUS_TURN_KEY,
    _get_config,
    _msg_ctx,
    _started_msg_ids,
    _started_msg_ids_lock,
    _thread_local_ctx,
    _logger,
    _send_result_ok,
)

_chat_serial_locks: dict[tuple[int, str], asyncio.Lock] = {}
_chat_serial_owners: dict[tuple[int, str], asyncio.Task[Any] | None] = {}
_chat_serial_locks_guard = threading.Lock()
_UNAVAILABLE_MESSAGE_CODES = {230011, 231003, 1000023}


def _wrap_send_or_update_status(orig: Callable) -> Callable:
    """Tag one Hermes status send with its originating Feishu turn.

    ``TurnRunner._status_callback_sync`` runs on the agent worker thread and
    creates the delivery coroutine before scheduling it on the gateway event
    loop.  Capturing the HLS ContextVar/thread-local here preserves the exact
    inbound message id; trying to discover it later inside ``adapter.send`` is
    racy and often sees an empty context.

    Only Feishu/Lark chat ids use the ``oc_`` prefix.  Other gateway platforms
    keep byte-for-byte metadata behavior.
    """

    @functools.wraps(orig)
    def wrapper(adapter, chat_id, status_key, content, metadata=None):
        if not str(chat_id or "").startswith("oc_"):
            return orig(
                adapter,
                chat_id,
                status_key,
                content,
                metadata=metadata,
            )

        try:
            marked_metadata = dict(metadata or {})
        except (TypeError, ValueError):
            return orig(
                adapter,
                chat_id,
                status_key,
                content,
                metadata=metadata,
            )

        ctx = _msg_ctx.get(None)
        if ctx is None:
            ctx = getattr(_thread_local_ctx, "data", None)
        marked_metadata[_HLS_STATUS_DELIVERY_KEY] = True
        marked_metadata[_HLS_STATUS_KEY] = str(status_key or "status")
        if isinstance(ctx, dict):
            turn_message_id = str(
                ctx.get("event_message_id")
                or ctx.get("message_id")
                or ""
            )
            if turn_message_id:
                marked_metadata[_HLS_STATUS_TURN_KEY] = turn_message_id

        return orig(
            adapter,
            chat_id,
            status_key,
            content,
            metadata=marked_metadata,
        )

    wrapper._hls_status_metadata = True  # type: ignore[attr-defined]
    return wrapper


def _is_serialized_feishu_chat(source: Any) -> bool:
    platform = getattr(getattr(source, "platform", None), "value", "")
    chat_id = str(getattr(source, "chat_id", "") or "")
    if str(platform).lower() not in ("feishu", "lark") or not chat_id:
        return False
    try:
        from ..config import Config

        return chat_id in Config().serialized_chat_ids
    except Exception:
        _logger.debug("HLS: serialized chat config lookup failed", exc_info=True)
        return False


def _gateway_terminal_payload(result: Any) -> tuple[str, str]:
    """Extract a terminal answer/error returned outside ``_run_agent``.

    Hermes can fail during preflight context compression before the wrapped
    ``_run_agent`` completion hook runs. In that case
    ``_handle_message_with_agent`` returns a user-safe plain-text error while
    the already-published CardKit session is still STREAMING. Keep that
    terminal payload in the same card instead of suppressing it and leaving
    the card open forever.
    """
    if isinstance(result, str):
        text = result.strip()
        if not text:
            return "", ""
        lowered = text.lower()
        is_error = lowered.startswith(
            (
                "sorry, i encountered an unexpected error",
                "agent error",
                "error:",
            )
        ) or text.startswith(("⚠", "❌"))
        return ("", text) if is_error else (text, "")
    if isinstance(result, dict):
        error = str(
            result.get("error")
            or result.get("interrupt_message")
            or ""
        ).strip()
        answer = str(result.get("final_response") or "").strip()
        return answer, error
    return "", ""


def _chat_serial_lock(chat_id: str) -> tuple[tuple[int, str], asyncio.Lock]:
    loop = asyncio.get_running_loop()
    key = (id(loop), chat_id)
    with _chat_serial_locks_guard:
        lock = _chat_serial_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _chat_serial_locks[key] = lock
    return key, lock


def _is_serial_control_command(event: Any) -> bool:
    text = str(getattr(event, "text", "") or "").strip().lower()
    return text == "/stop" or text.startswith("/stop ")


async def _waiting_message_is_available(runner: Any, event: Any) -> bool:
    """Fail closed only for an explicit Feishu recalled/deleted response."""
    message_id = str(getattr(event, "message_id", "") or "").strip()
    source = getattr(event, "source", None)
    if not message_id or source is None:
        return True
    try:
        adapter = runner._adapter_for_source(source)
        client = getattr(adapter, "_client", None)
        if (
            adapter is None
            or client is None
            or not hasattr(adapter, "_build_get_message_request")
            or not hasattr(adapter, "_run_blocking")
        ):
            return True
        request = adapter._build_get_message_request(message_id)
        response = await asyncio.wait_for(
            adapter._run_blocking(client.im.v1.message.get, request), timeout=3.0,
        )
        if response and getattr(response, "success", lambda: False)():
            return True
        code = int(getattr(response, "code", 0) or 0)
        if code in _UNAVAILABLE_MESSAGE_CODES:
            _logger.info(
                "HLS: dropping unavailable queued message msg=%s code=%s",
                message_id[:12],
                code,
            )
            return False
        _logger.warning(
            "HLS: queued message availability check inconclusive; failing open "
            "msg=%s code=%s",
            message_id[:12],
            code,
        )
    except Exception:
        _logger.warning(
            "HLS: queued message availability check failed; failing open msg=%s",
            message_id[:12],
            exc_info=True,
        )
    return True

# ── GatewayRunner method wrappers ──────────────────────────────────

def _wrap_deliver_queued_first_response(orig: Callable) -> Callable:
    """Seal the current task card before Hermes starts its queued follow-up."""

    @functools.wraps(orig)
    async def wrapper(
        self,
        response,
        source,
        adapter,
        metadata=None,
        event_message_id=None,
        text_already_delivered=False,
        deliver_media=True,
    ):
        if _is_serialized_feishu_chat(source) and event_message_id:
            try:
                from ..config import Config
                from ..controller import get_controller

                ctrl = get_controller()
                session = ctrl._sess_get(event_message_id) if ctrl else None
                if ctrl and ctrl.enabled and session is not None:
                    card_owned = ctrl.on_completed(
                        message_id=event_message_id,
                        answer=response or "",
                    )
                    card_or_fallback_delivered = False
                    if card_owned and not _get_config().independent_final_delivery:
                        card_or_fallback_delivered = await ctrl.wait_for_delivery(
                            event_message_id,
                            timeout=Config().delivery_wait_timeout_sec,
                        )
                    if card_or_fallback_delivered:
                        text_already_delivered = True
                        _logger.info(
                            "HLS: queued-turn delivery confirmed before follow-up "
                            "msg=%s state=%s",
                            str(event_message_id)[:12],
                            session.state,
                        )
                    else:
                        _logger.warning(
                            "HLS: queued-turn delivery unconfirmed; retaining Hermes "
                            "text fallback msg=%s state=%s",
                            str(event_message_id)[:12],
                            session.state,
                        )
            except Exception:
                _logger.warning(
                    "HLS: queued-turn pre-follow-up completion failed msg=%s",
                    str(event_message_id or "?")[:12],
                    exc_info=True,
                )

        return await orig(
            self,
            response,
            source,
            adapter,
            metadata=metadata,
            event_message_id=event_message_id,
            text_already_delivered=text_already_delivered,
            deliver_media=deliver_media,
        )

    return wrapper


def _wrap_handle_message(orig: Callable) -> Callable:
    """Inject NORMALIZE hook at the top of GatewayRunner._handle_message."""

    @functools.wraps(orig)
    async def wrapper(self, event, *args, **kwargs):
        source = getattr(event, "source", None)
        if _is_serialized_feishu_chat(source) and not _is_serial_control_command(event):
            chat_id = str(getattr(source, "chat_id", "") or "")
            key, lock = _chat_serial_lock(chat_id)
            current_task = asyncio.current_task()
            if _chat_serial_owners.get(key) is not current_task:
                queued = lock.locked()
                wait_started = time.monotonic()
                if queued:
                    _logger.info(
                        "HLS: task-group queue waiting chat=%s msg=%s",
                        chat_id[:12],
                        str(getattr(event, "message_id", "") or "?")[:12],
                    )
                async with lock:
                    _chat_serial_owners[key] = current_task
                    try:
                        waited = time.monotonic() - wait_started
                        if queued:
                            try:
                                from ..config import Config

                                verify_available = Config().verify_waiting_message_available
                            except Exception:
                                verify_available = True
                            if verify_available and not await _waiting_message_is_available(self, event):
                                return None
                            _logger.info(
                                "HLS: task-group queue acquired chat=%s msg=%s wait=%.3fs",
                                chat_id[:12],
                                str(getattr(event, "message_id", "") or "?")[:12],
                                waited,
                            )
                        return await wrapper(self, event, *args, **kwargs)
                    finally:
                        if _chat_serial_owners.get(key) is current_task:
                            _chat_serial_owners.pop(key, None)

        # NORMALIZE hook — fires before any message processing
        try:
            from .hooks import on_feishu_normalize

            on_feishu_normalize(
                message_id=event.message_id,
                source=event.source,
                event=event,
                reply_anchor_id=self._reply_anchor_for_event(event),
            )
        except Exception:
            _logger.warning("HLS: suppressed exception", exc_info=True)

        try:
            _text = (getattr(event, "text", "") or "").strip()
            if _text.lower().startswith("/aowen"):
                _source = getattr(event, "source", None)
                _platform = getattr(getattr(_source, "platform", None), "value", "")
                if _platform == "feishu" and hasattr(self, "_running_agents"):
                    _quick_key = None
                    try:
                        _quick_key = self._session_key_for_source(_source)
                    except Exception:
                        _logger.debug("HLS: _session_key_for_source failed", exc_info=True)
                    if _quick_key and _quick_key in self._running_agents:
                        # Agent is running — send interrupt hint card
                        from ..aowen import build_interrupt_hint_card, _send_card_async
                        _chat_id = getattr(_source, "chat_id", "") if _source else ""
                        if _chat_id:
                            _logger.info(
                                "HLS: /aowen during active agent (session=%s), "
                                "sending interrupt hint card",
                                str(_quick_key)[:12],
                            )
                            _send_card_async(_chat_id, build_interrupt_hint_card(), "interrupt_hint")
                            return ""
        except Exception:
            _logger.debug("HLS: /aowen interrupt hint check failed", exc_info=True)

        return await orig(self, event, *args, **kwargs)

    return wrapper

def _wrap_handle_message_with_agent(orig: Callable) -> Callable:
    """Inject START hook at entry and ABORT/INTERRUPT detection on return."""

    @functools.wraps(orig)
    async def wrapper(self, event, source, *args, **kwargs):
        # This wrapper owns Feishu/Lark card lifecycle only.  GatewayRunner
        # also routes A2A and other platforms through the same method; letting
        # those messages enter HLS creates cards with non-Feishu message IDs
        # and causes avoidable OpenAPI failures.
        platform_name = getattr(getattr(source, "platform", None), "value", "").lower()
        if platform_name not in ("feishu", "lark"):
            return await orig(self, event, source, *args, **kwargs)

        mid = event.message_id
        anchor_id = self._reply_anchor_for_event(event)
        chat_id = source.chat_id if hasattr(source, "chat_id") else ""

        # Track this message as started (for interrupt detection)
        with _started_msg_ids_lock:
            _started_msg_ids.add(mid)

        # ── START hook ──
        try:
            from .hooks import on_message_started

            on_message_started(
                message_id=mid,
                chat_id=chat_id,
                anchor_id=anchor_id,
            )
        except Exception:
            _logger.warning("HLS: suppressed exception", exc_info=True)
        msg_context = {
            "message_id": mid,
            "chat_id": chat_id,
            "anchor_id": anchor_id,
            "event_message_id": "",  # filled by _wrap_run_agent
            "card_sent": False,
            "_msg_start_time": time.monotonic(),  # 自计时：替代无法获取的 _response_time 局部变量
        }
        _msg_ctx.set(msg_context)

        # v1.3.4 fix (P1): 确保 orig() 抛异常时 _msg_ctx / _started_msg_ids
        # 导致 _msg_ctx 保留 stale event_message_id，下一条消息的
        # FeishuAdapter.send() 被静默抑制（"卡片不出现" bug）。
        def _hls_cleanup_ctx() -> None:
            with _started_msg_ids_lock:
                _started_msg_ids.discard(mid)
            _msg_ctx.set(None)
            _thread_local_ctx.data = None

        try:
            result = await orig(self, event, source, *args, **kwargs)
        except BaseException:
            _hls_cleanup_ctx()
            raise

        # point to the new message's context. We must use the original
        ctx = msg_context
        if ctx.get("final_yielded"):
            _hls_cleanup_ctx()
            return result

        # Hermes 0.21 can fail closed during preflight compression before
        # _wrap_run_agent reaches its normal COMPLETE hook. The gateway then
        # returns a safe text error here while the message-start card is still
        # STREAMING. Previously the session-existence fallback below merely
        # suppressed that text, leaving the card spinning indefinitely.
        # Promote the gateway result into the existing card's terminal state;
        # serialized chats will wait for the delivery ACK below before the
        # native reply is suppressed.
        if result is not None and not _get_config().independent_final_delivery:
            try:
                from ..controller import get_controller
                from .hooks import on_message_completed

                _ctrl = get_controller()
                _eid = ctx.get("event_message_id") or mid
                _sess = _ctrl._sess_get(_eid) if _ctrl and _ctrl.enabled else None
                if (
                    _sess
                    and _sess.card_msg_id
                    and _sess.state not in TERMINAL_PHASES
                    and _sess.state != "completing"
                ):
                    _answer, _error = _gateway_terminal_payload(result)
                    if _answer or _error:
                        _owned = on_message_completed(
                            message_id=_eid,
                            answer=_answer,
                            duration=time.monotonic()
                            - ctx.get("_msg_start_time", time.monotonic()),
                            error_message=_error,
                        )
                        if _owned:
                            ctx["card_sent"] = True
                            _logger.info(
                                "HLS: terminal gateway result promoted into active "
                                "card msg=%s kind=%s",
                                (_eid or "?")[:12],
                                "error" if _error else "answer",
                            )
            except Exception:
                _logger.warning(
                    "HLS: failed to promote terminal gateway result into card",
                    exc_info=True,
                )
        serialized_chat = _is_serialized_feishu_chat(source)
        if serialized_chat and ctx.get("card_sent") and not _get_config().independent_final_delivery:
            try:
                from ..config import Config
                from ..controller import get_controller

                _ctrl = get_controller()
                _eid = ctx.get("event_message_id") or mid
                ctx["delivery_confirmed"] = await _ctrl.wait_for_delivery(
                    _eid,
                    timeout=Config().delivery_wait_timeout_sec,
                )
                if not ctx["delivery_confirmed"]:
                    _logger.warning(
                        "HLS: serialized card delivery not confirmed msg=%s; "
                        "gateway fallback remains enabled",
                        (_eid or "?")[:12],
                    )
            except Exception:
                ctx["delivery_confirmed"] = False
                _logger.warning(
                    "HLS: serialized card delivery wait failed msg=%s",
                    mid[:12],
                    exc_info=True,
                )

        if _get_config().independent_final_delivery:
            # Progress-card completion is asynchronous and has no delivery ACK.
            # Leave the final response intact for the native gateway to send.
            if result is None:
                from ..controller import get_controller
                _session = get_controller()._sess_get(mid)
                if _session and _session.state not in TERMINAL_PHASES and _session.state != "completing":
                    from .hooks import on_message_aborted
                    on_message_aborted(message_id=mid)
            _hls_cleanup_ctx()
            return result

        # AST injection, so we return None to simulate "stale agent result",
        if result is not None:
            if ctx and ctx.get("card_sent") and (
                not serialized_chat or ctx.get("delivery_confirmed")
            ):
                _logger.info(
                    "card already sent for msg=%s, suppressing gateway reply",
                    mid[:12],
                )
                _hls_cleanup_ctx()
                return None
            try:
                from ..controller import get_controller
                _ctrl = get_controller()
                if _ctrl and _ctrl.enabled:
                    _eid = ctx.get("event_message_id", "") if ctx else ""
                    if _eid:
                        _sess = _ctrl._sess_get(_eid)
                        if _sess and _sess.card_msg_id:
                            if serialized_chat and not ctx.get("delivery_confirmed"):
                                _logger.warning(
                                    "card session exists but delivery is unconfirmed for "
                                    "serialized msg=%s (state=%s); allowing gateway fallback",
                                    mid[:12],
                                    _sess.state,
                                )
                            else:
                                _logger.info(
                                    "card session exists for msg=%s (state=%s), suppressing gateway reply",
                                    mid[:12], _sess.state,
                                )
                                ctx["card_sent"] = True
                                _hls_cleanup_ctx()
                                return None
            except Exception:
                _logger.warning("HLS: suppressed exception", exc_info=True)
        # None (the "Discarding stale agent result" path or the
        if result is None:
            if ctx and ctx.get("card_sent"):
                # Bug fix: Hermes returns None when already_sent=True (our
                # interrupt. Without the session-active check, stale message
                with _started_msg_ids_lock:
                    others = _started_msg_ids - {mid}
                _real_interrupt = False
                if others:
                    # Verify the "other" message is genuinely active:
                    # it must have an active (non-terminal) card session.
                    try:
                        from ..controller import get_controller
                        _ctrl = get_controller()
                        if _ctrl and _ctrl.enabled:
                            for _other_mid in others:
                                _other_sess = _ctrl._sess_get(_other_mid)
                                if _other_sess and _other_sess.state not in TERMINAL_PHASES and _other_sess.state != "completing":
                                    _real_interrupt = True
                                    _interrupt_new_mid = _other_mid
                                    break
                        else:
                            # No controller — fall back to old behavior
                            _real_interrupt = True
                            _interrupt_new_mid = next(iter(others))
                    except Exception:
                        _real_interrupt = bool(others)
                        _interrupt_new_mid = next(iter(others)) if others else None
                if _real_interrupt:
                    try:
                        from .hooks import on_message_interrupted

                        on_message_interrupted(
                            message_id=mid,
                            new_message_id=_interrupt_new_mid,
                            chat_id=chat_id,
                            anchor_id=anchor_id,
                        )
                    except Exception:
                        _logger.warning("HLS: suppressed exception", exc_info=True)
                # else: card completed normally, Hermes returned None
                #       to suppress text reply — NOT an abort.
            else:
                # Card was never sent — real abort (error, reset, /stop, etc.)
                try:
                    from .hooks import on_message_aborted

                    on_message_aborted(message_id=mid)
                except Exception:
                    _logger.warning("HLS: suppressed exception", exc_info=True)
        elif ctx and ctx.get("card_sent"):
            try:
                from ..controller import get_controller
                _ctrl = get_controller()
                if _ctrl and _ctrl.enabled:
                    _eid = ctx.get("event_message_id", "")
                    if _eid:
                        _sess = _ctrl._sess_get(_eid)
                        if _sess and _sess.state not in TERMINAL_PHASES and _sess.state != "completing":
                            _logger.info(
                                "card session stuck in non-terminal state for msg=%s "
                                "(state=%s, card_sent=%s), firing abort",
                                mid[:12], _sess.state, ctx.get("card_sent"),
                            )
                            try:
                                from .hooks import on_message_aborted
                                on_message_aborted(message_id=mid)
                            except Exception:
                                _logger.warning("HLS: suppressed exception", exc_info=True)
            except Exception:
                _logger.warning("HLS: suppressed exception", exc_info=True)
        # v1.3.4 fix (P1): cleanup on normal exit path (early returns and
        # exceptions handled by _hls_cleanup_ctx above).
        _hls_cleanup_ctx()

        return result

    return wrapper

def _wrap_run_agent(orig: Callable) -> Callable:
    """Inject COMPLETE hook after agent runs; propagate event_message_id."""

    @functools.wraps(orig)
    async def wrapper(
        self,
        message,
        context_prompt,
        history,
        source,
        session_id,
        session_key=None,
        run_generation=None,
        _interrupt_depth=0,
        event_message_id=None,
        channel_prompt=None,
        **kwargs,
    ):
        # message's ID. We must create a fresh context for the recursive call
        _saved_parent_ctx = None  # Will hold parent context for restoration
        _original_msg_context_ref = None  # Reference to the original msg_context dict
        ctx = _msg_ctx.get()
        if ctx is not None and event_message_id:
            if _interrupt_depth > 0 and ctx.get("event_message_id") != event_message_id:
                # BUG FIX (v0.15.4): We must keep a reference to the original
                _original_msg_context_ref = ctx.get("_original_msg_context_ref") or ctx
                _saved_parent_ctx = dict(ctx)  # Save a copy for restoration after orig()
                ctx = {
                    "message_id": event_message_id,
                    "chat_id": ctx.get("chat_id", ""),
                    "anchor_id": ctx.get("anchor_id"),
                    "event_message_id": event_message_id,
                    "card_sent": False,
                    "_msg_start_time": time.monotonic(),
                    "_agent_ref": None,
                    "_interrupt_depth": _interrupt_depth,
                    "_parent_message_id": ctx.get("message_id"),  # Track parent for cleanup
                    "_original_msg_context_ref": _original_msg_context_ref,  # Propagate ref to original
                }
                _msg_ctx.set(ctx)
                _thread_local_ctx.data = dict(ctx)

                # anchor_id fix: use event_message_id as the new card's
                try:
                    from .hooks import on_message_interrupted
                    on_message_interrupted(
                        message_id=_saved_parent_ctx.get("message_id", ""),
                        new_message_id=event_message_id,
                        chat_id=ctx["chat_id"],
                        anchor_id=event_message_id,
                    )
                except Exception:
                    _logger.debug("run_agent: interrupt hook failed", exc_info=True)

                # Fire START hook for the new (interrupted-into) message
                try:
                    from .hooks import on_message_started
                    on_message_started(
                        message_id=event_message_id,
                        chat_id=ctx["chat_id"],
                        anchor_id=event_message_id,
                    )
                except Exception:
                    _logger.warning("HLS: suppressed exception", exc_info=True)
            else:
                ctx["event_message_id"] = event_message_id
            # Copy to thread-local for thread-pool workers
            _thread_local_ctx.data = dict(ctx)

        # v1.3.4 fix (P1): 确保 orig() 抛异常时 _saved_parent_ctx 被恢复。
        # 错误的 message_id（"wrong card gets completion" bug）。
        try:
            result = await orig(
                self,
                message,
                context_prompt,
                history,
                source,
                session_id,
                session_key=session_key,
                run_generation=run_generation,
                _interrupt_depth=_interrupt_depth,
                event_message_id=event_message_id,
                channel_prompt=channel_prompt,
                **kwargs,
            )
        except BaseException:
            if _saved_parent_ctx is not None:
                _msg_ctx.set(_saved_parent_ctx)
                _thread_local_ctx.data = dict(_saved_parent_ctx)
            raise

        # We must fire B's COMPLETE hook first (with B's result), then
        # Previous bug: only A's ABORTED COMPLETE was fired, leaving
        # - B's card quotes A's text (stale session content)
        ctx = _msg_ctx.get()
        if _saved_parent_ctx is not None:
            child_card_owned = False
            # Step 1: Fire B's (child) COMPLETE hook normally
            if ctx is not None:
                try:
                    from .hooks import on_message_completed

                    _elapsed_child = time.monotonic() - ctx.get("_msg_start_time", time.monotonic())
                    is_interrupted_child = result.get("interrupted", False) or result.get("partial", False)

                    _finish_reason_child = result.get("finish_reason", "")
                    _error_msg_child = result.get("error") or result.get("interrupt_message", "")
                    if _finish_reason_child and _finish_reason_child != "stop":
                        _logger.warning(
                            "hermes-lark-streaming v%s: child non-stop finish_reason=%s model=%s msg=%s",
                            __version__,
                            _finish_reason_child,
                            result.get("model", "?"),
                            (ctx["message_id"] or "?")[:12],
                        )
                    if _error_msg_child:
                        _logger.warning(
                            "hermes-lark-streaming v%s: child agent error: %s model=%s msg=%s",
                            __version__,
                            _error_msg_child[:200],
                            result.get("model", "?"),
                            (ctx["message_id"] or "?")[:12],
                        )

                    _agent_ref_child = ctx.get("_agent_ref")
                    cache_read_child = getattr(_agent_ref_child, "session_cache_read_tokens", 0) if _agent_ref_child else 0
                    cache_write_child = getattr(_agent_ref_child, "session_cache_write_tokens", 0) if _agent_ref_child else 0
                    reasoning_tokens = getattr(_agent_ref_child, "session_reasoning_tokens", 0) if _agent_ref_child else 0
                    estimated_cost_usd = getattr(_agent_ref_child, "session_estimated_cost_usd", 0) if _agent_ref_child else 0
                    cost_status = getattr(_agent_ref_child, "session_cost_status", "unknown") if _agent_ref_child else "unknown"

                    card_sent_child = on_message_completed(
                        message_id=ctx["message_id"],
                        answer=result.get("final_response", ""),
                        duration=_elapsed_child,
                        model=result.get("model", ""),
                        tokens={
                            "input_tokens": result.get("input_tokens", 0),
                            "output_tokens": result.get("output_tokens", 0),
                            "cache_read_tokens": cache_read_child,
                            "cache_write_tokens": cache_write_child,
                        },
                        context={
                            "used_tokens": result.get("last_prompt_tokens", 0),
                            "max_tokens": result.get("context_length", 0),
                        },
                        api_calls=result.get("api_calls", 0),
                        history_offset=result.get("history_offset", 0),
                        compression_exhausted=result.get("compression_exhausted", False),
                        aborted=is_interrupted_child,
                        error_message=_error_msg_child,
                        reasoning_tokens=reasoning_tokens,
                        estimated_cost_usd=estimated_cost_usd,
                        cost_status=cost_status,
                    )
                    if card_sent_child and not _get_config().independent_final_delivery:
                        child_card_owned = True
                        result["already_sent"] = True
                        ctx["card_sent"] = True
                        ctx["final_yielded"] = False
                        _logger.info(
                            "run_agent: child COMPLETE hook fired for msg=%s card_sent=True",
                            (ctx["message_id"] or "?")[:12],
                        )
                    elif not card_sent_child:
                        ctx["card_sent"] = False
                        ctx["final_yielded"] = True
                except Exception:
                    _logger.debug("run_agent: child COMPLETE hook failed", exc_info=True)

            try:
                # Step 2: Fire A's (parent) ABORTED COMPLETE
                from .hooks import on_message_completed
                on_message_completed(
                    message_id=_saved_parent_ctx["message_id"],
                    answer="",
                    duration=time.monotonic() - _saved_parent_ctx.get("_msg_start_time", time.monotonic()),
                    aborted=True,
                    error_message="Interrupted by new message",
                )
                if not _get_config().independent_final_delivery:
                    _saved_parent_ctx["card_sent"] = True
                # BUG FIX (v0.15.4): Also set card_sent on the original
                if _original_msg_context_ref is not None and not _get_config().independent_final_delivery:
                    _original_msg_context_ref["card_sent"] = True
                # Also mark already_sent so Hermes's gateway doesn't send text reply
                if isinstance(result, dict) and child_card_owned:
                    result["already_sent"] = True
                if not child_card_owned:
                    _saved_parent_ctx["final_yielded"] = True
                    if _original_msg_context_ref is not None:
                        _original_msg_context_ref["final_yielded"] = True
            except Exception:
                _logger.debug("run_agent: parent ABORTED completion failed", exc_info=True)
        elif ctx is not None:
            try:
                from .hooks import on_message_completed

                _elapsed = time.monotonic() - ctx.get("_msg_start_time", time.monotonic())

                is_interrupted = result.get("interrupted", False) or result.get("partial", False)

                _finish_reason = result.get("finish_reason", "")
                _error_msg = result.get("error") or result.get("interrupt_message", "")
                if _finish_reason and _finish_reason != "stop":
                    _logger.warning(
                        "hermes-lark-streaming v%s: non-stop finish_reason=%s model=%s msg=%s",
                        __version__,
                        _finish_reason,
                        result.get("model", "?"),
                        (ctx["message_id"] or "?")[:12],
                    )
                if _error_msg:
                    _logger.warning(
                        "hermes-lark-streaming v%s: agent error: %s model=%s msg=%s",
                        __version__,
                        _error_msg[:200],
                        result.get("model", "?"),
                        (ctx["message_id"] or "?")[:12],
                    )

                _agent_ref = ctx.get("_agent_ref")
                cache_read = getattr(_agent_ref, "session_cache_read_tokens", 0) if _agent_ref else 0
                cache_write = getattr(_agent_ref, "session_cache_write_tokens", 0) if _agent_ref else 0
                reasoning_tokens = getattr(_agent_ref, "session_reasoning_tokens", 0) if _agent_ref else 0
                estimated_cost_usd = getattr(_agent_ref, "session_estimated_cost_usd", 0) if _agent_ref else 0
                cost_status = getattr(_agent_ref, "session_cost_status", "unknown") if _agent_ref else "unknown"

                card_sent = on_message_completed(
                    message_id=ctx["message_id"],
                    answer=result.get("final_response", ""),
                    duration=_elapsed,
                    model=result.get("model", ""),
                    tokens={
                        "input_tokens": result.get("input_tokens", 0),
                        "output_tokens": result.get("output_tokens", 0),
                        "cache_read_tokens": cache_read,
                        "cache_write_tokens": cache_write,
                    },
                    context={
                        "used_tokens": result.get("last_prompt_tokens", 0),
                        "max_tokens": result.get("context_length", 0),
                    },
                    api_calls=result.get("api_calls", 0),
                    history_offset=result.get("history_offset", 0),
                    compression_exhausted=result.get("compression_exhausted", False),
                    aborted=is_interrupted,
                    error_message=_error_msg,
                    reasoning_tokens=reasoning_tokens,
                    estimated_cost_usd=estimated_cost_usd,
                    cost_status=cost_status,
                )
                if card_sent and not _get_config().independent_final_delivery:
                    result["already_sent"] = True
                    ctx["card_sent"] = True
                    ctx["final_yielded"] = False
                elif not card_sent:
                    ctx["card_sent"] = False
                    ctx["final_yielded"] = True
            except Exception:
                _logger.warning("HLS: suppressed exception", exc_info=True)
        # _msg_ctx now points to the child message's context. We must
        if _saved_parent_ctx is not None:
            _msg_ctx.set(_saved_parent_ctx)
            _thread_local_ctx.data = dict(_saved_parent_ctx)

        return result

    return wrapper

def _wrap_run_conversation(orig: Callable) -> Callable:
    """Wrap all 6 streaming callbacks right before run_conversation executes."""
    # Lazy import to avoid circular dependency at module load time
    from .callbacks import _maybe_wrap_callbacks  # noqa: F811

    # v1.3.4 fix (P1): inspect.signature 可能对 C 扩展函数/wrapped callable
    import inspect
    try:
        _has_persist_ts = "persist_user_timestamp" in inspect.signature(orig).parameters
    except (ValueError, TypeError):
        _has_persist_ts = False

    @functools.wraps(orig)
    def wrapper(
        self,
        user_message,
        system_message=None,
        conversation_history=None,
        task_id=None,
        stream_callback=None,
        persist_user_message=None,
        persist_user_timestamp=None,
        **kwargs,
    ):
        # v1.3.0: inject_time removed — Hermes v0.17.0+ has built-in
        # gateway.message_timestamps.enabled for this purpose.

        _maybe_wrap_callbacks(self)
        try:
            # 用关键字参数传递，兼容有/无 persist_user_timestamp 的 Hermes 版本
            call_kwargs = {
                "system_message": system_message,
                "conversation_history": conversation_history,
                "task_id": task_id,
                "stream_callback": stream_callback,
                "persist_user_message": persist_user_message,
            }
            if _has_persist_ts:
                call_kwargs["persist_user_timestamp"] = persist_user_timestamp
            call_kwargs.update(kwargs)
            return orig(self, user_message, **call_kwargs)
        finally:
            pass  # v1.3.0: inject_time guard removed

    return wrapper

# ── Background task wrapper ───────────────────────────────────────

def _wrap_run_background_task(orig: Callable) -> Callable:
    """Inject START/COMPLETE hooks for ``/background`` tasks so they get streaming cards."""

    @functools.wraps(orig)
    async def wrapper(self, prompt, source, task_id, **kwargs):
        # Only intercept Feishu platform
        platform_name = getattr(getattr(source, "platform", None), "value", "").lower()
        if platform_name not in ("feishu", "lark"):
            return await orig(self, prompt, source, task_id, **kwargs)

        chat_id = getattr(source, "chat_id", "")

        # Set up message context so _maybe_wrap_callbacks works
        _msg_ctx.set({
            "message_id": task_id,
            "chat_id": chat_id,
            "anchor_id": None,  # No reply anchor for background tasks
            "event_message_id": task_id,  # Use task_id so callbacks find a valid eid
            "card_sent": False,
            "_msg_start_time": time.monotonic(),
            "_agent_ref": None,  # Will be filled by _maybe_wrap_callbacks
        })
        _thread_local_ctx.data = dict(_msg_ctx.get())

        # ── Fire START hook ──
        try:
            from .hooks import on_message_started
            on_message_started(message_id=task_id, chat_id=chat_id, anchor_id=None)
        except Exception:
            _logger.debug("background task START hook failed", exc_info=True)

        # ── Wrap adapter.send to suppress duplicate text delivery ──
        adapter = None
        original_send = None

        try:
            if hasattr(self, "adapters") and source.platform:
                adapter = self.adapters.get(source.platform)
        except Exception:
            _logger.warning("HLS: suppressed exception", exc_info=True)
        if adapter:
            original_send = adapter.send

            async def _intercepting_send(chat_id, content, **send_kwargs):
                """Suppress plain text delivery when our card was sent."""
                if _get_config().independent_final_delivery:
                    return await original_send(chat_id, content, **send_kwargs)
                ctx = _msg_ctx.get()
                if ctx and ctx.get("card_sent"):
                    try:
                        from gateway.platforms.base import SendResult
                        return SendResult(success=True)
                    except (ImportError, AttributeError):
                        return None
                return await original_send(chat_id, content, **send_kwargs)

            adapter.send = _intercepting_send
            # run concurrently on the same adapter, the first to finish must
            adapter._hls_bg_sending = getattr(adapter, '_hls_bg_sending', 0) + 1

        # v1.3.4 fix (P1): orig() + COMPLETE hook 都在 try 块内，finally
        try:
            result = await orig(self, prompt, source, task_id, **kwargs)

            # ── Fire COMPLETE hook ──
            ctx = _msg_ctx.get()
            if ctx is not None:
                try:
                    from .hooks import on_message_completed

                    _elapsed = time.monotonic() - ctx.get("_msg_start_time", time.monotonic())

                    # Extract cache tokens from agent reference (set by _maybe_wrap_callbacks)
                    _agent_ref = ctx.get("_agent_ref")
                    cache_read = getattr(_agent_ref, "session_cache_read_tokens", 0) if _agent_ref else 0
                    cache_write = getattr(_agent_ref, "session_cache_write_tokens", 0) if _agent_ref else 0
                    reasoning_tokens = getattr(_agent_ref, "session_reasoning_tokens", 0) if _agent_ref else 0
                    estimated_cost_usd = getattr(_agent_ref, "session_estimated_cost_usd", 0) if _agent_ref else 0
                    cost_status = getattr(_agent_ref, "session_cost_status", "unknown") if _agent_ref else "unknown"

                    card_sent = on_message_completed(
                        message_id=task_id,
                        answer=(result or {}).get("final_response", ""),
                        duration=_elapsed,
                        model=(result or {}).get("model", ""),
                        tokens={
                            "input_tokens": (result or {}).get("input_tokens", 0),
                            "output_tokens": (result or {}).get("output_tokens", 0),
                            "cache_read_tokens": cache_read,
                            "cache_write_tokens": cache_write,
                        },
                        context={
                            "used_tokens": (result or {}).get("last_prompt_tokens", 0),
                            "max_tokens": (result or {}).get("context_length", 0),
                        },
                        api_calls=(result or {}).get("api_calls", 0),
                        history_offset=(result or {}).get("history_offset", 0),
                        compression_exhausted=(result or {}).get("compression_exhausted", False),
                        aborted=False,
                        error_message=(result or {}).get("error") or "",
                        reasoning_tokens=reasoning_tokens,
                        estimated_cost_usd=estimated_cost_usd,
                        cost_status=cost_status,
                    )

                    if card_sent and not _get_config().independent_final_delivery:
                        ctx["card_sent"] = True
                        # Mark result so upstream knows card was sent
                        if result is not None and isinstance(result, dict):
                            result["_hls_card_sent"] = True
                except Exception:
                    _logger.debug("background task COMPLETE hook failed", exc_info=True)

            return result
        finally:
            if original_send and adapter:
                # v1.7.0 (R1-08): restore only what WE installed. With two
                # concurrent background tasks on one adapter, task A's old
                # `adapter.send = original_send` clobbered task B's still-live
                # wrapper mid-flight (B's interception silently died). Only
                # restore when our wrapper is still the active attribute.
                if getattr(adapter, "send", None) is _intercepting_send:
                    adapter.send = original_send
                # v1.3.2 fix (B3-06): default to 0 (not 1) for consistency —
                # a counter should never default to 1 when decrementing.
                adapter._hls_bg_sending = getattr(adapter, '_hls_bg_sending', 0) - 1
            # v1.3.4 fix (P1): clear context in finally — runs on ALL paths
            _msg_ctx.set(None)
            _thread_local_ctx.data = None

    return wrapper

# ── Cron delivery wrapper ──────────────────────────────────────────

def _wrap_cron_deliver(orig: Callable) -> Callable:
    """Intercept cron ``_deliver_result`` and redirect Feishu deliveries to CardKit cards."""

    @functools.wraps(orig)
    def wrapper(job, content, adapters=None, loop=None, **kwargs):
        # Only intercept when there are adapters with a Feishu/Lark platform
        if not adapters:
            return orig(job, content, adapters=adapters, loop=loop, **kwargs)

        feishu_adapter = None

        try:
            from gateway.config import Platform

            for p in list(adapters.keys()):
                pn = p.value.lower() if hasattr(p, "value") else str(p).lower()
                if pn in ("feishu", "lark"):
                    feishu_adapter = adapters[p]
                    break
        except (ImportError, AttributeError):
            pass

        if feishu_adapter is None:
            # v1.7.0 (P1 RELAY fix): relay-fronted deployment — there is no
            # native FeishuAdapter in ``adapters`` (the connector process owns
            # the credentials). Cron cards are handled by the class-level
            # RelayAdapter.send_for_platform wrapper, which detects hermes'
            # cron delivery metadata (job_id) and redirects to the cron card
            # (see patching/adapter.py _dispatch_feishu_outbound). Just log
            # and pass through to hermes' normal delivery.
            try:
                from gateway.config import Platform as _Platform

                _relay = adapters.get(_Platform.RELAY) if adapters else None
                if (
                    _relay is not None
                    and callable(getattr(_relay, "fronts_platform", None))
                    and _relay.fronts_platform("feishu")
                ):
                    _logger.info(
                        "hermes-lark-streaming v%s: cron delivery on RELAY lane "
                        "(no native feishu adapter; card redirect happens at "
                        "RelayAdapter.send_for_platform, job=%s)",
                        __version__,
                        job.get("id", "?")[:12],
                    )
            except Exception:
                _logger.debug("HLS: relay lane detection failed", exc_info=True)
            return orig(job, content, adapters=adapters, loop=loop, **kwargs)

        _logger.info(
            "hermes-lark-streaming v%s: cron delivery intercepted, redirecting to card (job=%s)",
            __version__,
            job.get("id", "?")[:12],
        )

        # ── Temporarily replace Feishu adapter.send with card-sending version ──
        original_send = feishu_adapter.send

        async def _card_sending_send(chat_id, content, **send_kwargs):
            """Redirect Feishu adapter.send to CardKit card delivery."""
            try:
                from ..controller import get_controller
                ctrl = get_controller()
                _logger.info(
                    "cron _card_sending_send: ctrl.enabled=%s chat=%s content_len=%d",
                    ctrl.enabled,
                    chat_id[:12] if chat_id else "?",
                    len(content) if content else 0,
                )
                if ctrl.enabled and content:
                    cleaned = content
                    if not cleaned.strip():
                        cleaned = content

                    await ctrl._do_cron_deliver(chat_id, cleaned.strip())

                    _logger.info(
                        "hermes-lark-streaming v%s: cron card delivered: chat=%s",
                        __version__,
                        chat_id[:12],
                    )
                    # Return a success result so the original _deliver_result
                    # thinks the send succeeded
                    return _send_result_ok()
            except Exception:
                # v1.7.0: was a bare `pass` — a failed cron card silently fell
                # back to plain text with zero log evidence.
                _logger.warning(
                    "cron _card_sending_send: card delivery failed, falling "
                    "back to plain text chat=%s",
                    (chat_id or "?")[:12],
                    exc_info=True,
                )

            # Fallback: send plain text via the original adapter
            return await original_send(chat_id, content, **send_kwargs)

        feishu_adapter.send = _card_sending_send
        # Use counter instead of boolean flag — same rationale as _hls_bg_sending.
        feishu_adapter._hls_cron_sending = getattr(feishu_adapter, '_hls_cron_sending', 0) + 1
        try:
            return orig(job, content, adapters=adapters, loop=loop, **kwargs)
        finally:
            # v1.7.0 (R1-08): same conditional-restore as the bg-task wrapper —
            # concurrent cron deliveries on one adapter must not clobber each
            # other's interception.
            if getattr(feishu_adapter, "send", None) is _card_sending_send:
                feishu_adapter.send = original_send
            # v1.3.2 fix (B3-06): default to 0 (not 1) for consistency.
            feishu_adapter._hls_cron_sending = getattr(feishu_adapter, '_hls_cron_sending', 0) - 1

    return wrapper
