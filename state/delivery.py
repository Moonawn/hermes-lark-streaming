"""Profile-local, write-ahead receipts for final Feishu text delivery.

This database is deliberately separate from Hermes conversation history. It
contains no credentials, never deletes history, and has exactly one recovery
owner per job. Network calls must happen OUTSIDE database transactions.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class DeliveryLedger:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.path.is_symlink() or self.path.parent.is_symlink():
            raise ValueError("delivery ledger must not be a symlink")
        os.chmod(self.path.parent, 0o700)
        fd = os.open(self.path, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(fd)
        os.chmod(self.path, 0o600)
        with self.transaction() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS deliveries (
                    id TEXT PRIMARY KEY, scope TEXT NOT NULL,
                    app_id TEXT NOT NULL, chat_id TEXT NOT NULL,
                    reply_to TEXT, thread_id TEXT, event_ref TEXT,
                    session_key TEXT, content TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    next_attempt_at REAL NOT NULL DEFAULT 0,
                    lease_owner TEXT, lease_until REAL NOT NULL DEFAULT 0,
                    last_error TEXT, notice_attempted INTEGER NOT NULL DEFAULT 0,
                    notice_message_id TEXT
                );
                CREATE INDEX IF NOT EXISTS deliveries_due
                    ON deliveries(scope, state, next_attempt_at);
                CREATE TABLE IF NOT EXISTS delivery_parts (
                    delivery_id TEXT NOT NULL, part_no INTEGER NOT NULL,
                    msg_type TEXT NOT NULL, payload TEXT NOT NULL,
                    original_chunk TEXT NOT NULL, expected TEXT NOT NULL,
                    request_uuid TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    send_attempts INTEGER NOT NULL DEFAULT 0,
                    read_attempts INTEGER NOT NULL DEFAULT 0,
                    first_attempt_at REAL, message_id TEXT,
                    verified_at REAL, observed_hash TEXT,
                    PRIMARY KEY(delivery_id, part_no),
                    FOREIGN KEY(delivery_id) REFERENCES deliveries(id)
                );
            """)

    @contextmanager
    def transaction(self):
        db = sqlite3.connect(self.path, timeout=1.0)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            with db:
                yield db
        finally:
            db.close()

    def stage(self, *, scope, app_id, chat_id, reply_to, thread_id,
              event_ref, session_key, content, parts, now=None):
        now = time.time() if now is None else now
        # Different turns can have the SAME reply anchor (topic root). Prefer
        # the inbound event identity; never deduplicate solely by answer text.
        # Without a turn identity, two identical sends to a shared topic root
        # may be legitimate separate operations. Never use the anchor as ID.
        ref = event_ref or str(uuid.uuid4())
        key = digest(json.dumps(
            [scope, chat_id, reply_to, thread_id, ref, content],
            ensure_ascii=False, separators=(",", ":"),
        ))
        if not parts:
            raise ValueError("cannot stage an empty delivery")
        with self.transaction() as db:
            db.execute("BEGIN IMMEDIATE")
            exists = db.execute("SELECT id FROM deliveries WHERE id=?", (key,)).fetchone()
            if exists:
                return key
            db.execute("""
                INSERT INTO deliveries
                (id, scope, app_id, chat_id, reply_to, thread_id, event_ref,
                 session_key, content, content_hash, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (key, scope, app_id, chat_id, reply_to, thread_id, ref,
                  session_key, content, digest(content), now, now))
            for i, part in enumerate(parts):
                db.execute("""
                    INSERT INTO delivery_parts
                    (delivery_id, part_no, msg_type, payload, original_chunk,
                     expected, request_uuid) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (key, i, part["msg_type"], part["payload"], part["chunk"],
                      part["expected"], digest(f"{key}:{i}")[:32]))
        return key

    def get(self, key, scope):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM deliveries WHERE id=? AND scope=?",
                             (key, scope)).fetchone()
            if not row:
                return None
            job = dict(row)
            job["parts"] = [dict(p) for p in db.execute(
                "SELECT * FROM delivery_parts WHERE delivery_id=? ORDER BY part_no", (key,)
            )]
            return job

    def due(self, scope, *, now=None, limit=5):
        now = time.time() if now is None else now
        with self.transaction() as db:
            return [r[0] for r in db.execute("""
                SELECT id FROM deliveries WHERE scope=? AND state='pending'
                AND next_attempt_at<=? AND lease_until<=?
                ORDER BY created_at LIMIT ?
            """, (scope, now, now, limit))]

    def claim(self, key, scope, owner, *, now=None):
        now = time.time() if now is None else now
        with self.transaction() as db:
            return bool(db.execute("""
                UPDATE deliveries SET lease_owner=?, lease_until=?, updated_at=?
                WHERE id=? AND scope=? AND state='pending'
                  AND lease_until<=? AND next_attempt_at<=?
            """, (owner, now + 90, now, key, scope, now, now)).rowcount)

    def change_part(self, key, owner, part_no, **fields):
        allowed = {"msg_type", "payload", "expected", "request_uuid", "state",
                   "send_attempts", "read_attempts", "first_attempt_at",
                   "message_id", "verified_at", "observed_hash"}
        if not fields or not set(fields).issubset(allowed):
            raise ValueError("unsupported receipt field")
        with self.transaction() as db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM deliveries WHERE id=? AND lease_owner=?",
                              (key, owner)).fetchone():
                raise RuntimeError("delivery lease no longer owned")
            db.execute("UPDATE delivery_parts SET " +
                       ", ".join(f"{k}=?" for k in fields) +
                       " WHERE delivery_id=? AND part_no=?",
                       (*fields.values(), key, part_no))
            db.execute("UPDATE deliveries SET lease_until=? WHERE id=? AND lease_owner=?",
                       (time.time() + 90, key, owner))

    def release(self, key, owner, *, state="pending", error="", delay=0, now=None):
        now = time.time() if now is None else now
        with self.transaction() as db:
            if state == "verified":
                unverified = db.execute("""
                    SELECT COUNT(*) FROM delivery_parts
                    WHERE delivery_id=? AND state!='verified'
                """, (key,)).fetchone()[0]
                if unverified:
                    raise ValueError("cannot confirm a delivery with unverified parts")
            db.execute("""
                UPDATE deliveries SET state=?, last_error=?, updated_at=?,
                next_attempt_at=?, lease_owner=NULL, lease_until=0
                WHERE id=? AND lease_owner=?
            """, (state, error[:240], now, now + delay, key, owner))

    def claim_notice(self, key, scope):
        with self.transaction() as db:
            return bool(db.execute("""
                UPDATE deliveries SET notice_attempted=1
                WHERE id=? AND scope=? AND state='needs_attention' AND notice_attempted=0
            """, (key, scope)).rowcount)

    def notice_ack(self, key, scope, message_id):
        with self.transaction() as db:
            db.execute("UPDATE deliveries SET notice_message_id=? WHERE id=? AND scope=?",
                       (message_id, key, scope))

    def counts(self, scope):
        with self.transaction() as db:
            return {r[0]: r[1] for r in db.execute(
                "SELECT state,COUNT(*) FROM deliveries WHERE scope=? GROUP BY state", (scope,)
            )}
