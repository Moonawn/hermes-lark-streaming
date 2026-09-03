"""One ordered writer per streaming card, including create and final seal."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from functools import wraps


class CardWriter:
    """Serialize card mutations while allowing a seal to call another writer.

    The same event-loop task may enter recursively; other tasks queue on the
    lock. Closing rejects queued writes and cancels an in-flight transport so
    an expired card cannot overwrite a later terminal result.
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._owner = None
        self._tasks = set()
        self.closed = False

    @asynccontextmanager
    async def writing(self):
        task = asyncio.current_task()
        if self.closed:
            raise asyncio.CancelledError("card writer is closed")
        if self._owner is task:
            yield
            return
        self._tasks.add(task)
        try:
            async with self._lock:
                if self.closed:
                    raise asyncio.CancelledError("card writer is closed")
                self._owner = task
                try:
                    yield
                finally:
                    self._owner = None
        finally:
            self._tasks.discard(task)

    def close(self):
        self.closed = True
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        for task in tuple(self._tasks):
            if task is None or task is current or task.done():
                continue
            loop = task.get_loop()
            if not loop.is_closed():
                loop.call_soon_threadsafe(task.cancel)


def serialized_card_write(method):
    @wraps(method)
    async def wrapper(self, session, *args, **kwargs):
        async with session.writer.writing():
            return await method(self, session, *args, **kwargs)
    return wrapper
