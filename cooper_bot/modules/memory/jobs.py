from __future__ import annotations

import asyncio


class MemoryJobWorker:
    """Single bounded worker. SQLite, rather than message tasks, owns jobs."""
    def __init__(self, store, handler, log=None):
        self.store, self.handler, self.log = store, handler, log
        self._wake, self._task, self._closing = asyncio.Event(), None, False

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="memory-worker")

    async def enqueue(self, *args, **kwargs):
        await self.store.enqueue_job(*args, **kwargs)
        self._wake.set()

    def wake(self):
        self._wake.set()

    async def _run(self):
        while not self._closing:
            job = await self.store.claim_job()
            if job is None:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=30)
                except asyncio.TimeoutError:
                    pass
                continue
            try:
                await self.handler(job)
                await self.store.finish_job(job["job_id"], True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.store.finish_job(job["job_id"], False, type(exc).__name__)

    async def aclose(self):
        self._closing = True
        self._wake.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
