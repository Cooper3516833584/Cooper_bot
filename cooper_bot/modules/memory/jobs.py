from __future__ import annotations

import asyncio

from .models import ExtractionDeferred

# 抽取被 defer（预算耗尽 / 提供方暂不可用）后的重试间隔；不计入普通 max_attempts。
EXTRACTION_DEFER_SECONDS = 3600


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
            except ExtractionDeferred:
                # 暂时没执行（预算/提供方）：推迟一小时再试，不因 attempts=3 变成永久 failed。
                await self.store.defer_job(job["job_id"], EXTRACTION_DEFER_SECONDS)
            except Exception as exc:
                await self.store.finish_job(job["job_id"], False, type(exc).__name__)

    async def aclose(self):
        self._closing = True
        self._wake.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
