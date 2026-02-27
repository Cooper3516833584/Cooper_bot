# client.py
import asyncio
import json
import websockets
from typing import Dict, Set

from logger import Logger
from config import (
    WS_URI,
    HTTP_BASE,
    HTTP_TOKEN,
    LOG_DIR,
    PERM_DB_PATH,
    AUTO_APPROVE_FRIEND_REQUEST,
    AUTO_APPROVE_FRIEND_REMARK,
)
from router import build_ctx, get_text
from onebot import OneBotAPI
from filesvc import FileService
from logsvc import LogService
from commands import dispatch, BotState, conv_key
from permsvc import PermService
from handinsvc import HandinService

log = Logger("bot", "INFO")

# 允许不同会话并发处理，避免大文件发送阻塞全局。
MAX_DISPATCH_CONCURRENCY = 32

async def run_forever():
    filesvc = FileService()
    filesvc.ensure_dirs()
    state = BotState()
    perm = PermService(PERM_DB_PATH)
    handin = HandinService(log)

    while True:
        try:
            async with websockets.connect(
                WS_URI,
                open_timeout=120,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=2 ** 22,
            ) as ws:
                # action 走 HTTP，WS 仅收事件（可显著减少超时/卡顿/误报失败）
                api = OneBotAPI(ws, log, http_base=HTTP_BASE, http_token=HTTP_TOKEN)
                logsvc = LogService(LOG_DIR, log)
                dispatch_sem = asyncio.Semaphore(MAX_DISPATCH_CONCURRENCY)
                conv_locks: Dict[str, asyncio.Lock] = {}
                inflight: Set[asyncio.Task] = set()

                async def _handle_one_event(ctx, data: dict, text: str):
                    key = conv_key(ctx)
                    lock = conv_locks.get(key)
                    if lock is None:
                        lock = asyncio.Lock()
                        conv_locks[key] = lock

                    async with dispatch_sem:
                        async with lock:
                            try:
                                await dispatch(api, ctx, data, text, filesvc, logsvc, state, handin, perm)
                            except Exception as e:
                                log.exception(f"dispatch 异常: {e}")

                log.info("已连接至服务器")
                cleanup_task = asyncio.create_task(logsvc.cleanup_loop())
                scheduler_task = asyncio.create_task(handin.scheduler_loop(api))

                try:
                    async for message in ws:
                        data = json.loads(message)

                        # ===== 自动通过好友申请（post_type=request）=====
                        if data.get("post_type") == "request" and data.get("request_type") == "friend":
                            if AUTO_APPROVE_FRIEND_REQUEST:
                                flag = data.get("flag")
                                req_uid = int(data.get("user_id") or 0)
                                comment = str(data.get("comment") or "").strip()
                                if flag:
                                    log.info(f"收到好友申请：user_id={req_uid} comment={comment!r} -> 自动通过")
                                    asyncio.create_task(
                                        api.set_friend_add_request(
                                            flag=str(flag),
                                            approve=True,
                                            remark=AUTO_APPROVE_FRIEND_REMARK,
                                        )
                                    )
                                else:
                                    log.warning(f"收到好友申请但缺少 flag：{data}")
                            continue

                        # action 回包
                        if "echo" in data:
                            api.feed_response(data)
                            if "post_type" not in data:
                                continue

                        ctx = build_ctx(data, perm=perm)
                        if not ctx:
                            continue

                        text = get_text(data)
                        task = asyncio.create_task(_handle_one_event(ctx, data, text))
                        inflight.add(task)
                        task.add_done_callback(lambda t: inflight.discard(t))
                finally:
                    for t in (cleanup_task, scheduler_task):
                        t.cancel()
                    await asyncio.gather(cleanup_task, scheduler_task, return_exceptions=True)

                    # 连接断开时尽量回收在途任务，避免跨连接残留。
                    pending = list(inflight)
                    if pending:
                        done, waiting = await asyncio.wait(pending, timeout=2.0)
                        for t in waiting:
                            t.cancel()
                        if waiting:
                            await asyncio.gather(*waiting, return_exceptions=True)

        except Exception as e:
            log.error(f"连接断开/异常：{e}")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_forever())
