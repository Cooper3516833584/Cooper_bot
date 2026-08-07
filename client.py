# client.py
import os
import asyncio
import json
import time
import websockets
from pathlib import Path
from typing import Dict, Set

from logger import Logger
from config import (
    WS_URI,
    HTTP_BASE,
    HTTP_TOKEN,
    DATA_DIR,
    LOG_DIR,
    PERM_DB_PATH,
    AUTO_APPROVE_FRIEND_REQUEST,
    AUTO_APPROVE_FRIEND_REMARK,
)
from router import build_ctx, get_text
from onebot import OneBotAPI
from filesvc import FileService
from logsvc import LogService
from commands import dispatch, BotState, conv_key, notify_admin_error
from permsvc import PermService
from handinsvc import HandinService
from aisvc import AIService
from daily_calendar import DailyCalendarService
from vision_skill import VisionSkill

log = Logger("bot", "INFO")

# 允许不同会话并发处理，避免大文件发送阻塞全局。
MAX_DISPATCH_CONCURRENCY = 32
CONV_LOCK_TTL_SECONDS = 30.0 * 60.0
CONV_LOCK_SWEEP_INTERVAL_SECONDS = 60.0
# Manual switch:
# - Set True for one startup after you manually adjust subject folders.
# - It writes current classified file hashes into ai_material_scan_marks.json.
# - Then set it back to False.
REBUILD_MATERIAL_SCAN_MARKS_ON_STARTUP = False
_INSTANCE_LOCK_HANDLE = None


def _acquire_single_instance_lock() -> bool:
    global _INSTANCE_LOCK_HANDLE
    if _INSTANCE_LOCK_HANDLE is not None:
        return True

    lock_path = Path(DATA_DIR) / "_client.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "a+b")
    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

        fh.seek(0)
        fh.truncate()
        fh.write(f"{os.getpid()}\n".encode("ascii", errors="ignore"))
        fh.flush()
    except Exception:
        fh.close()
        return False

    _INSTANCE_LOCK_HANDLE = fh
    return True


def _lock_path_text() -> str:
    return str((Path(DATA_DIR) / "_client.lock").resolve())

async def run_forever():
    if not _acquire_single_instance_lock():
        log.warning(f"检测到已有其他 bot 实例在运行，当前进程退出。lock={_lock_path_text()}")
        return

    log.info(f"Bot 启动：pid={os.getpid()} lock={_lock_path_text()}")
    filesvc = FileService(log)
    filesvc.ensure_dirs()
    state = BotState()
    perm = PermService(PERM_DB_PATH)
    handin = HandinService(log)
    aisvc = AIService(log)
    vision_skill = VisionSkill(log)
    calendar_service = DailyCalendarService(log, aisvc)

    if REBUILD_MATERIAL_SCAN_MARKS_ON_STARTUP:
        try:
            mark_stats = await aisvc.rebuild_material_scan_marks_from_current_layout()
            log.info(
                "AI 整理：已根据当前目录重建标记 "
                f"(scanned={int(mark_stats.get('scanned', 0))}, "
                f"marked={int(mark_stats.get('marked', 0))}, "
                f"hash_failed={int(mark_stats.get('hash_failed', 0))}, "
                f"duplicates={int(mark_stats.get('duplicates', 0))})"
            )
        except Exception as e:
            log.warning(f"AI 整理：根据当前目录重建标记失败: {e}")

    try:
        await aisvc.bootstrap_sync()
    except Exception as e:
        log.warning(f"AI 快速启动失败（将继续运行基础功能）: {e}")
    else:
        log.info("AI 快速启动就绪；Bot 已可响应，启动后同步将在后台继续运行")

    async def _run_post_startup_sync_tasks() -> None:
        ai_ok = False
        try:
            log.info("普通 /find 索引：启动后后台构建开始")
            find_stats = await asyncio.to_thread(filesvc.build_find_index)
            log.info(
                "普通 /find 索引：启动后后台构建完成 "
                f"(目录={int(find_stats.get('dirs', 0))}, 文件={int(find_stats.get('files', 0))}, 条目={int(find_stats.get('entries', 0))})"
            )
        except Exception as e:
            log.warning(f"普通 /find 索引：启动后后台构建失败: {e}")

        try:
            await aisvc.bootstrap_post_startup_sync()
            ai_ok = True
        except Exception as e:
            log.warning(f"AI 启动后同步失败: {e}")

        if ai_ok:
            log.info("AI 启动后同步已完成")

    def _on_post_sync_done(task: asyncio.Task) -> None:
        try:
            task.result()
        except Exception as e:
            log.warning(f"启动后后台同步任务异常: {e}")

    try:
        post_sync_task = asyncio.create_task(_run_post_startup_sync_tasks())
        post_sync_task.add_done_callback(_on_post_sync_done)
    except Exception as e:
        log.warning(f"启动后后台同步任务调度失败: {e}")

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
                conv_lock_last_used: Dict[str, float] = {}
                last_conv_lock_sweep = 0.0
                inflight: Set[asyncio.Task] = set()

                def _sweep_conv_locks(now_ts: float) -> None:
                    nonlocal last_conv_lock_sweep
                    if (now_ts - last_conv_lock_sweep) < CONV_LOCK_SWEEP_INTERVAL_SECONDS:
                        return
                    stale_before = now_ts - CONV_LOCK_TTL_SECONDS
                    for k, ts in list(conv_lock_last_used.items()):
                        try:
                            ts_val = float(ts)
                        except Exception:
                            ts_val = 0.0
                        if ts_val >= stale_before:
                            continue
                        lk = conv_locks.get(k)
                        if lk is not None and lk.locked():
                            continue
                        conv_lock_last_used.pop(k, None)
                        conv_locks.pop(k, None)
                    last_conv_lock_sweep = now_ts

                async def _handle_one_event(ctx, data: dict, text: str):
                    now_ts = time.time()
                    _sweep_conv_locks(now_ts)
                    key = conv_key(ctx)
                    lock = conv_locks.get(key)
                    if lock is None:
                        lock = asyncio.Lock()
                        conv_locks[key] = lock
                    conv_lock_last_used[key] = now_ts

                    async with dispatch_sem:
                        async with lock:
                            conv_lock_last_used[key] = time.time()
                            try:
                                await dispatch(
                                    api=api,
                                    ctx=ctx,
                                    evt=data,
                                    text=text,
                                    filesvc=filesvc,
                                    logsvc=logsvc,
                                    state=state,
                                    handin=handin,
                                    perm=perm,
                                    aisvc=aisvc,
                                    vision_skill=vision_skill,
                                    calendar_service=calendar_service,
                                )
                            except Exception as e:
                                log.exception(f"dispatch 异常: {e}")
                                await notify_admin_error(api, ctx, "dispatch", e, logsvc)
                            finally:
                                conv_lock_last_used[key] = time.time()

                log.info("已连接至服务器")
                cleanup_task = asyncio.create_task(logsvc.cleanup_loop())
                scheduler_task = asyncio.create_task(handin.scheduler_loop(api))
                calendar_task = asyncio.create_task(calendar_service.scheduler_loop(api))

                try:
                    async for message in ws:
                        data = None
                        try:
                            try:
                                data = json.loads(message)
                            except Exception as e:
                                raw_preview = str(message)
                                if len(raw_preview) > 240:
                                    raw_preview = raw_preview[:240] + "...(truncated)"
                                log.warning(f"WS message parse failed: err={e}; raw={raw_preview!r}")
                                continue

                            if not isinstance(data, dict):
                                log.warning(f"WS message ignored: expected JSON object, got {type(data).__name__}")
                                continue

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
                        except Exception as e:
                            post_type = data.get("post_type") if isinstance(data, dict) else None
                            request_type = data.get("request_type") if isinstance(data, dict) else None
                            notice_type = data.get("notice_type") if isinstance(data, dict) else None
                            message_type = data.get("message_type") if isinstance(data, dict) else None
                            has_echo = ("echo" in data) if isinstance(data, dict) else False
                            log.exception(
                                "WS message handling failed: "
                                f"post_type={post_type!r} request_type={request_type!r} "
                                f"notice_type={notice_type!r} message_type={message_type!r} "
                                f"has_echo={has_echo} err={e}"
                            )
                            continue
                finally:
                    for t in (cleanup_task, scheduler_task, calendar_task):
                        t.cancel()
                    await asyncio.gather(cleanup_task, scheduler_task, calendar_task, return_exceptions=True)

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
