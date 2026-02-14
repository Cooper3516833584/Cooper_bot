# client.py
import asyncio
import json
import websockets

from logger import Logger
from config import WS_URI, HTTP_BASE, HTTP_TOKEN, LOG_DIR, PERM_DB_PATH
from router import build_ctx, get_text
from onebot import OneBotAPI
from filesvc import FileService
from logsvc import LogService
from commands import dispatch, BotState
from permsvc import PermService
from handinsvc import HandinService

log = Logger("bot", "INFO")

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

                log.info("已连接至服务器")
                asyncio.create_task(logsvc.cleanup_loop())
                asyncio.create_task(handin.scheduler_loop(api))

                async for message in ws:
                    data = json.loads(message)

                    # action 回包
                    if "echo" in data:
                        api.feed_response(data)
                        if "post_type" not in data:
                            continue

                    ctx = build_ctx(data, perm=perm)
                    if not ctx:
                        continue

                    text = get_text(data)
                    try:
                        await dispatch(api, ctx, data, text, filesvc, logsvc, state, handin, perm)
                    except Exception as e:
                        log.exception(f"dispatch 异常: {e}")

        except Exception as e:
            log.error(f"连接断开/异常：{e}")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_forever())
