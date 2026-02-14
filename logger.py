import asyncio
import logging
import websockets
import colorlog
from zoneinfo import ZoneInfo
from datetime import datetime


class TZFormatter(colorlog.ColoredFormatter):
    def __init__(self, *args, tz="Asia/Shanghai", **kwargs):
        super().__init__(*args, **kwargs)
        self.tz = ZoneInfo(tz)

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(timespec="seconds")


class Logger:
    def __init__(self, log_name="root", level="INFO", tz="Asia/Shanghai"):
        self.logger = logging.getLogger(log_name)
        self.logger.setLevel(level)

        # 避免重复添加 handler（否则每次 new Logger 都会重复打印）
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            fmt = "%(log_color)s[%(asctime)s Shanghai][%(name)s][%(levelname)s] %(message)s"
            handler.setFormatter(
                TZFormatter(fmt, datefmt="%Y-%m-%d %H:%M:%S", tz=tz)
            )
            self.logger.addHandler(handler)

        self.logger.propagate = False

    def info(self, message): self.logger.info(message)
    def debug(self, message): self.logger.debug(message)
    def warning(self, message): self.logger.warning(message)
    def error(self, message): self.logger.error(message)



    def exception(self, message): self.logger.exception(message)
log = Logger("bot", "INFO")