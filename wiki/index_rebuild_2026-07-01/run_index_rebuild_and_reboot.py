from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any


RUN_DIR = Path(__file__).resolve().parent
ROOT = RUN_DIR.parents[1]
CLIENT_PATH = ROOT / "client.py"
LOCK_PATH = RUN_DIR / "index_rebuild.lock"
LOG_PATH = RUN_DIR / "index_rebuild_runner.log"
SUMMARY_PATH = RUN_DIR / "index_rebuild_summary.json"
RESTART_SCRIPT = Path.home() / "Desktop" / "\u91cd\u542f\u7535\u8111.ps1"
FLAG_NAME = b"REBUILD_MATERIAL_SCAN_MARKS_ON_STARTUP"


class RunLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _line(self, level: str, message: Any) -> None:
        ts = datetime.now().isoformat(timespec="seconds")
        text = f"[{ts}][{level}] {message}"
        with self.path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(text + "\n")
        print(text, flush=True)

    def info(self, message: Any) -> None:
        self._line("INFO", message)

    def warning(self, message: Any) -> None:
        self._line("WARNING", message)

    def error(self, message: Any) -> None:
        self._line("ERROR", message)

    def exception(self, message: Any) -> None:
        self._line("ERROR", message)
        self._line("ERROR", traceback.format_exc())


def acquire_lock():
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = LOCK_PATH.open("a+b")
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
        return fh
    except Exception:
        fh.close()
        return None


def set_rebuild_flag_false() -> str:
    data = CLIENT_PATH.read_bytes()
    pattern = (
        rb"(?m)^("
        + re.escape(FLAG_NAME)
        + rb"\s*=\s*)True(\s*(?:#.*)?)$"
    )
    new_data, changed = re.subn(pattern, rb"\1False\2", data, count=1)
    if changed:
        CLIENT_PATH.write_bytes(new_data)
        return "changed_true_to_false"
    if re.search(rb"(?m)^" + re.escape(FLAG_NAME) + rb"\s*=\s*False\b", data):
        return "already_false"
    raise RuntimeError(f"could not find {FLAG_NAME.decode('ascii')} assignment")


def write_summary(summary: dict[str, Any]) -> None:
    SUMMARY_PATH.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def invoke_restart(log: RunLogger) -> dict[str, Any]:
    if not RESTART_SCRIPT.exists():
        raise FileNotFoundError(f"restart script not found: {RESTART_SCRIPT}")
    cmd = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(RESTART_SCRIPT),
    ]
    log.info(f"calling restart script: {RESTART_SCRIPT}")
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    result = {
        "returncode": proc.returncode,
        "stdout": proc.stdout[-4000:],
        "stderr": proc.stderr[-4000:],
    }
    if proc.returncode != 0:
        raise RuntimeError(f"restart script failed: {result}")
    return result


async def run() -> int:
    started = datetime.now().isoformat(timespec="seconds")
    t0 = time.monotonic()
    log = RunLogger(LOG_PATH)
    LOG_PATH.write_text("", encoding="utf-8")
    summary: dict[str, Any] = {
        "started_at": started,
        "root": str(ROOT),
        "client_path": str(CLIENT_PATH),
        "restart_script": str(RESTART_SCRIPT),
        "stages": {},
        "success": False,
        "flag_reset": None,
        "restart_invoked": False,
    }

    lock_fh = acquire_lock()
    if lock_fh is None:
        log.warning("another index rebuild runner is already active; exiting")
        summary["error"] = "lock_not_acquired"
        write_summary(summary)
        return 2

    try:
        os.chdir(ROOT)
        sys.path.insert(0, str(ROOT))
        log.info("index rebuild runner started")
        log.info("this runner does not start client websocket or message dispatch")

        from aisvc import AIService
        from filesvc import FileService

        filesvc = FileService(log)
        filesvc.ensure_dirs()
        aisvc = AIService(log)

        log.info("stage material_scan_marks: start")
        stage_t0 = time.monotonic()
        mark_stats = await aisvc.rebuild_material_scan_marks_from_current_layout()
        summary["stages"]["material_scan_marks"] = {
            "ok": True,
            "seconds": round(time.monotonic() - stage_t0, 3),
            "stats": mark_stats,
        }
        write_summary(summary)
        log.info(f"stage material_scan_marks: done {mark_stats}")

        log.info("stage find_index: start")
        stage_t0 = time.monotonic()
        find_stats = await asyncio.to_thread(filesvc.build_find_index)
        summary["stages"]["find_index"] = {
            "ok": True,
            "seconds": round(time.monotonic() - stage_t0, 3),
            "stats": find_stats,
        }
        write_summary(summary)
        log.info(f"stage find_index: done {find_stats}")

        log.info("stage reset_flag: start")
        flag_result = set_rebuild_flag_false()
        summary["flag_reset"] = flag_result
        summary["stages"]["reset_flag"] = {"ok": True, "result": flag_result}
        write_summary(summary)
        log.info(f"stage reset_flag: done {flag_result}")

        summary["success"] = True
        summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
        summary["seconds"] = round(time.monotonic() - t0, 3)
        write_summary(summary)

        restart_result = invoke_restart(log)
        summary["restart_invoked"] = True
        summary["restart_result"] = restart_result
        write_summary(summary)
        log.info("restart script returned successfully")
        return 0
    except Exception as exc:
        log.exception(f"index rebuild runner failed: {exc}")
        summary["success"] = False
        summary["error"] = str(exc)
        summary["failed_at"] = datetime.now().isoformat(timespec="seconds")
        summary["seconds"] = round(time.monotonic() - t0, 3)
        write_summary(summary)
        return 1
    finally:
        try:
            lock_fh.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
