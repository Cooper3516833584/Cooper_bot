from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


@dataclass
class ControlledTime:
    """Mutable test clock."""

    current_ts: float

    def time(self) -> float:
        return float(self.current_ts)

    def now(self, tz: timezone | None = timezone.utc) -> datetime:
        if tz is None:
            return datetime.fromtimestamp(self.current_ts)
        return datetime.fromtimestamp(self.current_ts, tz)

    def set(self, ts: float | datetime) -> float:
        self.current_ts = _to_timestamp(ts)
        return self.current_ts

    def advance(self, seconds: float) -> float:
        self.current_ts += float(seconds)
        return self.current_ts


def _to_timestamp(value: float | datetime | str) -> float:
    if isinstance(value, datetime):
        return float(value.timestamp())
    if isinstance(value, str):
        return float(datetime.fromisoformat(value).timestamp())
    return float(value)


def freeze_time(at: float | datetime | str = "2026-01-01T00:00:00+00:00") -> ControlledTime:
    return ControlledTime(current_ts=_to_timestamp(at))


def patch_time_function(monkeypatch: Any, controller: ControlledTime, module_or_name: Any) -> None:
    module = module_or_name
    if isinstance(module_or_name, str):
        module = importlib.import_module(module_or_name)

    local_time = getattr(module, "time", None)
    if local_time is not None and hasattr(local_time, "time"):
        monkeypatch.setattr(local_time, "time", controller.time, raising=False)
        return

    monkeypatch.setattr(module, "time", controller.time, raising=False)


def patch_project_time(
    monkeypatch: Any,
    controller: ControlledTime,
    module_names: Iterable[str] = ("commands", "handinsvc", "filesvc", "aisvc", "client"),
) -> None:
    for module_name in module_names:
        try:
            patch_time_function(monkeypatch, controller, module_name)
        except Exception:
            continue
