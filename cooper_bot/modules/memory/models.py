from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class MemoryIdentity:
    bot_id: int
    actor_user_id: int
    scene: Literal["private", "group"]
    group_id: int | None
    profile: Literal["public", "admin"]
    personal_admin: bool = False


@dataclass(frozen=True)
class CapturedInput:
    source_event_id: str
    own_text: str
    quoted_text: str = ""
    visual_text: str = ""
    created_at_utc: float = 0.0
    source_kind: Literal["direct_chat", "passive_chat", "explicit_memory", "request_metadata"] = "direct_chat"


@dataclass(frozen=True)
class MemorySnapshot:
    scope_id: str
    conversation_id: str
    epoch: int
    current_input_seq: int
    recent_events: tuple[dict, ...]
    recalled_facts: tuple[dict, ...] = ()


class MemoryError(RuntimeError):
    pass


class MemoryDisabled(MemoryError):
    pass
