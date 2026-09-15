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
    # 权限等级 >= 2：可以 /memory on 打开当前会话（与 personal_admin 的电脑控制权限分离）。
    memory_operator: bool = False


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
    summary: dict | None = None


@dataclass(frozen=True)
class ExtractionResult:
    """自动抽取的显式结果：区分「执行后没有事实」与「模型根本没执行」。"""

    executed: bool
    facts: tuple[dict, ...] = ()
    retryable: bool = False


class MemoryError(RuntimeError):
    pass


class MemoryDisabled(MemoryError):
    pass


class ExtractionDeferred(MemoryError):
    """抽取未真正执行（预算耗尽或提供方不可用）。cursor 不得推进，作业应退避重试。"""
