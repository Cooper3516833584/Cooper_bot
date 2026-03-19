from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def make_group_event(
    text: str = "/ping",
    *,
    user_id: int = 10001,
    group_id: int = 20001,
    nickname: str = "TestUser",
    card: str = "TestCard",
    role: str = "member",
    message_id: int = 1,
    ts: int | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "time": int(_now_ts() if ts is None else ts),
        "post_type": "message",
        "message_type": "group",
        "sub_type": "normal",
        "message_id": int(message_id),
        "group_id": int(group_id),
        "user_id": int(user_id),
        "raw_message": str(text),
        "message": [{"type": "text", "data": {"text": str(text)}}],
        "sender": {
            "user_id": int(user_id),
            "nickname": str(nickname),
            "card": str(card),
            "role": str(role),
        },
    }
    if extra:
        event.update(extra)
    return event


def make_private_event(
    text: str = "/ping",
    *,
    user_id: int = 10001,
    sub_type: str = "friend",
    nickname: str = "TestUser",
    message_id: int = 1,
    ts: int | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "time": int(_now_ts() if ts is None else ts),
        "post_type": "message",
        "message_type": "private",
        "sub_type": str(sub_type),
        "message_id": int(message_id),
        "user_id": int(user_id),
        "raw_message": str(text),
        "message": [{"type": "text", "data": {"text": str(text)}}],
        "sender": {
            "user_id": int(user_id),
            "nickname": str(nickname),
        },
    }
    if extra:
        event.update(extra)
    return event


def make_admin_event(
    text: str = "/admin",
    *,
    user_id: int = 900001,
    group_id: int = 20001,
    nickname: str = "AdminUser",
    card: str = "AdminCard",
    message_id: int = 1,
    ts: int | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return make_group_event(
        text=text,
        user_id=user_id,
        group_id=group_id,
        nickname=nickname,
        card=card,
        role="admin",
        message_id=message_id,
        ts=ts,
        extra=extra,
    )


# Backward-compatible alias used by early scaffold code.
make_group_message_event = make_group_event
