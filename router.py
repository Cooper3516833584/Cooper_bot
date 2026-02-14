# router.py
from dataclasses import dataclass
import re
from typing import Optional

from config import ADMIN_USERS, GROUP_LEVEL, DEFAULT_LEVEL
from permsvc import PermService

@dataclass
class Ctx:
    scene: str                 # group / private_friend / private_group / private_stranger
    user_id: int
    nickname: str              # QQ 昵称（全局）
    card: str                  # 群名片（群昵称），私聊一般为空
    group_id: Optional[int]
    group_name: Optional[str]  # 群名称（需要额外 API 获取，可为空）
    level: int                 # 0~3

def get_text(evt: dict) -> str:
    raw = evt.get("raw_message")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()

    msg = evt.get("message")
    if isinstance(msg, str):
        return msg.strip()

    # OneBot v11 array segments
    if isinstance(msg, list):
        parts = []
        for seg in msg:
            if not isinstance(seg, dict):
                continue
            if seg.get("type") == "text":
                data = seg.get("data") or {}
                t = data.get("text")
                if t:
                    parts.append(str(t))
        return "".join(parts).strip()

    return ""

def build_ctx(evt: dict, perm: Optional[PermService] = None) -> Optional[Ctx]:
    if evt.get("post_type") != "message":
        return None

    mtype = evt.get("message_type")
    sender = evt.get("sender") or {}
    user_id = int(sender.get("user_id") or evt.get("user_id") or 0)
    if not user_id:
        return None

    # QQ 昵称（全局）与群名片（群昵称）分开存
    nickname = str(sender.get("nickname") or "").strip() or str(user_id)
    card = str(sender.get("card") or "").strip()
    group_id = evt.get("group_id")
    if group_id is not None:
        group_id = int(group_id)

    # scene
    if mtype == "group":
        scene = "group"
    elif mtype == "private":
        sub = (evt.get("sub_type") or "").lower()
        if sub == "friend":
            scene = "private_friend"
        elif sub == "group":
            scene = "private_group"   # “通过群临时私聊”
        else:
            scene = "private_stranger"
    else:
        return None

    # === 权限库规则 ===
    # 需求：在 bot 加入的所有群里，只要发过言，就至少 level=1
    if perm and scene == "group":
        perm.touch_group_speaker(user_id)

    # level
    if user_id in ADMIN_USERS:
        level = 3
    elif scene == "private_friend":
        base = perm.get_level(user_id) if perm else int(DEFAULT_LEVEL)
        level = max(int(base), 1)
    elif scene == "private_stranger":
        level = 0
    else:
        base = perm.get_level(user_id) if perm else int(DEFAULT_LEVEL)
        group_floor = int(GROUP_LEVEL.get(group_id, 0)) if group_id is not None else 0
        level = max(int(base), group_floor)

    return Ctx(
        scene=scene,
        user_id=user_id,
        nickname=nickname,
        card=card,
        group_id=group_id,
        group_name=str(evt.get('group_name') or '').strip() or None,
        level=level,
    )


def get_files(evt: dict) -> list[dict]:
    """提取 OneBot v11 file 段。返回 [{'name','file_id','url','size'}...]"""
    out = []
    msg = evt.get("message")
    # array segments
    if isinstance(msg, list):
        for seg in msg:
            if not isinstance(seg, dict):
                continue
            tp = (seg.get("type") or "").lower()
            if tp in ("file", "file_upload", "file_msg"):
                data = seg.get("data") or {}
                name = data.get("file") or data.get("name") or ""
                fid = data.get("file_id") or data.get("id") or ""
                url = data.get("url") or ""
                size = data.get("file_size") or data.get("size") or ""
                out.append({
                    "name": str(name),
                    "file_id": str(fid),
                    "url": str(url),
                    "size": str(size),
                })
        return out

    # raw_message CQ fallback
    raw = evt.get("raw_message") or ""
    if isinstance(raw, str) and "CQ:file" in raw:
        # very light parse
        m = re.search(r"\[CQ:file,([^\]]+)\]", raw)
        if m:
            kvs = m.group(1).split(",")
            data = {}
            for kv in kvs:
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    data[k.strip()] = v.strip()
            out.append({
                "name": data.get("file",""),
                "file_id": data.get("file_id",""),
                "url": data.get("url",""),
                "size": data.get("file_size",""),
            })
    return out
