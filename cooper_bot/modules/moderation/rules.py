from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable


VOTE_ORDER = ("L", "B", "U", "I", "K", "G")
REVIEW_THRESHOLD = 3
VOTE_WINDOW_SECONDS = 20.0
_ZERO_WIDTH = dict.fromkeys(map(ord, "\u200b\u200c\u200d\u2060\ufeff"), None)
_URL_RE = re.compile(r"(?i)(?:https?://[^\s<>\"'，。；,;]+|www\.[^\s<>\"'，。；,;]+)")
_AD_PHRASES = (
    "招家教", "招聘家教", "家教兼职", "有偿家教", "兼职招聘", "日结兼职",
    "招代理", "返佣", "刷单", "返利", "高收益", "付费培训", "培训招生",
    "报名优惠", "限时优惠", "私聊领取", "免费领取", "添加微信", "加微信",
)
_GROUP_INVITE_ACTION_RE = re.compile(
    r"加群|进群|入群|加入新群|转入新群|扫码进群|进交流群|加入交流群|家教群|资料群|技能学习交流群|学习交流群|进入新群|进新群|前往新群"
)
_GROUP_MIGRATION_RE = re.compile(r"本群(?:即将|将|马上)?(?:解散|停用|关闭|停止使用)")
_NEW_GROUP_HINT_RE = re.compile(r"新群|换群|转群|群二维码")
_GROUP_NUMBER_RE = re.compile(r"(?:QQ群号?|群号|加群|进群)[^\d]{0,8}\d{5,12}|\d{5,12}[^\d]{0,5}(?:群号|QQ群)")


def normalize_visible_text(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).translate(_ZERO_WIDTH)
    return text


def countable_text(value: object) -> str:
    return "".join(ch for ch in normalize_visible_text(value) if not ch.isspace())


def parse_group_level(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if not isinstance(value, str):
        return None
    normalized = normalize_visible_text(value).strip()
    if not normalized or not normalized.isascii() or not normalized.isdigit():
        return None
    try:
        return int(normalized)
    except ValueError:
        return None


def _segments(record: dict) -> list[dict]:
    raw = record.get("segments", record.get("segments_json", []))
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    return raw if isinstance(raw, list) else []


def _card_fields(segment: dict) -> tuple[str, str, bool]:
    kind = str(segment.get("type") or "").lower()
    if kind not in {"share", "json", "xml", "contact"}:
        return "", "", False
    data = segment.get("data")
    if not isinstance(data, dict):
        return "", "", False
    nested = data
    raw = data.get("data")
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            decoded = json.loads(raw)
            if isinstance(decoded, dict):
                nested = {**data, **decoded}
        except Exception:
            pass
    title = str(nested.get("title") or nested.get("prompt") or "").strip()
    body = str(nested.get("content") or nested.get("desc") or nested.get("description") or "").strip()
    target = str(
        nested.get("url") or nested.get("jumpUrl") or nested.get("jump_url")
        or nested.get("link") or nested.get("target") or ""
    ).strip()
    invite_type = str(nested.get("type") or nested.get("scene") or "").lower()
    is_group_invite = invite_type in {"group_invite", "group", "group_share"}
    return " ".join(part for part in (title, body) if part), target, is_group_invite


def record_text(record: dict) -> str:
    value = record.get("plain_text")
    if value is not None:
        return str(value or "")
    out: list[str] = []
    for segment in _segments(record):
        if str(segment.get("type") or "").lower() == "text":
            data = segment.get("data")
            if isinstance(data, dict):
                out.append(str(data.get("text") or ""))
    return "".join(out)


def record_card_text_and_targets(record: dict) -> tuple[str, list[str], bool]:
    card_text: list[str] = []
    targets: list[str] = []
    group_invite = False
    for segment in _segments(record):
        text, target, invite = _card_fields(segment)
        if text:
            card_text.append(text)
        if target:
            targets.append(target)
        group_invite = group_invite or invite
    return " ".join(card_text), targets, group_invite


def record_has_image(record: dict) -> bool:
    for segment in _segments(record):
        kind = str(segment.get("type") or "").lower()
        data = segment.get("data") if isinstance(segment.get("data"), dict) else {}
        if kind == "image":
            return True
        if kind in {"mface", "market_face"} and any(
            str(data.get(key) or "").strip()
            for key in ("url", "download_url", "file_url", "file", "file_id", "id", "path", "file_path")
        ):
            return True
    return False


def _record_time(record: dict) -> float:
    try:
        value = float(record.get("occurred_at"))
        return value
    except (TypeError, ValueError):
        return 0.0


@dataclass(frozen=True)
class VoteResult:
    votes: dict[str, int]
    total: int
    hit_features: tuple[str, ...]
    evidence_message_ids: tuple[str, ...]
    text_count: int

    @property
    def needs_review(self) -> bool:
        return self.total >= REVIEW_THRESHOLD


def calculate_local_votes(records: Iterable[dict], anchor_at: float) -> VoteResult:
    window_start = float(anchor_at) - VOTE_WINDOW_SECONDS
    in_window = [
        row for row in records
        if window_start <= _record_time(row) <= float(anchor_at)
    ]
    texts = [record_text(row) for row in in_window]
    count = sum(len(countable_text(text)) for text in texts)
    ids = tuple(str(row.get("message_id") or "") for row in in_window)

    b_vote = int(len(set(ids)) >= 2 and count >= 100)
    any_link = False
    any_image = False
    any_ad_phrase = False
    any_group_invite = False
    window_combined_parts: list[str] = []
    for row, text in zip(in_window, texts):
        card_text, card_targets, card_invite = record_card_text_and_targets(row)
        combined = normalize_visible_text(text + " " + card_text)
        window_combined_parts.append(combined)
        targets = _URL_RE.findall(normalize_visible_text(text)) + card_targets
        if targets:
            any_link = True
        if record_has_image(row):
            any_image = True
        compact = re.sub(r"\s+", "", combined)
        if any(phrase in compact for phrase in _AD_PHRASES):
            any_ad_phrase = True
        if card_invite or _GROUP_INVITE_ACTION_RE.search(combined) or _GROUP_NUMBER_RE.search(combined):
            any_group_invite = True
        for target in targets:
            try:
                host = re.sub(r"(?i)^https?://", "", target).split("/", 1)[0].split(":", 1)[0].lower()
            except Exception:
                host = ""
            if host in {"jq.qq.com", "qun.qq.com"}:
                any_group_invite = True
    window_combined = " ".join(window_combined_parts)
    if _GROUP_MIGRATION_RE.search(window_combined) and _NEW_GROUP_HINT_RE.search(window_combined):
        any_group_invite = True

    votes = {
        "L": 0,
        "B": 2 * b_vote,
        "U": int(any_link),
        "I": int(any_image),
        "K": 3 * int(any_ad_phrase),
        "G": 3 * int(any_group_invite),
    }
    hits = tuple(name for name in VOTE_ORDER if votes[name])
    return VoteResult(votes, sum(votes.values()), hits, ids, count)


def with_member_level(local: VoteResult, member_level: object) -> VoteResult:
    level = parse_group_level(member_level)
    votes = dict(local.votes)
    votes["L"] = int(level is not None and level <= 4)
    return VoteResult(
        votes=votes,
        total=sum(votes.values()),
        hit_features=tuple(name for name in VOTE_ORDER if votes[name]),
        evidence_message_ids=local.evidence_message_ids,
        text_count=local.text_count,
    )
