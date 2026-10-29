"""Private business prompt snapshots; examples are never runtime fallbacks."""
from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
import re

from cooper_bot.core import config


# Template IDs, variable names and output protocol requirements are code contracts.
SPECS = {
    "web.judge": ("web/judge.txt", (), ("[WEB_SEARCH]",)),
    "web.compose": ("web/compose.txt", (), ()),
    "web.compose_input": ("web/compose_input.txt", ("question", "material"), ()),
    "email.classify": ("email/classify.txt", (), ("JSON", "category", "notify", "summary")),
    "materials.classify_tbd": ("materials/classify_tbd.txt", ("subjects", "relative_path", "filename", "summary", "keywords", "snippet"), ("JSON", "action", "target_subject")),
    "materials.review_subject": ("materials/review_subject.txt", ("subjects", "current_subject", "relative_path", "filename", "summary", "keywords", "snippet"), ("JSON", "action", "target_subject")),
    "materials.summary_title": ("materials/summary_title.txt", ("subject", "filename", "file_type"), ("JSON", "keywords", "summary")),
    "materials.summary_content": ("materials/summary_content.txt", ("subject", "filename", "file_type", "snippet"), ("JSON", "keywords", "summary")),
    "notice.defaults": ("notice/defaults.json", ("source", "content", "silent_token"), ()),
    "calendar.events": ("calendar/events.txt", ("date", "snapshot"), ("JSON", "date", "events")),
    "calendar.holidays": ("calendar/holidays.txt", ("year",), ("JSON", "year", "days")),
    "calendar.render_note": ("calendar/render_note.txt", ("date", "snapshot", "events"), ()),
    "memory.extract": ("memory/extract.txt", (), ("json", "schema_version", "facts", "evidence_input_seq", "evidence_quote")),
    "memory.summary": ("memory/summary.txt", (), ("json", "schema_version", "topics", "current_goal", "decisions", "open_items", "uncertainties")),
    "vision.describe": ("vision/describe.txt", (), ("JSON", "kind", "scene", "visible_text", "emotion", "intent")),
    "moderation.review": ("moderation/review.txt", (), ("JSON", "decision", "category", "reason", "evidence", "message_id", "slot_id", "quote")),
}
_SLOT = re.compile(r"\{\{([^{}\n]+)\}\}")
_MAX_TEMPLATE_BYTES = 128 * 1024


class PromptConfigError(ValueError):
    """A prompt is unavailable or violates its template contract."""


def _validate(template_id: str, text: str, variables, required=()) -> None:
    if not text.strip():
        raise PromptConfigError(f"{template_id}: empty template")
    slots = set(_SLOT.findall(text))
    rest = _SLOT.sub("", text)
    if slots != set(variables) or "{{" in rest:
        raise PromptConfigError(f"{template_id}: invalid template variables")
    if any(part not in text for part in required):
        raise PromptConfigError(f"{template_id}: missing output protocol")


def render_text(text: str, *, template_id: str = "notice.override", **values) -> str:
    """Substitute the template once; substituted user data is never reinterpreted."""
    slots = set(_SLOT.findall(text))
    if not slots.issubset(values) or slots - {"source", "content", "silent_token"}:
        raise PromptConfigError(f"{template_id}: invalid template variables")
    if "{{" in _SLOT.sub("", text):
        raise PromptConfigError(f"{template_id}: invalid template variables")
    return _SLOT.sub(lambda match: str(values[match[1]]), text)


class PromptStore:
    def __init__(self, root: Path):
        self.root = Path(root).resolve()
        self._texts: dict[str, str] = {}
        self._errors: dict[str, str] = {}
        for template_id, (relative, variables, required) in SPECS.items():
            try:
                path = (self.root / relative).resolve()
                if not path.is_relative_to(self.root):
                    raise PromptConfigError(f"{template_id}: template outside prompt root")
                with path.open("rb") as stream:
                    raw = stream.read(_MAX_TEMPLATE_BYTES + 1)
                if len(raw) > _MAX_TEMPLATE_BYTES:
                    raise PromptConfigError(f"{template_id}: template too large")
                text = raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
                _validate(template_id, text, variables, required)
                if template_id == "notice.defaults":
                    data = json.loads(text)
                    if not isinstance(data, dict) or not isinstance(data.get("default"), dict):
                        raise PromptConfigError(f"{template_id}: invalid JSON structure")
                self._texts[template_id] = text
            except (OSError, UnicodeError, ValueError):
                # Never expose a private template, filename supplied by it, or input data.
                self._errors[template_id] = f"{template_id}: template unavailable or invalid"

    def get(self, template_id: str) -> str:
        if template_id not in SPECS:
            raise PromptConfigError("unknown template ID")
        if template_id in self._errors:
            raise PromptConfigError(self._errors[template_id])
        return self._texts[template_id]


@lru_cache(maxsize=8)
def _snapshot(root: str) -> PromptStore:
    return PromptStore(Path(root))


def get_prompt(template_id: str) -> str:
    return _snapshot(str(config.AI_PROMPTS_DIR)).get(template_id)


def optional_prompt(template_id: str) -> str:
    try:
        return get_prompt(template_id)
    except PromptConfigError:
        return ""


def render_prompt(template_id: str, **values) -> str:
    text = get_prompt(template_id)
    if set(values) != set(SPECS[template_id][1]):
        raise PromptConfigError(f"{template_id}: invalid render arguments")
    return _SLOT.sub(lambda match: str(values[match[1]]), text)


def notice_defaults() -> dict:
    try:
        return json.loads(get_prompt("notice.defaults"))
    except PromptConfigError:
        return {"default": {}, "groups": {}}


if __name__ == "__main__":
    failures = 0
    for name in SPECS:
        try:
            get_prompt(name)
            print(f"OK {name}")
        except PromptConfigError as error:
            failures += 1
            print(str(error))
    raise SystemExit(bool(failures))
