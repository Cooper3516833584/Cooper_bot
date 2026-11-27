_EXPLICIT_COMMAND_NAMES = frozenset({
    "autoat",
    "chandin",
    "calendartest",
    "count",
    "countlist",
    "countremove",
    "find",
    "get",
    "h",
    "handin",
    "handincheck",
    "handinget",
    "handinstat",
    "help",
    "kick",
    "level",
    "memory",
    "ping",
    "signin",
    "whoami",
})


def is_known_explicit_command(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw.startswith(("/", "／")):
        return False
    parts = raw[1:].strip().split(maxsplit=1)
    return bool(parts) and parts[0].lower() in _EXPLICIT_COMMAND_NAMES
