from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AdminStep:
    tool: str
    args: dict


@dataclass
class AdminPlan:
    source: str
    summary: str
    steps: list[AdminStep] = field(default_factory=list)
    need_confirm: bool = False
    confidence: float = 0.0
