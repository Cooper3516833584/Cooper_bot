from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Sequence


ChatSideEffect = Callable[[str], str]
EmbedSideEffect = Callable[[list[str]], list[list[float]]]


@dataclass
class FakeAIClient:
    """Deterministic fake AI client used by tests."""

    default_chat_response: str = "fake response"
    default_embedding: list[float] = field(default_factory=lambda: [0.1, 0.2, 0.3])
    fail_chat_with: Exception | None = None
    fail_embed_with: Exception | None = None
    chat_exact_map: dict[str, str] = field(default_factory=dict)
    chat_contains_map: dict[str, str] = field(default_factory=dict)
    embed_map: dict[str, list[float]] = field(default_factory=dict)
    chat_error_on_call: dict[int, Exception] = field(default_factory=dict)
    embed_error_on_call: dict[int, Exception] = field(default_factory=dict)
    chat_side_effect: ChatSideEffect | None = None
    embed_side_effect: EmbedSideEffect | None = None
    chat_calls: list[dict[str, Any]] = field(default_factory=list)
    embed_calls: list[dict[str, Any]] = field(default_factory=list)

    def add_chat_rule(self, prompt_key: str, response: str, *, exact: bool = False) -> None:
        if exact:
            self.chat_exact_map[str(prompt_key)] = str(response)
            return
        self.chat_contains_map[str(prompt_key)] = str(response)

    def add_embed_rule(self, text: str, vector: Sequence[float]) -> None:
        self.embed_map[str(text)] = [float(x) for x in vector]

    def raise_chat_on(self, call_no: int, exc: Exception | None = None) -> None:
        self.chat_error_on_call[int(call_no)] = exc or RuntimeError(
            f"fake chat failure on call #{int(call_no)}"
        )

    def raise_embed_on(self, call_no: int, exc: Exception | None = None) -> None:
        self.embed_error_on_call[int(call_no)] = exc or RuntimeError(
            f"fake embed failure on call #{int(call_no)}"
        )

    def reset(self) -> None:
        self.chat_calls.clear()
        self.embed_calls.clear()
        self.chat_error_on_call.clear()
        self.embed_error_on_call.clear()

    def _chat_response(self, prompt: str) -> str:
        key = str(prompt)
        if key in self.chat_exact_map:
            return self.chat_exact_map[key]
        for needle, response in self.chat_contains_map.items():
            if needle and needle in key:
                return response
        return self.default_chat_response

    def chat(self, prompt: str) -> str:
        prompt_text = str(prompt)
        call_no = len(self.chat_calls) + 1
        self.chat_calls.append({"call_no": call_no, "prompt": prompt_text})

        if call_no in self.chat_error_on_call:
            raise self.chat_error_on_call[call_no]
        if self.fail_chat_with is not None:
            raise self.fail_chat_with
        if self.chat_side_effect is not None:
            return str(self.chat_side_effect(prompt_text))
        return str(self._chat_response(prompt_text))

    async def achat(self, prompt: str) -> str:
        return self.chat(prompt)

    def embed(self, texts: str | Sequence[str]) -> list[float] | list[list[float]]:
        is_single = isinstance(texts, str)
        batch = [str(texts)] if is_single else [str(x) for x in texts]
        call_no = len(self.embed_calls) + 1
        self.embed_calls.append({"call_no": call_no, "texts": list(batch)})

        if call_no in self.embed_error_on_call:
            raise self.embed_error_on_call[call_no]
        if self.fail_embed_with is not None:
            raise self.fail_embed_with
        if self.embed_side_effect is not None:
            vectors = [[float(v) for v in row] for row in self.embed_side_effect(batch)]
        else:
            vectors = [
                [float(v) for v in self.embed_map.get(text, self.default_embedding)]
                for text in batch
            ]
        return vectors[0] if is_single else vectors

    async def aembed(self, texts: str | Sequence[str]) -> list[float] | list[list[float]]:
        return self.embed(texts)


# Backward-compatible alias used by early scaffold code.
FakeAIService = FakeAIClient
