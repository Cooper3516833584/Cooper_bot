from __future__ import annotations

from pathlib import Path

from cooper_bot.core import config


def _vision_from_file(tmp_path: Path, monkeypatch, lines: list[str]) -> tuple[str, str]:
    path = tmp_path / "api_key.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.setattr(config, "AI_API_KEY_PATH", path)
    return config._read_vision_config_from_api_key_txt()


def test_vision_file_config_keeps_line_position_when_embedding_blank(tmp_path, monkeypatch) -> None:
    """Embedding 留空或残留旧视觉凭据时，识图仍复用 DeepSeek。"""
    base, key = _vision_from_file(
        tmp_path,
        monkeypatch,
        ["https://deepseek.example/", "deepseek-key", "", "", "https://vision.example", "vision-key"],
    )

    assert base == "https://deepseek.example"  # 末尾斜杠被去掉
    assert key == "deepseek-key"
    assert bool(base and key) is True


def test_vision_file_config_reuses_deepseek_with_full_six_lines(tmp_path, monkeypatch) -> None:
    base, key = _vision_from_file(
        tmp_path,
        monkeypatch,
        [
            "https://deepseek.example",
            "deepseek-key",
            "https://embed.example",
            "embed-key",
            "https://vision.example",
            "vision-key",
        ],
    )

    assert base == "https://deepseek.example"
    assert key == "deepseek-key"
    assert bool(base and key) is True


def test_vision_file_config_needs_enough_lines_without_index_error(tmp_path, monkeypatch) -> None:
    for lines in ([], ["", "", "", "", "https://old.example", "old-key"]):
        base, key = _vision_from_file(tmp_path, monkeypatch, lines)
        assert (base, key) == ("", "")

    # 只有 DeepSeek base、缺 key 时，仍然判定为未启用。
    base, key = _vision_from_file(tmp_path, monkeypatch, ["https://deepseek.example"])
    assert base == "https://deepseek.example"
    assert key == ""
    assert bool(base and key) is False

    assert _vision_from_file(tmp_path, monkeypatch, ["https://deepseek.example", "key"]) == ("https://deepseek.example", "key")


def test_vision_file_config_survives_unreadable_path(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(config, "AI_API_KEY_PATH", tmp_path / "missing" / "api_key.txt")

    assert config._read_vision_config_from_api_key_txt() == ("", "")
