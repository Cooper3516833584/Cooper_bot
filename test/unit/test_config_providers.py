from __future__ import annotations

from pathlib import Path

from cooper_bot.core import config


def _vision_from_file(tmp_path: Path, monkeypatch, lines: list[str]) -> tuple[str, str]:
    path = tmp_path / "api_key.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.setattr(config, "AI_API_KEY_PATH", path)
    return config._read_vision_config_from_api_key_txt()


def test_vision_file_config_keeps_line_position_when_embedding_blank(tmp_path, monkeypatch) -> None:
    """embedding 第 3、4 行留空时，vision 仍必须取第 5、6 行。"""
    base, key = _vision_from_file(
        tmp_path,
        monkeypatch,
        ["https://deepseek.example", "deepseek-key", "", "", "https://vision.example/", "vision-key"],
    )

    assert base == "https://vision.example"  # 末尾斜杠被去掉
    assert key == "vision-key"
    # VISION_ENABLED 由这两项推导；留空的 embedding 不能再把 vision 顶掉。
    assert bool(base and key) is True


def test_vision_file_config_unchanged_for_full_six_lines(tmp_path, monkeypatch) -> None:
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

    assert base == "https://vision.example"
    assert key == "vision-key"
    assert bool(base and key) is True


def test_vision_file_config_needs_enough_lines_without_index_error(tmp_path, monkeypatch) -> None:
    for lines in ([], ["only-one"], ["a", "b", "c", "d"]):
        base, key = _vision_from_file(tmp_path, monkeypatch, lines)
        assert (base, key) == ("", "")

    # 只有第 5 行、缺第 6 行：base 有值但 key 为空，仍然判定为未启用。
    base, key = _vision_from_file(tmp_path, monkeypatch, ["a", "b", "c", "d", "e"])
    assert key == ""
    assert bool(base and key) is False


def test_vision_file_config_survives_unreadable_path(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(config, "AI_API_KEY_PATH", tmp_path / "missing" / "api_key.txt")

    assert config._read_vision_config_from_api_key_txt() == ("", "")
