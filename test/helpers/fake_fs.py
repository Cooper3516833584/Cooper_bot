from pathlib import Path


class FakeFS:
    def __init__(self) -> None:
        self._files: dict[Path, str] = {}

    def write_text(self, path: Path, text: str) -> None:
        self._files[path] = text

    def read_text(self, path: Path) -> str:
        return self._files[path]

    def exists(self, path: Path) -> bool:
        return path in self._files
