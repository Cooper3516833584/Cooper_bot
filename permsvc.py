# permsvc.py
import json
from pathlib import Path
from typing import Dict

class PermService:
    """简单权限库：记录 user_id -> level。
    需求：在群里发过言的人自动至少为 level=1；未见过默认 level=0。
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, int] = {}
        self._dirty = False
        self._load()

    def _load(self):
        try:
            if self.db_path.exists():
                obj = json.loads(self.db_path.read_text(encoding="utf-8"))
                if isinstance(obj, dict):
                    self._data = {str(k): int(v) for k, v in obj.items()}
        except Exception:
            self._data = {}

    def _flush(self):
        if not self._dirty:
            return
        tmp = self.db_path.with_suffix(self.db_path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.db_path)
        self._dirty = False

    def get_level(self, user_id: int) -> int:
        return int(self._data.get(str(int(user_id)), 0))

    def set_level(self, user_id: int, level: int):
        uid = str(int(user_id))
        level = int(level)
        cur = int(self._data.get(uid, 0))
        if level != cur:
            self._data[uid] = level
            self._dirty = True
            self._flush()

    def bump_min(self, user_id: int, min_level: int):
        uid = str(int(user_id))
        cur = int(self._data.get(uid, 0))
        if cur < int(min_level):
            self._data[uid] = int(min_level)
            self._dirty = True
            self._flush()

    def touch_group_speaker(self, user_id: int):
        """群里出现过发言，就至少 level=1。"""
        self.bump_min(user_id, 1)
