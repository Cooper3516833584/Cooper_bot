from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from cooper_bot.core.config import DATABASES_DIR


MESSAGE_RETENTION_SECONDS = 2700
CASE_RETENTION_SECONDS = 7 * 24 * 3600
CASE_META_RETENTION_SECONDS = 24 * 3600


class ModerationStore:
    """Small SQLite store for moderation switches, message evidence and cases."""

    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path is not None else Path(DATABASES_DIR) / "moderation" / "moderation.sqlite3"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        with self._connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS groups (
                    self_id INTEGER NOT NULL,
                    group_id INTEGER NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 0,
                    enabled_at REAL,
                    last_capture_gap_at REAL,
                    PRIMARY KEY (self_id, group_id)
                );
                CREATE TABLE IF NOT EXISTS messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    self_id INTEGER NOT NULL,
                    group_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    message_id TEXT NOT NULL,
                    occurred_at REAL NOT NULL,
                    received_at REAL NOT NULL,
                    time_source TEXT NOT NULL DEFAULT 'event',
                    nickname TEXT NOT NULL DEFAULT '',
                    card TEXT NOT NULL DEFAULT '',
                    segments_json TEXT NOT NULL,
                    plain_text TEXT NOT NULL DEFAULT '',
                    UNIQUE (self_id, group_id, message_id)
                );
                CREATE INDEX IF NOT EXISTS ix_messages_sender_window
                    ON messages (self_id, group_id, user_id, occurred_at, seq);
                CREATE INDEX IF NOT EXISTS ix_messages_retention ON messages (occurred_at);
                CREATE TABLE IF NOT EXISTS cases (
                    id TEXT PRIMARY KEY,
                    self_id INTEGER NOT NULL,
                    group_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    revision INTEGER NOT NULL,
                    anchor_message_id TEXT NOT NULL,
                    anchor_seq INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    completed_at REAL,
                    review_snapshot_at REAL,
                    review_snapshot_seq INTEGER,
                    action_at REAL,
                    action_snapshot_seq INTEGER,
                    votes_json TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    stop_reason TEXT NOT NULL DEFAULT '',
                    evidence_json TEXT NOT NULL DEFAULT '[]',
                    audit_snapshot_json TEXT NOT NULL DEFAULT '',
                    effects_json TEXT NOT NULL DEFAULT '{}',
                    UNIQUE (self_id, group_id, user_id, revision, anchor_seq)
                );
                CREATE INDEX IF NOT EXISTS ix_cases_budget ON cases (self_id, created_at);
                CREATE INDEX IF NOT EXISTS ix_cases_sender ON cases (self_id, group_id, user_id, created_at);
                """
            )
            conn.commit()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.path), timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock, self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        value = dict(row)
        try:
            value["segments"] = json.loads(value.get("segments_json") or "[]")
        except Exception:
            value["segments"] = []
        return value

    def load_group_states(self) -> dict[tuple[int, int], dict]:
        with self._lock, self._connection() as conn:
            rows = conn.execute("SELECT * FROM groups").fetchall()
        return {
            (int(row["self_id"]), int(row["group_id"])): {
                "enabled": bool(row["enabled"]),
                "revision": int(row["revision"]),
                "enabled_at": row["enabled_at"],
                "last_capture_gap_at": row["last_capture_gap_at"],
            }
            for row in rows
        }

    def get_group_state(self, self_id: int, group_id: int) -> dict:
        with self._lock, self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM groups WHERE self_id=? AND group_id=?",
                (int(self_id), int(group_id)),
            ).fetchone()
        if row is None:
            return {"enabled": False, "revision": 0, "enabled_at": None, "last_capture_gap_at": None}
        return {
            "enabled": bool(row["enabled"]), "revision": int(row["revision"]),
            "enabled_at": row["enabled_at"], "last_capture_gap_at": row["last_capture_gap_at"],
        }

    def set_group_enabled(self, self_id: int, group_id: int, enabled: bool, revision: int, now: float) -> bool:
        with self._transaction() as conn:
            previous = conn.execute(
                "SELECT enabled,enabled_at,last_capture_gap_at,revision FROM groups WHERE self_id=? AND group_id=?",
                (int(self_id), int(group_id)),
            ).fetchone()
            if previous is not None and int(revision) < int(previous["revision"]):
                return False
            enabled_at = float(now) if enabled and (previous is None or not previous["enabled_at"]) else (previous["enabled_at"] if enabled else None)
            gap_at = (
                float(now) if not enabled and previous is not None and bool(previous["enabled"])
                else (previous["last_capture_gap_at"] if previous is not None else None)
            )
            conn.execute(
                """INSERT INTO groups (self_id, group_id, enabled, revision, enabled_at, last_capture_gap_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(self_id, group_id) DO UPDATE SET
                     enabled=excluded.enabled, revision=excluded.revision,
                     enabled_at=excluded.enabled_at, last_capture_gap_at=excluded.last_capture_gap_at
                   WHERE excluded.revision >= groups.revision""",
                (int(self_id), int(group_id), int(bool(enabled)), int(revision), enabled_at, gap_at),
            )
            return True

    def mark_capture_gap(self, now: float) -> None:
        with self._transaction() as conn:
            conn.execute("UPDATE groups SET last_capture_gap_at=? WHERE enabled=1", (float(now),))

    def append_message_if_enabled(self, record: dict, expected_revision: int) -> tuple[bool, int | None]:
        required = ("self_id", "group_id", "user_id", "message_id", "occurred_at", "received_at")
        if any(record.get(key) in (None, "") for key in required):
            return False, None
        segments = record.get("segments")
        if not isinstance(segments, list):
            return False, None
        segments_json = json.dumps(segments, ensure_ascii=False, separators=(",", ":"))
        with self._transaction() as conn:
            group = conn.execute(
                "SELECT enabled, revision FROM groups WHERE self_id=? AND group_id=?",
                (int(record["self_id"]), int(record["group_id"])),
            ).fetchone()
            if group is None or not bool(group["enabled"]) or int(group["revision"]) != int(expected_revision):
                return False, None
            cur = conn.execute(
                """INSERT OR IGNORE INTO messages
                   (self_id, group_id, user_id, message_id, occurred_at, received_at, time_source,
                    nickname, card, segments_json, plain_text)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    int(record["self_id"]), int(record["group_id"]), int(record["user_id"]),
                    str(record["message_id"]), float(record["occurred_at"]), float(record["received_at"]),
                    str(record.get("time_source") or "event"), str(record.get("nickname") or ""),
                    str(record.get("card") or ""), segments_json, str(record.get("plain_text") or ""),
                ),
            )
            inserted = cur.rowcount == 1
            if inserted:
                return True, int(cur.lastrowid)
            row = conn.execute(
                "SELECT seq FROM messages WHERE self_id=? AND group_id=? AND message_id=?",
                (int(record["self_id"]), int(record["group_id"]), str(record["message_id"])),
            ).fetchone()
            return False, int(row["seq"]) if row is not None else None

    def read_sender_window(
        self, self_id: int, group_id: int, user_id: int, start: float, end: float, through_seq: int | None = None
    ) -> list[dict]:
        sql = """SELECT * FROM messages
                 WHERE self_id=? AND group_id=? AND user_id=? AND occurred_at>=? AND occurred_at<=?"""
        params: list = [int(self_id), int(group_id), int(user_id), float(start), float(end)]
        if through_seq is not None:
            sql += " AND seq<=?"
            params.append(int(through_seq))
        sql += " ORDER BY occurred_at ASC, seq ASC"
        with self._lock, self._connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def read_sender_since_seq(self, self_id: int, group_id: int, user_id: int, through_seq: int) -> list[dict]:
        with self._lock, self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM messages WHERE self_id=? AND group_id=? AND user_id=? AND seq<=? ORDER BY occurred_at, seq",
                (int(self_id), int(group_id), int(user_id), int(through_seq)),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def latest_seq(self) -> int:
        with self._lock, self._connection() as conn:
            row = conn.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM messages").fetchone()
        return int(row["seq"] or 0)

    def cooldown_available(self, self_id: int, group_id: int, user_id: int, now: float) -> bool:
        with self._lock, self._connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM cases WHERE self_id=? AND group_id=? AND user_id=? AND created_at>? LIMIT 1",
                (int(self_id), int(group_id), int(user_id), float(now) - 60.0),
            ).fetchone()
        return row is None

    def reserve_review(
        self, self_id: int, group_id: int, user_id: int, revision: int, anchor_message_id: str,
        anchor_seq: int, now: float, votes: dict,
    ) -> str | None:
        case_id = f"{int(now):x}-{int(anchor_seq):x}-{int(user_id):x}"
        with self._transaction() as conn:
            duplicate = conn.execute(
                "SELECT id FROM cases WHERE self_id=? AND group_id=? AND user_id=? AND revision=? AND anchor_seq=?",
                (int(self_id), int(group_id), int(user_id), int(revision), int(anchor_seq)),
            ).fetchone()
            if duplicate is not None:
                return None
            recent = conn.execute(
                "SELECT 1 FROM cases WHERE self_id=? AND group_id=? AND user_id=? AND created_at>? LIMIT 1",
                (int(self_id), int(group_id), int(user_id), float(now) - 60.0),
            ).fetchone()
            if recent is not None:
                return None
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM cases WHERE self_id=? AND created_at>?",
                (int(self_id), float(now) - 3600.0),
            ).fetchone()
            if int(count["n"] or 0) >= 60:
                return None
            try:
                conn.execute(
                    """INSERT INTO cases
                       (id,self_id,group_id,user_id,revision,anchor_message_id,anchor_seq,created_at,
                        votes_json,decision)
                       VALUES (?,?,?,?,?,?,?,?,?,'running')""",
                    (
                        case_id, int(self_id), int(group_id), int(user_id), int(revision),
                        str(anchor_message_id), int(anchor_seq), float(now),
                        json.dumps(votes, ensure_ascii=False, separators=(",", ":")),
                    ),
                )
            except sqlite3.IntegrityError:
                return None
        return case_id

    def set_review_snapshot(self, case_id: str, at: float, through_seq: int) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE cases SET review_snapshot_at=?, review_snapshot_seq=? WHERE id=? AND decision='running'",
                (float(at), int(through_seq), str(case_id)),
            )

    def update_case_decision(
        self, case_id: str, decision: str, reason: str = "", evidence: list | None = None,
        completed_at: float | None = None,
    ) -> None:
        with self._transaction() as conn:
            completed = float(completed_at) if completed_at is not None else None
            if evidence is None:
                conn.execute(
                    """UPDATE cases SET decision=?, stop_reason=?,
                       completed_at=COALESCE(?, completed_at) WHERE id=?""",
                    (str(decision), str(reason)[:120], completed, str(case_id)),
                )
            else:
                conn.execute(
                    """UPDATE cases SET decision=?, stop_reason=?, evidence_json=?,
                       completed_at=COALESCE(?, completed_at) WHERE id=?""",
                    (
                        str(decision), str(reason)[:120],
                        json.dumps(evidence, ensure_ascii=False, separators=(",", ":")),
                        completed, str(case_id),
                    ),
                )

    def freeze_audit_snapshot(
        self, case_id: str, action_at: float, through_seq: int, messages: list[dict], evidence: list[dict]
    ) -> None:
        payload = json.dumps(messages, ensure_ascii=False, separators=(",", ":"))
        evidence_json = json.dumps(evidence, ensure_ascii=False, separators=(",", ":"))
        with self._transaction() as conn:
            cur = conn.execute(
                """UPDATE cases SET action_at=?, action_snapshot_seq=?, audit_snapshot_json=?, evidence_json=?
                   WHERE id=? AND decision='running' AND audit_snapshot_json=''""",
                (float(action_at), int(through_seq), payload, evidence_json, str(case_id)),
            )
            if cur.rowcount != 1:
                raise RuntimeError("case snapshot is not writable")

    def get_case(self, case_id: str) -> dict | None:
        with self._lock, self._connection() as conn:
            row = conn.execute("SELECT * FROM cases WHERE id=?", (str(case_id),)).fetchone()
        if row is None:
            return None
        result = dict(row)
        for key in ("votes_json", "evidence_json", "audit_snapshot_json", "effects_json"):
            raw = result.get(key) or ("[]" if key == "evidence_json" else "{}")
            try:
                result[key[:-5] if key.endswith("_json") else key] = json.loads(raw)
            except Exception:
                result[key[:-5] if key.endswith("_json") else key] = [] if key == "evidence_json" else {}
        return result

    def mark_effect_attempted(self, case_id: str, effect_key: str, details: dict | None = None) -> bool:
        with self._transaction() as conn:
            row = conn.execute("SELECT effects_json FROM cases WHERE id=?", (str(case_id),)).fetchone()
            if row is None:
                return False
            try:
                effects = json.loads(row["effects_json"] or "{}")
            except Exception:
                return False
            if effect_key in effects:
                return False
            effects[effect_key] = {"state": "attempted", "details": details or {}, "attempted_at": time.time()}
            conn.execute("UPDATE cases SET effects_json=? WHERE id=?", (json.dumps(effects, ensure_ascii=False), str(case_id)))
            return True

    def mark_effect_result(self, case_id: str, effect_key: str, state: str, details: dict | None = None) -> None:
        with self._transaction() as conn:
            row = conn.execute("SELECT effects_json FROM cases WHERE id=?", (str(case_id),)).fetchone()
            if row is None:
                return
            try:
                effects = json.loads(row["effects_json"] or "{}")
            except Exception:
                effects = {}
            effect = effects.get(effect_key)
            if not isinstance(effect, dict) or effect.get("state") != "attempted":
                return
            effect.update({"state": str(state), "details": details or {}, "result_at": time.time()})
            conn.execute("UPDATE cases SET effects_json=? WHERE id=?", (json.dumps(effects, ensure_ascii=False), str(case_id)))

    def mark_running_interrupted(self, now: float) -> None:
        with self._transaction() as conn:
            rows = conn.execute("SELECT id,effects_json FROM cases WHERE decision='running'").fetchall()
            for row in rows:
                try:
                    effects = json.loads(row["effects_json"] or "{}")
                except Exception:
                    effects = {}
                for effect in effects.values():
                    if isinstance(effect, dict) and effect.get("state") == "attempted":
                        effect["state"] = "unknown"
                conn.execute(
                    "UPDATE cases SET decision='interrupted', stop_reason='process_restart', completed_at=?, effects_json=? WHERE id=?",
                    (float(now), json.dumps(effects, ensure_ascii=False), str(row["id"])),
                )

    def cleanup_expired(self, now: float) -> tuple[int, int]:
        with self._transaction() as conn:
            msg = conn.execute("DELETE FROM messages WHERE occurred_at<?", (float(now) - MESSAGE_RETENTION_SECONDS,)).rowcount
            cases = conn.execute(
                "DELETE FROM cases WHERE (audit_snapshot_json!='' AND created_at<?) OR (audit_snapshot_json='' AND created_at<?)",
                (float(now) - CASE_RETENTION_SECONDS, float(now) - CASE_META_RETENTION_SECONDS),
            ).rowcount
        return int(msg), int(cases)
