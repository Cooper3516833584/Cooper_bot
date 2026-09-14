from __future__ import annotations

import json
import shutil
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable


_SCHEMA_VERSION = 2


class MemoryStore:
    """SQLite facade whose connection never escapes its one worker thread."""

    def __init__(self, path: Path, *, busy_timeout_ms: int = 2000):
        self.path = Path(path)
        self.busy_timeout_ms = int(busy_timeout_ms)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="memory-db")
        self._conn: sqlite3.Connection | None = None
        self._closed = False

    async def _call(self, fn: Callable[..., Any], *args: Any) -> Any:
        if self._closed:
            raise RuntimeError("memory store is closed")
        import asyncio
        return await asyncio.get_running_loop().run_in_executor(self._executor, fn, *args)

    def _open_sync(self) -> None:
        if self._conn is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=self.busy_timeout_ms / 1000)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            pass
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version > _SCHEMA_VERSION:
            conn.close()
            raise RuntimeError("memory database was created by a newer version")
        try:
            with conn:
                conn.executescript("""
                CREATE TABLE IF NOT EXISTS memory_scopes (
                  scope_id TEXT PRIMARY KEY, bot_id INTEGER NOT NULL, profile TEXT NOT NULL,
                  kind TEXT NOT NULL, target_id INTEGER NOT NULL, owner_user_id INTEGER,
                  enabled INTEGER NOT NULL DEFAULT 0, capture_mode TEXT NOT NULL DEFAULT 'directed',
                  active_conversation_id TEXT NOT NULL, epoch INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS memory_members (
                  scope_id TEXT NOT NULL REFERENCES memory_scopes(scope_id) ON DELETE CASCADE,
                  actor_user_id INTEGER NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                  extracted_through_input_seq INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL,
                  PRIMARY KEY(scope_id, actor_user_id));
                CREATE TABLE IF NOT EXISTS memory_events (
                  seq INTEGER PRIMARY KEY AUTOINCREMENT, scope_id TEXT NOT NULL REFERENCES memory_scopes(scope_id) ON DELETE CASCADE,
                  conversation_id TEXT NOT NULL, epoch INTEGER NOT NULL, source_event_id TEXT NOT NULL,
                  event_kind TEXT NOT NULL, source_kind TEXT NOT NULL, turn_id TEXT, role TEXT NOT NULL,
                  actor_user_id INTEGER NOT NULL, parent_input_seq INTEGER, own_text TEXT NOT NULL DEFAULT '',
                  quoted_text TEXT NOT NULL DEFAULT '', visual_text TEXT NOT NULL DEFAULT '', flags_json TEXT NOT NULL DEFAULT '{}',
                  state TEXT NOT NULL, created_at REAL NOT NULL, confirmed_at REAL,
                  UNIQUE(scope_id, source_event_id, event_kind));
                CREATE INDEX IF NOT EXISTS idx_memory_events_scope_conv_seq ON memory_events(scope_id, conversation_id, seq);
                CREATE INDEX IF NOT EXISTS idx_memory_events_parent ON memory_events(scope_id, parent_input_seq, state);
                CREATE TABLE IF NOT EXISTS memory_summaries (
                  scope_id TEXT NOT NULL, conversation_id TEXT NOT NULL, epoch INTEGER NOT NULL, version INTEGER NOT NULL,
                  through_input_seq INTEGER NOT NULL, summary_json TEXT NOT NULL, updated_at REAL NOT NULL,
                  PRIMARY KEY(scope_id, conversation_id));
                CREATE TABLE IF NOT EXISTS memory_facts (
                  fact_id TEXT PRIMARY KEY, scope_id TEXT NOT NULL REFERENCES memory_scopes(scope_id) ON DELETE CASCADE,
                  subject_id TEXT NOT NULL, fact_key TEXT NOT NULL, revision INTEGER NOT NULL, status TEXT NOT NULL,
                  text TEXT NOT NULL, source_kind TEXT NOT NULL, source_input_seqs_json TEXT NOT NULL DEFAULT '[]',
                  evidence_json TEXT NOT NULL DEFAULT '[]', valid_from REAL NOT NULL, expires_at REAL,
                  supersedes_id TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS idx_memory_facts_scope_subject ON memory_facts(scope_id, subject_id, status, updated_at);
                CREATE TABLE IF NOT EXISTS memory_jobs (
                  job_id TEXT PRIMARY KEY, scope_id TEXT NOT NULL, conversation_id TEXT, epoch INTEGER NOT NULL,
                  kind TEXT NOT NULL, target_input_seq INTEGER, payload_json TEXT NOT NULL DEFAULT '{}', state TEXT NOT NULL,
                  attempts INTEGER NOT NULL DEFAULT 0, not_before REAL NOT NULL, lease_until REAL, last_error_code TEXT,
                  dedupe_key TEXT NOT NULL UNIQUE);
                CREATE TABLE IF NOT EXISTS memory_embeddings (
                  fact_id TEXT NOT NULL REFERENCES memory_facts(fact_id) ON DELETE CASCADE, fingerprint TEXT NOT NULL,
                  scope_id TEXT NOT NULL, fact_revision INTEGER NOT NULL, dimension INTEGER NOT NULL, vector BLOB NOT NULL,
                  updated_at REAL NOT NULL, PRIMARY KEY(fact_id, fingerprint));
                CREATE TABLE IF NOT EXISTS memory_forget_markers (
                  scope_id TEXT NOT NULL, subject_id TEXT NOT NULL, fact_key TEXT NOT NULL,
                  blocked_through_input_seq INTEGER NOT NULL, created_at REAL NOT NULL,
                  PRIMARY KEY(scope_id, subject_id, fact_key));
                CREATE TABLE IF NOT EXISTS memory_usage_daily (
                  day_key TEXT NOT NULL, kind TEXT NOT NULL, attempts INTEGER NOT NULL, updated_at REAL NOT NULL,
                  PRIMARY KEY(day_key, kind));
                """)
                conn.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")
        except Exception:
            conn.close()
            raise
        self._conn = conn

    async def start(self) -> None:
        await self._call(self._open_sync)

    def _c(self) -> sqlite3.Connection:
        if self._conn is None:
            self._open_sync()
        assert self._conn is not None
        return self._conn

    async def ensure_scope(self, scope_id: str, *, bot_id: int, profile: str, kind: str, target_id: int, owner_user_id: int | None) -> dict:
        return await self._call(self._ensure_scope, scope_id, bot_id, profile, kind, target_id, owner_user_id)

    def _ensure_scope(self, scope_id: str, bot_id: int, profile: str, kind: str, target_id: int, owner_user_id: int | None) -> dict:
        c, now = self._c(), time.time()
        with c:
            c.execute("INSERT OR IGNORE INTO memory_scopes(scope_id,bot_id,profile,kind,target_id,owner_user_id,active_conversation_id,updated_at) VALUES(?,?,?,?,?,?,?,?)", (scope_id,bot_id,profile,kind,target_id,owner_user_id,uuid.uuid4().hex,now))
            c.execute("UPDATE memory_scopes SET updated_at=? WHERE scope_id=?", (now, scope_id))
        return dict(c.execute("SELECT * FROM memory_scopes WHERE scope_id=?", (scope_id,)).fetchone())

    async def scope(self, scope_id: str) -> dict | None:
        return await self._call(lambda: (lambda r: dict(r) if r else None)(self._c().execute("SELECT * FROM memory_scopes WHERE scope_id=?", (scope_id,)).fetchone()))

    async def set_scope_policy(self, scope_id: str, enabled: bool, capture_mode: str = "directed") -> None:
        await self._call(self._set_scope_policy, scope_id, enabled, capture_mode)

    def _set_scope_policy(self, scope_id: str, enabled: bool, capture_mode: str) -> None:
        if capture_mode not in {"directed", "all"}:
            raise ValueError("invalid capture mode")
        with self._c(): self._c().execute("UPDATE memory_scopes SET enabled=?,capture_mode=?,epoch=epoch+1,updated_at=? WHERE scope_id=?", (int(enabled), capture_mode, time.time(), scope_id))

    async def set_member_policy(self, scope_id: str, actor: int, enabled: bool) -> None:
        await self._call(self._set_member_policy, scope_id, actor, enabled)

    def _set_member_policy(self, scope_id: str, actor: int, enabled: bool) -> None:
        c, now = self._c(), time.time()
        with c: c.execute("INSERT INTO memory_members(scope_id,actor_user_id,enabled,updated_at) VALUES(?,?,?,?) ON CONFLICT(scope_id,actor_user_id) DO UPDATE SET enabled=excluded.enabled,updated_at=excluded.updated_at", (scope_id, actor, int(enabled), now))

    async def member_enabled(self, scope_id: str, actor: int) -> bool:
        return await self._call(lambda: self._member_enabled(scope_id, actor))
    def _member_enabled(self, scope_id: str, actor: int) -> bool:
        r=self._c().execute("SELECT enabled FROM memory_members WHERE scope_id=? AND actor_user_id=?",(scope_id,actor)).fetchone(); return True if r is None else bool(r[0])

    async def append_input_once(self, scope_id: str, actor: int, source_id: str, own: str, quoted: str, visual: str, source_kind: str, *, metadata_only: bool=False) -> dict:
        return await self._call(self._append_input_once, scope_id, actor, source_id, own, quoted, visual, source_kind, metadata_only)

    def _append_input_once(self, scope_id: str, actor: int, source_id: str, own: str, quoted: str, visual: str, source_kind: str, metadata_only: bool) -> dict:
        c=self._c(); scope=c.execute("SELECT active_conversation_id,epoch FROM memory_scopes WHERE scope_id=?",(scope_id,)).fetchone()
        if not scope: raise RuntimeError("unknown memory scope")
        row=c.execute("SELECT * FROM memory_events WHERE scope_id=? AND source_event_id=? AND event_kind='input'",(scope_id,source_id)).fetchone()
        if row: return dict(row)
        now=time.time(); state="explicit" if source_kind=="explicit_memory" else ("observed" if source_kind=="passive_chat" else "pending")
        with c:
            c.execute("INSERT INTO memory_events(scope_id,conversation_id,epoch,source_event_id,event_kind,source_kind,turn_id,role,actor_user_id,own_text,quoted_text,visual_text,flags_json,state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(scope_id,scope[0],scope[1],source_id,"input",source_kind,uuid.uuid4().hex,"user",actor,"" if metadata_only else own,"" if metadata_only else quoted,"" if metadata_only else visual,json.dumps({"metadata_only":metadata_only}),state,now))
        return dict(c.execute("SELECT * FROM memory_events WHERE scope_id=? AND source_event_id=? AND event_kind='input'",(scope_id,source_id)).fetchone())

    async def insert_assistant_once(self, scope_id: str, input_seq: int, text: str, *, metadata_only: bool=False) -> dict:
        return await self._call(self._insert_assistant_once,scope_id,input_seq,text,metadata_only)
    def _insert_assistant_once(self,scope_id:str,input_seq:int,text:str,metadata_only:bool)->dict:
        c=self._c(); inp=c.execute("SELECT * FROM memory_events WHERE scope_id=? AND seq=?",(scope_id,input_seq)).fetchone()
        if not inp: raise RuntimeError("unknown input event")
        sid=f"turn:{inp['turn_id']}:assistant"; existing=c.execute("SELECT * FROM memory_events WHERE scope_id=? AND source_event_id=? AND event_kind='assistant'",(scope_id,sid)).fetchone()
        if existing:return dict(existing)
        with c: c.execute("INSERT INTO memory_events(scope_id,conversation_id,epoch,source_event_id,event_kind,source_kind,turn_id,role,actor_user_id,parent_input_seq,own_text,flags_json,state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(scope_id,inp['conversation_id'],inp['epoch'],sid,'assistant','direct_chat',inp['turn_id'],'assistant',inp['actor_user_id'],input_seq,'' if metadata_only else text,json.dumps({'metadata_only':metadata_only}),'generated',time.time()))
        return dict(c.execute("SELECT * FROM memory_events WHERE scope_id=? AND source_event_id=? AND event_kind='assistant'",(scope_id,sid)).fetchone())

    async def finish_turn(self, scope_id: str, input_seq: int, confirmed: bool, *, failed: bool=False) -> bool:
        return await self._call(self._finish_turn,scope_id,input_seq,confirmed,failed)
    def _finish_turn(self,scope_id:str,input_seq:int,confirmed:bool,failed:bool)->bool:
        c=self._c(); now=time.time(); state='failed' if failed else ('confirmed' if confirmed else 'unconfirmed'); user='failed' if failed else ('completed' if confirmed else 'unconfirmed')
        with c:
            cur=c.execute("UPDATE memory_events SET state=?,confirmed_at=? WHERE scope_id=? AND parent_input_seq=? AND role='assistant' AND state='generated'",(state,now,scope_id,input_seq))
            if not cur.rowcount:return False
            c.execute("UPDATE memory_events SET state=?,confirmed_at=? WHERE scope_id=? AND seq=? AND state='pending'",(user,now,scope_id,input_seq))
        return True

    async def snapshot_rows(self, scope_id: str, conversation_id: str, current_seq: int, limit: int) -> list[dict]:
        return await self._call(self._snapshot_rows,scope_id,conversation_id,current_seq,limit)
    def _snapshot_rows(self,scope_id:str,cid:str,current:int,limit:int)->list[dict]:
        q="""SELECT * FROM memory_events WHERE scope_id=? AND conversation_id=? AND ((role='user' AND seq<? AND state IN ('completed','observed','explicit','unconfirmed')) OR (role='assistant' AND parent_input_seq<? AND state='confirmed')) ORDER BY seq"""
        rows=[dict(x) for x in self._c().execute(q,(scope_id,cid,current,current)).fetchall()]; return rows[-max(1,limit):]

    async def save_explicit_fact(self, scope_id: str, subject_id: str, text: str, *, fact_id: str | None=None, replace_id: str | None=None) -> dict:
        return await self._call(self._save_explicit_fact,scope_id,subject_id,text,fact_id,replace_id)
    def _save_explicit_fact(self,scope_id:str,subject:str,text:str,fact_id:str|None,replace_id:str|None)->dict:
        c=self._c(); now=time.time(); key="explicit."+uuid.uuid5(uuid.NAMESPACE_URL,text.strip()).hex[:20]
        if replace_id:
            old=c.execute("SELECT * FROM memory_facts WHERE fact_id=? AND scope_id=? AND subject_id=? AND status='active'",(replace_id,scope_id,subject)).fetchone()
            if not old: raise ValueError("fact not found")
            key, fact_id=old['fact_key'],uuid.uuid4().hex
            with c:c.execute("UPDATE memory_facts SET status='superseded',updated_at=? WHERE fact_id=?",(now,old['fact_id']))
        else:
            old=c.execute("SELECT * FROM memory_facts WHERE scope_id=? AND subject_id=? AND fact_key=? AND status='active'",(scope_id,subject,key)).fetchone()
            if old:return dict(old)
            fact_id=fact_id or uuid.uuid4().hex
        rev=int(c.execute("SELECT COALESCE(MAX(revision),0)+1 FROM memory_facts WHERE scope_id=? AND subject_id=? AND fact_key=?",(scope_id,subject,key)).fetchone()[0])
        with c:c.execute("INSERT INTO memory_facts(fact_id,scope_id,subject_id,fact_key,revision,status,text,source_kind,valid_from,created_at,updated_at) VALUES(?,?,?,?,?,'active',?,'explicit_memory',?,?,?)",(fact_id,scope_id,subject,key,rev,text,now,now,now))
        return dict(c.execute("SELECT * FROM memory_facts WHERE fact_id=?",(fact_id,)).fetchone())

    async def list_facts(self, scope_id:str, subjects:tuple[str,...]) -> list[dict]:
        return await self._call(self._list_facts,scope_id,subjects)
    def _list_facts(self,scope_id:str,subjects:tuple[str,...])->list[dict]:
        if not subjects:return []
        marks=','.join('?'*len(subjects)); q=f"SELECT * FROM memory_facts WHERE scope_id=? AND subject_id IN ({marks}) AND status='active' AND (expires_at IS NULL OR expires_at>?) ORDER BY updated_at DESC,fact_id"; return [dict(x) for x in self._c().execute(q,(scope_id,*subjects,time.time())).fetchall()]

    async def history(self, scope_id: str, *, before: int | None=None, limit: int=20) -> list[dict]:
        return await self._call(self._history, scope_id, before, limit)
    def _history(self, scope_id, before, limit):
        where="scope_id=? AND state IN ('completed','confirmed','observed','explicit')"
        args=[scope_id]
        if before is not None: where += " AND seq<?";args.append(before)
        args.append(max(1,min(int(limit),20)))
        return [dict(x) for x in self._c().execute(f"SELECT * FROM memory_events WHERE {where} ORDER BY seq DESC LIMIT ?",args).fetchall()][::-1]

    async def forget_fact(self, scope_id:str, subject:str, fact_id:str)->bool:
        return await self._call(self._forget_fact,scope_id,subject,fact_id)
    def _forget_fact(self,scope_id:str,subject:str,fact_id:str)->bool:
        c=self._c(); row=c.execute("SELECT fact_key FROM memory_facts WHERE fact_id=? AND scope_id=? AND subject_id=?",(fact_id,scope_id,subject)).fetchone()
        if not row:return False
        with c:
            c.execute("UPDATE memory_scopes SET epoch=epoch+1,updated_at=? WHERE scope_id=?",(time.time(),scope_id)); c.execute("DELETE FROM memory_facts WHERE scope_id=? AND subject_id=? AND fact_key=?",(scope_id,subject,row[0])); c.execute("INSERT INTO memory_forget_markers VALUES(?,?,?,?,?) ON CONFLICT(scope_id,subject_id,fact_key) DO UPDATE SET blocked_through_input_seq=excluded.blocked_through_input_seq,created_at=excluded.created_at",(scope_id,subject,row[0],10**18,time.time())); c.execute("DELETE FROM memory_summaries WHERE scope_id=?",(scope_id,))
        return True

    async def clear_subject(self,scope_id:str,subject:str)->None: await self._call(self._clear_subject,scope_id,subject)
    def _clear_subject(self,scope_id:str,subject:str)->None:
        c=self._c(); actor=int(subject.split(':',1)[1]) if subject.startswith('user:') else -1
        with c:
            c.execute("UPDATE memory_scopes SET epoch=epoch+1,updated_at=? WHERE scope_id=?",(time.time(),scope_id)); c.execute("DELETE FROM memory_facts WHERE scope_id=? AND subject_id=?",(scope_id,subject)); c.execute("DELETE FROM memory_events WHERE scope_id=? AND actor_user_id=?",(scope_id,actor)); c.execute("DELETE FROM memory_summaries WHERE scope_id=?",(scope_id,)); c.execute("UPDATE memory_jobs SET state='cancelled' WHERE scope_id=? AND state IN ('queued','running')",(scope_id,))

    async def rotate_conversation(self,scope_id:str)->str:return await self._call(self._rotate_conversation,scope_id)
    def _rotate_conversation(self,scope_id:str)->str:
        cid=uuid.uuid4().hex
        with self._c():self._c().execute("UPDATE memory_scopes SET active_conversation_id=?,epoch=epoch+1,updated_at=? WHERE scope_id=?",(cid,time.time(),scope_id))
        return cid
    async def enqueue_job(self, scope_id: str, conversation_id: str, epoch: int, kind: str, target: int, payload: dict, dedupe_key: str) -> None:
        await self._call(self._enqueue_job, scope_id, conversation_id, epoch, kind, target, payload, dedupe_key)
    def _enqueue_job(self, scope_id, cid, epoch, kind, target, payload, key):
        with self._c(): self._c().execute("INSERT OR IGNORE INTO memory_jobs(job_id,scope_id,conversation_id,epoch,kind,target_input_seq,payload_json,state,not_before,dedupe_key) VALUES(?,?,?,?,?,?,?,'queued',?,?)", (uuid.uuid4().hex,scope_id,cid,epoch,kind,target,json.dumps(payload,ensure_ascii=False),time.time(),key))
    async def claim_job(self) -> dict | None: return await self._call(self._claim_job)
    def _claim_job(self):
        c=self._c(); now=time.time(); row=c.execute("SELECT * FROM memory_jobs WHERE state='queued' AND not_before<=? ORDER BY not_before LIMIT 1",(now,)).fetchone()
        if not row:return None
        with c:c.execute("UPDATE memory_jobs SET state='running',attempts=attempts+1,lease_until=? WHERE job_id=? AND state='queued'",(now+120,row['job_id']))
        return dict(c.execute("SELECT * FROM memory_jobs WHERE job_id=?",(row['job_id'],)).fetchone())
    async def finish_job(self, job_id: str, success: bool, error: str = "") -> None: await self._call(self._finish_job,job_id,success,error)
    def _finish_job(self,job_id,success,error):
        with self._c():self._c().execute("UPDATE memory_jobs SET state=?,lease_until=NULL,last_error_code=? WHERE job_id=?",('succeeded' if success else 'failed',error[:80],job_id))
    async def backup(self,destination:Path)->None: await self._call(self._backup,destination)
    def _backup(self,destination:Path)->None:
        destination=Path(destination); destination.parent.mkdir(parents=True,exist_ok=True); dest=sqlite3.connect(str(destination)); self._c().backup(dest); dest.close()
    async def close(self)->None:
        if self._closed:return
        self._closed=True
        def close_sync():
            if self._conn is not None:self._conn.close(); self._conn=None
        self._executor.submit(close_sync).result();self._executor.shutdown(wait=True)
