from __future__ import annotations

import json
import shutil
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable


_SCHEMA_VERSION = 3


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
                  source_first_seq INTEGER NOT NULL DEFAULT 0, source_last_seq INTEGER NOT NULL DEFAULT 0,
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
                  base_version INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
                  not_before REAL NOT NULL, lease_until REAL, last_error_code TEXT,
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
                summary_columns={row[1] for row in conn.execute("PRAGMA table_info(memory_summaries)")}
                if "source_first_seq" not in summary_columns:
                    conn.execute("ALTER TABLE memory_summaries ADD COLUMN source_first_seq INTEGER NOT NULL DEFAULT 0")
                if "source_last_seq" not in summary_columns:
                    conn.execute("ALTER TABLE memory_summaries ADD COLUMN source_last_seq INTEGER NOT NULL DEFAULT 0")
                job_columns={row[1] for row in conn.execute("PRAGMA table_info(memory_jobs)")}
                if "base_version" not in job_columns:
                    conn.execute("ALTER TABLE memory_jobs ADD COLUMN base_version INTEGER NOT NULL DEFAULT 0")
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
        c=self._c();row=c.execute("SELECT enabled,capture_mode FROM memory_scopes WHERE scope_id=?",(scope_id,)).fetchone();changed=bool(row and (bool(row[0])!=bool(enabled) or row[1]!=capture_mode));now=time.time()
        with c:
            c.execute("UPDATE memory_scopes SET enabled=?,capture_mode=?,epoch=epoch+?,updated_at=? WHERE scope_id=?",(int(enabled),capture_mode,int(changed),now,scope_id))
            if changed:
                c.execute("DELETE FROM memory_events WHERE scope_id=? AND role='assistant'",(scope_id,))
                c.execute("DELETE FROM memory_summaries WHERE scope_id=?",(scope_id,))
                c.execute("UPDATE memory_jobs SET state='cancelled',lease_until=NULL WHERE scope_id=? AND state IN ('queued','running')",(scope_id,))

    async def set_member_policy(self, scope_id: str, actor: int, enabled: bool) -> None:
        await self._call(self._set_member_policy, scope_id, actor, enabled)

    def _set_member_policy(self, scope_id: str, actor: int, enabled: bool) -> None:
        c, now = self._c(), time.time()
        with c: c.execute("INSERT INTO memory_members(scope_id,actor_user_id,enabled,updated_at) VALUES(?,?,?,?) ON CONFLICT(scope_id,actor_user_id) DO UPDATE SET enabled=excluded.enabled,updated_at=excluded.updated_at", (scope_id, actor, int(enabled), now))

    async def set_member_policy_and_invalidate(self, scope_id: str, actor: int, enabled: bool) -> bool:
        return await self._call(self._set_member_policy_and_invalidate, scope_id, actor, enabled)
    def _set_member_policy_and_invalidate(self, scope_id: str, actor: int, enabled: bool) -> bool:
        c, now = self._c(), time.time()
        row=c.execute("SELECT enabled FROM memory_members WHERE scope_id=? AND actor_user_id=?",(scope_id,actor)).fetchone()
        changed=(True if row is None else bool(row[0])) != bool(enabled)
        with c:
            c.execute("INSERT INTO memory_members(scope_id,actor_user_id,enabled,updated_at) VALUES(?,?,?,?) ON CONFLICT(scope_id,actor_user_id) DO UPDATE SET enabled=excluded.enabled,updated_at=excluded.updated_at",(scope_id,actor,int(enabled),now))
            if changed:
                c.execute("UPDATE memory_scopes SET epoch=epoch+1,updated_at=? WHERE scope_id=?",(now,scope_id))
                c.execute("DELETE FROM memory_events WHERE scope_id=? AND role='assistant'",(scope_id,))
                c.execute("DELETE FROM memory_summaries WHERE scope_id=?",(scope_id,))
                c.execute("UPDATE memory_jobs SET state='cancelled',lease_until=NULL WHERE scope_id=? AND state IN ('queued','running')",(scope_id,))
                # 退出只停止"使用"该成员记忆，不再物理删除 scope 内全部派生事实：
                # 否则会把同 scope 其他成员的 auto facts 一并删掉，并让 extraction cursor 与实际事实不一致。
                # 该成员事实的可见性由 member.enabled + eligibility 查询控制。
        return changed

    async def member_enabled(self, scope_id: str, actor: int) -> bool:
        return await self._call(lambda: self._member_enabled(scope_id, actor))
    def _member_enabled(self, scope_id: str, actor: int) -> bool:
        r=self._c().execute("SELECT enabled FROM memory_members WHERE scope_id=? AND actor_user_id=?",(scope_id,actor)).fetchone(); return True if r is None else bool(r[0])

    async def append_input_once(self, scope_id: str, actor: int, source_id: str, own: str, quoted: str, visual: str, source_kind: str, *, metadata_only: bool=False, flags: dict | None=None) -> tuple[dict, bool]:
        return await self._call(self._append_input_once, scope_id, actor, source_id, own, quoted, visual, source_kind, metadata_only, flags or {})

    def _append_input_once(self, scope_id: str, actor: int, source_id: str, own: str, quoted: str, visual: str, source_kind: str, metadata_only: bool, flags: dict) -> tuple[dict, bool]:
        c=self._c(); scope=c.execute("SELECT active_conversation_id,epoch FROM memory_scopes WHERE scope_id=?",(scope_id,)).fetchone()
        if not scope: raise RuntimeError("unknown memory scope")
        row=c.execute("SELECT * FROM memory_events WHERE scope_id=? AND source_event_id=? AND event_kind='input'",(scope_id,source_id)).fetchone()
        if row: return dict(row), False
        now=time.time(); state="explicit" if source_kind=="explicit_memory" else ("observed" if source_kind=="passive_chat" else "pending")
        event_flags=dict(flags);event_flags["metadata_only"]=metadata_only
        with c:
            cur=c.execute("INSERT OR IGNORE INTO memory_events(scope_id,conversation_id,epoch,source_event_id,event_kind,source_kind,turn_id,role,actor_user_id,own_text,quoted_text,visual_text,flags_json,state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(scope_id,scope[0],scope[1],source_id,"input",source_kind,uuid.uuid4().hex,"user",actor,"" if metadata_only else own,"" if metadata_only else quoted,"" if metadata_only else visual,json.dumps(event_flags),state,now))
        row=c.execute("SELECT * FROM memory_events WHERE scope_id=? AND source_event_id=? AND event_kind='input'",(scope_id,source_id)).fetchone()
        if row is None: raise RuntimeError("memory input insert failed")
        return dict(row), bool(cur.rowcount)

    async def fail_pending_input(self, scope_id: str, input_seq: int) -> bool:
        return await self._call(self._fail_pending_input, scope_id, input_seq)
    def _fail_pending_input(self, scope_id: str, input_seq: int) -> bool:
        with self._c():
            cur=self._c().execute("UPDATE memory_events SET state=? WHERE scope_id=? AND seq=? AND role='user' AND state='pending'",("failed",scope_id,input_seq))
        return bool(cur.rowcount)

    async def recover_incomplete_turns(self) -> None:
        await self._call(self._recover_incomplete_turns)
    def _recover_incomplete_turns(self) -> None:
        c=self._c(); now=time.time()
        with c:
            c.execute("UPDATE memory_events SET state='unconfirmed',confirmed_at=? WHERE role='assistant' AND state='generated'",(now,))
            c.execute("""UPDATE memory_events AS input SET state='unconfirmed',confirmed_at=?
                       WHERE input.role='user' AND input.state='pending' AND EXISTS (
                         SELECT 1 FROM memory_events AS output
                         WHERE output.scope_id=input.scope_id AND output.parent_input_seq=input.seq
                       AND output.role='assistant' AND output.state='unconfirmed')""",(now,))
            c.execute("UPDATE memory_events SET state='failed',confirmed_at=? WHERE role='user' AND state='pending'",(now,))

    async def prune_terminal_events(self, retention_days: int, max_input_blocks: int) -> int:
        return await self._call(self._prune_terminal_events, retention_days, max_input_blocks)
    def _prune_terminal_events(self, retention_days: int, max_input_blocks: int) -> int:
        c=self._c(); cutoff=time.time()-(max(1,int(retention_days))*86400); maximum=max(1,int(max_input_blocks)); removed=0
        scope_ids=[row[0] for row in c.execute("SELECT scope_id FROM memory_scopes").fetchall()]
        with c:
            for scope_id in scope_ids:
                rows=c.execute("SELECT seq,created_at FROM memory_events WHERE scope_id=? AND role='user' AND state IN ('completed','observed','explicit','unconfirmed','failed') ORDER BY seq DESC",(scope_id,)).fetchall()
                remove_ids={int(row[0]) for row in rows if float(row[1]) < cutoff}
                remove_ids.update(int(row[0]) for row in rows[maximum:])
                for input_seq in remove_ids:
                    c.execute("DELETE FROM memory_events WHERE scope_id=? AND parent_input_seq=? AND role='assistant' AND state IN ('confirmed','unconfirmed','failed')",(scope_id,input_seq))
                    cur=c.execute("DELETE FROM memory_events WHERE scope_id=? AND seq=? AND role='user' AND state IN ('completed','observed','explicit','unconfirmed','failed')",(scope_id,input_seq))
                    removed += int(cur.rowcount)
        return removed

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

    async def snapshot_rows(self, scope_id: str, conversation_id: str, current_seq: int, limit: int, *, after_input_seq: int=0) -> list[dict]:
        return await self._call(self._snapshot_rows,scope_id,conversation_id,current_seq,limit,after_input_seq)
    def _snapshot_rows(self,scope_id:str,cid:str,current:int,limit:int,after:int)->list[dict]:
        q="""SELECT * FROM memory_events AS event WHERE scope_id=? AND conversation_id=? AND ((role='user' AND seq>? AND seq<? AND state IN ('completed','observed','explicit','unconfirmed') AND NOT EXISTS (SELECT 1 FROM memory_members AS member WHERE member.scope_id=event.scope_id AND member.actor_user_id=event.actor_user_id AND member.enabled=0)) OR (role='assistant' AND parent_input_seq>? AND parent_input_seq<? AND state='confirmed')) ORDER BY seq"""
        rows=[dict(x) for x in self._c().execute(q,(scope_id,cid,after,current,after,current)).fetchall()]; return rows[-max(1,limit):]

    async def summary(self, scope_id: str, conversation_id: str, epoch: int) -> dict | None:
        return await self._call(self._summary, scope_id, conversation_id, epoch)
    def _summary(self, scope_id: str, conversation_id: str, epoch: int) -> dict | None:
        row=self._c().execute("SELECT * FROM memory_summaries WHERE scope_id=? AND conversation_id=? AND epoch=?",(scope_id,conversation_id,epoch)).fetchone()
        if not row:return None
        result=dict(row)
        try:result["summary"]=json.loads(result["summary_json"])
        except (TypeError,ValueError):return None
        return result

    async def save_explicit_fact(self, scope_id: str, subject_id: str, text: str, *, fact_id: str | None=None, replace_id: str | None=None) -> dict:
        return await self._call(self._save_explicit_fact,scope_id,subject_id,text,fact_id,replace_id)
    def _save_explicit_fact(self,scope_id:str,subject:str,text:str,fact_id:str|None,replace_id:str|None)->dict:
        c=self._c(); now=time.time(); key="explicit."+uuid.uuid5(uuid.NAMESPACE_URL,text.strip()).hex[:20]
        if replace_id:
            old=c.execute("SELECT * FROM memory_facts WHERE fact_id=? AND scope_id=? AND subject_id=? AND status='active'",(replace_id,scope_id,subject)).fetchone()
            if not old: raise ValueError("fact not found")
            key, fact_id=old['fact_key'],uuid.uuid4().hex
            with c:c.execute("UPDATE memory_facts SET status='superseded',updated_at=? WHERE scope_id=? AND subject_id=? AND fact_key=? AND status='active'",(now,scope_id,subject,key))
        else:
            old=c.execute("SELECT * FROM memory_facts WHERE scope_id=? AND subject_id=? AND fact_key=? AND status='active'",(scope_id,subject,key)).fetchone()
            if old:
                if old['source_kind']=='explicit_memory':return dict(old)
                # 同一 fact_key 上 explicit 优先：先让 active auto 失效，避免同 key 出现两个 active revision。
                with c:c.execute("UPDATE memory_facts SET status='superseded',updated_at=? WHERE fact_id=?",(now,old['fact_id']))
            fact_id=fact_id or uuid.uuid4().hex
        rev=int(c.execute("SELECT COALESCE(MAX(revision),0)+1 FROM memory_facts WHERE scope_id=? AND subject_id=? AND fact_key=?",(scope_id,subject,key)).fetchone()[0])
        with c:c.execute("INSERT INTO memory_facts(fact_id,scope_id,subject_id,fact_key,revision,status,text,source_kind,valid_from,created_at,updated_at) VALUES(?,?,?,?,?,'active',?,'explicit_memory',?,?,?)",(fact_id,scope_id,subject,key,rev,text,now,now,now))
        return dict(c.execute("SELECT * FROM memory_facts WHERE fact_id=?",(fact_id,)).fetchone())

    async def list_facts(self, scope_id:str, subjects:tuple[str,...]) -> list[dict]:
        return await self._call(self._list_facts,scope_id,subjects)
    def _list_facts(self,scope_id:str,subjects:tuple[str,...])->list[dict]:
        if not subjects:return []
        marks=','.join('?'*len(subjects))
        # 退出记忆的成员，其 user:<id> 事实不再参与召回/列表（subject 为 user:<actor> 时与成员一一对应）。
        q=f"""SELECT * FROM memory_facts AS fact WHERE fact.scope_id=? AND fact.subject_id IN ({marks}) AND fact.status='active'
              AND (fact.expires_at IS NULL OR fact.expires_at>?)
              AND NOT EXISTS (SELECT 1 FROM memory_members AS member WHERE member.scope_id=fact.scope_id AND member.enabled=0 AND fact.subject_id='user:'||member.actor_user_id)
              ORDER BY fact.updated_at DESC,fact.fact_id"""
        return [dict(x) for x in self._c().execute(q,(scope_id,*subjects,time.time())).fetchall()]

    async def put_embedding(self, scope_id: str, fact_id: str, fingerprint: str, revision: int, vector: bytes, dimension: int) -> None:
        await self._call(self._put_embedding, scope_id, fact_id, fingerprint, revision, vector, dimension)
    def _put_embedding(self,scope_id,fact_id,fingerprint,revision,vector,dimension):
        c=self._c();now=time.time()
        with c:
            c.execute("INSERT OR REPLACE INTO memory_embeddings(fact_id,fingerprint,scope_id,fact_revision,dimension,vector,updated_at) VALUES(?,?,?,?,?,?,?)",(fact_id,fingerprint,scope_id,int(revision),int(dimension),sqlite3.Binary(vector),now))
            c.execute("DELETE FROM memory_embeddings WHERE scope_id=? AND fact_id=? AND fingerprint!=?",(scope_id,fact_id,fingerprint))

    async def load_embeddings(self, scope_id: str, subjects: tuple[str, ...], fingerprint: str) -> list[dict]:
        return await self._call(self._load_embeddings, scope_id, subjects, fingerprint)
    def _load_embeddings(self,scope_id,subjects,fingerprint):
        if not subjects:return []
        marks=','.join('?'*len(subjects))
        q=f"""SELECT embedding.fact_id,embedding.fact_revision,embedding.dimension,embedding.vector
              FROM memory_embeddings AS embedding JOIN memory_facts AS fact ON fact.fact_id=embedding.fact_id
              WHERE embedding.scope_id=? AND embedding.fingerprint=? AND fact.status='active'
                AND fact.subject_id IN ({marks}) AND fact.revision=embedding.fact_revision
                AND (fact.expires_at IS NULL OR fact.expires_at>?)"""
        return [dict(x) for x in self._c().execute(q,(scope_id,fingerprint,*subjects,time.time())).fetchall()]

    async def facts_missing_embeddings(self, scope_id: str, fingerprint: str, limit: int, after_fact_id: str = "") -> list[dict]:
        return await self._call(self._facts_missing_embeddings, scope_id, fingerprint, limit, after_fact_id)
    def _facts_missing_embeddings(self,scope_id,fingerprint,limit,after_fact_id):
        # 查询阶段先排除不该处理的 fact（失效/过期/成员已 opt-out/已有当前向量），
        # 避免队头全是 opt-out 事实时后面的正常 fact 永远轮不到回填；
        # provider 调用前 service 层仍会逐条复查，覆盖查询之后突然 /memory off 的竞态。
        q="""SELECT fact.fact_id,fact.subject_id,fact.revision,fact.text FROM memory_facts AS fact
             WHERE fact.scope_id=? AND fact.status='active' AND fact.fact_id>?
               AND (fact.expires_at IS NULL OR fact.expires_at>?)
               AND NOT EXISTS (SELECT 1 FROM memory_embeddings AS embedding WHERE embedding.fact_id=fact.fact_id AND embedding.fingerprint=? AND embedding.fact_revision=fact.revision)
               AND NOT EXISTS (SELECT 1 FROM memory_members AS member WHERE member.scope_id=fact.scope_id AND member.enabled=0 AND fact.subject_id='user:'||member.actor_user_id)
             ORDER BY fact.fact_id LIMIT ?"""
        return [dict(x) for x in self._c().execute(q,(scope_id,str(after_fact_id or ""),time.time(),fingerprint,max(1,int(limit)))).fetchall()]

    async def prune_stale_embeddings(self, scope_id: str) -> int:
        return await self._call(self._prune_stale_embeddings, scope_id)
    def _prune_stale_embeddings(self,scope_id):
        with self._c():
            cur=self._c().execute("""DELETE FROM memory_embeddings WHERE scope_id=? AND NOT EXISTS (
                SELECT 1 FROM memory_facts AS fact WHERE fact.fact_id=memory_embeddings.fact_id AND fact.status='active' AND fact.revision=memory_embeddings.fact_revision)""",(scope_id,))
        return int(cur.rowcount)

    async def count_embeddings(self, scope_id: str, fingerprint: str) -> int:
        return await self._call(self._count_embeddings, scope_id, fingerprint)
    def _count_embeddings(self,scope_id,fingerprint):
        return int(self._c().execute("SELECT COUNT(*) FROM memory_embeddings WHERE scope_id=? AND fingerprint=?",(scope_id,fingerprint)).fetchone()[0])

    async def embedding_eligible(self, scope_id: str, fact_id: str, revision: int) -> bool:
        """逐条外发前复核：scope 仍启用、成员未退出、事实仍 active 且 revision 未变。"""
        return await self._call(self._embedding_eligible, scope_id, fact_id, revision)
    def _embedding_eligible(self,scope_id,fact_id,revision):
        row=self._c().execute("""SELECT 1 FROM memory_facts AS fact JOIN memory_scopes AS scope ON scope.scope_id=fact.scope_id
              WHERE fact.fact_id=? AND fact.scope_id=? AND fact.status='active' AND fact.revision=? AND scope.enabled=1
                AND (fact.expires_at IS NULL OR fact.expires_at>?)
                AND NOT EXISTS (SELECT 1 FROM memory_members AS member WHERE member.scope_id=fact.scope_id AND member.enabled=0 AND fact.subject_id='user:'||member.actor_user_id)""",
            (fact_id,scope_id,int(revision),time.time())).fetchone()
        return row is not None

    async def scope_ids(self) -> list[str]:
        return await self._call(self._scope_ids)
    def _scope_ids(self):
        return [str(row[0]) for row in self._c().execute("SELECT scope_id FROM memory_scopes").fetchall()]

    async def resolve_fact_prefix(self, scope_id: str, subject: str, prefix: str) -> list[str]:
        return await self._call(self._resolve_fact_prefix, scope_id, subject, prefix)
    def _resolve_fact_prefix(self, scope_id: str, subject: str, prefix: str) -> list[str]:
        value=str(prefix or "").strip()
        if not value:return []
        return [str(row[0]) for row in self._c().execute("SELECT fact_id FROM memory_facts WHERE scope_id=? AND subject_id=? AND status='active' AND instr(fact_id,?)=1 ORDER BY fact_id LIMIT 2",(scope_id,subject,value)).fetchall()]

    async def history(self, scope_id: str, *, query: str="", before: int | None=None, limit: int=20) -> list[dict]:
        return await self._call(self._history, scope_id, query, before, limit)
    def _history(self, scope_id, query, before, limit):
        # 与 snapshot 口径一致：已退出记忆的成员，其输入与为该输入生成的回复都不再出现在 history。
        where=("scope_id=? AND state IN ('completed','confirmed','observed','explicit')"
               " AND NOT EXISTS (SELECT 1 FROM memory_members AS member WHERE member.scope_id=memory_events.scope_id AND member.actor_user_id=memory_events.actor_user_id AND member.enabled=0)")
        args=[scope_id]
        if query:
            where += " AND (own_text LIKE ? OR quoted_text LIKE ? OR visual_text LIKE ?)"
            pattern=f"%{query}%";args.extend((pattern,pattern,pattern))
        if before is not None: where += " AND seq<?";args.append(before)
        args.append(max(1,min(int(limit),20)))
        return [dict(x) for x in self._c().execute(f"SELECT * FROM memory_events WHERE {where} ORDER BY seq DESC LIMIT ?",args).fetchall()][::-1]

    async def forget_fact(self, scope_id:str, subject:str, fact_id:str)->bool:
        return await self._call(self._forget_fact,scope_id,subject,fact_id)
    def _forget_fact(self,scope_id:str,subject:str,fact_id:str)->bool:
        c=self._c(); row=c.execute("SELECT fact_key FROM memory_facts WHERE fact_id=? AND scope_id=? AND subject_id=?",(fact_id,scope_id,subject)).fetchone()
        if not row:return False
        source_seqs=[]
        for source_row in c.execute("SELECT source_input_seqs_json FROM memory_facts WHERE scope_id=? AND subject_id=? AND fact_key=?",(scope_id,subject,row[0])).fetchall():
            try:source_seqs.extend(int(value) for value in json.loads(source_row[0] or "[]") if int(value)>0)
            except (TypeError,ValueError):continue
        blocked=max(source_seqs,default=0);now=time.time()
        with c:
            c.execute("UPDATE memory_scopes SET epoch=epoch+1,updated_at=? WHERE scope_id=?",(now,scope_id))
            c.execute("DELETE FROM memory_facts WHERE scope_id=? AND subject_id=? AND fact_key=?",(scope_id,subject,row[0]))
            c.execute("INSERT INTO memory_forget_markers VALUES(?,?,?,?,?) ON CONFLICT(scope_id,subject_id,fact_key) DO UPDATE SET blocked_through_input_seq=MAX(blocked_through_input_seq,excluded.blocked_through_input_seq),created_at=excluded.created_at",(scope_id,subject,row[0],blocked,now))
            for source_seq in source_seqs:c.execute("DELETE FROM memory_events WHERE scope_id=? AND role='user' AND seq=?",(scope_id,source_seq))
            c.execute("DELETE FROM memory_events WHERE scope_id=? AND role='assistant'",(scope_id,))
            c.execute("DELETE FROM memory_summaries WHERE scope_id=?",(scope_id,))
            c.execute("UPDATE memory_jobs SET state='cancelled',lease_until=NULL WHERE scope_id=? AND state IN ('queued','running')",(scope_id,))
        return True

    async def clear_subject(self,scope_id:str,subject:str)->None: await self._call(self._clear_subject,scope_id,subject)
    def _clear_subject(self,scope_id:str,subject:str)->None:
        c=self._c(); actor=int(subject.split(':',1)[1]) if subject.startswith('user:') else -1
        with c:
            c.execute("UPDATE memory_scopes SET epoch=epoch+1,updated_at=? WHERE scope_id=?",(time.time(),scope_id)); c.execute("DELETE FROM memory_facts WHERE scope_id=? AND subject_id=?",(scope_id,subject)); c.execute("DELETE FROM memory_events WHERE scope_id=? AND (actor_user_id=? OR role='assistant')",(scope_id,actor)); c.execute("DELETE FROM memory_summaries WHERE scope_id=?",(scope_id,)); c.execute("DELETE FROM memory_forget_markers WHERE scope_id=? AND subject_id=?",(scope_id,subject)); c.execute("UPDATE memory_jobs SET state='cancelled',lease_until=NULL WHERE scope_id=? AND state IN ('queued','running')",(scope_id,))

    async def clear_scope_data(self, scope_id: str) -> None: await self._call(self._clear_scope_data, scope_id)
    def _clear_scope_data(self, scope_id: str) -> None:
        c=self._c(); now=time.time()
        with c:
            c.execute("UPDATE memory_scopes SET active_conversation_id=?,epoch=epoch+1,updated_at=? WHERE scope_id=?",(uuid.uuid4().hex,now,scope_id))
            c.execute("DELETE FROM memory_events WHERE scope_id=?",(scope_id,))
            c.execute("DELETE FROM memory_summaries WHERE scope_id=?",(scope_id,))
            c.execute("DELETE FROM memory_embeddings WHERE scope_id=?",(scope_id,))
            c.execute("DELETE FROM memory_facts WHERE scope_id=?",(scope_id,))
            c.execute("DELETE FROM memory_jobs WHERE scope_id=?",(scope_id,))
            c.execute("DELETE FROM memory_forget_markers WHERE scope_id=?",(scope_id,))

    async def rotate_conversation(self,scope_id:str)->str:return await self._call(self._rotate_conversation,scope_id)
    def _rotate_conversation(self,scope_id:str)->str:
        cid=uuid.uuid4().hex
        with self._c():self._c().execute("UPDATE memory_scopes SET active_conversation_id=?,epoch=epoch+1,updated_at=? WHERE scope_id=?",(cid,time.time(),scope_id))
        return cid
    async def extraction_candidate(self, scope_id: str, actor: int, min_events: int) -> dict | None:
        return await self._call(self._extraction_candidate, scope_id, actor, min_events)
    def _extraction_candidate(self, scope_id: str, actor: int, min_events: int) -> dict | None:
        c=self._c();scope=c.execute("SELECT * FROM memory_scopes WHERE scope_id=?",(scope_id,)).fetchone()
        if not scope or not bool(scope["enabled"]) or scope["profile"]!="public" or not self._member_enabled(scope_id,actor):return None
        member=c.execute("SELECT extracted_through_input_seq FROM memory_members WHERE scope_id=? AND actor_user_id=?",(scope_id,actor)).fetchone();cursor=int(member[0]) if member else 0
        pending=c.execute("SELECT MIN(seq) FROM memory_events WHERE scope_id=? AND conversation_id=? AND actor_user_id=? AND role='user' AND state='pending'",(scope_id,scope["active_conversation_id"],actor)).fetchone()[0]
        where="scope_id=? AND conversation_id=? AND actor_user_id=? AND role='user' AND seq>? AND source_kind IN ('direct_chat','passive_chat') AND state IN ('completed','observed','unconfirmed')"
        args=[scope_id,scope["active_conversation_id"],actor,cursor]
        if pending is not None:where+=" AND seq<?";args.append(int(pending))
        rows=c.execute(f"SELECT seq FROM memory_events WHERE {where} ORDER BY seq",args).fetchall()
        if len(rows)<max(1,int(min_events)):return None
        return {"scope_id":scope_id,"conversation_id":scope["active_conversation_id"],"epoch":int(scope["epoch"]),"actor_user_id":actor,"cursor":cursor,"target_input_seq":int(rows[-1][0])}

    async def extraction_job_context(self, job: dict) -> dict | None:
        return await self._call(self._extraction_job_context, job)
    def _extraction_job_context(self, job: dict) -> dict | None:
        c=self._c()
        try:payload=json.loads(job.get("payload_json") or "{}");actor=int(payload["actor_user_id"]);cursor=int(payload["cursor"])
        except (KeyError,TypeError,ValueError):return None
        scope=c.execute("SELECT * FROM memory_scopes WHERE scope_id=?",(job["scope_id"],)).fetchone()
        if not scope or not bool(scope["enabled"]) or scope["profile"]!="public" or int(scope["epoch"])!=int(job["epoch"]) or scope["active_conversation_id"]!=job["conversation_id"] or not self._member_enabled(job["scope_id"],actor):return None
        member=c.execute("SELECT extracted_through_input_seq FROM memory_members WHERE scope_id=? AND actor_user_id=?",(job["scope_id"],actor)).fetchone();current_cursor=int(member[0]) if member else 0
        if current_cursor!=cursor:return None
        rows=[]
        for item in c.execute("SELECT * FROM memory_events WHERE scope_id=? AND conversation_id=? AND actor_user_id=? AND role='user' AND seq>? AND seq<=? AND source_kind IN ('direct_chat','passive_chat') AND state IN ('completed','observed','unconfirmed') ORDER BY seq",(job["scope_id"],job["conversation_id"],actor,cursor,job["target_input_seq"])).fetchall():
            row=dict(item)
            try:flags=json.loads(row.get("flags_json") or "{}")
            except (TypeError,ValueError):flags={}
            if flags.get("metadata_only") or flags.get("truncated") or flags.get("unsafe_context_redacted") or not row.get("own_text"):continue
            rows.append(row)
        return {"actor_user_id":actor,"cursor":cursor,"target_input_seq":int(job["target_input_seq"]), "events":rows}

    async def apply_extracted_facts(self, job: dict, actor: int, cursor: int, target: int, candidates: list[dict]) -> bool:
        return await self._call(self._apply_extracted_facts, job, actor, cursor, target, candidates)
    def _apply_extracted_facts(self, job: dict, actor: int, cursor: int, target: int, candidates: list[dict]) -> bool:
        c=self._c();subject=f"user:{actor}";now=time.time()
        with c:
            c.execute("BEGIN IMMEDIATE")
            scope=c.execute("SELECT * FROM memory_scopes WHERE scope_id=?",(job["scope_id"],)).fetchone()
            if not scope or not bool(scope["enabled"]) or scope["profile"]!="public" or int(scope["epoch"])!=int(job["epoch"]) or scope["active_conversation_id"]!=job["conversation_id"] or not self._member_enabled(job["scope_id"],actor):return False
            member=c.execute("SELECT extracted_through_input_seq FROM memory_members WHERE scope_id=? AND actor_user_id=?",(job["scope_id"],actor)).fetchone();current_cursor=int(member[0]) if member else 0
            if current_cursor!=int(cursor):return False
            for candidate in candidates:
                key=str(candidate["fact_key"]);text=str(candidate["text"]);evidence_seq=int(candidate["evidence_input_seq"]);quote=str(candidate["evidence_quote"])
                evidence=c.execute("SELECT own_text FROM memory_events WHERE scope_id=? AND conversation_id=? AND actor_user_id=? AND role='user' AND seq=? AND seq>? AND seq<=? AND source_kind IN ('direct_chat','passive_chat') AND state IN ('completed','observed','unconfirmed')",(job["scope_id"],job["conversation_id"],actor,evidence_seq,cursor,target)).fetchone()
                if not evidence or not quote or quote not in str(evidence[0]):continue
                marker=c.execute("SELECT blocked_through_input_seq FROM memory_forget_markers WHERE scope_id=? AND subject_id=? AND fact_key=?",(job["scope_id"],subject,key)).fetchone()
                if marker and evidence_seq<=int(marker[0]):continue
                active=c.execute("SELECT * FROM memory_facts WHERE scope_id=? AND subject_id=? AND fact_key=? AND status='active' ORDER BY revision DESC",(job["scope_id"],subject,key)).fetchone()
                # 优先级只按同一 fact_key 判定：explicit > auto。与该 key 无关的 explicit（freeform key）不得阻断 auto 候选。
                if active and active["source_kind"]=="explicit_memory":continue
                if active and active["text"]==text:continue
                if active:c.execute("UPDATE memory_facts SET status='superseded',updated_at=? WHERE scope_id=? AND subject_id=? AND fact_key=? AND status='active'",(now,job["scope_id"],subject,key))
                revision=int(c.execute("SELECT COALESCE(MAX(revision),0)+1 FROM memory_facts WHERE scope_id=? AND subject_id=? AND fact_key=?",(job["scope_id"],subject,key)).fetchone()[0])
                c.execute("INSERT INTO memory_facts(fact_id,scope_id,subject_id,fact_key,revision,status,text,source_kind,source_input_seqs_json,evidence_json,valid_from,created_at,updated_at) VALUES(?,?,?,?,?,'active',?,'auto_extracted',?,?,?,?,?)",(uuid.uuid4().hex,job["scope_id"],subject,key,revision,text,json.dumps([evidence_seq]),json.dumps([{"input_seq":evidence_seq,"quote":quote}],ensure_ascii=False),now,now,now))
            c.execute("INSERT INTO memory_members(scope_id,actor_user_id,enabled,extracted_through_input_seq,updated_at) VALUES(?,?,1,?,?) ON CONFLICT(scope_id,actor_user_id) DO UPDATE SET extracted_through_input_seq=excluded.extracted_through_input_seq,updated_at=excluded.updated_at",(job["scope_id"],actor,target,now))
        return True

    async def summary_candidate(self, scope_id: str, min_events: int) -> dict | None:
        return await self._call(self._summary_candidate, scope_id, min_events)
    def _summary_candidate(self, scope_id: str, min_events: int) -> dict | None:
        c=self._c();scope=c.execute("SELECT * FROM memory_scopes WHERE scope_id=?",(scope_id,)).fetchone()
        if not scope or not bool(scope["enabled"]) or scope["profile"]=="admin":return None
        current=c.execute("SELECT * FROM memory_summaries WHERE scope_id=? AND conversation_id=? AND epoch=?",(scope_id,scope["active_conversation_id"],scope["epoch"])).fetchone()
        through=int(current["through_input_seq"]) if current else 0;base_version=int(current["version"]) if current else 0
        pending=c.execute("SELECT MIN(seq) FROM memory_events WHERE scope_id=? AND conversation_id=? AND role='user' AND state='pending'",(scope_id,scope["active_conversation_id"])).fetchone()[0]
        where="event.scope_id=? AND event.conversation_id=? AND event.role='user' AND event.seq>? AND event.state IN ('completed','observed','explicit','unconfirmed') AND NOT EXISTS (SELECT 1 FROM memory_members AS member WHERE member.scope_id=event.scope_id AND member.actor_user_id=event.actor_user_id AND member.enabled=0)"
        args=[scope_id,scope["active_conversation_id"],through]
        if pending is not None:where+=" AND event.seq<?";args.append(int(pending))
        rows=c.execute(f"SELECT event.seq FROM memory_events AS event WHERE {where} ORDER BY event.seq",args).fetchall()
        if len(rows)<max(1,int(min_events)):return None
        return {"scope_id":scope_id,"conversation_id":scope["active_conversation_id"],"epoch":int(scope["epoch"]),"base_version":base_version,"target_input_seq":int(rows[-1][0])}

    async def summary_job_context(self, job: dict) -> dict | None:
        return await self._call(self._summary_job_context, job)
    def _summary_job_context(self, job: dict) -> dict | None:
        c=self._c();scope=c.execute("SELECT * FROM memory_scopes WHERE scope_id=?",(job["scope_id"],)).fetchone()
        if not scope or not bool(scope["enabled"]) or scope["profile"]=="admin" or int(scope["epoch"])!=int(job["epoch"]) or scope["active_conversation_id"]!=job["conversation_id"]:return None
        current=c.execute("SELECT * FROM memory_summaries WHERE scope_id=? AND conversation_id=? AND epoch=?",(job["scope_id"],job["conversation_id"],job["epoch"])).fetchone()
        version=int(current["version"]) if current else 0;through=int(current["through_input_seq"]) if current else 0
        if version!=int(job.get("base_version") or 0) or int(job["target_input_seq"] or 0)<=through:return None
        user_rows=[dict(row) for row in c.execute("""SELECT event.* FROM memory_events AS event WHERE event.scope_id=? AND event.conversation_id=? AND event.role='user' AND event.seq>? AND event.seq<=? AND event.state IN ('completed','observed','explicit','unconfirmed') AND NOT EXISTS (SELECT 1 FROM memory_members AS member WHERE member.scope_id=event.scope_id AND member.actor_user_id=event.actor_user_id AND member.enabled=0) ORDER BY event.seq""",(job["scope_id"],job["conversation_id"],through,job["target_input_seq"])).fetchall()]
        events=[];included_input_seqs=[]
        for row in user_rows:
            try:flags=json.loads(row.get("flags_json") or "{}")
            except (TypeError,ValueError):flags={}
            if flags.get("metadata_only") or flags.get("truncated") or flags.get("unsafe_context_redacted"):continue
            included_input_seqs.append(int(row["seq"]))
            events.append(row)
            events.extend(dict(item) for item in c.execute("SELECT * FROM memory_events WHERE scope_id=? AND conversation_id=? AND role='assistant' AND parent_input_seq=? AND state='confirmed' ORDER BY seq",(job["scope_id"],job["conversation_id"],row["seq"])).fetchall())
        if not events:return None
        previous=None
        if current:
            try:previous=json.loads(current["summary_json"])
            except (TypeError,ValueError):return None
        return {"previous_summary":previous,"events":events,"source_first_seq":included_input_seqs[0],"source_last_seq":included_input_seqs[-1],"through_input_seq":int(job["target_input_seq"])}

    async def write_summary_cas(self, job: dict, summary: dict, source_first_seq: int, source_last_seq: int) -> bool:
        return await self._call(self._write_summary_cas, job, summary, source_first_seq, source_last_seq)
    def _write_summary_cas(self, job: dict, summary: dict, source_first_seq: int, source_last_seq: int) -> bool:
        c=self._c()
        with c:
            c.execute("BEGIN IMMEDIATE")
            scope=c.execute("SELECT * FROM memory_scopes WHERE scope_id=?",(job["scope_id"],)).fetchone()
            if not scope or not bool(scope["enabled"]) or int(scope["epoch"])!=int(job["epoch"]) or scope["active_conversation_id"]!=job["conversation_id"]:return False
            current=c.execute("SELECT version,through_input_seq FROM memory_summaries WHERE scope_id=? AND conversation_id=?",(job["scope_id"],job["conversation_id"])).fetchone()
            version=int(current["version"]) if current else 0;through=int(current["through_input_seq"]) if current else 0;target=int(job["target_input_seq"] or 0)
            if version!=int(job.get("base_version") or 0) or target<=through:return False
            target_row=c.execute("SELECT 1 FROM memory_events WHERE scope_id=? AND conversation_id=? AND role='user' AND seq=? AND state IN ('completed','observed','explicit','unconfirmed')",(job["scope_id"],job["conversation_id"],target)).fetchone()
            pending=c.execute("SELECT 1 FROM memory_events WHERE scope_id=? AND conversation_id=? AND role='user' AND seq<=? AND state='pending' LIMIT 1",(job["scope_id"],job["conversation_id"],target)).fetchone()
            if not target_row or pending:return False
            now=time.time();payload=json.dumps(summary,ensure_ascii=False,separators=(",",":"))
            c.execute("""INSERT INTO memory_summaries(scope_id,conversation_id,epoch,version,source_first_seq,source_last_seq,through_input_seq,summary_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(scope_id,conversation_id) DO UPDATE SET epoch=excluded.epoch,version=excluded.version,source_first_seq=excluded.source_first_seq,source_last_seq=excluded.source_last_seq,through_input_seq=excluded.through_input_seq,summary_json=excluded.summary_json,updated_at=excluded.updated_at""",(job["scope_id"],job["conversation_id"],job["epoch"],version+1,source_first_seq,source_last_seq,target,payload,now))
        return True

    async def reserve_daily_usage(self, kind: str, limit: int) -> bool:
        return await self._call(self._reserve_daily_usage, kind, limit)
    def _reserve_daily_usage(self, kind: str, limit: int) -> bool:
        c=self._c();day=time.strftime("%Y-%m-%d",time.gmtime());maximum=max(0,int(limit));row=c.execute("SELECT attempts FROM memory_usage_daily WHERE day_key=? AND kind=?",(day,kind)).fetchone()
        if maximum<=0 or (row and int(row[0])>=maximum):return False
        now=time.time()
        with c:c.execute("INSERT INTO memory_usage_daily(day_key,kind,attempts,updated_at) VALUES(?,?,1,?) ON CONFLICT(day_key,kind) DO UPDATE SET attempts=attempts+1,updated_at=excluded.updated_at",(day,kind,now))
        return True

    async def enqueue_job(self, scope_id: str, conversation_id: str, epoch: int, kind: str, target: int, payload: dict, dedupe_key: str, *, base_version: int=0) -> None:
        await self._call(self._enqueue_job, scope_id, conversation_id, epoch, kind, target, payload, dedupe_key, base_version)
    def _enqueue_job(self, scope_id, cid, epoch, kind, target, payload, key, base_version):
        with self._c(): self._c().execute("INSERT OR IGNORE INTO memory_jobs(job_id,scope_id,conversation_id,epoch,kind,target_input_seq,payload_json,state,base_version,not_before,dedupe_key) VALUES(?,?,?,?,?,?,?,'queued',?,?,?)", (uuid.uuid4().hex,scope_id,cid,epoch,kind,target,json.dumps(payload,ensure_ascii=False),base_version,time.time(),key))
    async def requeue_job(self, scope_id: str, conversation_id: str, epoch: int, kind: str, target: int, payload: dict, dedupe_key: str, *, base_version: int=0) -> None:
        """INSERT OR UPDATE：同一 dedupe_key 的作业重新排队（回填触发必须可重复）。

        包含 running：回填作业在扫描/请求 provider 期间新写入的事实也必须能被再次排队，
        否则这批新增事实会永久停留在"缺向量"状态。
        """
        await self._call(self._requeue_job, scope_id, conversation_id, epoch, kind, target, payload, dedupe_key, base_version)
    def _requeue_job(self, scope_id, cid, epoch, kind, target, payload, key, base_version):
        c=self._c();now=time.time()
        with c:
            c.execute("UPDATE memory_jobs SET state='queued',attempts=0,not_before=?,lease_until=NULL,last_error_code=NULL WHERE dedupe_key=? AND state IN ('succeeded','failed','cancelled','running')",(now,key))
            c.execute("INSERT OR IGNORE INTO memory_jobs(job_id,scope_id,conversation_id,epoch,kind,target_input_seq,payload_json,state,base_version,not_before,dedupe_key) VALUES(?,?,?,?,?,?,?,'queued',?,?,?)", (uuid.uuid4().hex,scope_id,cid,epoch,kind,target,json.dumps(payload,ensure_ascii=False),base_version,now,key))
    async def claim_job(self, *, max_attempts: int=3, lease_seconds: int=120) -> dict | None: return await self._call(self._claim_job,max_attempts,lease_seconds)
    def _claim_job(self,max_attempts,lease_seconds):
        c=self._c();now=time.time()
        with c:
            c.execute("UPDATE memory_jobs SET state='failed',lease_until=NULL,last_error_code='attempts_exhausted' WHERE state='running' AND lease_until<=? AND attempts>=?",(now,max(1,int(max_attempts))))
            c.execute("UPDATE memory_jobs SET state='queued',lease_until=NULL WHERE state='running' AND lease_until<=? AND attempts<?",(now,max(1,int(max_attempts))))
            row=c.execute("SELECT job_id FROM memory_jobs WHERE state='queued' AND not_before<=? AND attempts<? ORDER BY not_before,job_id LIMIT 1",(now,max(1,int(max_attempts)))).fetchone()
            if not row:return None
            cur=c.execute("UPDATE memory_jobs SET state='running',attempts=attempts+1,lease_until=? WHERE job_id=? AND state='queued'",(now+max(1,int(lease_seconds)),row["job_id"]))
            if not cur.rowcount:return None
        return dict(c.execute("SELECT * FROM memory_jobs WHERE job_id=?",(row["job_id"],)).fetchone())
    async def finish_job(self, job_id: str, success: bool, error: str = "", *, max_attempts: int=3) -> None: await self._call(self._finish_job,job_id,success,error,max_attempts)
    def _finish_job(self,job_id,success,error,max_attempts):
        c=self._c();row=c.execute("SELECT attempts,state FROM memory_jobs WHERE job_id=?",(job_id,)).fetchone()
        if not row or row["state"]!="running":return
        if success:state="succeeded";not_before=time.time()
        elif int(row["attempts"])>=max(1,int(max_attempts)):state="failed";not_before=time.time()
        else:state="queued";not_before=time.time()+min(300,2**int(row["attempts"]))
        with c:c.execute("UPDATE memory_jobs SET state=?,not_before=?,lease_until=NULL,last_error_code=? WHERE job_id=? AND state='running'",(state,not_before,error[:80],job_id))
    async def defer_job(self, job_id: str, delay_seconds: float) -> None: await self._call(self._defer_job,job_id,delay_seconds)
    def _defer_job(self,job_id,delay_seconds):
        """把作业推迟到 not_before 再跑：attempts 清零，不占用普通 max_attempts，也不会变成 failed。"""
        with self._c():
            self._c().execute("UPDATE memory_jobs SET state='queued',attempts=0,not_before=?,lease_until=NULL,last_error_code='deferred' WHERE job_id=? AND state='running'",(time.time()+max(0.0,float(delay_seconds)),job_id))
    async def backup(self,destination:Path)->None: await self._call(self._backup,destination)
    def _backup(self,destination:Path)->None:
        destination=Path(destination); destination.parent.mkdir(parents=True,exist_ok=True); dest=sqlite3.connect(str(destination)); self._c().backup(dest); dest.close()
    async def close(self)->None:
        if self._closed:return
        self._closed=True
        def close_sync():
            if self._conn is not None:self._conn.close(); self._conn=None
        self._executor.submit(close_sync).result();self._executor.shutdown(wait=True)
