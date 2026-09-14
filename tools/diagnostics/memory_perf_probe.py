# tools/diagnostics/memory_perf_probe.py
"""诊断脚本：chat memory 本地读取性能探针。

按修复包 08 阶段第 7 节的最低性能测试要求，在临时 SQLite 库上生成虚构数据并测量本地读取延迟：

    20 个 scope × 3000 条终结事件、500 条事实、500 次查询。

脚本只使用本地 SQLite 与 ``cooper_bot.modules.memory``，不访问网络、不读写真实记忆库；
临时库位于 ``runtime/temp/`` 并在结束时清理。输出顺序查询的 p50/p95/p99，以及 500 次查询
同时进入单线程 DB executor 时的 p95 与最大排队完成时间。

用法：
    python tools/diagnostics/memory_perf_probe.py
"""
from __future__ import annotations

import asyncio
import shutil
import sys
import time
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cooper_bot.core import config  # noqa: E402
from cooper_bot.modules.memory.retrieval import search  # noqa: E402
from cooper_bot.modules.memory.store import MemoryStore  # noqa: E402

SCOPE_COUNT = 20
EVENTS_PER_SCOPE = 3000
FACT_COUNT = 500
QUERY_COUNT = 500
BOT_ID = 10101
GROUP_BASE = 60000
USER_BASE = 70000


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round(ratio * len(ordered))) - 1))
    return ordered[index]


def _build_dataset(store: MemoryStore) -> dict:
    """Seed the fictional dataset; runs inside the store's single DB worker thread."""
    conn = store._c()
    now = time.time()
    scope_ids: list[str] = []
    conversation_ids: list[str] = []
    actors: list[int] = []
    with conn:
        for index in range(SCOPE_COUNT):
            scope_id = f"qq:{BOT_ID}:public:scope:{index}"
            conversation_id = uuid.uuid5(uuid.NAMESPACE_URL, scope_id).hex
            actor = USER_BASE + index
            kind = "group" if index % 2 == 0 else "private"
            target_id = GROUP_BASE + index if kind == "group" else actor
            conn.execute(
                "INSERT INTO memory_scopes(scope_id,bot_id,profile,kind,target_id,owner_user_id,enabled,capture_mode,active_conversation_id,epoch,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (scope_id, BOT_ID, "public", kind, target_id, actor, 1, "all", conversation_id, 0, now),
            )
            scope_ids.append(scope_id)
            conversation_ids.append(conversation_id)
            actors.append(actor)
            seq = 0
            user_rows: list[tuple] = []
            assistant_rows: list[tuple] = []
            for block in range(EVENTS_PER_SCOPE // 2):
                seq += 1
                turn_id = f"{index}-{block}"
                user_seq = seq
                user_rows.append(
                    (
                        scope_id, conversation_id, 0, f"user-{index}-{block}", "input", "direct_chat", turn_id, "user",
                        actor, f"第{index}个群第{block}轮提问：请整理课程安排和作业截止时间", "", "", "{}", "completed", now,
                    )
                )
                seq += 1
                assistant_rows.append(
                    (
                        scope_id, conversation_id, 0, f"turn:{turn_id}:assistant", "assistant", "direct_chat", turn_id, "assistant",
                        actor, user_seq, f"第{index}个群第{block}轮回复：周三交作业，周五有答疑", "{}", "confirmed", now,
                    )
                )
            conn.executemany(
                "INSERT INTO memory_events(scope_id,conversation_id,epoch,source_event_id,event_kind,source_kind,turn_id,role,actor_user_id,own_text,quoted_text,visual_text,flags_json,state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                user_rows,
            )
            conn.executemany(
                "INSERT INTO memory_events(scope_id,conversation_id,epoch,source_event_id,event_kind,source_kind,turn_id,role,actor_user_id,parent_input_seq,own_text,flags_json,state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                assistant_rows,
            )
        for index in range(FACT_COUNT):
            slot = index % SCOPE_COUNT
            conn.execute(
                "INSERT INTO memory_facts(fact_id,scope_id,subject_id,fact_key,revision,status,text,source_kind,source_input_seqs_json,evidence_json,valid_from,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (uuid.uuid5(uuid.NAMESPACE_URL, f"fact-{index}").hex, scope_ids[slot], f"user:{actors[slot]}", f"explicit.{index}", 1, "active",
                 f"第{index}条显式记忆：周三交作业", "explicit_memory", "[]", "[]", now, now, now),
            )
    return {
        "scopes": scope_ids,
        "conversations": conversation_ids,
        "actors": actors,
        "max_seq": int(conn.execute("SELECT MAX(seq) FROM memory_events").fetchone()[0] or 0),
        "events_total": int(conn.execute("SELECT COUNT(*) FROM memory_events").fetchone()[0]),
        "facts_total": int(conn.execute("SELECT COUNT(*) FROM memory_facts").fetchone()[0]),
    }


async def main() -> int:
    workdir = PROJECT_ROOT / "runtime" / "temp" / f"memory_perf_{uuid.uuid4().hex[:8]}"
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "memory.sqlite3"
    store = MemoryStore(db_path)
    try:
        await store.start()
        started = time.perf_counter()
        stats = await store._call(lambda: _build_dataset(store))
        seed_seconds = time.perf_counter() - started

        scope_ids = stats["scopes"]
        conversations = stats["conversations"]
        actors = stats["actors"]
        limit = max(1, int(config.AI_MEMORY_RECENT_EVENTS))
        top_k = max(1, int(config.AI_MEMORY_TOP_K))

        async def snapshot(index: int) -> float:
            slot = index % SCOPE_COUNT
            began = time.perf_counter()
            await store.snapshot_rows(scope_ids[slot], conversations[slot], stats["max_seq"], limit)
            return (time.perf_counter() - began) * 1000

        async def facts_only(index: int) -> float:
            slot = index % SCOPE_COUNT
            began = time.perf_counter()
            rows = await store.list_facts(scope_ids[slot], (f"user:{actors[slot]}",))
            search(rows, "作业", top_k)
            return (time.perf_counter() - began) * 1000

        async def query(index: int) -> float:
            return await snapshot(index) + await facts_only(index)

        async def timed(call) -> list[float]:
            samples: list[float] = []
            for index in range(QUERY_COUNT):
                began = time.perf_counter()
                await call(index)
                samples.append((time.perf_counter() - began) * 1000)
            return samples

        snapshot_ms = await timed(snapshot)
        facts_ms = await timed(facts_only)
        sequential = await timed(query)

        burst_started = time.perf_counter()
        concurrent = await asyncio.gather(*(query(index) for index in range(QUERY_COUNT)))
        burst_wall = time.perf_counter() - burst_started
        await store.close()
        size = sum(path.stat().st_size for path in workdir.glob("memory.sqlite3*"))
    finally:
        try:
            await store.close()
        except Exception:
            pass
        shutil.rmtree(workdir, ignore_errors=True)

    print("=== memory_perf_probe ===")
    print(f"scopes={SCOPE_COUNT} events_per_scope={EVENTS_PER_SCOPE} events_total={stats['events_total']} facts={stats['facts_total']} queries={QUERY_COUNT}")
    print(f"db_bytes={size} seed_seconds={seed_seconds:.3f} recent_events_limit={limit} top_k={top_k}")
    print(f"snapshot_ms: p50={percentile(snapshot_ms, 0.5):.3f} p95={percentile(snapshot_ms, 0.95):.3f} p99={percentile(snapshot_ms, 0.99):.3f}")
    print(f"facts_search_ms: p50={percentile(facts_ms, 0.5):.3f} p95={percentile(facts_ms, 0.95):.3f} p99={percentile(facts_ms, 0.99):.3f}")
    print(f"sequential_ms: p50={percentile(sequential, 0.5):.3f} p95={percentile(sequential, 0.95):.3f} p99={percentile(sequential, 0.99):.3f}")
    print(f"burst_{QUERY_COUNT}_ms: p50={percentile(concurrent, 0.5):.3f} p95={percentile(concurrent, 0.95):.3f} p99={percentile(concurrent, 0.99):.3f} max={max(concurrent):.3f} wall={burst_wall * 1000:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
