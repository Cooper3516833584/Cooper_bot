# tools/diagnostics/memory_backfill_run.py
"""诊断脚本：把存量聊天记忆按生产同一条路径抽完，并统计 token 用量。

它不是旁路实现：脚本实例化真实的 ``MemoryService`` / ``MemoryJobWorker`` / ``MemoryJobStore``，
用真实的 ``ModelGateway``，逐个认领并执行真实的 extract 作业（``_handle_extraction_job`` →
``_extract_facts`` → ``apply_extracted_facts``，事务内仍会复核 scope/epoch/成员/证据子串），
只是在进程内临时抬高 ``AI_MEMORY_AUTO_EXTRACT_DAILY_BUDGET``，并逐批串行、失败即停。

代码里没有任何 token 统计，因此脚本给唯一的 HTTP 出口 ``http_post_json`` 包一层，
记录每次调用的 prompt/completion tokens、耗时与状态。

用法：
    python tools/diagnostics/memory_backfill_run.py --backup-only   # 只做 SQLite backup
    python tools/diagnostics/memory_backfill_run.py --list          # 只列出待抽取批次
    python tools/diagnostics/memory_backfill_run.py                 # 真正跑（默认预算 200）
    python tools/diagnostics/memory_backfill_run.py --budget 400 --max-calls 100
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cooper_bot.core import config  # noqa: E402
from cooper_bot.modules.ai import model_gateway as gateway_module  # noqa: E402
from cooper_bot.modules.memory.jobs import MemoryJobWorker  # noqa: E402
from cooper_bot.modules.memory.models import ExtractionDeferred  # noqa: E402
from cooper_bot.modules.memory.service import MemoryService  # noqa: E402

DEFAULT_DB = Path(config.AI_MEMORY_DB_PATH)
BACKUP_PATH = Path(config.RUNTIME_DIR) / "temp" / "chat_memory_backup_pre_backfill.sqlite3"

USAGE_LOG: list[dict] = []


def install_usage_capture() -> None:
    original = gateway_module.http_post_json

    def wrapped(url, payload, api_key, timeout=60.0):
        began = time.perf_counter()
        record: dict = {"bytes": len(json.dumps(payload, ensure_ascii=False))}
        try:
            data = original(url, payload, api_key, timeout=timeout)
        except Exception as exc:
            record.update(ok=False, seconds=round(time.perf_counter() - began, 3), error=f"{type(exc).__name__}: {str(exc)[:200]}")
            USAGE_LOG.append(record)
            raise
        usage = (data or {}).get("usage") or {}
        record.update(
            ok=True,
            seconds=round(time.perf_counter() - began, 3),
            prompt=int(usage.get("prompt_tokens") or 0),
            completion=int(usage.get("completion_tokens") or 0),
            total=int(usage.get("total_tokens") or 0),
        )
        USAGE_LOG.append(record)
        return data

    gateway_module.http_post_json = wrapped


def build_gateway():
    providers = gateway_module.load_providers(config.AI_API_KEY_PATH)
    return gateway_module.ModelGateway(None, api_key_path=config.AI_API_KEY_PATH, provider_source=lambda: providers)


async def backup(db_path: Path) -> None:
    service = MemoryService(None, enabled=True, db_path=db_path, gateway=None)
    await service.store.start()
    try:
        await service.store.backup(BACKUP_PATH)
        print(f"backup -> {BACKUP_PATH} ({BACKUP_PATH.stat().st_size} bytes)")
    finally:
        await service.store.close()


async def undefer(store) -> list[tuple[str, float]]:
    """把 not_before 在未来的 queued extract 作业提前（含被 defer 到 +1h 的那些），返回被改动的行。"""
    now = time.time()
    changed = await store._call(lambda: store._c().execute(
        "SELECT job_id,target_input_seq,not_before FROM memory_jobs "
        "WHERE kind='extract' AND state='queued' AND not_before>? ORDER BY not_before", (now,)
    ).fetchall())
    if changed:
        await store._call(lambda: store._c().execute(
            "UPDATE memory_jobs SET not_before=? WHERE kind='extract' AND state='queued' AND not_before>?", (now, now)
        ))
    return [(str(row[0]), float(row[2])) for row in changed]


async def list_pending(store) -> None:
    rows = await store._call(lambda: store._c().execute(
        "SELECT job_id,scope_id,state,attempts,target_input_seq,not_before,last_error_code FROM memory_jobs "
        "WHERE kind='extract' ORDER BY target_input_seq"
    ).fetchall())
    for row in rows:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[5]))
        print(f"  job={row[0][:8]} scope={row[1].split(':')[-1]} state={row[2]:<9} attempts={row[3]} target={row[4]:>5} not_before={stamp} err={row[6]}")


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--budget", type=int, default=200)
    parser.add_argument("--max-calls", type=int, default=50)
    parser.add_argument("--backup-only", action="store_true")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    if args.backup_only:
        await backup(db_path)
        return 0

    install_usage_capture()
    gateway = build_gateway()
    service = MemoryService(None, enabled=True, db_path=db_path, gateway=gateway)
    # worker 只用于让 service 内部能排队（_maybe_schedule_extraction 要求 worker 非 None），
    # 但不启动它：作业由本脚本逐个认领执行，保证串行、可打印、失败即停。
    service.worker = MemoryJobWorker(service.store, service._handle_job, None)

    await service.store.start()
    await service.store.recover_interrupted_jobs()

    if args.list:
        print("=== extract 作业 ===")
        await list_pending(service.store)
        await service.store.close()
        return 0

    # 进程内临时抬高上限，不写任何配置文件；今天已消耗的计数照常保留。
    before_budget = config.AI_MEMORY_AUTO_EXTRACT_DAILY_BUDGET
    config.AI_MEMORY_AUTO_EXTRACT_DAILY_BUDGET = max(int(before_budget), int(args.budget))
    print(f"budget: {before_budget} -> {config.AI_MEMORY_AUTO_EXTRACT_DAILY_BUDGET} (仅本进程)")

    pairs = await service.store.active_extraction_pairs()
    for scope_id, actor in pairs:
        await service._maybe_schedule_extraction(scope_id, actor)
    print(f"已开启作用域中曾发言的成员：{len(pairs)} 个；补排完成")

    changed = await undefer(service.store)
    for job_id, old in changed:
        print(f"提前作业 {job_id[:8]}：not_before {time.strftime('%H:%M:%S', time.localtime(old))} -> now")

    print("\n=== 逐批执行 ===")
    calls = 0
    written = 0
    aborted = ""
    rounds = 0
    while True:
        job = await service.store.claim_job()
        if job is None:
            rounds += 1
            changed = await undefer(service.store)
            if changed and rounds <= 5:
                print(f"再次提前 {len(changed)} 条被推迟的作业（第 {rounds} 轮）")
                continue
            break
        if job.get("kind") != "extract":
            print(f"跳过非 extract 作业 {job['job_id'][:8]} kind={job.get('kind')}")
            await service.store.defer_job(job["job_id"], 0.0)
            continue
        context = await service.store.extraction_job_context(job)
        events = len((context or {}).get("events") or [])
        actor = int(((context or {}).get("actor_user_id") or 0))
        facts_before = await service.store._call(lambda: service.store._c().execute("SELECT COUNT(*) FROM memory_facts").fetchone()[0])
        began = time.perf_counter()
        try:
            await service._handle_job(job)
        except ExtractionDeferred as exc:
            await service.store.defer_job(job["job_id"], 3600.0)
            aborted = f"预算/提供方暂不可用：{exc}"
            print(f"  scope={job['scope_id'].split(':')[-1]} actor={actor} events={events} -> DEFERRED")
            break
        except Exception as exc:
            await service.store.finish_job(job["job_id"], False, type(exc).__name__)
            aborted = f"{type(exc).__name__}: {exc}"
            print(f"  scope={job['scope_id'].split(':')[-1]} actor={actor} events={events} -> FAIL {aborted}")
            break
        await service.store.finish_job(job["job_id"], True)
        facts_after = await service.store._call(lambda: service.store._c().execute("SELECT COUNT(*) FROM memory_facts").fetchone()[0])
        delta = int(facts_after) - int(facts_before)
        written += delta
        calls += 1
        span = time.perf_counter() - began
        print(f"  scope={job['scope_id'].split(':')[-1]} actor={actor} events={events} target={job['target_input_seq']} -> facts+{delta} ({span:.1f}s)")
        if calls >= args.max_calls:
            aborted = f"达到 --max-calls={args.max_calls} 上限，主动停止"
            print(aborted)
            break

    print("\n=== token 账目 ===")
    prompt = sum(int(r.get("prompt") or 0) for r in USAGE_LOG)
    completion = sum(int(r.get("completion") or 0) for r in USAGE_LOG)
    failed = [r for r in USAGE_LOG if not r.get("ok")]
    print(f"model_calls={len(USAGE_LOG)} ok={len(USAGE_LOG) - len(failed)} failed={len(failed)}")
    print(f"prompt_tokens={prompt} completion_tokens={completion} total_tokens={prompt + completion}")
    for index, record in enumerate(USAGE_LOG, 1):
        print(f"  call{index}: {json.dumps(record, ensure_ascii=False)}")

    print("\n=== 写入的记忆 ===")
    rows = await service.store._call(lambda: [
        dict(row) for row in service.store._c().execute(
            "SELECT fact_id,scope_id,subject_id,fact_key,status,text,source_kind,evidence_json,created_at "
            "FROM memory_facts ORDER BY created_at"
        ).fetchall()
    ])
    for row in rows:
        print(f"  [{row['fact_id'][:8]}] scope={row['scope_id'].split(':')[-1]} subject={row['subject_id']} key={row['fact_key']} src={row['source_kind']}")
        print(f"      text={row['text']}")
        print(f"      evidence={str(row['evidence_json'])[:200]}")
    print(f"facts_total={len(rows)}（本次新增 {written}）")

    jobs = await service.store._call(lambda: [
        dict(row) for row in service.store._c().execute(
            "SELECT kind,state,COUNT(*) c FROM memory_jobs GROUP BY kind,state ORDER BY kind,state"
        ).fetchall()
    ])
    print("\n=== 作业终态 ===")
    for row in jobs:
        print(f"  {row['kind']:<11} {row['state']:<10} {row['c']}")
    if aborted:
        print(f"\n中止原因：{aborted}")

    await service.store.close()
    return 0 if not aborted else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
